"""
Module for validating data consistency using Great Expectations with Polars DataFrame.
"""

from typing import Dict

import great_expectations as gx
import polars as pl
from loguru import logger

from vtit_gx.polars.gx_helper import (
    _handle_result,
    _setup_polars_validator,
    _validate_expectation,
)


def gx_check_column_a_greater_than_b(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that values in column A are greater than (or equal to) values in column B.

    Uses Great Expectations' ``ExpectColumnPairValuesAToBeGreaterThanB`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_a (str): First column name.

            - column_b (str): Second column name.

            - or_equal (bool, optional): If True, check A >= B. If False, check A > B.
              Defaults to True.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_a = config.get("column_a")
        column_b = config.get("column_b")
        or_equal = config.get("or_equal", True)

        if column_a is None or column_a == "" or column_b is None or column_b == "":
            raise KeyError("Missing required key 'column_a' or 'column_b' in config.")

        missing_cols = []
        if column_a not in df.columns:
            missing_cols.append(column_a)
        if column_b not in df.columns:
            missing_cols.append(column_b)
        if missing_cols:
            error_msg = (
                f"Validation failed: The following columns are missing from the DataFrame: "
                f"{missing_cols}"
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A=column_a, column_B=column_b, or_equal=or_equal
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            comp = ">=" if or_equal else ">"
            error_msg = (
                f"Validation failed: Values in '{column_a}' are not {comp} values "
                f"in '{column_b}' for all rows."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Values in '{}' are {} values in '{}' (per expectation).",
            column_a,
            ">=" if or_equal else ">",
            column_b,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error(
            "Unexpected error during GX 'A greater than B' validation."
        )
        raise


def gx_check_multicolumn_sum_to_equal(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the row-wise sum of specified columns equals a given total.

    Uses Great Expectations' ``ExpectMulticolumnSumToEqual`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_list (list[str]): List of column names to sum.

            - sum_total (int | float): Expected sum value for each row.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_list = config.get("column_list", [])
        sum_total = config.get("sum_total")

        if not column_list:
            raise KeyError("Missing required key 'column_list' in config.")
        column_list = list(column_list)
        if sum_total is None:
            raise KeyError("Missing required key 'sum_total' in config.")

        missing = [c for c in column_list if c not in df.columns]
        if missing:
            error_msg = (
                "Validation failed: The following columns are missing from the DataFrame: "
                f"{missing}"
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectMulticolumnSumToEqual(
            column_list=column_list, sum_total=sum_total
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Row-wise sum of columns {column_list} does not "
                f"equal {sum_total} consistently."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Row-wise sum of columns {} equals {}.",
            column_list,
            sum_total,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error(
            "Unexpected error during GX 'multicolumn sum to equal' validation."
        )
        raise


def pl_check_group_consistency(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    """
    Ensure each group value in `group_col` maps to exactly one unique value in `value_col`.

    This is a Polars-native validation (not using GX). It validates that for each
    unique value in the group column, there is exactly one unique value in the
    value column. For example, each order_id should be associated with only one
    customer_id.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - group_col (str): Column name to group by.

            - value_col (str): Column name that should have unique values per group.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        group_col = config.get("group_col")
        value_col = config.get("value_col")

        if not group_col or not value_col:
            raise KeyError("Missing required key 'group_col' or 'value_col' in config.")

        missing_cols = []
        if group_col not in df.columns:
            missing_cols.append(group_col)
        if value_col not in df.columns:
            missing_cols.append(value_col)
        if missing_cols:
            error_msg = (
                "Validation failed: The following columns are missing from the DataFrame: "
                f"{missing_cols}"
            )
            raise ValueError(error_msg)

        violations_groups = (
            df.group_by(group_col)
            .agg(pl.col(value_col).n_unique().alias("unique_count"))
            .filter(pl.col("unique_count") > 1)
        )

        if violations_groups.height > 0:
            # Get indices of invalid rows
            violating_group_values = violations_groups.select(group_col).to_series().to_list()
            df_with_index = df.with_row_index("row_index")
            invalid_rows = df_with_index.filter(
                pl.col(group_col).is_in(violating_group_values)
            )
            invalid_indices = invalid_rows.select("row_index").to_series().to_list()

            error_msg = (
                "Validation failed: "
                f"{violations_groups.height} '{group_col}' values map to multiple "
                f"'{value_col}' values. Violating row indices: {invalid_indices}"
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Each '{}' maps to exactly one unique '{}'.",
            group_col,
            value_col,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during group consistency validation.")
        raise
