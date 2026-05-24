"""
Module for validating data completeness using Great Expectations with Polars DataFrame.
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


# Expect the column not be null.
def gx_check_column_not_null(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    """
    Validate that a single specified column contains no null values using Great Expectations.

    Uses Great Expectations' ``ExpectColumnValuesToNotBeNull`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - mostly (float, optional): Fraction of rows that must pass (0.0-1.0).
              Defaults to 1.0.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        mostly = config.get("mostly", 1.0)

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")

        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
            column=column, mostly=mostly
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' contains null values."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' contains no null values.", column
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX not-null column validation.")
        raise


# Expect the column values of columns set to not be null.
def gx_check_columns_not_null(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    """
    Validate that specified columns contain no null values using Great Expectations.

    Uses Great Expectations' ``ExpectColumnValuesToNotBeNull`` expectation
    to check that all values in the given column set are non-null.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - columns (list[str]): List of column names to validate.

            - mostly (float, optional): Fraction of rows that must pass (0.0-1.0).
              Defaults to 1.0.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        columns = config.get("columns", [])
        mostly = config.get("mostly", 1.0)

        if not columns:
            raise KeyError("Missing required key 'columns' in config.")
        columns = list(columns)

        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            error_msg = (
                "Validation failed: The following columns are missing from the DataFrame: "
                f"{missing_cols}"
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        failed_columns = []
        for col in columns:
            expectation = gx.expectations.ExpectColumnValuesToNotBeNull(
                column=col, mostly=mostly
            )
            result = _validate_expectation(
                    validator,
                    expectation,
                )
            if not _handle_result(result):
                failed_columns.append(col)

        if failed_columns:
            error_msg = (
                f"Validation failed: Columns {failed_columns} contain null values."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: All specified columns contain no null values."
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX not-null column validation.")
        raise


# Expect the column be null.
def gx_check_column_be_null(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    """
    Validate that a specified column in the DataFrame contains only null values.

    Uses Great Expectations' ``ExpectColumnValuesToBeNull`` expectation
    to ensure that all values in the target column are null.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Name of the column to validate.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeNull(column=column)
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' contains non-null values."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' contains only null values.",
            column,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error(
            "Unexpected error during GX 'column to be null' validation."
        )
        raise


# Expect the column values of columns set to be null.
def gx_check_columns_be_null(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    """
    Validate that specified columns in the DataFrame contain only null values.

    Uses Great Expectations' ``ExpectColumnValuesToBeNull`` expectation
    to ensure that all values in the given columns are null.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - columns (list[str]): List of column names to check for null values.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        columns = config.get("columns", [])
        if not columns:
            raise KeyError("Missing required key 'columns' in config.")
        columns = list(columns)

        missing_cols = [c for c in columns if c not in df.columns]
        if missing_cols:
            error_msg = (
                "Validation failed: The following columns are missing from the DataFrame: "
                f"{missing_cols}"
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        failed_columns = []
        for col in columns:
            expectation = gx.expectations.ExpectColumnValuesToBeNull(column=col)
            result = _validate_expectation(
                    validator,
                    expectation,
                )
            if not _handle_result(result):
                failed_columns.append(col)

        if failed_columns:
            error_msg = (
                f"Validation failed: Columns {failed_columns} contain non-null values."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: All specified columns contain only null values: {}.",
            columns,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error(
            "Unexpected error during GX 'columns to be null' validation."
        )
        raise


# Expect the proportion of non-null values in a column lies between in range
def gx_check_column_non_null_proportion_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the proportion of non-null values in a column lies within a given range.

    Uses Great Expectations' ``ExpectColumnProportionOfNonNullValuesToBeBetween``
    to ensure that the fraction of non-null values in the specified column
    falls between min_value and max_value.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (float): Minimum proportion of non-null values (0.0-1.0).

            - max_value (float): Maximum proportion of non-null values (0.0-1.0).

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        min_value = config.get("min_value")
        max_value = config.get("max_value")

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if min_value is None or max_value is None:
            raise KeyError("Missing required key 'min_value' or 'max_value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnProportionOfNonNullValuesToBeBetween(
            column=column, min_value=min_value, max_value=max_value
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Proportion of non-null values in "
                f"'{column}' is not within the expected range "
                f"[{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' has non-null proportion within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error(
            "Unexpected error during GX non-null proportion validation."
        )
        raise


# Validate that for every row, at least one column from the specified set is not null.
def pl_check_row_has_at_least_one_non_null(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that for every row, at least one of the specified columns is not null.

    Passes only if each row has at least one non-null value among the columns
    listed in the config. This is a Polars-native validation (not using GX).

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - columns (list[str]): List of column names to check.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        columns = config.get("columns", [])
        if not columns:
            raise KeyError("Missing required key 'columns' in config.")
        columns = list(columns)

        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            error_msg = (
                f"Validation failed: Columns not found in DataFrame: {missing_cols}"
            )
            raise ValueError(error_msg)

        # Add a column to check if row has at least one non-null value and add row index
        df_with_check = df.with_columns([
            pl.int_range(pl.len()).alias("__row_index__"),
            pl.any_horizontal([pl.col(col).is_not_null() for col in columns]).alias(
                "row_has_non_null"
            )
        ])
        # Get indices of invalid rows (where all columns are null)
        invalid_indices = (
            df_with_check
            .filter(~pl.col("row_has_non_null"))
            .select("__row_index__")
            .to_series()
            .to_list()
        )

        if invalid_indices:
            error_msg = (
                "Validation failed: Found "
                f"{len(invalid_indices)} rows where all columns {columns} are null. "
                f"Invalid row indices: {invalid_indices}"
            )
            raise ValueError(error_msg)

        logger.success(
            "Validation passed: Every row has at least one non-null value in {}.",
            columns,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during non-all-null validation.")
        raise
