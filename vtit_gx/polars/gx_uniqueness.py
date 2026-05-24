"""
Module for validating data uniqueness using Great Expectations with Polars DataFrame.
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


# Expect each column value to be unique
def gx_check_column_values_unique(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that values in a column are unique.

    Uses Great Expectations' ``ExpectColumnValuesToBeUnique`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist in the DataFrame."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeUnique(column=column)
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = "Validation failed: Column contains duplicate values."
            raise ValueError(error_msg)

        logger.info("Validation successful: Column values are unique.")
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'values unique' validation.")
        raise


# Expect the compound columns to be unique
def gx_check_compound_columns_unique(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the combination of values across a list of columns is unique.

    Uses Great Expectations' ``ExpectCompoundColumnsToBeUnique`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_list (list[str]): List of column names to check for uniqueness.

            - ignore_row_if (str, optional): When to ignore a row. Defaults to "never".
              Options: "never", "all_values_are_missing", "any_value_is_missing".

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_list = config.get("column_list", [])
        ignore_row_if = config.get("ignore_row_if", "never")

        if not column_list:
            raise KeyError("Missing required key 'column_list' in config.")
        column_list = list(column_list)

        missing = [c for c in column_list if c not in df.columns]
        if missing:
            error_msg = f"Validation failed: Missing columns: {missing}"
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectCompoundColumnsToBeUnique(
            column_list=column_list, ignore_row_if=ignore_row_if
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = "Validation failed: Compound columns are not unique."
            raise ValueError(error_msg)

        logger.info("Validation successful: Compound columns are unique.")
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'compound columns unique' validation.")
        raise


# Expect the set of distinct column values to be contained by a given set
def gx_check_column_distinct_values_in_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the set of distinct values in a column is contained in a given set.

    Uses Great Expectations' ``ExpectColumnDistinctValuesToBeInSet`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - value_set (iterable): Set of allowed distinct values.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        value_set = config.get("value_set")

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if value_set is None:
            raise KeyError("Missing required key 'value_set' in config.")
        value_set = list(value_set)
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist in the DataFrame."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnDistinctValuesToBeInSet(
            column=column, value_set=value_set
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Distinct values of {column} are not a subset of {value_set}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Distinct values of {} are within {}.",
            column,
            value_set,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'distinct values in set' validation.")
        raise


# Expect the set of distinct column values to equal a given set
def gx_check_column_distinct_values_to_equal_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the set of distinct values in a column exactly equals a given set.

    Uses Great Expectations' ``ExpectColumnDistinctValuesToEqualSet`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - value_set (iterable): Set of values that must exactly match the distinct values.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        value_set = config.get("value_set")

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if value_set is None:
            raise KeyError("Missing required key 'value_set' in config.")
        value_set = list(value_set)
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist in the DataFrame." 
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnDistinctValuesToEqualSet(
            column=column, value_set=value_set
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Distinct values of {column} do not equal {value_set}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Distinct values of {} equal {}.", column, value_set
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'distinct values equal set' validation.")
        raise


# Expect the set of distinct column values to contain a given set
def gx_check_column_distinct_values_contain_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the set of distinct values in a column contains the provided set.

    Uses Great Expectations' ``ExpectColumnDistinctValuesToContainSet`` column-aggregate
    expectation. The expectation succeeds when every element of the provided
    `value_set` appears among the distinct values of the specified column.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to check.

            - value_set (iterable): Collection of values that must be contained in
              the column's distinct values.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        value_set = config.get("value_set")

        if column is None or column == "" or value_set is None:
            raise KeyError("Missing required key 'column' or 'value_set' in config.")
        value_set = list(value_set)
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist in the DataFrame."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnDistinctValuesToContainSet(
            column=column, value_set=value_set
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Distinct values of '{column}' do not contain required set {value_set}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Distinct values of '{}' contain {}.",
            column,
            value_set,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'distinct values contain set' validation.")
        raise


# Expect the selected column values are unique within each record
def gx_check_select_column_values_unique_within_record(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that selected column values are unique within each record (row).

    Uses Great Expectations' ``ExpectSelectColumnValuesToBeUniqueWithinRecord`` expectation.
    This ensures that within each row, the values in the specified columns are unique.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_list (list[str]): List of column names to check for uniqueness within each row.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_list = config.get("column_list", [])
        if not column_list:
            raise KeyError("Missing required key 'column_list' in config.")
        column_list = list(column_list)
        missing = [c for c in column_list if c not in df.columns]
        if missing:
            error_msg = f"Validation failed: Missing columns: {missing}."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectSelectColumnValuesToBeUniqueWithinRecord(
            column_list=column_list
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Duplicate values found within columns {column_list} for some rows."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Selected columns {} are unique within each record.",
            column_list,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'unique within record' validation.")
        raise


def gx_check_column_unique_value_count_to_be_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the number of distinct (unique) values in a column is within a given range.

    Uses Great Expectations' ``ExpectColumnUniqueValueCountToBeBetween`` column-aggregate
    expectation. The expectation computes the number of unique values for the given
    column and checks it against the provided `min_value` and `max_value`.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | None, optional): Minimum allowed unique value count (inclusive by default).

            - max_value (int | None, optional): Maximum allowed unique value count (inclusive by default).

            - strict_min (bool, optional): If True, unique count must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, unique count must be strictly less than max_value.
              Defaults to False.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        min_value = config.get("min_value")
        max_value = config.get("max_value")
        strict_min = config.get("strict_min", False)
        strict_max = config.get("strict_max", False)

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if min_value is None and max_value is None:
            error_msg = (
                "Config must provide at least one of 'min_value' or 'max_value'."
            )
            raise ValueError(error_msg)
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist in the DataFrame."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnUniqueValueCountToBeBetween(
            column=column,
            min_value=min_value,
            max_value=max_value,
            strict_min=strict_min,
            strict_max=strict_max,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Unique value count for column '{column}' not within {min_value} and {max_value}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Unique value count for column '{}' is within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'unique value count to be between' validation.")
        raise


def gx_check_column_proportion_of_unique_values_to_be_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the proportion of unique values in a column is within a given range.

    Uses Great Expectations' ``ExpectColumnProportionOfUniqueValuesToBeBetween``.
    The expectation computes (number of distinct values) / (total rows) and checks
    whether that proportion lies between `min_value` and `max_value`.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (float | None, optional): Minimum allowed proportion (0.0 - 1.0) or None.

            - max_value (float | None, optional): Maximum allowed proportion (0.0 - 1.0) or None.

            - strict_min (bool, optional): If True, proportion must be strictly > min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, proportion must be strictly < max_value.
              Defaults to False.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        min_value = config.get("min_value")
        max_value = config.get("max_value")
        strict_min = config.get("strict_min", False)
        strict_max = config.get("strict_max", False)

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if min_value is None and max_value is None:
            error_msg = (
                "Config must provide at least one of 'min_value' or 'max_value'."
            )
            raise ValueError(error_msg)
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist in the DataFrame."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnProportionOfUniqueValuesToBeBetween(
            column=column,
            min_value=min_value,
            max_value=max_value,
            strict_min=strict_min,
            strict_max=strict_max,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Proportion of unique values for column '{column}' not within "
                f"[{min_value}, {max_value}] (strict_min={strict_min}, strict_max={strict_max})"
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Proportion of unique values for column '{}' is within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'proportion of unique values' validation.")
        raise
