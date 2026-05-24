"""
Module for validating data schema validity using Great Expectations with Polars DataFrame.
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


# Expect the columns in a table to match an unordered set.
def gx_check_columns_to_match_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the DataFrame's columns match an unordered set using Great Expectations.

    Uses Great Expectations' ``ExpectTableColumnsToMatchSet`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_set (list[str]): Expected set of column names.

            - exact_match (bool, optional): If True, columns must exactly match the set.
              Defaults to True.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_set = config.get("column_set", [])
        exact_match = config.get("exact_match", True)

        if not column_set:
            raise KeyError("Missing required key 'column_set' in config.")
        
        column_set = list(column_set)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectTableColumnsToMatchSet(
            column_set=column_set,
            exact_match=exact_match,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = "Validation failed: DataFrame columns do not match the expected set."
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: DataFrame columns match the expected set: {}.",
            column_set,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX column-set validation.")
        raise


# Expect the columns in a table to exactly match a specified list.
def gx_check_columns_to_match_ordered_list(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the DataFrame's columns match an ordered list.

    Uses Great Expectations' ``ExpectTableColumnsToMatchOrderedList`` expectation
    to check whether the DataFrame's columns exactly match the specified order.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_list (list[str]): Expected ordered list of column names.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_list = config.get("column_list", [])

        if not column_list:
            raise KeyError("Missing required key 'column_list' in config.")

        column_list = list(column_list)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectTableColumnsToMatchOrderedList(
            column_list=column_list
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: DataFrame columns do not match the expected "
                f"ordered list: {column_list}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: DataFrame columns match the expected ordered list: {}.",
            column_list,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX ordered-column validation.")
        raise


# Expect the number of columns in a table to equal a value.
def gx_check_table_column_count_equal(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the number of columns equals a specified value.

    Uses Great Expectations' ``ExpectTableColumnCountToEqual`` expectation
    to check that the DataFrame has exactly the expected number of columns.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - value (int): Expected number of columns.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        value = config.get("value")

        if value is None:
            raise KeyError("Missing required key 'value' in config.")

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectTableColumnCountToEqual(value=value)
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: DataFrame column count does not match "
                f"expected value '{value}'."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: DataFrame column count matches expected value '{}'.",
            value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX table-column-count validation.")
        raise


# Expect a column to contain values from a specified type list.
def gx_check_column_values_to_be_in_type_list(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that a column's values belong to a set of types.

    Uses Great Expectations' ``ExpectColumnValuesToBeInTypeList`` expectation
    to ensure that all values in a given column belong to one of the allowed types.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Name of the column to validate.

            - type_list (list[str]): List of allowed types (e.g., ["int", "float"]).

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        type_list = config.get("type_list", [])

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if not type_list:
            raise KeyError("Missing required key 'type_list' in config.")
        type_list = list(type_list)
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeInTypeList(
            column=column,
            type_list=type_list,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' does not match expected "
                f"type list {type_list}."
            )            
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' matches expected type list {}.",
            column,
            type_list,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX column-type-list validation.")
        raise


def gx_check_column_values_to_be_of_type(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that all values in a column are of a specified data type.

    Uses Great Expectations' ``ExpectColumnValuesToBeOfType`` expectation
    to ensure that entries in the target column conform to the provided data type
    (e.g. "int", "float", "str", backend-specific types).

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Name of the column to validate.

            - expected_type (str): The expected data type for each value in the column.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        expected_type = config.get("expected_type")

        if column is None or column == "" or expected_type is None:
            raise KeyError("Missing required key 'column' or 'expected_type' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeOfType(
            column=column,
            type_=expected_type,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' contains values not of type "
                f"'{expected_type}'."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' values are of type '{}'.",
            column,
            expected_type,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'values to be of type' validation.")
        raise


def gx_check_column_to_exist(df: pl.DataFrame, config: Dict) -> pl.DataFrame:
    """
    Validate that a specified column exists in the DataFrame.

    Uses Great Expectations' ``ExpectColumnToExist`` Batch expectation
    to assert that the named column is present.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): The column name to check for existence.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnToExist(column=column)
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = f"Validation failed: Column '{column}' does not exist."
            raise ValueError(error_msg)

        logger.info("Validation successful: Column '{}' exists.", column)
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'column to exist' validation.")
        raise


def gx_check_table_column_count_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the number of columns in the DataFrame is within a specified range.

    Uses Great Expectations' ``ExpectTableColumnCountToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - min_value (int | None, optional): Minimum allowed number of columns (inclusive).

            - max_value (int | None, optional): Maximum allowed number of columns (inclusive).

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        min_value = config.get("min_value")
        max_value = config.get("max_value")

        if min_value is None and max_value is None:
            raise KeyError(
                "Config must provide at least one of 'min_value' or 'max_value'."
            )

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectTableColumnCountToBeBetween(
            min_value=min_value,
            max_value=max_value,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Table column count not within "
                f"{min_value} and {max_value}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Table column count is within {} and {}.",
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'table column count between' validation.")
        raise
