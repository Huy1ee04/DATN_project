"""
Module for validating data volume using Great Expectations with Polars DataFrame.
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


# Expect the number of rows to be between two values. Only applies if row condition is satisfied
def gx_check_table_row_count_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the DataFrame's row count is within a specified range.

    Uses Great Expectations' ``ExpectTableRowCountToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - min_value (int | None, optional): Minimum allowed row count (inclusive by default).

            - max_value (int | None, optional): Maximum allowed row count (inclusive by default).

            - strict_min (bool, optional): If True, row count must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, row count must be strictly less than max_value.
              Defaults to False.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        min_value = config.get("min_value")
        max_value = config.get("max_value")
        if min_value is None and max_value is None:
            error_msg = (
                "Config must provide at least one of 'min_value' or 'max_value'."
            )
            raise ValueError(error_msg)
        strict_min = config.get("strict_min", False)
        strict_max = config.get("strict_max", False)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectTableRowCountToBeBetween(
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
                f"Validation failed: Table row count not within [{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Table row count is within [{}, {}].",
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'row count between' validation.")
        raise


# Expect the number of rows to equal a value. Only applies if row condition is satisfied
def gx_check_table_row_count_to_equal(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the DataFrame's row count equals a specified value.

    Uses Great Expectations' ``ExpectTableRowCountToEqual`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - value (int): Expected number of rows.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        value = config.get("value")
        if value is None:
            raise KeyError("Missing required key 'value' in config.")

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectTableRowCountToEqual(value=value)
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Table row count does not equal {value}."
            )
            raise ValueError(error_msg)

        logger.info("Validation successful: Table row count equals {}.", value)
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'row count to equal' validation.")
        raise
