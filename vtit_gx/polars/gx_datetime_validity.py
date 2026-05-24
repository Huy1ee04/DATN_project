"""
Module for validating data datetime validity using Great Expectations with Polars DataFrame.
"""

from datetime import datetime
from typing import Dict

import great_expectations as gx
import polars as pl
from loguru import logger

from vtit_gx.polars.gx_helper import (
    _handle_result,
    _setup_polars_validator,
    _validate_expectation,
)


def gx_check_column_datetime_range(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that datetime column values are within the specified time range.

    Uses Great Expectations' ``ExpectColumnValuesToBeBetween`` expectation.
    This function validates datetime/date columns against a specified range.
    The start_datetime and end_datetime can be datetime objects, date objects, or strings in ISO format.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate (must be datetime or date type).

            - start_time (datetime | date | str): Start of datetime range (inclusive by default).
              Can be datetime object, date object, or ISO format string.

            - end_time (datetime | date | str): End of datetime range (inclusive by default).
              Can be datetime object, date object, or ISO format string.

            - strict_start (bool, optional): If True, values must be strictly after start_time.
              Defaults to False.

            - strict_end (bool, optional): If True, values must be strictly before end_time.
              Defaults to False.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        start_time = config.get("start_time")
        end_time = config.get("end_time")
        strict_start = config.get("strict_start", False)
        strict_end = config.get("strict_end", False)

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if start_time is None or end_time is None:
            raise KeyError("Missing required key 'start_time' or 'end_time' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
            column=column,
            min_value=start_time,
            max_value=end_time,
            strict_min=strict_start,
            strict_max=strict_end,
        )
        result = _validate_expectation(
            validator,
            expectation,
        )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Datetime values in column '{column}' are not within range "
                f"[{start_time}, {end_time}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Datetime values in column '{}' are within range [{}, {}].",
            column,
            start_time,
            end_time,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'datetime between' validation: {}",e)
        raise
