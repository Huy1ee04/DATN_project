"""
Module for validating data numeric validity using Great Expectations with Polars DataFrame.
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


# Expect the column entries to be between a minimum value and a maximum value (inclusive).
def gx_check_column_values_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column values are within the specified numeric range.

    Uses Great Expectations' ``ExpectColumnValuesToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | float): Minimum allowed value (inclusive by default).

            - max_value (int | float): Maximum allowed value (inclusive by default).

            - strict_min (bool, optional): If True, values must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, values must be strictly less than max_value.
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
        if min_value is None or max_value is None:
            raise KeyError("Missing required key 'min_value' or 'max_value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeBetween(
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
                f"Validation failed: Values in column '{column}' are not within range "
                f"[{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Values in column '{}' are within range [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'values between' validation.")
        raise


# Expect the column maximum to be between a minimum value and a maximum value.
def gx_check_column_max_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the maximum value of a column is within the specified range.

    Uses Great Expectations' ``ExpectColumnMaxToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | float): Minimum allowed maximum value (inclusive by default).

            - max_value (int | float): Maximum allowed maximum value (inclusive by default).

            - strict_min (bool, optional): If True, max must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, max must be strictly less than max_value.
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
        if min_value is None or max_value is None:
            raise KeyError("Missing required key 'min_value' or 'max_value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnMaxToBeBetween(
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
                f"Validation failed: Max of column '{column}' is not within range "
                f"[{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Max of column '{}' is within range [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'max between' validation.")
        raise


# Expect the column minimum to be between a minimum value and a maximum value.
def gx_check_column_min_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the minimum value of a column is within the specified range.

    Uses Great Expectations' ``ExpectColumnMinToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | float): Minimum allowed minimum value (inclusive by default).

            - max_value (int | float): Maximum allowed minimum value (inclusive by default).

            - strict_min (bool, optional): If True, min must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, min must be strictly less than max_value.
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
        if min_value is None or max_value is None:
            raise KeyError("Missing required key 'min_value' or 'max_value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnMinToBeBetween(
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
                f"Validation failed: Min of column '{column}' is not within range "
                f"[{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Min of column '{}' is within range [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'min between' validation.")
        raise


# Expect the column mean to be between a minimum value and a maximum value.
def gx_check_column_mean_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the mean value of a column is within the specified range.

    Uses Great Expectations' ``ExpectColumnMeanToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | float): Minimum allowed mean value (inclusive by default).

            - max_value (int | float): Maximum allowed mean value (inclusive by default).

            - strict_min (bool, optional): If True, mean must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, mean must be strictly less than max_value.
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
        if min_value is None or max_value is None:
            raise KeyError("Missing required key 'min_value' or 'max_value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnMeanToBeBetween(
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
                f"Validation failed: Mean of column '{column}' not within "
                f"[{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Mean of column '{}' is within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'mean between' validation.")
        raise


# Expect the column median to be between a minimum value and a maximum value.
def gx_check_column_median_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the median value of a column is within the specified range.

    Uses Great Expectations' ``ExpectColumnMedianToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | float): Minimum allowed median value (inclusive by default).

            - max_value (int | float): Maximum allowed median value (inclusive by default).

            - strict_min (bool, optional): If True, median must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, median must be strictly less than max_value.
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
        if min_value is None or max_value is None:
            raise KeyError("Missing required key 'min_value' or 'max_value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnMedianToBeBetween(
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
                f"Validation failed: Median of column '{column}' not within "
                f"[{min_value}, {max_value}]."
            )           
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Median of column '{}' is within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error(
            "Unexpected error during GX 'median between' validation."
        )
        raise


def gx_check_column_kl_divergence_less_than(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the KL divergence between the column's empirical distribution and
    the provided partition_object is less than the given threshold.

    Uses Great Expectations' ``ExpectColumnKLDivergenceToBeLessThan`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - partition_object (dict): Expected distribution with keys "values" and "weights".

            - threshold (float): Maximum allowed KL divergence.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        partition_object = config.get("partition_object")
        threshold = config.get("threshold")

        if column is None or column == "" or partition_object is None or threshold is None:
            raise KeyError(
                "Missing required key 'column', 'partition_object', or 'threshold' in config."
            )
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnKLDivergenceToBeLessThan(
            column=column,
            partition_object=partition_object,
            threshold=threshold,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: KL divergence of column "
                f"'{column}' exceeds threshold {threshold}."
            )
            raise ValueError(error_msg)
        logger.info(
            "Validation successful: KL divergence for column '{}' is below threshold {}.",
            column,
            threshold,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'KL divergence' validation.")
        raise


def gx_check_column_quantile_values_to_be_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that specific quantiles of a column fall within provided value ranges.

    Uses Great Expectations' ``ExpectColumnQuantileValuesToBeBetween`` column-aggregate
    expectation. The `quantile_ranges` config must be a dict with keys:
    - "quantiles": list[float] (in increasing order; values between 0 and 1)
    - "value_ranges": list[list[min, max]] (same length as "quantiles")

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - quantile_ranges (dict): Dictionary with keys:
                - "quantiles" (list[float]): List of quantiles in increasing order (0-1).
                - "value_ranges" (list[list[min, max]]): List of ranges for each quantile.

            - allow_relative_error (bool, optional): Passed through to GX. Defaults to False.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        quantile_ranges = config.get("quantile_ranges")
        allow_relative_error = config.get("allow_relative_error", False)

        if column is None or column == "" or quantile_ranges is None:
            raise KeyError("Missing required key 'column' or 'quantile_ranges'.")
        if column not in df.columns:
            error_msg = f"Validation failed: Column '{column}' does not exist."
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnQuantileValuesToBeBetween(
            column=column,
            quantile_ranges=quantile_ranges,
            allow_relative_error=allow_relative_error,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Quantile values of '{column}' "
                "not within expected ranges."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Quantile values of '{}' are within expected ranges.",
            column,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'quantile values to be between' validation.")
        raise


def gx_check_column_stdev_to_be_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the sample standard deviation (normalized by N-1) of a column
    falls within the specified range.

    Uses Great Expectations' ``ExpectColumnStdevToBeBetween`` column-aggregate
    expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (float | None, optional): Minimum allowed stdev.

            - max_value (float | None, optional): Maximum allowed stdev.

            - strict_min (bool, optional): If True, stdev must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, stdev must be strictly less than max_value.
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
            raise KeyError(
                "Config must provide at least one of 'min_value' or 'max_value'."
            )
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnStdevToBeBetween(
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
                "Validation failed: Standard deviation for column "
                f"'{column}' not within {min_value} and {max_value}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Standard deviation for column '{}' is within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'stdev between' validation.")
        raise


def gx_check_column_sum_to_be_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the sum of values in a column is within a specified range.

    Uses Great Expectations' ``ExpectColumnSumToBeBetween`` column-aggregate expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int | float | None, optional): Minimum allowed sum.

            - max_value (int | float | None, optional): Maximum allowed sum.

            - strict_min (bool, optional): If True, sum must be strictly greater than min_value.
              Defaults to False.

            - strict_max (bool, optional): If True, sum must be strictly less than max_value.
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
            raise KeyError(
                "Config must provide at least one of 'min_value' or 'max_value'."
            )
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnSumToBeBetween(
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
                f"Validation failed: Sum of column '{column}' not within "
                f"{min_value} and {max_value}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Sum of column '{}' is within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'sum between' validation.")
        raise


def gx_check_column_value_zscores_to_be_less_than(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that Z-scores of a column's values are less than a given threshold.

    Uses Great Expectations' ``ExpectColumnValueZScoresToBeLessThan`` column-map
    expectation. This checks each row's Z-score (based on column mean/std) and
    validates that the proportion of rows with Z-scores within the allowed bound
    meets the expectation (defaults to 100% unless configured otherwise elsewhere).

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - threshold (float): Maximum Z-score threshold.

            - double_sided (bool, optional): If True, check absolute Z-score < threshold (two-sided).
              If False, check Z-score < threshold (one-sided). Defaults to True.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        threshold = config.get("threshold")
        double_sided = config.get("double_sided", True)

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if threshold is None:
            raise KeyError("Missing required key 'threshold' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValueZScoresToBeLessThan(
            column=column,
            threshold=threshold,
            double_sided=double_sided,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            side = "two-sided" if double_sided else "one-sided"
            error_msg = (
                "Z-scores for column "
                f"'{column}' exceed threshold {threshold} ({side}) for some rows."
            )
            raise ValueError(error_msg)

        logger.info(
            "Z-scores for column '{}' are within threshold {} (double_sided={}).",
            column,
            threshold,
            double_sided,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'Z-scores less than' validation.")
        raise
