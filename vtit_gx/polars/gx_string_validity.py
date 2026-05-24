"""
Module for validating data string validity using Great Expectations with Polars DataFrame.
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


# Expect each column value to be in a given set.
def gx_check_column_values_in_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column values are contained within a given set.

    Uses Great Expectations' ``ExpectColumnValuesToBeInSet`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - value_set (list): The allowed set of values for the column.

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
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToBeInSet(
            column=column,
            value_set=value_set,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' contains values "
                f"not in allowed set {value_set}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' values are within allowed set {}.",
            column,
            value_set,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'values in set' validation.")
        raise


def gx_check_column_most_common_value_in_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that the most common value in a column is contained in a given set.

    Uses Great Expectations' ``ExpectColumnMostCommonValueToBeInSet`` column-aggregate
    expectation. The expectation succeeds when the most-common value for the
    specified column is one of the provided values. If ``ties_okay`` is True,
    ties between allowed and disallowed values are permitted.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to check.

            - value_set (iterable): Collection of allowed most-common values.

            - ties_okay (bool, optional): If True, ties between allowed and not-allowed
              most-common values are acceptable. Defaults to False.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        value_set = config.get("value_set")
        ties_okay = config.get("ties_okay", False)

        if column is None or column == "" or value_set is None:
            raise KeyError("Missing required key 'column' or 'value_set' in config.")
        value_set = list(value_set)
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnMostCommonValueToBeInSet(
            column=column, value_set=value_set, ties_okay=ties_okay
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Most common value in column "
                f"'{column}' is not in {value_set} (ties_okay={ties_okay})."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Most common value in column '{}' is within {} (ties_okay={}).",
            column,
            value_set,
            ties_okay,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'most common value in set' validation.")
        raise


# Expect the column entries to be strings with length equal to the provided value.
def gx_check_column_value_lengths_to_equal(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that string lengths in a column equal a specified value.

    Uses Great Expectations' ``ExpectColumnValueLengthsToEqual`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - value (int): The required length for string values.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        value = config.get("value")

        if column is None or column == "":
            raise KeyError("Missing required key 'column' in config.")
        if value is None:
            raise KeyError("Missing required key 'value' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValueLengthsToEqual(
            column=column,
            value=value,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' does not have string "
                f"lengths equal to {value}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' string lengths equal {}.",
            column,
            value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'value lengths to equal' validation.")
        raise


# Expect the column entries to be strings with length between min and max value.
def gx_check_column_value_lengths_between(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that string lengths in a column fall within a specified inclusive range.

    Uses Great Expectations' ``ExpectColumnValueLengthsToBeBetween`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - min_value (int): Minimum allowed length.

            - max_value (int): Maximum allowed length.

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
        expectation = gx.expectations.ExpectColumnValueLengthsToBeBetween(
            column=column,
            min_value=min_value,
            max_value=max_value,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Column "
                f"'{column}' has string lengths outside range [{min_value}, {max_value}]."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' string lengths are within [{}, {}].",
            column,
            min_value,
            max_value,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'value lengths between' validation.")
        raise


# Expect paired values from two columns belong to a set of valid pairs.
def gx_check_column_pair_values_in_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that paired values from two columns belong to a set of allowed value pairs.

    Uses Great Expectations' ``ExpectColumnPairValuesToBeInSet`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_a (str): First column name.

            - column_b (str): Second column name.

            - value_pairs_set (list[tuple|list]): List of allowed value pairs.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_a = config.get("column_a")
        column_b = config.get("column_b")
        value_pairs_set = config.get("value_pairs_set")

        if column_a is None or column_a == "" or column_b is None or column_b == "":
            raise KeyError("Missing required keys 'column_a' or 'column_b'.")
        if not value_pairs_set:
            raise KeyError("Missing required key 'value_pairs_set'.")
        value_pairs_set = list(value_pairs_set)

        missing_cols = []
        if column_a not in df.columns:
            missing_cols.append(column_a)
        if column_b not in df.columns:
            missing_cols.append(column_b)

        if missing_cols:
            error_msg = (
                f"Validation failed: The following columns are missing: {missing_cols}"
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnPairValuesToBeInSet(
            column_A=column_a,
            column_B=column_b,
            value_pairs_set=value_pairs_set,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Value pairs from "
                f"'{column_a}' and '{column_b}' contain invalid combinations."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Paired values in '{}' and '{}' belong to the allowed pair set.",
            column_a,
            column_b,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX pair-value-in-set validation.")
        raise


def gx_check_column_pair_values_equal(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that values in two columns are equal for each row.

    Uses Great Expectations' ``ExpectColumnPairValuesToBeEqual`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column_a (str): First column name.

            - column_b (str): Second column name.

            - ignore_row_if (str, optional): Ignore a row. Defaults to "both_values_are_missing".
              Options: "both_values_are_missing", "either_value_is_missing", "never".

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column_a = config.get("column_a")
        column_b = config.get("column_b")
        ignore_row_if = config.get("ignore_row_if", "both_values_are_missing")

        if column_a is None or column_a == "" or column_b is None or column_b == "":
            raise KeyError("Missing required keys 'column_a' or 'column_b'.")

        missing_cols = []
        if column_a not in df.columns:
            missing_cols.append(column_a)
        if column_b not in df.columns:
            missing_cols.append(column_b)

        if missing_cols:
            error_msg = (
                f"Validation failed: The following columns are missing: {missing_cols}"
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnPairValuesToBeEqual(
            column_A=column_a,
            column_B=column_b,
            ignore_row_if=ignore_row_if,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Columns "
                f"'{column_a}' and '{column_b}' contain mismatched values."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' and '{}' values are equal.",
            column_a,
            column_b,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX pair-value-equal validation.")
        raise


def gx_check_column_values_match_regex(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column values match a regular expression.

    Uses Great Expectations' ``ExpectColumnValuesToMatchRegex`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - regex (str): Regular expression pattern to match.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        regex = config.get("regex")

        if column is None or column == "" or regex is None or regex == "":
            raise KeyError("Missing required keys 'column' or 'regex'.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToMatchRegex(
            column=column,
            regex=regex,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Column "
                f"'{column}' contains values that do not match regex '{regex}'."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' values match regex '{}'.",
            column,
            regex,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX regex-match validation.")
        raise


def gx_check_column_values_match_regex_list(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column values match one or more regular expressions.

    Uses Great Expectations' ``ExpectColumnValuesToMatchRegexList`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - regex_list (list[str]): List of regex patterns.

            - match_on (str, optional): "any" or "all". Defaults to "any".
              If "any", value must match at least one regex. If "all", value must match all regexes.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        regex_list = config.get("regex_list", [])
        match_on = config.get("match_on", "any")

        if column is None or column == "" or not regex_list:
            raise KeyError("Missing required key 'column' or 'regex_list' in config.")
        if match_on not in {"any", "all"}:
            error_msg = (
                f"Invalid 'match_on' value '{match_on}' — expected 'any' or 'all'."
            )
            raise ValueError(error_msg)

        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToMatchRegexList(
            column=column,
            regex_list=regex_list,
            match_on=match_on,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Column '{column}' contains values that do not match "
                f"regex list {regex_list} (match_on={match_on})."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' values match regex list {} (match_on={}).",
            column,
            regex_list,
            match_on,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX regex-list validation.")
        raise


def gx_check_column_values_match_like_list(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column values match one or more SQL LIKE patterns.

    Uses Great Expectations' ``ExpectColumnValuesToMatchLikePatternList`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - like_pattern_list (list[str]): List of SQL LIKE patterns.

            - match_on (str, optional): "any" or "all". Defaults to "any".
              If "any", value matches at least one pattern. If "all", value matches all patterns.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        like_pattern_list = config.get("like_pattern_list", [])
        match_on = config.get("match_on", "any")

        if column is None or column == "" or not like_pattern_list:
            raise KeyError("Missing required key 'column' or 'like_pattern_list' in config.")
        like_pattern_list = list(like_pattern_list)
        if match_on not in {"any", "all"}:
            error_msg = (
                f"Invalid 'match_on' value '{match_on}' — expected 'any' or 'all'."
            )
            raise ValueError(error_msg)
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToMatchLikePatternList(
            column=column,
            like_pattern_list=like_pattern_list,
            match_on=match_on,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Column '{column}' contains values that do not match LIKE patterns "
                f"{like_pattern_list} (match_on={match_on})."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' values match LIKE patterns {} (match_on={}).",
            column,
            like_pattern_list,
            match_on,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX LIKE-pattern-list validation.")
        raise


def gx_check_column_values_not_in_set(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column values are NOT contained within a given set.

    Uses Great Expectations' ``ExpectColumnValuesToNotBeInSet`` expectation.

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - value_set (iterable): The disallowed set of values for the column.

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
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToNotBeInSet(
            column=column,
            value_set=value_set,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Validation failed: Column '{column}' contains values "
                f"that are in disallowed set {value_set}."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' contains no values from disallowed set {}.",
            column,
            value_set,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'values not in set' validation.")
        raise


def gx_check_column_values_not_match_regex(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column string values do NOT match a given regular expression.

    Uses Great Expectations' ``ExpectColumnValuesToNotMatchRegex`` expectation.
    The expectation checks each row in the specified column and passes if the
    fraction of rows that do NOT match the provided regex meets the acceptance
    threshold (default 100%).

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - regex (str): The regular expression that values should NOT match.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        regex = config.get("regex")

        if column is None or column == "" or regex is None or regex == "":
            raise KeyError("Missing required key 'column' or 'regex' in config.")
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToNotMatchRegex(
            column=column,
            regex=regex,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                "Validation failed: Column "
                f"'{column}' contains values matching disallowed regex '{regex}'."
            )
            raise ValueError(error_msg)

        logger.info(
            "Validation successful: Column '{}' contains no values matching regex '{}'.",
            column,
            regex,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'values not match regex' validation.")
        raise


def gx_check_column_values_not_match_regex_list(
    df: pl.DataFrame, config: Dict
) -> pl.DataFrame:
    """
    Validate that column string values do NOT match any regex in a list.

    Uses Great Expectations' ``ExpectColumnValuesToNotMatchRegexList`` expectation.
    The expectation checks each row in the specified column and passes if the
    fraction of rows that do NOT match any of the provided regexes meets the
    acceptance threshold (default 100%).

    Args:
        df: Input Polars DataFrame to validate.
        config (Dict):

            - column (str): Column name to validate.

            - regex_list (list[str]): List of regular expression patterns that
              values should NOT match.

            - match_on (str, optional): "any" or "all". Defaults to "any".
              If "any", a value is considered a match if it matches any regex in the list;
              if "all", it must match all regexes to be considered a match.

    Returns:
        The original DataFrame if validation passes.
    """
    try:
        column = config.get("column")
        regex_list = config.get("regex_list", [])
        match_on = config.get("match_on", "any")

        if column is None or column == "" or not regex_list:
            raise KeyError("Missing required key 'column' or 'regex_list' in config.")
        regex_list = list(regex_list)
        if match_on not in {"any", "all"}:
            error_msg = (
                f"Invalid 'match_on' value '{match_on}' — expected 'any' or 'all'."
            )
            raise ValueError(error_msg)
        if column not in df.columns:
            error_msg = (
                f"Validation failed: Column '{column}' does not exist in the DataFrame."
            )
            raise ValueError(error_msg)

        validator = _setup_polars_validator(pl_df=df)
        expectation = gx.expectations.ExpectColumnValuesToNotMatchRegexList(
            column=column,
            regex_list=regex_list,
            match_on=match_on,
        )
        result = _validate_expectation(
                validator,
                expectation,
            )

        if not _handle_result(result):
            error_msg = (
                f"Column '{column}' contains values that match regex list "
                f"{regex_list} (match_on={match_on})."
            )     
            raise ValueError(error_msg)
        logger.info(
            "Successful: Column '{}' contains no values matching regex list {} (match_on={}).",
            column,
            regex_list,
            match_on,
        )
        return df

    except (ValueError, KeyError, TypeError) as e:
        logger.error("Unexpected error during GX 'regex-list not match' validation.")
        raise
