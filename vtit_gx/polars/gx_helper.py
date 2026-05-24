"""
Helper functions for validating data functions using Great Expectation.
"""
# Configure logging to reduce Great Expectations INFO logs
import logging
logging.getLogger("great_expectations._docs_decorators").setLevel(logging.WARNING)
logging.getLogger("great_expectations").setLevel(logging.WARNING)
logging.getLogger("great_expectations.data_context.types.base").setLevel(logging.WARNING)

import great_expectations as gx
import polars as pl
from loguru import logger


# Setup components
def _setup_polars_validator(pl_df: pl.DataFrame):
   """Convert a Polars DataFrame to a Great Expectations validator.

   This function first converts the input Polars DataFrame into a Pandas
   DataFrame. It then creates an ephemeral Great Expectations (GX) context
   and initializes a validator using the default Pandas datasource.

   Args:
       pl_df: The input Polars DataFrame to be converted and validated.

   Returns:
       A Great Expectations validator object initialized from the
       Pandas DataFrame.

   Raises:
       Exception: If conversion to Pandas, context creation, or
           validator initialization fails.
   """
   try:
       pd_df = pl_df.to_pandas()
       context = gx.get_context(mode="ephemeral")
       validator = context.data_sources.pandas_default.read_dataframe(pd_df)
       return validator
   except Exception as e:
       logger.error("Failed to create validator: {}", e)
       raise


def _handle_result(validation_result) -> bool:
   """
   Process and log the result of a Great Expectations validation
   in a clean, readable, production-friendly format.
   """
   exp_type = validation_result.expectation_config.type
   kwargs = dict(validation_result.expectation_config.kwargs or {})
   passed = validation_result.success
   result = validation_result.result or {}

   # Remove noisy / non-business kwargs
   kwargs.pop("batch_id", None)

   # Extract core metrics
   element_count = result.get("element_count")
   unexpected_count = result.get("unexpected_count")
   unexpected_percent = result.get("unexpected_percent")
   observed_value = result.get("observed_value")  # Chỉ có ở một số expectation (aggregate)

   unexpected_index_list = result.get("unexpected_index_list")
   unexpected_rows = result.get("unexpected_rows")

   # Exception info (rare but important)
   exception_info = getattr(validation_result, "exception_info", {}) or {}
   exception_msg = None
   if isinstance(exception_info, dict) and exception_info.get("raised_exception"):
       exception_msg = exception_info.get("exception_message")

   # ---- Summary log ----
   status = "[PASSED]" if passed else "[FAILED]"

   observed_line = (
       f"\n  observed_value = {observed_value}" if observed_value is not None else ""
   )
   logger.info(
       "{} Expectation = '{}'\n"
       "  kwargs = {}\n"
       "  element_count = {}\n"
       "  unexpected_count = {} ({:.2f}%){}{}",
       status,
       exp_type,
       kwargs,
       element_count,
       unexpected_count,
       unexpected_percent if unexpected_percent is not None else 0.0,
       observed_line,
       f"\n  unexpected_index_list = {unexpected_index_list}"
       if unexpected_index_list
       else "",
   )
   # ---- Unexpected rows (separate, readable) ----
   if not passed and unexpected_rows is not None:
       logger.warning(
           "Unexpected rows (showing {} rows):\n{}",
           len(unexpected_rows),
           unexpected_rows,
       )
   # ---- Exception details ----
   if exception_msg:
       logger.error("GX raised exception: {}", exception_msg)

   return passed


def _validate_expectation(
   validator,
   expectation,
   *,
   partial_unexpected_count: int = 20,
   include_unexpected_rows: bool = True,
   result_format: str = "COMPLETE",
):
   """
   Run Great Expectations validation with a unified result_format configuration.

   This helper standardizes how GX validations are executed across the codebase,
   avoiding duplicated validate(...) calls and ensuring consistent behavior.

   Args:
       validator: Great Expectations validator instance.
       expectation: A GX Expectation object.
       partial_unexpected_count: Maximum number of unexpected records to return.
                                 Defaults to 20 (GX default). Up to 200.
       include_unexpected_rows: Whether to include full unexpected rows.
                                Defaults to True.
       result_format: GX result format. Defaults to "COMPLETE".

   Returns:
       ExpectationValidationResult returned by GX.

   Raises:
       Exception: Propagates any unexpected GX validation errors.
  
   Example:
       result = _validate_expectation(
           validator,
           expectation,
           partial_unexpected_count=50,
           include_unexpected_rows=True,
           )
   """
   return validator.validate(
       expect=expectation,
       result_format={
           "result_format": result_format,
           "include_unexpected_rows": include_unexpected_rows,
           "partial_unexpected_count": partial_unexpected_count,
       },
   )
