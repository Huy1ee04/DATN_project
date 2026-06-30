#!/usr/bin/env python3
"""
sector_info_ingestion.py

Upload sector_info.parquet từ local → MinIO raw layer.

Source (local):
  sector_info.parquet (project root)

Destination (MinIO):
  raw/reference/equity/sector_info.parquet
"""

from __future__ import annotations

import os
import logging
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
from vtit_gx.polars import (
    gx_check_columns_not_null,
    gx_check_column_values_unique,
    gx_check_table_row_count_between,
)

from _company_ingest_common import (
    build_s3fs,
    get_logger,
    load_minio_config,
    replace_parquet_on_s3,
)

logger = get_logger("sector_info_ingestion")
ICT = timezone(timedelta(hours=7))

# Local source file
_script_dir = os.path.dirname(os.path.abspath(__file__))
_project_root = os.path.join(_script_dir, "..", "..")
LOCAL_SECTOR_FILE = os.path.join(_project_root, "sector_info.parquet")


def main() -> None:
    cfg = load_minio_config()

    parser = ArgumentParser(description="Upload sector_info.parquet to MinIO raw layer.")
    parser.add_argument("--bucket", default=cfg.default_bucket, help="MinIO bucket name")
    parser.add_argument("--prefix", default=cfg.default_prefix, help="Path prefix inside bucket")
    parser.add_argument("--source", default=LOCAL_SECTOR_FILE, help="Path to local sector_info.parquet")
    args = parser.parse_args()

    prefix = f"{args.bucket}/{args.prefix}"
    sector_path = f"{prefix}/equity/sector_info.parquet"
    sep = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %z")
    logger.info(
        "\n%s\nUpload sector_info.parquet to MinIO raw layer\n%s\n"
        "MinIO Endpoint : %s\n"
        "Source (local)  : %s\n"
        "Target         : s3://%s\n"
        "Run at         : %s\n%s",
        sep, sep, cfg.endpoint, args.source, sector_path, run_at, sep,
    )

    if not os.path.isfile(args.source):
        logger.error("Source file not found: %s", args.source)
        return

    # Read local file
    df = pd.read_parquet(args.source)
    logger.info("Read %d rows × %d cols from %s", len(df), len(df.columns), args.source)

    if df.empty:
        logger.warning("Empty DataFrame — skipping write.")
        return

    # GX validation
    df_pl = pl.from_pandas(df)
    logger.info("Running GX validation...")
    gx_check_columns_not_null(df_pl, {"columns": ["sector_id", "sector"]})
    gx_check_column_values_unique(df_pl, {"column": "sector_id"})
    gx_check_table_row_count_between(df_pl, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # Upload to MinIO
    fs = build_s3fs(cfg)
    replace_parquet_on_s3(df, fs, sector_path, logger)

    logger.info("\n%s\nSector info ingestion complete!\n%s", sep, sep)


if __name__ == "__main__":
    main()
