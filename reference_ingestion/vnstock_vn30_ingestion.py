#!/usr/bin/env python3
"""
vnstock_vn30_ingestion.py

Fetch thành phần chỉ số VN30 và ghi MinIO:

  s3://…/raw/reference/equity/vn30.parquet

Mặc định (replace snapshot):
  Mỗi lần chạy ghi đè toàn bộ file (atomic .tmp → thay object).
  DataFrame rỗng sau API lỗi → không ghi (giữ file cũ).

--append (tuỳ chọn):
  Read → concat → dedup theo symbol → atomic write.
"""

from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
from vnstock_data import Reference
from vtit_gx.polars.gx_schema_validity import gx_check_columns_to_match_set

from _company_ingest_common import (
    announce_vnstock_key,
    append_parquet_to_s3,
    build_s3fs,
    get_logger,
    load_minio_config,
    replace_parquet_on_s3,
)

logger = get_logger("vn30_ingestion")
ICT = timezone(timedelta(hours=7))

INDEX_CODE = "VN30"
DEDUP_PRIMARY = ("symbol",)
DEDUP_FALLBACK = ("symbol", "exchange")
VN30_EXPECTED_COLUMNS = ("symbol",)


def _coerce_members_df(df_vn30: pd.DataFrame | pd.Series) -> pd.DataFrame:
    if isinstance(df_vn30, pd.Series):
        return df_vn30.to_frame().reset_index(drop=True)
    return df_vn30


def main() -> None:
    cfg = load_minio_config()
    announce_vnstock_key(logger)

    parser = ArgumentParser(description="Fetch VN30 index members to MinIO S3.")
    parser.add_argument("--bucket", default=cfg.default_bucket, help="MinIO bucket name")
    parser.add_argument("--prefix", default=cfg.default_prefix, help="Path prefix inside bucket")
    parser.add_argument(
        "--index",
        default=INDEX_CODE,
        help=f"Mã chỉ số (default: {INDEX_CODE})",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Merge với file cũ + dedup theo symbol (thay vì ghi đè snapshot)",
    )
    args = parser.parse_args()

    prefix = f"{args.bucket}/{args.prefix}"
    vn30_path = f"{prefix}/equity/vn30.parquet"
    mode = "append (merge)" if args.append else "replace (ghi đè snapshot)"
    sep = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %z")
    logger.info(
        "\n%s\nFetch VN30 index members to MinIO S3\n%s\n"
        "MinIO Endpoint : %s\n"
        "Index          : %s\n"
        "Target         : s3://%s\n"
        "Mode           : %s\n"
        "Run at         : %s\n%s",
        sep, sep, cfg.endpoint, args.index, vn30_path, mode, run_at, sep,
    )

    fs = build_s3fs(cfg)
    ref = Reference()

    logger.info("Fetching %s index members...", args.index)
    try:
        df_vn30 = _coerce_members_df(ref.index.members(args.index))
        if df_vn30.empty:
            logger.warning("Empty %s members DataFrame — skipping write.", args.index)
            return

        pl_df = pl.from_pandas(df_vn30)
        gx_check_columns_to_match_set(
            pl_df,
            {"column_set": list(VN30_EXPECTED_COLUMNS), "exact_match": True},
        )

        if args.append:
            append_parquet_to_s3(
                df_vn30,
                fs,
                vn30_path,
                primary_keys=DEDUP_PRIMARY,
                fallback_keys=DEDUP_FALLBACK,
                logger=logger,
            )
        else:
            replace_parquet_on_s3(df_vn30, fs, vn30_path, logger)
        logger.info("  %s %s rows → vn30.parquet", f"{len(df_vn30):,}", args.index)
    except Exception as e:
        logger.error("Error fetching %s members: %s", args.index, e)

    logger.info("\n%s\nVN30 ingestion complete!\n%s", sep, sep)


if __name__ == "__main__":
    main()
