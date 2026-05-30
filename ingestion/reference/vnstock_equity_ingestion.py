#!/usr/bin/env python3
"""
vnstock_equity_ingestion.py

Fetch danh sách cổ phiếu HOSE/STOCK và ghi MinIO:

  s3://…/raw/reference/equity/equity.parquet

Mặc định (replace snapshot):
  Mỗi lần chạy ghi đè toàn bộ file (atomic .tmp → thay object).
  DataFrame rỗng sau filter/API lỗi → không ghi (giữ file cũ).

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

logger = get_logger("equity_ingestion")
ICT = timezone(timedelta(hours=7))

DEDUP_PRIMARY = ("symbol",)
DEDUP_FALLBACK = ("symbol", "exchange")
EQUITY_EXPECTED_COLUMNS = (
    "symbol",
    "exchange",
    "type",
    "sid",
    "organ_short_name",
    "organ_name",
    "product_grp_id",
    "icb_code2",
)


def _filter_equity_hose_stock(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    need = {"exchange", "type"}
    if not need.issubset(df.columns):
        logger.error("list_by_exchange() thiếu cột exchange/type: %s", list(df.columns))
        return pd.DataFrame()
    mask = (
        df["exchange"].astype(str).str.upper().eq("HOSE")
        & df["type"].astype(str).str.upper().eq("STOCK")
    )
    return df.loc[mask].reset_index(drop=True)


def main() -> None:
    cfg = load_minio_config()
    announce_vnstock_key(logger)

    parser = ArgumentParser(description="Fetch VNStock Equity list (HOSE/STOCK) to MinIO S3.")
    parser.add_argument("--bucket", default=cfg.default_bucket, help="MinIO bucket name")
    parser.add_argument("--prefix", default=cfg.default_prefix, help="Path prefix inside bucket")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Merge với file cũ + dedup theo symbol (thay vì ghi đè snapshot)",
    )
    args = parser.parse_args()

    prefix = f"{args.bucket}/{args.prefix}"
    equity_path = f"{prefix}/equity/equity.parquet"
    mode = "append (merge)" if args.append else "replace (ghi đè snapshot)"
    sep = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %z")
    logger.info(
        "\n%s\nFetch VNStock Equity list to MinIO S3\n%s\n"
        "MinIO Endpoint : %s\n"
        "Target         : s3://%s\n"
        "Mode           : %s\n"
        "Run at         : %s\n%s",
        sep, sep, cfg.endpoint, equity_path, mode, run_at, sep,
    )

    fs = build_s3fs(cfg)
    ref = Reference()

    logger.info("Fetching equity list (HOSE, STOCK)...")
    try:
        df_equity = _filter_equity_hose_stock(ref.equity.list_by_exchange())
        if df_equity.empty:
            logger.warning("Empty equity DataFrame after filter — skipping write.")
            return

        gx_check_columns_to_match_set(
            pl.from_pandas(df_equity),
            {"column_set": list(EQUITY_EXPECTED_COLUMNS), "exact_match": True},
        )

        if args.append:
            append_parquet_to_s3(
                df_equity,
                fs,
                equity_path,
                primary_keys=DEDUP_PRIMARY,
                fallback_keys=DEDUP_FALLBACK,
                logger=logger,
            )
        else:
            replace_parquet_on_s3(df_equity, fs, equity_path, logger)
        logger.info("  %s HOSE STOCK rows → equity.parquet", f"{len(df_equity):,}")
    except Exception as e:
        logger.error("Error fetching equity: %s", e)

    logger.info("\n%s\nEquity ingestion complete!\n%s", sep, sep)


if __name__ == "__main__":
    main()
