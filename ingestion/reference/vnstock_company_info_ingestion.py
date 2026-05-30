#!/usr/bin/env python3
"""
vnstock_company_info_ingestion.py

Fetch Reference().company(symbol).info() (overview VCI) cho từng mã và ghi MinIO:

  Mặc định — replace snapshot (ghi đè file gốc, dữ liệu cũ được thay toàn bộ):
    raw/reference/company/info/info.parquet

  --append (tuỳ chọn): read → concat với file cũ → dedup theo symbol → atomic write;
    dùng khi cần merge tăng dần thay vì full replace.

company info là 1 dòng / mã (master data) — full replace mỗi lần chạy là hợp lý nhất.
"""

from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
import sys

import polars as pl
from vnstock_data import Reference
from vtit_gx.polars.gx_schema_validity import gx_check_columns_to_match_set

from _company_ingest_common import (
    announce_vnstock_key,
    append_parquet_to_s3,
    build_s3fs,
    fetch_exchange_symbols,
    fetch_per_symbol_concat,
    get_logger,
    load_minio_config,
    replace_parquet_on_s3,
)

DOMAIN = "info"
FILENAME = "info.parquet"
# info(): 1 row/symbol; dedup merge chỉ cần symbol (fallback nếu thiếu id-style field)
DEDUP_KEYS_PRIMARY = ("symbol",)
DEDUP_KEYS_FALLBACK = ("symbol", "issue_share")
INFO_EXPECTED_COLUMNS = (
    "symbol",
    "name",
    "sector",
    "profile",
    "listing_date",
    "issued_share",
)

logger = get_logger("company_info_ingestion")

# Riêng info: gọi API nặng hơn; giữ delay hơi thấp như bản cũ
INFO_PER_REQ_DELAY = 0.33
ICT = timezone(timedelta(hours=7))


def main() -> None:
    cfg = load_minio_config()
    announce_vnstock_key(logger)

    parser = ArgumentParser(description="Fetch VNStock Company Info to MinIO S3.")
    parser.add_argument("--bucket", default=cfg.default_bucket, help="MinIO bucket name")
    parser.add_argument("--prefix", default=cfg.default_prefix, help="Path prefix inside bucket")
    parser.add_argument(
        "--exchange", default="HOSE",
        help="Sàn lọc từ Reference API: HOSE, HNX, UPCOM (default: HOSE)",
    )
    parser.add_argument(
        "--instrument-type", default="STOCK", dest="instrument_type",
        help="Loại CK: STOCK, ETF, … (default: STOCK)",
    )
    parser.add_argument(
        "--append", action="store_true",
        help="Merge với file cũ + dedup theo symbol (thay vì ghi đè snapshot)",
    )
    args = parser.parse_args()

    prefix = f"{args.bucket}/{args.prefix}"
    info_s3_path = f"{prefix}/company/{DOMAIN}/{FILENAME}"
    mode = "append (merge)" if args.append else "replace (ghi đè snapshot)"
    sep = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %z")

    logger.info(
        "\n%s\nFetch VNStock Company Info to MinIO S3\n%s\n"
        "MinIO Endpoint    : %s\n"
        "Symbol source     : Reference API (exchange=%s, type=%s)\n"
        "Target output     : s3://%s\n"
        "Mode              : %s\n"
        "Run at            : %s\n%s",
        sep, sep,
        cfg.endpoint,
        args.exchange,
        args.instrument_type,
        info_s3_path,
        mode,
        run_at,
        sep,
    )

    fs = build_s3fs(cfg)
    ref = Reference()

    symbols = fetch_exchange_symbols(
        ref,
        exchange=args.exchange,
        instrument_type=args.instrument_type,
        logger=logger,
    )
    if not symbols:
        logger.error("No symbols loaded — aborting.")
        sys.exit(1)

    df_company = fetch_per_symbol_concat(
        symbols=symbols,
        fetch_one=lambda s: ref.company(s).info(),
        method_label="company.info",
        logger=logger,
        per_req_delay=INFO_PER_REQ_DELAY,
    )

    if df_company.empty:
        logger.warning("Empty company info DataFrame — aborting.")
        sys.exit(1)

    gx_check_columns_to_match_set(
        pl.from_pandas(df_company),
        {"column_set": list(INFO_EXPECTED_COLUMNS), "exact_match": True},
    )

    if args.append:
        append_parquet_to_s3(
            df_company,
            fs,
            info_s3_path,
            primary_keys=DEDUP_KEYS_PRIMARY,
            fallback_keys=DEDUP_KEYS_FALLBACK,
            logger=logger,
        )
    else:
        replace_parquet_on_s3(df_company, fs, info_s3_path, logger)

    logger.info("\n%s\nCompany info ingestion complete!\n%s", sep, sep)


if __name__ == "__main__":
    main()

