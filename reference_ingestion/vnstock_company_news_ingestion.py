#!/usr/bin/env python3
"""
vnstock_company_news_ingestion.py

Fetch Reference().company(symbol).news() for each HOSE/STOCK symbol and write to MinIO:
  - default: raw/reference/company/news/news.parquet
  - --append: merge vào file gốc, dedup theo (symbol, id), atomic write, không mất dữ liệu cũ.
  - --append-only: raw/reference/company/news/incremental/news_gte_YYYY-MM-DD.parquet
    (không read/merge file cũ; consumer downstream gộp toàn bộ *.parquet dưới news/)
  - --date YYYY-MM-DD: chỉ lưu bản ghi có public_date >= ngày này.
"""

from __future__ import annotations

import sys

import polars as pl
from vnstock_data import Reference
from vtit_gx.polars.gx_schema_validity import (
    gx_check_column_values_to_be_of_type,
    gx_check_columns_to_match_set,
)

from _company_ingest_common import (
    announce_vnstock_key,
    append_only_parquet_to_s3,
    append_parquet_to_s3,
    build_arg_parser,
    build_s3fs,
    coerce_int_column,
    fetch_exchange_symbols,
    fetch_per_symbol_concat,
    filter_by_date_col,
    get_logger,
    incremental_s3_path,
    load_minio_config,
    log_run_info,
    write_parquet_to_s3,
)

DOMAIN = "news"
FILENAME = "news.parquet"
LABEL = "news"
DATE_COL = "public_date"
DEDUP_KEYS_PRIMARY = ("symbol", "id")
DEDUP_KEYS_FALLBACK = ("symbol", "public_date", "news_title")

logger = get_logger("company_news_ingestion")
NEWS_ID_COL = "news_id"
NEWS_EXPECTED_COLUMNS = (
    "id",
    "news_id",
    "language",
    "news_category_code",
    "icb_code",
    "com_group_code",
    "ticker",
    "news_title",
    "friendly_title",
    "news_sub_title",
    "friendly_sub_title",
    "news_short_content",
    "news_full_content",
    "news_image_url",
    "news_small_image_url",
    "news_source",
    "news_source_link",
    "news_author",
    "news_keyword",
    "friendly_keyword",
    "public_date",
    "symbol",
)


def main() -> None:
    cfg = load_minio_config()
    announce_vnstock_key(logger)

    parser = build_arg_parser(
        description="Fetch VNStock Company News to MinIO S3.",
        default_bucket=cfg.default_bucket,
        default_prefix=cfg.default_prefix,
        date_help=f"Chỉ lưu bản ghi có {DATE_COL} >= ngày này (YYYY-MM-DD, ví dụ 2026-05-14)",
    )
    args = parser.parse_args()
    if args.append and args.append_only:
        parser.error("--append và --append-only không dùng chung; chọn một mode.")

    prefix = f"{args.bucket}/{args.prefix}"
    main_s3_path = f"{prefix}/company/{DOMAIN}/{FILENAME}"
    output_s3_path = (
        incremental_s3_path(prefix=prefix, domain=DOMAIN, label=LABEL, min_date=args.date)
        if args.append_only
        else main_s3_path
    )
    log_run_info(
        title="Fetch VNStock Company News to MinIO S3",
        cfg=cfg, args=args, output_s3_path=output_s3_path,
        date_col=DATE_COL, logger=logger,
    )

    fs = build_s3fs(cfg)
    ref = Reference()

    symbols = fetch_exchange_symbols(
        ref, exchange=args.exchange,
        instrument_type=args.instrument_type, logger=logger,
    )
    if not symbols:
        logger.error("No symbols loaded — aborting.")
        sys.exit(1)

    if not args.append and not args.append_only and fs.exists(main_s3_path):
        logger.info("Exists, skipping: s3://%s  (use --append to merge)", main_s3_path)
        return

    df = fetch_per_symbol_concat(
        symbols=symbols,
        fetch_one=lambda s: ref.company(s).news(),
        method_label="company.news",
        logger=logger,
    )
    if args.date:
        df = filter_by_date_col(df, min_date=args.date, date_col=DATE_COL, logger=logger)

    pl_df = pl.from_pandas(df)
    gx_check_columns_to_match_set(
        pl_df,
        {"column_set": list(NEWS_EXPECTED_COLUMNS), "exact_match": True},
    )
    gx_check_column_values_to_be_of_type(
        pl_df,
        {"column": NEWS_ID_COL, "expected_type": "int64"},
    )
    df = coerce_int_column(df, NEWS_ID_COL, logger=logger, drop_invalid=True)
    if df.empty:
        logger.warning("No valid rows after %s sanitization — aborting.", NEWS_ID_COL)
        sys.exit(1)

    if args.append_only:
        append_only_parquet_to_s3(
            df, fs, output_s3_path,
            primary_keys=DEDUP_KEYS_PRIMARY, fallback_keys=DEDUP_KEYS_FALLBACK,
            logger=logger,
        )
    elif args.append:
        append_parquet_to_s3(
            df, fs, main_s3_path,
            primary_keys=DEDUP_KEYS_PRIMARY, fallback_keys=DEDUP_KEYS_FALLBACK,
            logger=logger,
            transform_df=lambda d: coerce_int_column(
                d, NEWS_ID_COL, logger=logger, drop_invalid=True,
            ),
        )
    else:
        write_parquet_to_s3(df, fs, main_s3_path, logger)

    separator = "=" * 80
    logger.info("\n%s\nCompany news ingestion complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
