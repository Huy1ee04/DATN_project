#!/usr/bin/env python3
"""
vnstock_index_members_ingestion.py

Fetch index member symbols and write one parquet file per index:

  s3://.../raw/reference/equity/vn30.parquet
  s3://.../raw/reference/equity/vn100.parquet

Default mode replaces each snapshot atomically. Use --append to merge with existing files.
"""

from __future__ import annotations

from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone

import pandas as pd
from vnstock_data import Reference

from _company_ingest_common import (
    announce_vnstock_key,
    append_parquet_to_s3,
    build_s3fs,
    get_logger,
    load_minio_config,
    replace_parquet_on_s3,
)

logger = get_logger("index_members_ingestion")
ICT = timezone(timedelta(hours=7))

DEFAULT_INDICES = ("VN30", "VN100")
DEDUP_PRIMARY = ("index_symbol", "symbol")
DEDUP_FALLBACK = ("symbol",)


def parse_indices(value: str) -> list[str]:
    indices = [item.strip().upper() for item in value.split(",") if item.strip()]
    if not indices:
        raise ValueError("At least one index code is required.")
    return list(dict.fromkeys(indices))


def coerce_members_df(data: pd.DataFrame | pd.Series, index_symbol: str) -> pd.DataFrame:
    if isinstance(data, pd.Series):
        df = data.to_frame(name="symbol").reset_index(drop=True)
    else:
        df = data.copy()

    if "symbol" not in df.columns:
        raise ValueError(f"{index_symbol}: missing required column 'symbol'")

    df = df.dropna(subset=["symbol"]).copy()
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    df = df[df["symbol"] != ""].drop_duplicates(subset=["symbol"], keep="first")
    if "index_symbol" in df.columns:
        df["index_symbol"] = index_symbol
    else:
        df.insert(0, "index_symbol", index_symbol)
    return df.reset_index(drop=True)


def build_target_path(bucket: str, prefix: str, index_symbol: str) -> str:
    return f"{bucket}/{prefix}/equity/{index_symbol.lower()}.parquet"


def main() -> None:
    cfg = load_minio_config()
    announce_vnstock_key(logger)

    parser = ArgumentParser(description="Fetch index member symbols to MinIO S3.")
    parser.add_argument("--bucket", default=cfg.default_bucket, help="MinIO bucket name")
    parser.add_argument("--prefix", default=cfg.default_prefix, help="Path prefix inside bucket")
    parser.add_argument(
        "--indices",
        default=",".join(DEFAULT_INDICES),
        help="Comma-separated index codes, e.g. VN30,VN100",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Merge with existing parquet files instead of replacing snapshots",
    )
    args = parser.parse_args()

    indices = parse_indices(args.indices)
    mode = "append (merge)" if args.append else "replace (snapshot)"
    sep = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %z")
    logger.info(
        "\n%s\nFetch index members to MinIO S3\n%s\n"
        "MinIO Endpoint : %s\n"
        "Indices        : %s\n"
        "Target prefix  : s3://%s/%s/equity/{index}.parquet\n"
        "Mode           : %s\n"
        "Run at         : %s\n%s",
        sep,
        sep,
        cfg.endpoint,
        ", ".join(indices),
        args.bucket,
        args.prefix,
        mode,
        run_at,
        sep,
    )

    fs = build_s3fs(cfg)
    ref = Reference()

    written = 0
    failed: list[str] = []
    for index_symbol in indices:
        target_path = build_target_path(args.bucket, args.prefix, index_symbol)
        logger.info("Fetching %s members...", index_symbol)

        try:
            df_members = coerce_members_df(ref.index.members(index_symbol), index_symbol)
        except Exception as exc:
            logger.error("Error fetching %s members: %s", index_symbol, exc)
            failed.append(index_symbol)
            continue

        if df_members.empty:
            logger.warning("Empty %s members DataFrame — skipping write.", index_symbol)
            failed.append(index_symbol)
            continue

        if args.append:
            append_parquet_to_s3(
                df_members,
                fs,
                target_path,
                primary_keys=DEDUP_PRIMARY,
                fallback_keys=DEDUP_FALLBACK,
                logger=logger,
            )
        else:
            replace_parquet_on_s3(df_members, fs, target_path, logger)

        written += 1
        logger.info(
            "  %s rows → %s.parquet",
            f"{len(df_members):,}",
            index_symbol.lower(),
        )

    logger.info(
        "\n%s\nIndex members ingestion complete: %d/%d file(s) processed.\n%s",
        sep,
        written,
        len(indices),
        sep,
    )
    if failed:
        raise SystemExit(f"Failed index member ingestion for: {', '.join(failed)}")


if __name__ == "__main__":
    main()
