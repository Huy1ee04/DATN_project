#!/usr/bin/env python3
"""
fact_master_equity.py (master)

Đọc fact_market_equity từ stage_2, resolve surrogate keys từ dimension tables.
Theo Kimball: fact table chứa FK surrogate keys trỏ đến dimensions.

Source (MinIO):
  transformed/stage_2/fact/fact_market_equity.parquet
  master/dimension/dim_master_stock.parquet    → symbol  → stock_key
  master/dimension/dim_master_event.parquet    → trade_date → date_key

Destination (MinIO):
  master/fact/fact_master_equity.parquet

Logic:
  1. Đọc stage_2 fact (đã join + tính indicators)
  2. JOIN với dim_master_stock → symbol → stock_key (Int32)
  3. Tính date_key = year*10000 + month*100 + day (Int32, format yyyymmdd)
  4. Đặt stock_key, date_key làm 2 cột đầu (composite FK)
  5. Bỏ natural keys (symbol, trade_date) — tra ngược qua dim nếu cần.
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
    gx_check_columns_not_null,
    gx_check_compound_columns_unique,
    gx_check_table_row_count_between,
)

_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("fact_master_equity_publish")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_FACT_PATH = "transformed/stage_2/fact/fact_market_equity.parquet"
SRC_DIM_STOCK_PATH = "master/dimension/dim_master_stock.parquet"
DST_PREFIX = "master/fact"
DST_FILENAME = "fact_master_equity.parquet"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    logger.info("Reading s3://%s ...", s3_path)
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    overwrite: bool = False,
) -> None:
    if fs.exists(s3_path) and not overwrite:
        logger.info("Exists, skipping: s3://%s  (dùng --overwrite để ghi đè)", s3_path)
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


# ── Resolve FK ───────────────────────────────────────────────────────────────

def resolve_surrogate_keys(
    df_fact: pl.DataFrame,
    df_dim_stock: pl.DataFrame,
) -> pl.DataFrame:
    """
    Resolve natural keys → surrogate keys:
    - symbol → stock_key  (JOIN dim_master_stock)
    - trade_date → date_key  (tính trực tiếp: yyyymmdd Int32)
    """
    # ── stock_key ──
    if "stock_key" not in df_dim_stock.columns:
        raise ValueError("dim_master_stock thiếu cột 'stock_key'. Chạy dim_master_stock.py trước.")

    stock_lookup = (
        df_dim_stock
        .select(["symbol", "stock_key"])
        .unique(subset=["symbol"])
    )
    df_fact = df_fact.join(stock_lookup, on="symbol", how="left")

    n_orphan = df_fact.filter(pl.col("stock_key").is_null()).shape[0]
    if n_orphan > 0:
        orphan_samples = (
            df_fact.filter(pl.col("stock_key").is_null())
            .select("symbol").unique().to_series().to_list()[:10]
        )
        logger.warning(
            "%d rows have no matching stock_key (orphan symbols: %s) — dropping.",
            n_orphan, orphan_samples,
        )
        df_fact = df_fact.filter(pl.col("stock_key").is_not_null())

    # ── date_key (tính trực tiếp, không cần JOIN dim_master_event) ──
    # Format: yyyymmdd (Int32) — cùng format với dim_master_event.date_key
    df_fact = df_fact.with_columns(
        (
            pl.col("trade_date").dt.year().cast(pl.Int32) * 10000
            + pl.col("trade_date").dt.month().cast(pl.Int32) * 100
            + pl.col("trade_date").dt.day().cast(pl.Int32)
        ).alias("date_key")
    )

    logger.info(
        "FK resolution: %d/%d stock_key resolved, date_key computed for all rows.",
        df_fact.shape[0] - n_orphan, df_fact.shape[0],
    )

    # Sắp xếp cột: FK đầu → measures (bỏ natural keys theo Kimball)
    measure_cols = [
        c for c in df_fact.columns
        if c not in ("stock_key", "date_key", "symbol", "trade_date")
    ]
    output_order = ["stock_key", "date_key", *measure_cols]
    output_order = [c for c in output_order if c in df_fact.columns]

    return df_fact.select(output_order).sort(["stock_key", "date_key"])


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Master: publish fact_market_equity with surrogate keys (stock_key, date_key)."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file master nếu đã tồn tại")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    fact_path = f"{bucket}/{SRC_FACT_PATH}"
    stock_path = f"{bucket}/{SRC_DIM_STOCK_PATH}"
    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nMaster: fact_market_equity → fact_master_equity (resolve FK)\n%s\n"
        "MinIO Endpoint  : %s\n"
        "Fact source     : s3://%s\n"
        "Dim stock       : s3://%s\n"
        "Destination     : s3://%s\n"
        "Overwrite       : %s\n"
        "Run at          : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        fact_path, stock_path, dst_path,
        args.overwrite, run_at, separator,
    )

    fs = _build_fs()

    # Validate sources
    for label, path in [
        ("Fact (stage_2)", fact_path),
        ("Dim stock (master)", stock_path),
    ]:
        if not fs.exists(path):
            logger.error("%s not found: s3://%s — run upstream scripts first.", label, path)
            return

    df_fact = read_parquet(fs, fact_path)
    df_dim_stock = read_parquet(fs, stock_path)

    if df_fact.is_empty():
        logger.error("Fact source is empty — aborting.")
        return

    # Resolve FK
    df_result = resolve_surrogate_keys(df_fact, df_dim_stock)

    logger.info("Final: %s rows × %s cols", f"{df_result.shape[0]:,}", df_result.shape[1])
    logger.info("Schema: %s", df_result.schema)
    logger.info("Sample:\n%s", df_result.head(3))

    # ── GX Gate: Referential Integrity ───────────────────────────────
    logger.info("Running GX validation (Master: FK integrity + PK uniqueness)...")
    gx_check_columns_not_null(df_result, {"columns": ["stock_key", "date_key"]})
    gx_check_compound_columns_unique(df_result, {"column_list": ["stock_key", "date_key"]})
    gx_check_table_row_count_between(df_result, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df_result, fs, dst_path, overwrite=args.overwrite)
    logger.info("\n%s\nfact_master_equity publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
