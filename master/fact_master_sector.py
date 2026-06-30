#!/usr/bin/env python3
"""
fact_master_sector.py (master)

Aggregate fact_market_equity theo sector, tạo fact_market_sector.

Sources (MinIO):
  master/fact/fact_master_equity.parquet       (stock_key, date_key, close, total_volume, market_cap, pe, pb, eps, price_change_pct)
  master/dimension/dim_master_stock.parquet    (stock_key → sector)
  master/dimension/dim_master_sector.parquet   (sector → sector_key)

Destination (MinIO):
  master/fact/fact_master_sector.parquet

Logic:
  1. JOIN equity + dim_stock → lấy sector per stock.
  2. JOIN sector name → sector_key.
  3. GROUP BY (sector_key, date_key):
     - price_change_pct: market-cap weighted average
     - total_trade_value: Σ(close × total_volume)
     - total_market_cap: Σ(market_cap)
     - avg_pe: harmonic weighted = Σ(market_cap) / Σ(market_cap / pe)
     - avg_pb: harmonic weighted = Σ(market_cap) / Σ(market_cap / pb)
     - avg_eps: simple average
     - stock_count: count distinct stocks
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
logger = logging.getLogger("fact_master_sector")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_EQUITY_PATH = "master/fact/fact_master_equity.parquet"
SRC_DIM_STOCK_PATH = "master/dimension/dim_master_stock.parquet"
SRC_DIM_SECTOR_PATH = "master/dimension/dim_master_sector.parquet"
DST_PREFIX = "master/fact"
DST_FILENAME = "fact_master_sector.parquet"

OUTPUT_COLUMNS = [
    "sector_key", "date_key",
    "price_change_pct", "total_trade_value", "total_market_cap",
    "avg_pe", "avg_pb", "avg_eps",
    "stock_count",
]


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


# ── Aggregate Logic ─────────────────────────────────────────────────────────

def aggregate_sector(
    df_equity: pl.DataFrame,
    df_dim_stock: pl.DataFrame,
    df_dim_sector: pl.DataFrame,
) -> pl.DataFrame:
    """
    JOIN equity → dim_stock (sector) → dim_sector (sector_key),
    then GROUP BY (sector_key, date_key) to compute aggregate metrics.
    """

    # 1. Get stock_key → sector mapping from dim_stock
    stock_sector = df_dim_stock.select(["stock_key", "sector"]).filter(
        pl.col("sector").is_not_null() & (pl.col("sector") != "")
    )
    logger.info("Stocks with sector: %s", f"{stock_sector.shape[0]:,}")

    # 2. Get sector → sector_key mapping
    sector_lookup = df_dim_sector.select(["sector_key", "sector"])

    # 3. JOIN equity → stock_sector → sector_key
    df = df_equity.join(stock_sector, on="stock_key", how="inner")
    logger.info("After JOIN dim_stock: %s rows (only stocks with sector)", f"{df.shape[0]:,}")

    df = df.join(sector_lookup, on="sector", how="inner")
    logger.info("After JOIN dim_sector: %s rows", f"{df.shape[0]:,}")

    # 4. Precompute helper columns for weighted aggregation
    df = df.with_columns([
        # trade_value = close × total_volume
        (pl.col("close").fill_null(0) * pl.col("total_volume").fill_null(0)).alias("_trade_value"),
        # market_cap (already exists, fill null for safety)
        pl.col("market_cap").fill_null(0).alias("_mcap"),
        # weighted price_change = price_change_pct × market_cap
        (pl.col("price_change_pct").fill_null(0) * pl.col("market_cap").fill_null(0)).alias("_weighted_pcp"),
        # For harmonic P/E: market_cap / pe (only where pe > 0)
        pl.when((pl.col("pe").is_not_null()) & (pl.col("pe") > 0))
        .then(pl.col("market_cap").fill_null(0) / pl.col("pe"))
        .otherwise(pl.lit(None))
        .alias("_mcap_div_pe"),
        # For harmonic P/B: market_cap / pb (only where pb > 0)
        pl.when((pl.col("pb").is_not_null()) & (pl.col("pb") > 0))
        .then(pl.col("market_cap").fill_null(0) / pl.col("pb"))
        .otherwise(pl.lit(None))
        .alias("_mcap_div_pb"),
        # For P/E weighted: market_cap where pe > 0
        pl.when((pl.col("pe").is_not_null()) & (pl.col("pe") > 0))
        .then(pl.col("market_cap").fill_null(0))
        .otherwise(pl.lit(None))
        .alias("_mcap_pe_valid"),
        # For P/B weighted: market_cap where pb > 0
        pl.when((pl.col("pb").is_not_null()) & (pl.col("pb") > 0))
        .then(pl.col("market_cap").fill_null(0))
        .otherwise(pl.lit(None))
        .alias("_mcap_pb_valid"),
    ])

    # 5. GROUP BY (sector_key, date_key)
    df_agg = df.group_by(["sector_key", "date_key"]).agg([
        # price_change_pct: weighted average by market_cap
        (pl.col("_weighted_pcp").sum() / pl.col("_mcap").sum()).alias("price_change_pct"),
        # total_trade_value
        pl.col("_trade_value").sum().alias("total_trade_value"),
        # total_market_cap
        pl.col("_mcap").sum().alias("total_market_cap"),
        # avg_pe: harmonic weighted = Σ(mcap_pe_valid) / Σ(mcap/pe)
        (pl.col("_mcap_pe_valid").sum() / pl.col("_mcap_div_pe").sum()).alias("avg_pe"),
        # avg_pb: harmonic weighted = Σ(mcap_pb_valid) / Σ(mcap/pb)
        (pl.col("_mcap_pb_valid").sum() / pl.col("_mcap_div_pb").sum()).alias("avg_pb"),
        # avg_eps: simple average (exclude nulls)
        pl.col("eps").mean().alias("avg_eps"),
        # stock_count
        pl.col("stock_key").n_unique().cast(pl.Int32).alias("stock_count"),
    ])

    logger.info("Aggregated: %s rows (sector × date)", f"{df_agg.shape[0]:,}")

    # 6. Select and sort
    final_cols = [c for c in OUTPUT_COLUMNS if c in df_agg.columns]
    df_agg = df_agg.select(final_cols).sort(["sector_key", "date_key"])

    return df_agg


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Master: aggregate equity data → fact_market_sector."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file output")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    equity_path = f"{bucket}/{SRC_EQUITY_PATH}"
    stock_path = f"{bucket}/{SRC_DIM_STOCK_PATH}"
    sector_path = f"{bucket}/{SRC_DIM_SECTOR_PATH}"
    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nMaster: fact_market_sector (aggregate equity → sector)\n%s\n"
        "MinIO Endpoint : %s\n"
        "Equity source  : s3://%s\n"
        "Stock dim      : s3://%s\n"
        "Sector dim     : s3://%s\n"
        "Destination    : s3://%s\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        equity_path, stock_path, sector_path, dst_path,
        args.overwrite, run_at, separator,
    )

    fs = _build_fs()

    # Check sources
    for label, path in [
        ("Equity (master)", equity_path),
        ("Dim stock (master)", stock_path),
        ("Dim sector (master)", sector_path),
    ]:
        if not fs.exists(path):
            logger.error("%s not found: s3://%s — run upstream scripts first.", label, path)
            return

    df_equity = read_parquet(fs, equity_path)
    df_dim_stock = read_parquet(fs, stock_path)
    df_dim_sector = read_parquet(fs, sector_path)

    if df_equity.is_empty():
        logger.error("Equity source is empty — aborting.")
        return

    # Aggregate
    df_result = aggregate_sector(df_equity, df_dim_stock, df_dim_sector)

    logger.info("Final: %s rows × %s cols", f"{df_result.shape[0]:,}", df_result.shape[1])
    logger.info("Schema: %s", df_result.schema)
    logger.info("Sample:\n%s", df_result.head(5))

    # ── GX Gate ──────────────────────────────────────────────────────────
    logger.info("Running GX validation...")
    gx_check_columns_not_null(df_result, {"columns": ["sector_key", "date_key"]})
    gx_check_compound_columns_unique(df_result, {"column_list": ["sector_key", "date_key"]})
    gx_check_table_row_count_between(df_result, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # Write
    write_parquet(df_result, fs, dst_path, overwrite=args.overwrite)
    logger.info("\n%s\nfact_master_sector publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
