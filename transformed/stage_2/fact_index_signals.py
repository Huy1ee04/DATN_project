#!/usr/bin/env python3
"""
fact_index_signals.py (stage_2)

Gán nhãn tín hiệu thị trường cho dữ liệu index, tách riêng khỏi fact_market_index
để nhất quán với pattern equity (fact_stock_signals tách khỏi fact_market_equity).

Source (MinIO):
  transformed/stage_2/fact/fact_market_index.parquet

Destination (MinIO):
  transformed/stage_2/fact/fact_index_signals.parquet

Logic:
  signal_market_trend : Golden/Death cross (SMA_20 vs SMA_50)
  signal_market_rsi   : Quá mua / Quá bán / Trung tính
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
logger = logging.getLogger("fact_index_signals_stage2")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_FACT_PATH = "transformed/stage_2/fact/fact_market_index.parquet"
DST_PREFIX = "transformed/stage_2/fact"
DST_FILENAME = "fact_index_signals.parquet"

OUTPUT_COLUMNS = [
    "symbol", "trade_date",
    "signal_market_trend", "signal_market_rsi",
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


def write_parquet(df: pl.DataFrame, fs: s3fs.S3FileSystem, s3_path: str) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


# ── Signal Labeling ─────────────────────────────────────────────────────────

def label_market_trend(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tín hiệu xu hướng thị trường: SMA_20 vs SMA_50 (Golden/Death cross).
    """
    has_sma = pl.col("sma_20").is_not_null() & pl.col("sma_50").is_not_null()
    above_sma20 = pl.col("close") > pl.col("sma_20")
    sma20_above_sma50 = pl.col("sma_20") > pl.col("sma_50")

    return df.with_columns(
        pl.when(~has_sma).then(pl.lit(None).cast(pl.Utf8))
        .when(above_sma20 & sma20_above_sma50).then(pl.lit("Tăng mạnh"))
        .when(above_sma20 & ~sma20_above_sma50).then(pl.lit("Tăng nhẹ"))
        .when(~above_sma20 & sma20_above_sma50).then(pl.lit("Giảm nhẹ"))
        .otherwise(pl.lit("Giảm mạnh"))
        .alias("signal_market_trend")
    )


def label_market_rsi(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tín hiệu RSI thị trường: xác định thị trường chung quá mua/quá bán.
    """
    return df.with_columns(
        pl.when(pl.col("rsi_14").is_null()).then(pl.lit(None).cast(pl.Utf8))
        .when(pl.col("rsi_14") <= 30).then(pl.lit("Quá bán"))
        .when(pl.col("rsi_14") >= 70).then(pl.lit("Quá mua"))
        .otherwise(pl.lit("Trung tính"))
        .alias("signal_market_rsi")
    )


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 2: Generate index signal labels from fact_market_index."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    fact_path = f"{bucket}/{SRC_FACT_PATH}"
    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 2: Generate Index Signal Labels\n%s\n"
        "MinIO Endpoint : %s\n"
        "Fact source    : s3://%s\n"
        "Destination    : s3://%s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        fact_path, dst_path,
        run_at, separator,
    )

    fs = _build_fs()

    if not fs.exists(fact_path):
        logger.error("Fact source not found: s3://%s", fact_path)
        return

    df = read_parquet(fs, fact_path)
    if df.is_empty():
        logger.error("Fact source is empty — aborting.")
        return

    # Sort trước khi label (cần cho shift operations nếu mở rộng)
    df = df.sort(["symbol", "trade_date"])

    # Label signals
    df = label_market_trend(df)
    df = label_market_rsi(df)

    # Select output
    final_cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    df = df.select(final_cols).sort(["symbol", "trade_date"])

    logger.info("Final: %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    logger.info("Schema: %s", df.schema)
    logger.info("Sample:\n%s", df.head(3))

    # Log phân bố nhãn
    for col in ("signal_market_trend", "signal_market_rsi"):
        if col in df.columns:
            vc = df[col].value_counts().sort("count", descending=True)
            logger.info("Distribution [%s]:\n%s", col, vc)

    # ── GX Gate ──────────────────────────────────────────────────────────
    logger.info("Running GX validation...")
    gx_check_compound_columns_unique(df, {"column_list": ["symbol", "trade_date"]})
    gx_check_columns_not_null(df, {"columns": ["symbol", "trade_date"]})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # Write (luôn overwrite)
    write_parquet(df, fs, dst_path)

    logger.info("\n%s\nfact_index_signals stage_2 complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
