#!/usr/bin/env python3
"""
fact_market_index.py (stage_2)

Tính technical indicators cho dữ liệu index daily.
Tín hiệu thị trường (signals) được tách sang fact_index_signals.py.

Source (MinIO):
  transformed/stage_1/fact/fact_market_index.parquet

Destination (MinIO):
  transformed/stage_2/fact/fact_market_index.parquet

Logic:
  1. Đọc OHLCV daily từ stage_1.
  2. Tính technical indicators per symbol (SMA, EMA, RSI, MACD).
  3. Ghi file (luôn rebuild — cần full history cho indicators).
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
    gx_check_column_a_greater_than_b,
    gx_check_compound_columns_unique,
    gx_check_columns_not_null,
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
logger = logging.getLogger("fact_market_index_stage2")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH = "transformed/stage_1/fact/fact_market_index.parquet"
DST_PREFIX = "transformed/stage_2/fact"
DST_FILENAME = "fact_market_index.parquet"

OUTPUT_COLUMNS = [
    "symbol", "trade_date",
    "open", "high", "low", "close", "total_volume",
    "price_change_pct",
    "sma_20", "sma_50", "ema_12", "ema_26",
    "rsi_14", "macd",
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


# ── Technical Indicators ─────────────────────────────────────────────────────

def compute_rsi(df: pl.DataFrame, period: int = 14) -> pl.DataFrame:
    """RSI = 100 - 100 / (1 + RS), Wilder's smoothing."""
    col_name = f"rsi_{period}"

    df = df.with_columns(
        (pl.col("close") - pl.col("close").shift(1).over("symbol")).alias("_delta")
    )
    df = df.with_columns(
        pl.col("_delta").clip(lower_bound=0).alias("_gain"),
        (-pl.col("_delta")).clip(lower_bound=0).alias("_loss"),
    )
    df = df.with_columns(
        pl.col("_gain").ewm_mean(com=period - 1, adjust=False).over("symbol").alias("_avg_gain"),
        pl.col("_loss").ewm_mean(com=period - 1, adjust=False).over("symbol").alias("_avg_loss"),
    )
    df = df.with_columns(
        pl.when(pl.col("_avg_loss") == 0)
        .then(pl.lit(100.0))
        .otherwise(100.0 - 100.0 / (1.0 + pl.col("_avg_gain") / pl.col("_avg_loss")))
        .alias(col_name)
    )
    df = df.drop(["_delta", "_gain", "_loss", "_avg_gain", "_avg_loss"])
    return df


def compute_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """Tính tất cả technical indicators per symbol."""
    df = df.sort(["symbol", "trade_date"])

    # Rename volume → total_volume
    if "volume" in df.columns:
        df = df.rename({"volume": "total_volume"})

    # price_change_pct
    df = df.with_columns(
        (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1)
        .alias("price_change_pct")
    )

    # SMA
    df = df.with_columns(
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
    )

    # EMA
    df = df.with_columns(
        pl.col("close").ewm_mean(span=12, adjust=False).over("symbol").alias("ema_12"),
        pl.col("close").ewm_mean(span=26, adjust=False).over("symbol").alias("ema_26"),
    )

    # MACD
    df = df.with_columns(
        (pl.col("ema_12") - pl.col("ema_26")).alias("macd")
    )

    # RSI
    df = compute_rsi(df, period=14)

    return df




# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 2: Compute technical indicators + market signals for index."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src_path = f"{bucket}/{SRC_PATH}"
    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 2: Compute indicators + signals for index\n%s\n"
        "MinIO Endpoint : %s\n"
        "Source         : s3://%s\n"
        "Destination    : s3://%s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        src_path, dst_path,
        run_at, separator,
    )

    fs = _build_fs()

    if not fs.exists(src_path):
        logger.error("Source not found: s3://%s", src_path)
        return

    df = read_parquet(fs, src_path)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    # 1. Compute technical indicators
    df = compute_indicators(df)

    # 2. Select output columns (signals tách sang fact_index_signals)
    final_cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    df = df.select(final_cols).sort(["symbol", "trade_date"])

    logger.info("Final: %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    logger.info("Schema: %s", df.schema)
    logger.info("Sample:\n%s", df.head(3))

    # ── GX Gate ──────────────────────────────────────────────────────────
    logger.info("Running GX validation...")
    gx_check_column_a_greater_than_b(df, {"column_a": "high", "column_b": "low", "or_equal": True})
    gx_check_compound_columns_unique(df, {"column_list": ["symbol", "trade_date"]})
    gx_check_columns_not_null(df, {"columns": ["symbol", "trade_date", "close"]})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # 4. Write (luôn overwrite — indicators cần full history)
    write_parquet(df, fs, dst_path)

    logger.info("\n%s\nfact_market_index stage_2 complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
