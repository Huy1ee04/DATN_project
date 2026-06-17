#!/usr/bin/env python3
"""
fact_market_equity.py (stage_2)

Join OHLCV daily + equity summary từ stage_1, tính technical indicators.

Sources (MinIO):
  transformed/stage_1/fact/fact_market_equity.parquet     (OHLCV daily)
  transformed/stage_1/fact/fact_equity_summary.parquet   (summary metrics)

Destination (MinIO):
  transformed/stage_2/fact/fact_market_equity.parquet

Logic:
  1. LEFT JOIN equity_daily + equity_summary trên (symbol, trade_date)
  2. Rename volume → total_volume
  3. Tính price_change_pct = close[t] / close[t-1] - 1  (per symbol)
  4. Tính technical indicators (native Polars, per symbol):
     - SMA_20, SMA_50:  rolling mean trên close
     - EMA_12, EMA_26:  exponential weighted mean
     - RSI_14:          relative strength index (Wilder's smoothing)
     - MACD:            EMA_12 - EMA_26
     - VWAP:            (high + low + close) / 3  (typical price proxy)
  5. Luôn rebuild toàn bộ (indicators cần full history)
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
    gx_check_column_values_between,
    gx_check_compound_columns_unique,
    gx_check_column_datetime_range,
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
logger = logging.getLogger("fact_market_equity_stage2")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_OHLCV_PATH = "transformed/stage_1/fact/fact_market_equity.parquet"
SRC_SUMMARY_PATH = "transformed/stage_1/fact/fact_equity_summary.parquet"
DST_PREFIX = "transformed/stage_2/fact"
DST_FILENAME = "fact_market_equity.parquet"

SUMMARY_METRICS = (
    "high_52w", "low_52w", "beta", "eps",
    "bvps", "market_cap", "roe", "dividend_yield",
    "pe", "pb",
)

OUTPUT_COLUMNS = [
    "symbol", "trade_date",
    "open", "high", "low", "close", "total_volume",
    "price_change_pct",
    "sma_20", "sma_50", "ema_12", "ema_26",
    "rsi_14", "macd", "vwap",
    *SUMMARY_METRICS,
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


# ── Join ─────────────────────────────────────────────────────────────────────

def join_ohlcv_summary(
    df_ohlcv: pl.DataFrame,
    df_summary: pl.DataFrame,
) -> pl.DataFrame:
    """
    LEFT JOIN equity_daily + equity_summary trên (symbol, trade_date).
    Giữ tất cả OHLCV rows, bổ sung summary metrics nếu match.
    """
    # Chỉ lấy cột cần từ summary (tránh trùng year/month/day)
    summary_cols = ["symbol", "trade_date", *SUMMARY_METRICS]
    actual_summary_cols = [c for c in summary_cols if c in df_summary.columns]
    df_summary_slim = df_summary.select(actual_summary_cols).unique(
        subset=["symbol", "trade_date"], keep="first"
    )

    # Thêm cột summary thiếu (nếu raw data không có)
    for col in SUMMARY_METRICS:
        if col not in df_summary_slim.columns:
            df_summary_slim = df_summary_slim.with_columns(
                pl.lit(None).cast(pl.Float64).alias(col)
            )

    df = df_ohlcv.join(df_summary_slim, on=["symbol", "trade_date"], how="left")

    n_matched = df.filter(pl.col(SUMMARY_METRICS[0]).is_not_null()).shape[0]
    logger.info(
        "JOIN: %s OHLCV rows, %s matched summary (%s%%)",
        f"{df.shape[0]:,}",
        f"{n_matched:,}",
        f"{n_matched * 100 // max(df.shape[0], 1)}",
    )
    return df


# ── Technical Indicators ─────────────────────────────────────────────────────

def compute_indicators(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tính tất cả technical indicators per symbol.
    Data phải sorted by (symbol, trade_date) trước khi gọi.
    """
    df = df.sort(["symbol", "trade_date"])

    # ── price_change_pct ──
    df = df.with_columns(
        (pl.col("close") / pl.col("close").shift(1).over("symbol") - 1)
        .alias("price_change_pct")
    )

    # ── SMA (Simple Moving Average) ──
    df = df.with_columns(
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
    )

    # ── EMA (Exponential Moving Average) ──
    df = df.with_columns(
        pl.col("close").ewm_mean(span=12, adjust=False).over("symbol").alias("ema_12"),
        pl.col("close").ewm_mean(span=26, adjust=False).over("symbol").alias("ema_26"),
    )

    # ── MACD ──
    df = df.with_columns(
        (pl.col("ema_12") - pl.col("ema_26")).alias("macd")
    )

    # ── RSI (Wilder's smoothing) ──
    df = compute_rsi(df, period=14)

    # ── VWAP (typical price proxy cho daily data) ──
    df = df.with_columns(
        ((pl.col("high") + pl.col("low") + pl.col("close")) / 3.0).alias("vwap")
    )

    # ── Rename volume → total_volume ──
    if "volume" in df.columns:
        df = df.rename({"volume": "total_volume"})

    return df


def compute_rsi(df: pl.DataFrame, period: int = 14) -> pl.DataFrame:
    """
    RSI = 100 - 100 / (1 + RS)
    RS  = avg_gain / avg_loss  (Wilder's smoothing: EMA with com = period - 1)
    """
    col_name = f"rsi_{period}"

    # Delta = close[t] - close[t-1]
    df = df.with_columns(
        (pl.col("close") - pl.col("close").shift(1).over("symbol")).alias("_delta")
    )

    # Gain = max(delta, 0),  Loss = max(-delta, 0)
    df = df.with_columns(
        pl.col("_delta").clip(lower_bound=0).alias("_gain"),
        (-pl.col("_delta")).clip(lower_bound=0).alias("_loss"),
    )

    # Wilder's smoothing: EMA with com = period - 1
    df = df.with_columns(
        pl.col("_gain").ewm_mean(com=period - 1, adjust=False).over("symbol").alias("_avg_gain"),
        pl.col("_loss").ewm_mean(com=period - 1, adjust=False).over("symbol").alias("_avg_loss"),
    )

    # RS = avg_gain / avg_loss;  RSI = 100 - 100 / (1 + RS)
    df = df.with_columns(
        pl.when(pl.col("_avg_loss") == 0)
        .then(pl.lit(100.0))
        .otherwise(100.0 - 100.0 / (1.0 + pl.col("_avg_gain") / pl.col("_avg_loss")))
        .alias(col_name)
    )

    # Cleanup temp columns
    df = df.drop(["_delta", "_gain", "_loss", "_avg_gain", "_avg_loss"])
    return df


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 2: Join OHLCV + Summary, compute technical indicators."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    ohlcv_path = f"{bucket}/{SRC_OHLCV_PATH}"
    summary_path = f"{bucket}/{SRC_SUMMARY_PATH}"
    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 2: Join OHLCV + Summary + Technical Indicators\n%s\n"
        "MinIO Endpoint : %s\n"
        "OHLCV source   : s3://%s\n"
        "Summary source : s3://%s\n"
        "Destination    : s3://%s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        ohlcv_path, summary_path, dst_path,
        run_at, separator,
    )

    fs = _build_fs()

    # 1. Đọc sources
    if not fs.exists(ohlcv_path):
        logger.error("OHLCV source not found: s3://%s", ohlcv_path)
        return

    df_ohlcv = read_parquet(fs, ohlcv_path)
    if df_ohlcv.is_empty():
        logger.error("OHLCV source is empty — aborting.")
        return

    # Summary là optional — nếu không có vẫn chạy được (chỉ thiếu summary metrics)
    if fs.exists(summary_path):
        df_summary = read_parquet(fs, summary_path)
    else:
        logger.warning("Summary source not found — proceeding without summary metrics.")
        df_summary = pl.DataFrame()

    # 2. JOIN
    if not df_summary.is_empty():
        df = join_ohlcv_summary(df_ohlcv, df_summary)
    else:
        df = df_ohlcv
        for col in SUMMARY_METRICS:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias(col))

    # 3. Compute indicators
    df = compute_indicators(df)

    # 4. Select output columns (chỉ lấy cột tồn tại)
    final_cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    df = df.select(final_cols).sort(["symbol", "trade_date"])

    logger.info("Final: %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    logger.info("Schema: %s", df.schema)
    logger.info("Sample:\n%s", df.head(3))

    # 5. Write (luôn overwrite — indicators cần full history context)

    # ── GX Gate: Business Logic ─────────────────────────────────────────
    logger.info("Running GX validation (Stage 2: Consistency + Validity + Uniqueness)...")

    # Consistency: OHLC relationship (hard check)
    gx_check_column_a_greater_than_b(df, {"column_a": "high", "column_b": "low", "or_equal": True})

    # Numeric validity: ranges
    gx_check_column_values_between(df, {"column": "close", "min_value": 0, "max_value": 10_000_000, "strict_min": True})
    gx_check_column_values_between(df, {"column": "total_volume", "min_value": 0, "max_value": 1e12})

    # RSI must be in [0, 100]
    df_rsi_valid = df.drop_nulls(["rsi_14"])  # RSI has NaN for first N rows
    if df_rsi_valid.shape[0] > 0:
        gx_check_column_values_between(df_rsi_valid, {"column": "rsi_14", "min_value": 0, "max_value": 100})

    # Uniqueness: composite key
    gx_check_compound_columns_unique(df, {"column_list": ["symbol", "trade_date"]})

    # Completeness: FK columns
    gx_check_columns_not_null(df, {"columns": ["symbol", "trade_date", "close"]})
    gx_check_table_row_count_between(df, {"min_value": 1})

    logger.info("GX validation passed ✓")

    write_parquet(df, fs, dst_path)

    logger.info("\n%s\nfact_market_equity stage_2 complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
