#!/usr/bin/env python3
"""
fact_stock_signals.py (stage_2)

Gán nhãn tín hiệu cho cổ phiếu dựa trên dữ liệu fact_market_equity.
Tạo bảng fact riêng `fact_stock_signals` cùng grain (symbol, trade_date).

Source (MinIO):
  transformed/stage_2/fact/fact_market_equity.parquet
  transformed/stage_2/dimension/dim_stock_info.parquet         → lấy sector (cho P/E ngành)

Destination (MinIO):
  transformed/stage_2/fact/fact_stock_signals.parquet

Logic:
  TẦNG 1 — Nhãn đơn lẻ (8 nhãn):
    signal_rsi, signal_trend, signal_macd, signal_dividend,
    signal_roe, signal_pe, signal_pb, signal_price_pos.

  TẦNG 2 — Nhãn tổng hợp (2 nhãn):
    label_stock_class:    Ma trận 3×3 (ROE × P/E)
    label_trading_action: RSI + MACD + SMA
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
logger = logging.getLogger("fact_stock_signals_stage2")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_FACT_PATH = "transformed/stage_2/fact/fact_market_equity.parquet"
SRC_DIM_STOCK_PATH = "transformed/stage_2/dimension/dim_stock_info.parquet"
DST_PREFIX = "transformed/stage_2/fact"
DST_FILENAME = "fact_stock_signals.parquet"

OUTPUT_COLUMNS = [
    "symbol", "trade_date",
    # Tầng 1: Nhãn đơn lẻ
    "signal_rsi", "signal_trend", "signal_macd", "signal_dividend",
    "signal_roe", "signal_pe", "signal_pb", "signal_price_pos",
    # Tầng 2: Nhãn tổng hợp
    "label_stock_class", "label_trading_action",
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


# ── TẦNG 1: Nhãn đơn lẻ ─────────────────────────────────────────────────────

def label_rsi(df: pl.DataFrame) -> pl.DataFrame:
    """RSI: Quá bán / Trung tính / Quá mua."""
    return df.with_columns(
        pl.when(pl.col("rsi_14").is_null()).then(pl.lit(None).cast(pl.Utf8))
        .when(pl.col("rsi_14") <= 30).then(pl.lit("Quá bán"))
        .when(pl.col("rsi_14") >= 70).then(pl.lit("Quá mua"))
        .otherwise(pl.lit("Trung tính"))
        .alias("signal_rsi")
    )


def label_trend(df: pl.DataFrame) -> pl.DataFrame:
    """Xu hướng giá: close vs SMA_20 vs SMA_50."""
    above_sma20 = pl.col("close") > pl.col("sma_20")
    sma20_above_sma50 = pl.col("sma_20") > pl.col("sma_50")
    has_sma = pl.col("sma_20").is_not_null() & pl.col("sma_50").is_not_null()

    return df.with_columns(
        pl.when(~has_sma).then(pl.lit(None).cast(pl.Utf8))
        .when(above_sma20 & sma20_above_sma50).then(pl.lit("Tăng mạnh"))
        .when(above_sma20 & ~sma20_above_sma50).then(pl.lit("Tăng nhẹ"))
        .when(~above_sma20 & sma20_above_sma50).then(pl.lit("Giảm nhẹ"))
        .otherwise(pl.lit("Giảm mạnh"))
        .alias("signal_trend")
    )


def label_macd(df: pl.DataFrame) -> pl.DataFrame:
    """MACD momentum: Tăng / Giảm / Trung tính."""
    # MACD(t-1) per symbol
    macd_prev = pl.col("macd").shift(1).over("symbol")

    return df.with_columns(
        pl.when(pl.col("macd").is_null()).then(pl.lit(None).cast(pl.Utf8))
        .when((pl.col("macd") > 0) & (pl.col("macd") > macd_prev)).then(pl.lit("Tăng"))
        .when((pl.col("macd") < 0) & (pl.col("macd") < macd_prev)).then(pl.lit("Giảm"))
        .otherwise(pl.lit("Trung tính"))
        .alias("signal_macd")
    )


def label_dividend(df: pl.DataFrame) -> pl.DataFrame:
    """Tỷ suất cổ tức: Hấp dẫn / Khá / Thấp / Không có."""
    dy = pl.col("dividend_yield")
    return df.with_columns(
        pl.when(dy.is_null() | (dy <= 0)).then(pl.lit("Không có"))
        .when(dy > 8).then(pl.lit("Hấp dẫn"))
        .when(dy >= 4).then(pl.lit("Khá"))
        .otherwise(pl.lit("Thấp"))
        .alias("signal_dividend")
    )


def label_roe(df: pl.DataFrame) -> pl.DataFrame:
    """Chất lượng ROE: Xuất sắc / Tốt / Trung bình / Yếu."""
    roe = pl.col("roe")
    return df.with_columns(
        pl.when(roe.is_null()).then(pl.lit(None).cast(pl.Utf8))
        .when(roe >= 20).then(pl.lit("Xuất sắc"))
        .when(roe >= 15).then(pl.lit("Tốt"))
        .when(roe >= 10).then(pl.lit("Trung bình"))
        .otherwise(pl.lit("Yếu"))
        .alias("signal_roe")
    )


def label_pe(df: pl.DataFrame) -> pl.DataFrame:
    """
    Định giá P/E so với median ngành.
    Cần cột `pe` và `pe_median_sector` đã được tính trước.
    """
    pe = pl.col("pe")
    pe_med = pl.col("pe_median_sector")
    ratio = pe / pe_med

    return df.with_columns(
        pl.when(pe.is_null() | (pe <= 0) | pe_med.is_null() | (pe_med <= 0))
        .then(pl.lit("N/A"))
        .when(ratio < 0.85).then(pl.lit("Rẻ"))
        .when(ratio <= 1.15).then(pl.lit("Hợp lý"))
        .otherwise(pl.lit("Đắt"))
        .alias("signal_pe")
    )


def label_pb(df: pl.DataFrame) -> pl.DataFrame:
    """Định giá P/B tuyệt đối."""
    pb = pl.col("pb")
    return df.with_columns(
        pl.when(pb.is_null() | (pb <= 0)).then(pl.lit("N/A"))
        .when(pb < 1.0).then(pl.lit("Rẻ"))
        .when(pb <= 3.0).then(pl.lit("Hợp lý"))
        .otherwise(pl.lit("Đắt"))
        .alias("signal_pb")
    )


def label_price_position(df: pl.DataFrame) -> pl.DataFrame:
    """Vị trí giá trong biên độ 52 tuần."""
    h52 = pl.col("high_52w")
    l52 = pl.col("low_52w")
    rng = h52 - l52
    pos = (pl.col("close") - l52) / rng  # ratio 0..1

    return df.with_columns(
        pl.when(h52.is_null() | l52.is_null() | (rng <= 0))
        .then(pl.lit(None).cast(pl.Utf8))
        .when(pos >= 0.8).then(pl.lit("Gần đỉnh"))
        .when(pos <= 0.2).then(pl.lit("Gần đáy"))
        .otherwise(pl.lit("Trung bình"))
        .alias("signal_price_pos")
    )


# ── TẦNG 2: Nhãn tổng hợp ───────────────────────────────────────────────────

def label_stock_class(df: pl.DataFrame) -> pl.DataFrame:
    """
    Ma trận 3×3: Chất lượng (ROE) × Định giá (P/E).
    """
    roe_good = pl.col("signal_roe").is_in(["Xuất sắc", "Tốt"])
    roe_mid = pl.col("signal_roe") == "Trung bình"
    # roe_bad = everything else

    pe_cheap = pl.col("signal_pe") == "Rẻ"
    pe_fair = pl.col("signal_pe") == "Hợp lý"
    pe_exp = pl.col("signal_pe") == "Đắt"

    return df.with_columns(
        pl.when(pl.col("signal_roe").is_null() | (pl.col("signal_pe") == "N/A"))
        .then(pl.lit(None).cast(pl.Utf8))
        # ROE tốt
        .when(roe_good & pe_cheap).then(pl.lit("Hàng hiệu giá hời"))
        .when(roe_good & pe_fair).then(pl.lit("Tăng trưởng bền vững"))
        .when(roe_good & pe_exp).then(pl.lit("Tăng trưởng nóng"))
        # ROE trung bình
        .when(roe_mid & pe_cheap).then(pl.lit("Đầu tư an toàn"))
        .when(roe_mid & pe_fair).then(pl.lit("Theo dõi thêm"))
        .when(roe_mid & pe_exp).then(pl.lit("Bị định giá cao"))
        # ROE yếu
        .when(pe_cheap).then(pl.lit("Bẫy định giá"))
        .when(pe_fair).then(pl.lit("Cổ phiếu yếu"))
        .when(pe_exp).then(pl.lit("Rủi ro cao"))
        .otherwise(pl.lit(None).cast(pl.Utf8))
        .alias("label_stock_class")
    )


def label_trading_action(df: pl.DataFrame) -> pl.DataFrame:
    """
    Tín hiệu giao dịch: RSI + MACD + Trend → Mua mạnh / Mua / Nắm giữ / Bán / Bán mạnh.
    """
    rsi_low = pl.col("rsi_14").is_not_null() & (pl.col("rsi_14") <= 40)
    rsi_high = pl.col("rsi_14").is_not_null() & (pl.col("rsi_14") >= 65)
    macd_up = pl.col("signal_macd") == "Tăng"
    macd_down = pl.col("signal_macd") == "Giảm"
    above_sma = pl.col("close") > pl.col("sma_20")
    below_sma = pl.col("close") < pl.col("sma_20")

    # Đếm tín hiệu tích cực / tiêu cực
    buy_signals = rsi_low.cast(pl.Int8) + macd_up.cast(pl.Int8) + above_sma.cast(pl.Int8)
    sell_signals = rsi_high.cast(pl.Int8) + macd_down.cast(pl.Int8) + below_sma.cast(pl.Int8)

    has_data = pl.col("rsi_14").is_not_null() & pl.col("sma_20").is_not_null()

    return df.with_columns(
        pl.when(~has_data).then(pl.lit(None).cast(pl.Utf8))
        .when(buy_signals == 3).then(pl.lit("Mua mạnh"))
        .when(buy_signals >= 2).then(pl.lit("Mua"))
        .when(sell_signals == 3).then(pl.lit("Bán mạnh"))
        .when(sell_signals >= 2).then(pl.lit("Bán"))
        .otherwise(pl.lit("Nắm giữ"))
        .alias("label_trading_action")
    )


# ── Pipeline ─────────────────────────────────────────────────────────────────

def compute_pe_median_sector(
    df_fact: pl.DataFrame,
    df_dim_stock: pl.DataFrame,
) -> pl.DataFrame:
    """
    JOIN với dim_stock_info để lấy sector, sau đó tính MEDIAN(pe) per (sector, trade_date).
    """
    # Lấy sector từ dim_stock_info
    if "sector" not in df_dim_stock.columns:
        logger.warning("dim_stock_info thiếu cột 'sector' — signal_pe sẽ là N/A.")
        return df_fact.with_columns(pl.lit(None).cast(pl.Float64).alias("pe_median_sector"))

    sector_lookup = (
        df_dim_stock
        .select(["symbol", "sector"])
        .unique(subset=["symbol"])
        .filter(pl.col("sector").is_not_null() & (pl.col("sector") != ""))
    )

    df = df_fact.join(sector_lookup, on="symbol", how="left")

    # MEDIAN(pe) per (sector, trade_date) — window function
    df = df.with_columns(
        pl.col("pe")
        .median()
        .over(["sector", "trade_date"])
        .alias("pe_median_sector")
    )

    n_with_sector = df.filter(pl.col("sector").is_not_null()).shape[0]
    n_sectors = df.filter(pl.col("sector").is_not_null()).select("sector").n_unique()
    logger.info(
        "P/E median ngành: %s/%s rows có sector, %s ngành distinct.",
        f"{n_with_sector:,}", f"{df.shape[0]:,}", n_sectors,
    )

    return df


def build_signals(df: pl.DataFrame) -> pl.DataFrame:
    """Apply tất cả labeling functions theo thứ tự."""
    df = df.sort(["symbol", "trade_date"])

    # Tầng 1: Nhãn đơn lẻ
    df = label_rsi(df)
    df = label_trend(df)
    df = label_macd(df)
    df = label_dividend(df)
    df = label_roe(df)
    df = label_pe(df)
    df = label_pb(df)
    df = label_price_position(df)

    # Tầng 2: Nhãn tổng hợp
    df = label_stock_class(df)
    df = label_trading_action(df)

    return df


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Stage 2: Generate stock signal labels from fact_market_equity."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    fact_path = f"{bucket}/{SRC_FACT_PATH}"
    dim_stock_path = f"{bucket}/{SRC_DIM_STOCK_PATH}"
    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 2: Generate Stock Signal Labels\n%s\n"
        "MinIO Endpoint   : %s\n"
        "Fact source      : s3://%s\n"
        "Dim stock source : s3://%s\n"
        "Destination      : s3://%s\n"
        "Run at           : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        fact_path, dim_stock_path, dst_path,
        run_at, separator,
    )

    fs = _build_fs()

    # 1. Đọc fact_market_equity
    if not fs.exists(fact_path):
        logger.error("Fact source not found: s3://%s", fact_path)
        return

    df_fact = read_parquet(fs, fact_path)
    if df_fact.is_empty():
        logger.error("Fact source is empty — aborting.")
        return

    # 2. Đọc dim_stock_info (cho sector)
    if fs.exists(dim_stock_path):
        df_dim_stock = read_parquet(fs, dim_stock_path)
    else:
        logger.warning("Dim stock not found — P/E ngành sẽ là N/A.")
        df_dim_stock = pl.DataFrame()

    # 3. Tính P/E median ngành
    df = compute_pe_median_sector(df_fact, df_dim_stock)

    # 4. Gán nhãn
    df = build_signals(df)

    # 5. Select output columns (chỉ lấy cột tồn tại)
    final_cols = [c for c in OUTPUT_COLUMNS if c in df.columns]
    df = df.select(final_cols).sort(["symbol", "trade_date"])

    logger.info("Final: %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    logger.info("Schema: %s", df.schema)
    logger.info("Sample:\n%s", df.head(3))

    # Log phân bố nhãn
    for col in [c for c in df.columns if c.startswith("signal_") or c.startswith("label_")]:
        vc = df[col].value_counts().sort("count", descending=True)
        logger.info("Distribution [%s]:\n%s", col, vc)

    # ── GX Gate: Business Logic ─────────────────────────────────────────
    logger.info("Running GX validation (Stage 2: Uniqueness + Completeness)...")
    gx_check_compound_columns_unique(df, {"column_list": ["symbol", "trade_date"]})
    gx_check_columns_not_null(df, {"columns": ["symbol", "trade_date"]})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # 6. Write
    write_parquet(df, fs, dst_path)

    logger.info("\n%s\nfact_stock_signals stage_2 complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
