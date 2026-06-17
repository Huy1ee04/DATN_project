#!/usr/bin/env python3
"""
bridge_stock_index.py (stage_1) — Extract + Normalize.

Tạo snapshot hiện tại của quan hệ stock ↔ index từ raw data.
CHỈ extract trạng thái hiện tại, KHÔNG có SCD logic.

Sources (MinIO):
  raw/reference/equity/equity.parquet       → danh sách cổ phiếu hiện tại
  raw/reference/equity/vn30.parquet         → thành viên VN30
  raw/reference/equity/vn100.parquet        → thành viên VN100
  raw/reference/index/info.parquet          → thông tin index (id, symbol, group)

Output (MinIO):
  transformed/stage_1/dimension/bridge_stock_index_snapshot.parquet

Output schema:
  symbol   (Utf8)  — mã chứng khoán
  index_id (Int64) — ID chỉ số

Rules:
  - Chỉ build mapping cho VNINDEX, VN30, VN100.
  - VNINDEX: lấy toàn bộ symbol đang có trong equity.parquet.
  - VN30/VN100: lấy member từ raw/reference/equity/vn30.parquet và vn100.parquet.
  - Chỉ giữ symbol đang có trong equity.parquet hiện tại.
"""

import argparse
import io
import logging
import os
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bridge_stock_index_stage1")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_EQUITY_PATH = "raw/reference/equity/equity.parquet"
SRC_INDEX_INFO_PATH = "raw/reference/index/info.parquet"
SRC_INDEX_MEMBER_PREFIX = "raw/reference/equity"
DST_PREFIX = "transformed/stage_1/dimension"
DST_FILENAME = "bridge_stock_index_snapshot.parquet"

OUTPUT_COLS = ["symbol", "index_id"]
TARGET_INDEX_SYMBOLS = {"VNINDEX", "VN30", "VN100"}
ALL_EQUITY_INDEX_SYMBOL = "VNINDEX"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    logger.info("Reading s3://%s ...", s3_path)
    with fs.open(s3_path, "rb") as file:
        df = pl.read_parquet(io.BytesIO(file.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def try_read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    if not fs.exists(s3_path):
        logger.warning("Optional source not found: s3://%s", s3_path)
        return pl.DataFrame()
    try:
        return read_parquet(fs, s3_path)
    except Exception as exc:
        logger.warning("Cannot read optional source s3://%s: %s", s3_path, exc)
        return pl.DataFrame()


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    overwrite: bool = True,
) -> None:
    if fs.exists(s3_path) and not overwrite:
        logger.info("Exists, skipping: s3://%s  (dùng --overwrite để ghi đè)", s3_path)
        return

    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    with fs.open(s3_path, "wb") as file:
        file.write(buffer.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


# ── Normalize ────────────────────────────────────────────────────────────────

def normalize_symbol_expr(column: str) -> pl.Expr:
    return pl.col(column).cast(pl.Utf8, strict=False).str.strip_chars().str.to_uppercase()


def normalize_equity(df_equity: pl.DataFrame) -> pl.DataFrame:
    required = {"symbol", "exchange"}
    missing = sorted(required - set(df_equity.columns))
    if missing:
        raise ValueError(f"equity source missing columns: {missing}")

    expressions = [
        normalize_symbol_expr("symbol").alias("symbol"),
        normalize_symbol_expr("exchange").alias("exchange"),
    ]
    if "type" in df_equity.columns:
        expressions.append(normalize_symbol_expr("type").alias("type"))

    return (
        df_equity.select(expressions)
        .drop_nulls(["symbol", "exchange"])
        .filter(pl.col("symbol") != "")
        .unique()
    )


def normalize_index_info(df_index: pl.DataFrame) -> pl.DataFrame:
    required = {"index_id", "symbol", "group"}
    missing = sorted(required - set(df_index.columns))
    if missing:
        raise ValueError(f"index info source missing columns: {missing}")

    return (
        df_index.select(
            pl.col("index_id").cast(pl.Int64, strict=False).alias("index_id"),
            normalize_symbol_expr("symbol").alias("index_symbol"),
            normalize_symbol_expr("group").alias("group"),
        )
        .drop_nulls(["index_id", "index_symbol"])
        .filter(pl.col("index_symbol").is_in(TARGET_INDEX_SYMBOLS))
        .unique()
    )


def normalize_member_file(df_members: pl.DataFrame) -> pl.DataFrame:
    if "symbol" not in df_members.columns:
        raise ValueError("index member source missing column: symbol")
    return (
        df_members.select(normalize_symbol_expr("symbol").alias("symbol"))
        .drop_nulls(["symbol"])
        .filter(pl.col("symbol") != "")
        .unique()
    )


# ── Build snapshot ───────────────────────────────────────────────────────────

def build_member_mapping(
    fs: s3fs.S3FileSystem,
    bucket: str,
    df_equity: pl.DataFrame,
    df_index: pl.DataFrame,
) -> pl.DataFrame:
    """
    Build snapshot hiện tại: mỗi cặp (symbol, index_id) đang active.
    KHÔNG có cột SCD (effective_from/to) — đó là việc của stage_2.
    """
    frames: list[pl.DataFrame] = []
    current_symbols = df_equity.select("symbol").unique()

    for row in df_index.iter_rows(named=True):
        index_id = int(row["index_id"])
        index_symbol = str(row["index_symbol"])

        if index_symbol == ALL_EQUITY_INDEX_SYMBOL:
            df_members = current_symbols
            source_label = "all equity symbols"
        else:
            member_path = f"{bucket}/{SRC_INDEX_MEMBER_PREFIX}/{index_symbol.lower()}.parquet"
            df_members_raw = try_read_parquet(fs, member_path)
            if df_members_raw.is_empty():
                logger.warning(
                    "No member source for index %s (index_id=%s); skipped.",
                    index_symbol,
                    index_id,
                )
                continue
            df_members = normalize_member_file(df_members_raw)
            source_label = member_path

        # Inner join với equity hiện tại + gán index_id
        df_members = (
            df_members.join(current_symbols, on="symbol", how="inner")
            .with_columns(
                pl.lit(index_id).cast(pl.Int64).alias("index_id"),
            )
            .select(OUTPUT_COLS)
            .unique()
        )
        logger.info(
            "Index %s (%s): %s symbol(s) from %s",
            index_symbol,
            index_id,
            f"{df_members.shape[0]:,}",
            source_label,
        )
        if not df_members.is_empty():
            frames.append(df_members)

    if not frames:
        return pl.DataFrame(
            schema={
                "symbol": pl.Utf8,
                "index_id": pl.Int64,
            }
        )

    return pl.concat(frames, how="vertical").unique().sort(["index_id", "symbol"])


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 1: Extract snapshot of stock-index membership from raw data."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    parser.add_argument("--overwrite", action="store_true", help="Ghi đè output nếu đã tồn tại")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    fs = _build_fs()
    equity_path = f"{args.bucket}/{SRC_EQUITY_PATH}"
    index_info_path = f"{args.bucket}/{SRC_INDEX_INFO_PATH}"
    dst_path = f"{args.bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 1: Extract stock-index membership snapshot\n%s\n"
        "MinIO Endpoint : %s\n"
        "Equity source  : s3://%s\n"
        "Index source   : s3://%s\n"
        "Destination    : s3://%s\n"
        "Run at         : %s\n%s",
        separator,
        separator,
        MINIO_ENDPOINT,
        equity_path,
        index_info_path,
        dst_path,
        run_at,
        separator,
    )

    for path in (equity_path, index_info_path):
        if not fs.exists(path):
            logger.error("Source not found: s3://%s", path)
            return

    df_equity = normalize_equity(read_parquet(fs, equity_path))
    df_index = normalize_index_info(read_parquet(fs, index_info_path))
    if df_equity.is_empty():
        logger.error("Equity source is empty — aborting.")
        return
    if df_index.is_empty():
        logger.error("Index info source is empty — aborting.")
        return

    # Build snapshot hiện tại (chỉ symbol + index_id, không SCD)
    df_snapshot = build_member_mapping(fs, args.bucket, df_equity, df_index)
    if df_snapshot.is_empty():
        logger.error("No stock-index relationship produced — aborting.")
        return

    logger.info(f"Snapshot: {df_snapshot.shape[0]:,} rows (current membership)")

    # ── GX Gate: Input Quality ────────────────────────────────────────────
    logger.info("Running GX validation (Stage 1: Compound unique + Not-null)...")
    gx_check_compound_columns_unique(df_snapshot, {"column_list": ["symbol", "index_id"]})
    gx_check_columns_not_null(df_snapshot, {"columns": ["symbol", "index_id"]})
    gx_check_table_row_count_between(df_snapshot, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df_snapshot, fs, dst_path, overwrite=args.overwrite)
    logger.info("\n%s\nbridge_stock_index stage_1 (snapshot) complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
