#!/usr/bin/env python3
"""
bridge_master_stock_index.py (master) — Resolve surrogate keys.

Đọc bridge SCD2 từ stage_2 và thay natural keys bằng surrogate keys
từ dim_master_stock và dim_master_index theo phương pháp Kimball.

Source (MinIO):
  transformed/stage_2/dimension/bridge_stock_index.parquet
  master/dimension/dim_master_stock.parquet   → symbol → stock_key
  master/dimension/dim_master_index.parquet   → index_id → index_key

Destination (MinIO):
  master/dimension/bridge_master_stock_index.parquet

Output schema:
  stock_key      (Int32) — surrogate key, FK → dim_master_stock
  index_key      (Int64) — surrogate key, FK → dim_master_index
  symbol         (String) — natural key (giữ lại cho audit/debug)
  effective_from  (Date)  — SCD2 start date
  effective_to    (Date)  — SCD2 end date (9999-12-31 = active)
  is_current      (Int8)  — 1 = active, 0 = historical
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
logger = logging.getLogger("bridge_master_stock_index_publish")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

# Sources
SRC_BRIDGE_PATH = "transformed/stage_2/dimension/bridge_stock_index.parquet"
SRC_DIM_STOCK_PATH = "master/dimension/dim_master_stock.parquet"
SRC_DIM_INDEX_PATH = "master/dimension/dim_master_index.parquet"

# Destination
DST_PREFIX = "master/dimension"
DST_FILENAME = "bridge_master_stock_index.parquet"

# Column mapping
STOCK_SURROGATE_KEY = "stock_key"
INDEX_SURROGATE_KEY = "index_key"
STOCK_NATURAL_KEY = "symbol"


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


# ── Resolve surrogate keys ──────────────────────────────────────────────────

def resolve_surrogate_keys(
    df_bridge: pl.DataFrame,
    df_dim_stock: pl.DataFrame,
    df_dim_index: pl.DataFrame,
) -> pl.DataFrame:
    """
    Thay natural keys (symbol, index_id) bằng surrogate keys (stock_key, index_key)
    từ master dimension tables.

    - LEFT JOIN với dim_master_stock trên symbol → lấy stock_key
    - LEFT JOIN với dim_master_index trên index_id (= index_key) → lấy index_key
    - Giữ lại natural keys bên cạnh cho audit/debug
    - Log warning nếu có orphan keys (symbol/index_id không tìm thấy trong dim)
    """

    # --- Resolve stock_key ---
    if STOCK_SURROGATE_KEY not in df_dim_stock.columns:
        raise ValueError(
            f"dim_master_stock thiếu cột '{STOCK_SURROGATE_KEY}'. "
            "Chạy master/dim_master_stock.py trước."
        )

    # Lấy mapping symbol → stock_key (chỉ cần 2 cột, unique)
    stock_lookup = (
        df_dim_stock
        .select([STOCK_NATURAL_KEY, STOCK_SURROGATE_KEY])
        .unique(subset=[STOCK_NATURAL_KEY])
    )

    df_bridge = df_bridge.join(stock_lookup, on=STOCK_NATURAL_KEY, how="left")

    # Kiểm tra orphan symbols
    n_orphan_stock = df_bridge.filter(pl.col(STOCK_SURROGATE_KEY).is_null()).shape[0]
    if n_orphan_stock > 0:
        orphan_symbols = (
            df_bridge
            .filter(pl.col(STOCK_SURROGATE_KEY).is_null())
            .select(STOCK_NATURAL_KEY)
            .unique()
            .to_series()
            .to_list()[:10]  # max 10 samples
        )
        logger.warning(
            "%d bridge rows have no matching stock_key (orphan symbols: %s...)",
            n_orphan_stock,
            orphan_symbols,
        )

    # --- Resolve index_key ---
    # index_id (stage_2) = index_key (dim) — giá trị giống nhau, rename trực tiếp
    if INDEX_SURROGATE_KEY not in df_dim_index.columns:
        raise ValueError(
            f"dim_master_index thiếu cột '{INDEX_SURROGATE_KEY}'. "
            "Chạy master/dim_master_index.py trước."
        )

    valid_index_keys = df_dim_index.select(INDEX_SURROGATE_KEY).unique()

    # stage_2 bridge có cột 'index_id', rename thành 'index_key' để match
    if "index_id" in df_bridge.columns and INDEX_SURROGATE_KEY not in df_bridge.columns:
        df_bridge = df_bridge.rename({"index_id": INDEX_SURROGATE_KEY})
    elif "index_id" in df_bridge.columns:
        df_bridge = df_bridge.drop("index_id")

    # Kiểm tra orphan index_keys
    df_with_check = df_bridge.join(valid_index_keys, on=INDEX_SURROGATE_KEY, how="left", suffix="_dim")
    n_orphan_index = df_bridge.join(
        valid_index_keys, on=INDEX_SURROGATE_KEY, how="anti"
    ).shape[0]
    if n_orphan_index > 0:
        orphan_ids = (
            df_bridge.join(valid_index_keys, on=INDEX_SURROGATE_KEY, how="anti")
            .select(INDEX_SURROGATE_KEY)
            .unique()
            .to_series()
            .to_list()[:10]
        )
        logger.warning(
            "%d bridge rows have no matching index_key (orphan keys: %s...)",
            n_orphan_index,
            orphan_ids,
        )

    logger.info(
        "Surrogate key resolution: %d/%d stock_key resolved, %d/%d index_key resolved.",
        df_bridge.shape[0] - n_orphan_stock,
        df_bridge.shape[0],
        df_bridge.shape[0] - n_orphan_index,
        df_bridge.shape[0],
    )

    # Sắp xếp cột: surrogate keys đầu → natural keys → SCD columns
    output_cols = [
        STOCK_SURROGATE_KEY,
        INDEX_SURROGATE_KEY,
        "effective_from",
        "effective_to",
        "is_current",
    ]
    # Chỉ select các cột thực sự tồn tại
    output_cols = [c for c in output_cols if c in df_bridge.columns]
    df_bridge = df_bridge.select(output_cols)

    return df_bridge.sort([INDEX_SURROGATE_KEY, STOCK_SURROGATE_KEY, "effective_from"])


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=(
            "Master: publish bridge_stock_index with surrogate keys "
            "(stock_key FK → dim_master_stock, index_key FK → dim_master_index)."
        )
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file master nếu đã tồn tại")
    return p.parse_args()


def log_run_info(args: argparse.Namespace, src_bridge: str, src_stock: str, src_index: str, dst: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nMaster: bridge_stock_index → bridge_master_stock_index (resolve FK)\n%s\n"
        "MinIO Endpoint  : %s\n"
        "Bridge source   : s3://%s\n"
        "Dim stock       : s3://%s\n"
        "Dim index       : s3://%s\n"
        "Destination     : s3://%s\n"
        "Overwrite       : %s\n"
        "Run at          : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        src_bridge, src_stock, src_index, dst,
        args.overwrite,
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src_bridge = f"{bucket}/{SRC_BRIDGE_PATH}"
    src_stock = f"{bucket}/{SRC_DIM_STOCK_PATH}"
    src_index = f"{bucket}/{SRC_DIM_INDEX_PATH}"
    dst = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    log_run_info(args, src_bridge, src_stock, src_index, dst)

    fs = _build_fs()

    # Kiểm tra tất cả source tồn tại
    for label, path in [
        ("Bridge (stage_2)", src_bridge),
        ("Dim stock (master)", src_stock),
        ("Dim index (master)", src_index),
    ]:
        if not fs.exists(path):
            logger.error("%s not found: s3://%s — run upstream scripts first.", label, path)
            return

    # Đọc tất cả sources
    df_bridge = read_parquet(fs, src_bridge)
    df_dim_stock = read_parquet(fs, src_stock)
    df_dim_index = read_parquet(fs, src_index)

    if df_bridge.is_empty():
        logger.error("Bridge source is empty — aborting.")
        return

    # Resolve surrogate keys
    df_result = resolve_surrogate_keys(df_bridge, df_dim_stock, df_dim_index)

    # Log stats
    n_active = df_result.filter(pl.col("is_current") == 1).shape[0]
    n_historical = df_result.filter(pl.col("is_current") == 0).shape[0]
    logger.info(
        "Master bridge: %s total (%s active, %s historical).",
        f"{df_result.shape[0]:,}",
        f"{n_active:,}",
        f"{n_historical:,}",
    )
    logger.info(f"Final schema: {df_result.schema}")
    logger.info(f"Sample:\n{df_result.head(5)}")

    # ── GX Gate: Referential Integrity ───────────────────────────────
    logger.info("Running GX validation (Master: FK not null)...")
    gx_check_columns_not_null(df_result, {"columns": ["stock_key", "index_key"]})
    gx_check_table_row_count_between(df_result, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df_result, fs, dst, overwrite=args.overwrite)

    separator = "=" * 80
    logger.info("\n%s\nbridge_master_stock_index publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
