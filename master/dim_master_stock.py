#!/usr/bin/env python3
"""
dim_master_stock.py

Đưa dữ liệu từ layer transformed (stage_2) lên master (dimension).
SCD Type 1 theo Kimball: overwrite dimension mỗi lần chạy, giữ immutable surrogate key.

Source (MinIO):
  transformed/stage_2/dimension/dim_stock_info.parquet

Destination (MinIO):
  master/dimension/dim_master_stock.parquet

Surrogate key mapping (persistent):
  master/dimension/_stock_key_mapping.parquet

Logic:
  - Đọc mapping cũ (nếu có) → giữ stock_key cũ nguyên vẹn.
  - Symbols mới → gán stock_key = max(existing) + 1.
  - Ghi mapping mới + dimension ra MinIO.
  - Kimball compliant: surrogate key IMMUTABLE sau khi gán.
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
    gx_check_column_not_null,
    gx_check_column_values_unique,
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
logger = logging.getLogger("dim_master_stock_publish")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH = "transformed/stage_2/dimension/dim_stock_info.parquet"
DST_PREFIX = "master/dimension"
DST_FILENAME = "dim_master_stock.parquet"
MAPPING_FILENAME = "_stock_key_mapping.parquet"

SURROGATE_KEY = "stock_key"
NATURAL_KEY = "symbol"


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    logger.info(f"Reading s3://{s3_path} ...")
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
    logger.info(f"  → {df.shape[0]:,} rows × {df.shape[1]} cols")
    return df


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
) -> None:
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Saved s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)")


# ── Surrogate key: persistent mapping ───────────────────────────────────────

def load_mapping(fs: s3fs.S3FileSystem, mapping_path: str) -> pl.DataFrame:
    """Đọc mapping cũ từ MinIO. Trả về DataFrame rỗng nếu chưa có."""
    if fs.exists(mapping_path):
        logger.info(f"Loading existing mapping: s3://{mapping_path}")
        with fs.open(mapping_path, "rb") as f:
            mapping = pl.read_parquet(io.BytesIO(f.read()))
        logger.info(f"  → {mapping.shape[0]:,} existing mappings")
        return mapping
    else:
        logger.info("No existing mapping found — creating new mapping.")
        return pl.DataFrame(schema={NATURAL_KEY: pl.Utf8, SURROGATE_KEY: pl.Int32})


def resolve_surrogate_keys(
    df: pl.DataFrame,
    mapping: pl.DataFrame,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Resolve surrogate keys using persistent mapping.
    - Symbols đã có trong mapping → giữ nguyên stock_key.
    - Symbols mới → gán stock_key = max(existing) + 1, tăng dần.
    Returns: (df_with_keys, updated_mapping)
    """
    current_symbols = set(df[NATURAL_KEY].unique().to_list())

    if mapping.is_empty():
        existing_symbols = set()
        max_key = 0
    else:
        existing_symbols = set(mapping[NATURAL_KEY].to_list())
        max_key = mapping[SURROGATE_KEY].max()

    new_symbols = sorted(current_symbols - existing_symbols)

    if new_symbols:
        logger.info(f"New symbols: {len(new_symbols)} → assigning keys {max_key + 1} to {max_key + len(new_symbols)}")
        new_mapping = pl.DataFrame({
            NATURAL_KEY: new_symbols,
            SURROGATE_KEY: list(range(max_key + 1, max_key + 1 + len(new_symbols))),
        }).with_columns(pl.col(SURROGATE_KEY).cast(pl.Int32))
        updated_mapping = pl.concat([mapping, new_mapping])
    else:
        logger.info("No new symbols — all symbols already have keys.")
        updated_mapping = mapping

    # JOIN mapping → df
    lookup = updated_mapping.select([NATURAL_KEY, SURROGATE_KEY])
    df = df.join(lookup, on=NATURAL_KEY, how="left")

    # Đặt stock_key làm cột đầu
    other_cols = [c for c in df.columns if c != SURROGATE_KEY]
    df = df.select([SURROGATE_KEY, *other_cols])
    df = df.sort(SURROGATE_KEY)

    logger.info(
        f"Surrogate key resolved: {df.shape[0]} rows, "
        f"{len(existing_symbols)} existing + {len(new_symbols)} new = "
        f"{updated_mapping.shape[0]} total mappings."
    )

    return df, updated_mapping


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Publish dim_stock_info (stage_2) → master/dimension/dim_master_stock (SCD Type 1)"
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file master nếu đã tồn tại")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src = f"{bucket}/{SRC_PATH}"
    dst = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"
    mapping_path = f"{bucket}/{DST_PREFIX}/{MAPPING_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nPublish: dim_stock_info (stage_2) → master/dimension/dim_master_stock\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s\n"
        "Destination    : s3://%s\n"
        "Mapping        : s3://%s\n"
        "Run at         : %s\n"
        "SCD Type       : Type 1 (full refresh, immutable surrogate key)\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        src, dst, mapping_path,
        run_at,
        separator,
    )

    fs = _build_fs()
    if not fs.exists(src):
        logger.error(f"Source not found: s3://{src}")
        return

    df = read_parquet(fs, src)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    # 1. Load existing mapping
    mapping = load_mapping(fs, mapping_path)

    # 2. Resolve surrogate keys (persistent)
    df, updated_mapping = resolve_surrogate_keys(df, mapping)

    # 3. Save updated mapping
    write_parquet(updated_mapping, fs, mapping_path)
    logger.info(f"Mapping saved: {updated_mapping.shape[0]} total entries.")

    # ── GX Gate: Star Schema Integrity ───────────────────────────────
    logger.info("Running GX validation (Master: SK unique + not null)...")
    gx_check_column_not_null(df, {"column": "stock_key"})
    gx_check_column_values_unique(df, {"column": "stock_key"})
    gx_check_column_values_unique(df, {"column": "symbol"})
    gx_check_table_row_count_between(df, {"min_value": 1})
    logger.info("GX validation passed ✓")

    logger.info(f"Final schema: {df.schema}")
    logger.info(f"Sample:\n{df.head(5)}")

    write_parquet(df, fs, dst)  # SCD1: luôn full refresh

    logger.info("\n%s\ndim_master_stock publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
