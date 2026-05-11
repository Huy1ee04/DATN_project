#!/usr/bin/env python3
"""
dim_stock_info.py

Transform: left join toàn bộ company info với equity để lấy tên công ty.

Sources (MinIO):
  raw/reference/company/info/  → tất cả trường
  raw/reference/equity/        → symbol, organ_short_name, organ_name

Join: info LEFT JOIN equity on symbol

Output (MinIO):
  transformed/dimension/dim_stock_info.parquet
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone
from typing import Optional

import polars as pl
import s3fs
from dotenv import load_dotenv

# ── Load .env ────────────────────────────────────────────────────────────────
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
logger = logging.getLogger("dim_stock_info_transform")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET   = os.getenv("MINIO_BUCKET", "stock-data")

SRC_INFO_PREFIX   = "raw/reference/company/info"
SRC_EQUITY_PREFIX = "raw/reference/equity"

DST_PREFIX   = "transformed/dimension"
DST_FILENAME = "dim_stock_info.parquet"

EQUITY_COLS = ["symbol", "organ_short_name", "organ_name"]


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_partition(
    fs: s3fs.S3FileSystem,
    bucket: str,
    prefix: str,
    select_cols: Optional[list[str]] = None,
) -> pl.DataFrame:
    """Đọc tất cả file .parquet trong prefix (partition), gộp thành 1 DataFrame."""
    pattern      = f"{bucket}/{prefix}/**/*.parquet"
    flat_pattern = f"{bucket}/{prefix}/*.parquet"
    paths = list(dict.fromkeys(fs.glob(pattern) + fs.glob(flat_pattern)))

    if not paths:
        logger.warning(f"No parquet files found at s3://{bucket}/{prefix}")
        return pl.DataFrame()

    logger.info(f"Reading {len(paths)} file(s) from s3://{bucket}/{prefix} ...")
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            with fs.open(path, "rb") as f:
                raw = f.read()
            df = pl.read_parquet(io.BytesIO(raw))
            df = df.drop([c for c in ("ingested_at",) if c in df.columns])
            if select_cols:
                available = [c for c in select_cols if c in df.columns]
                missing   = [c for c in select_cols if c not in df.columns]
                if missing:
                    logger.warning(f"  {path}: missing columns {missing}, skipped")
                df = df.select(available)
            frames.append(df)
            logger.info(f"  ✓ {path} → {df.shape[0]:,} rows × {df.shape[1]} cols")
        except Exception as e:
            logger.error(f"  ✗ Cannot read {path}: {e}")

    if not frames:
        return pl.DataFrame()

    combined = pl.concat(frames, how="diagonal").unique()
    logger.info(f"  → Combined: {combined.shape[0]:,} rows × {combined.shape[1]} cols")
    return combined


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    overwrite: bool = False,
) -> None:
    if fs.exists(s3_path) and not overwrite:
        logger.info(f"Exists, skipping: s3://{s3_path}  (dùng --overwrite để ghi đè)")
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Saved s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)")


# ── Transform ─────────────────────────────────────────────────────────────────

def transform(df_info: pl.DataFrame, df_equity: pl.DataFrame) -> pl.DataFrame:
    """
    info LEFT JOIN equity on symbol.
    Thêm updated_at, bỏ ingested_at (đã xử lý khi đọc).
    """
    logger.info("Left joining info with equity on symbol ...")

    # Loại bỏ cột trùng tên từ equity (nếu có) trước khi join
    equity_extra = [c for c in df_equity.columns if c not in df_info.columns or c == "symbol"]
    df_equity_sel = df_equity.select(equity_extra)

    df_result = df_info.join(df_equity_sel, on="symbol", how="left")

    updated_at = datetime.now(ICT).strftime("%Y-%m-%dT%H:%M:%S%z")
    df_result = df_result.with_columns(pl.lit(updated_at).alias("updated_at"))

    logger.info(f"Result: {df_result.shape[0]:,} rows × {df_result.shape[1]} cols")
    logger.info(f"Schema: {df_result.schema}")
    return df_result


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Transform: info LEFT JOIN equity → dim_stock_info."
    )
    p.add_argument("--bucket",    default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true",    help="Ghi đè output nếu đã tồn tại")
    return p.parse_args()


def log_run_info(args: argparse.Namespace, bucket: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nTransform: info LEFT JOIN equity → dim_stock_info\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source info    : s3://%s/%s/\n"
        "Source equity  : s3://%s/%s/\n"
        "Destination    : s3://%s/%s/%s\n"
        "Join key       : symbol\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        bucket,
        bucket, SRC_INFO_PREFIX,
        bucket, SRC_EQUITY_PREFIX,
        bucket, DST_PREFIX, DST_FILENAME,
        args.overwrite,
        run_at,
        separator,
    )


def main() -> None:
    args   = parse_args()
    bucket = args.bucket
    log_run_info(args, bucket)

    fs = _build_fs()

    df_info   = read_partition(fs, bucket, SRC_INFO_PREFIX)
    df_equity = read_partition(fs, bucket, SRC_EQUITY_PREFIX, select_cols=EQUITY_COLS)

    if df_info.is_empty():
        logger.error("info source is empty — aborting.")
        return
    if df_equity.is_empty():
        logger.error("equity source is empty — aborting.")
        return

    df_result = transform(df_info, df_equity)

    dst_path = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"
    write_parquet(df_result, fs, dst_path, overwrite=args.overwrite)

    separator = "=" * 80
    logger.info("\n%s\ndim_stock_info transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
