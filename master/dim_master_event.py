#!/usr/bin/env python3
"""
publish_dim_date_event_to_master.py

Đưa dữ liệu từ layer transformed lên master (dimension).

Source (MinIO):
  transformed/dimension/dim_date_event.parquet

Destination (MinIO):
  master/dimension/dim_master_event.parquet

Logic updated_at:
  Bỏ hoàn toàn cột updated_at từ file transformed (nếu có), rồi chỉ gán updated_at
  theo thời điểm publish lên master (ICT, một mốc cho cả batch). Các cột khác
  (ví dụ transformed_at) giữ nguyên từ nguồn.
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

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
logger = logging.getLogger("publish_dim_date_event_to_master")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PATH = "transformed/dimension/dim_date_event.parquet"
DST_PREFIX = "master/dimension"
DST_FILENAME = "dim_master_event.parquet"


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


def apply_master_updated_at(df: pl.DataFrame) -> pl.DataFrame:
    """Không mang updated_at từ transformed; chỉ có updated_at của master."""
    out = df.drop("updated_at") if "updated_at" in df.columns else df
    updated_at = datetime.now(ICT).strftime("%Y-%m-%dT%H:%M:%S%z")
    return out.with_columns(pl.lit(updated_at).alias("updated_at"))


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


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Publish dim_date_event.parquet → master/dimension/dim_master_event.parquet"
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file master nếu đã tồn tại")
    return p.parse_args()


def log_run_info(args: argparse.Namespace, src: str, dst: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nPublish: dim_date_event → master/dimension/dim_master_event\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s\n"
        "Destination    : s3://%s\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        src, dst,
        args.overwrite,
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src = f"{bucket}/{SRC_PATH}"
    dst = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"
    log_run_info(args, src, dst)

    fs = _build_fs()
    if not fs.exists(src):
        logger.error(f"Source not found: s3://{src}")
        return

    df = read_parquet(fs, src)
    if df.is_empty():
        logger.error("Source is empty — aborting.")
        return

    df_out = apply_master_updated_at(df)
    write_parquet(df_out, fs, dst, overwrite=args.overwrite)

    separator = "=" * 80
    logger.info("\n%s\ndim_master_event publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
