#!/usr/bin/env python3
"""
fact_market_equity_daily.py (master publish)

Đưa dữ liệu daily equity từ transformed lên master, giữ nguyên cấu trúc thư mục partition.

Source (MinIO):
  transformed/fact/market/equity_daily/year=YYYY/month=MM/day=DD/ohlc.parquet

Destination (MinIO):
  master/fact/market/equity_daily/year=YYYY/month=MM/day=DD/ohlc.parquet

Logic updated_at:
  Với mỗi file partition: bỏ updated_at từ transformed (nếu có), gán lại updated_at
  theo thời điểm publish lên master (ICT, một mốc cho toàn bộ batch — cùng giá trị
  trên mọi dòng trong mọi file của một lần chạy).
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
logger = logging.getLogger("fact_market_equity_daily_master")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PREFIX = "transformed/fact/market/equity_daily"
DST_PREFIX = "master/fact/market/equity_daily"


def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def apply_master_updated_at(df: pl.DataFrame, updated_at: str) -> pl.DataFrame:
    """Không mang updated_at từ transformed; chỉ có updated_at của master."""
    out = df.drop("updated_at") if "updated_at" in df.columns else df
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
        description=(
            "Publish transformed/fact/market/equity_daily → master/fact/market/equity_daily "
            "(cùng partition), refresh updated_at."
        )
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    p.add_argument("--overwrite", action="store_true", help="Ghi đè file master nếu đã tồn tại")
    return p.parse_args()


def log_run_info(args: argparse.Namespace, src_root: str, updated_at: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nPublish: transformed/.../equity_daily → master/.../equity_daily\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s/\n"
        "Destination    : s3://%s/%s/\n"
        "Overwrite      : %s\n"
        "updated_at     : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        src_root,
        args.bucket, DST_PREFIX,
        args.overwrite,
        updated_at,
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    bucket = args.bucket
    src_root = f"{bucket}/{SRC_PREFIX}"
    updated_at = datetime.now(ICT).strftime("%Y-%m-%dT%H:%M:%S%z")
    log_run_info(args, src_root, updated_at)

    fs = _build_fs()
    pattern = f"{src_root}/**/*.parquet"
    paths = sorted(fs.glob(pattern))

    if not paths:
        logger.error(f"No parquet under s3://{src_root}/ — aborting.")
        return

    written = 0
    skipped = 0
    for src_path in paths:
        if not src_path.startswith(f"{bucket}/{SRC_PREFIX}/"):
            logger.warning(f"Unexpected path, skip: {src_path}")
            skipped += 1
            continue
        rel = src_path[len(f"{bucket}/{SRC_PREFIX}/") :]
        dst_path = f"{bucket}/{DST_PREFIX}/{rel}"

        if fs.exists(dst_path) and not args.overwrite:
            logger.info(f"Exists, skipping: s3://{dst_path}")
            skipped += 1
            continue

        try:
            with fs.open(src_path, "rb") as f:
                df = pl.read_parquet(io.BytesIO(f.read()))
        except Exception as e:
            logger.error(f"Cannot read s3://{src_path}: {e}")
            skipped += 1
            continue

        if df.is_empty():
            logger.warning(f"Empty: s3://{src_path} — skip write.")
            skipped += 1
            continue

        df_out = apply_master_updated_at(df, updated_at)
        write_parquet(df_out, fs, dst_path, overwrite=args.overwrite)
        written += 1

    separator = "=" * 80
    logger.info("\n%s\nfact_market_equity_daily publish complete! %d file(s) written, %d skipped.\n%s",
                separator, written, skipped, separator)


if __name__ == "__main__":
    main()
