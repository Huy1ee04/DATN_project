#!/usr/bin/env python3
"""
vnstock_event_ingestion.py

Fetch market events via Reference().events.market() and save to MinIO S3:
  raw/reference/event/event.parquet

Behavior:
 - By default skips if file already exists.
 - Use --append to merge with existing data (dedup by event_name, notify_date, symbol).
"""

import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import s3fs
from dotenv import load_dotenv

# Load .env if present
_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, '.env'),
    os.path.join(_script_dir, '..', '.env')
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

from vnstock_data import Reference

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("event_ingestion")
ICT = timezone(timedelta(hours=7))

# Setup VNSTOCK Sponsor API Key
vnstock_key = os.getenv("VNSTOCK_API_KEY")
if vnstock_key:
    os.environ["VNSTOCK_API_KEY"] = vnstock_key
    logger.info(f"VNStock API Key ('{vnstock_key[:4]}***') found and configured successfully (Sponsor tier active).")
else:
    logger.warning("No VNSTOCK_API_KEY found in environment or .env, running on Community tier.")

# ---------------- Default Config ----------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET    = os.getenv("MINIO_BUCKET", "stock-data")
DEFAULT_S3_PREFIX = "raw/reference"


def _add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(ICT)
    return df


def _write_parquet_and_log(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    success_prefix: str = "Uploaded",
) -> None:
    with fs.open(s3_path, "wb") as f:
        df.to_parquet(f, engine="pyarrow", index=False, compression="snappy")
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"{success_prefix} s3://{s3_path} ({size_kb:.1f} KB, {len(df):,} rows)")


def write_parquet_to_s3(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
) -> None:
    """Write DataFrame to MinIO S3. Skips if file already exists (use --append to merge)."""
    if df is None or df.empty:
        logger.warning(f"DataFrame is empty, skipping upload for {s3_path}")
        return
    if fs.exists(s3_path):
        logger.info(f"Exists, skipping: s3://{s3_path}  (use --append to merge)")
        return
    df = _add_ingested_at(df)
    _write_parquet_and_log(df, fs, s3_path, success_prefix="Uploaded")


def append_parquet_to_s3(
    df_new: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    dedup_keys: list[str] = None,
) -> None:
    """
    Ghi tiếp df_new vào file Parquet đã có trên MinIO.
    - Nếu file chưa tồn tại → ghi mới bình thường.
    - Deduplicate theo dedup_keys, ưu tiên giữ row mới.
    - ingested_at của rows cũ được bảo toàn.
    """
    if df_new is None or df_new.empty:
        logger.warning(f"New DataFrame is empty, nothing to append for {s3_path}")
        return

    if dedup_keys is None:
        dedup_keys = ["event_name", "notify_date", "symbol"]

    df_new = _add_ingested_at(df_new)

    if fs.exists(s3_path):
        try:
            with fs.open(s3_path, "rb") as f:
                df_old = pd.read_parquet(f)
            logger.info(f"Read existing: {len(df_old):,} rows from s3://{s3_path}")
            df_combined = pd.concat([df_new, df_old], ignore_index=True)
        except Exception as e:
            logger.error(f"Cannot read existing parquet ({e}), will overwrite with new data.")
            df_combined = df_new
    else:
        logger.info(f"File not found, creating new: s3://{s3_path}")
        df_combined = df_new

    before = len(df_combined)
    valid_keys = [k for k in dedup_keys if k in df_combined.columns]
    if valid_keys:
        df_combined = df_combined.drop_duplicates(subset=valid_keys, keep="first")
    after = len(df_combined)
    if before != after:
        logger.info(f"Dedup: {before - after:,} duplicate rows removed → {after:,} rows kept")

    _write_parquet_and_log(df_combined, fs, s3_path, success_prefix="Appended ->")


def log_run_info(args: argparse.Namespace, event_s3_path: str) -> None:
    separator = "=" * 80
    mode = "append" if args.append else "write (skip if exists)"
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nFetch VNStock Event Data to MinIO S3\n%s\n"
        "MinIO Endpoint : %s\n"
        "Target output  : s3://%s\n"
        "Mode           : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        event_s3_path,
        mode,
        run_at,
        separator,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch VNStock Event Data to MinIO S3.")
    parser.add_argument("--bucket", default=DEFAULT_BUCKET,    help="MinIO bucket name")
    parser.add_argument("--prefix", default=DEFAULT_S3_PREFIX, help="Path prefix inside bucket")
    parser.add_argument("--append", action="store_true",       help="Ghi tiếp vào file cũ, deduplicate theo (event_name, notify_date, symbol)")
    args = parser.parse_args()

    prefix = f"{args.bucket}/{args.prefix}"
    event_s3_path = f"{prefix}/event/event.parquet"
    log_run_info(args, event_s3_path)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    ref = Reference()

    logger.info("Fetching market events via ref.events.market()...")
    try:
        df_events = ref.events.market()
        if args.append:
            append_parquet_to_s3(df_events, fs, event_s3_path,
                                 dedup_keys=["event_name", "notify_date", "symbol"])
        else:
            write_parquet_to_s3(df_events, fs, event_s3_path)
    except Exception as e:
        logger.error(f"Error fetching events: {e}")

    separator = "=" * 80
    logger.info("\n%s\nEvent ingestion complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
