#!/usr/bin/env python3
"""
vnstock_equity_ingestion.py

Fetch equity reference data and save to MinIO S3:
  raw/reference/equity/equity.parquet  — HOSE STOCK list (list_by_exchange)
  raw/reference/equity/vn30.parquet    — VN30 index members

Behavior:
 - By default skips if file already exists.
 - Use --append to merge with existing data.
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('equity_ingestion')
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
    """Add ingested_at timestamp column (ICT timezone)."""
    df = df.copy()
    df["ingested_at"] = datetime.now(ICT)
    return df


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
    with fs.open(s3_path, "wb") as f:
        df.to_parquet(f, engine="pyarrow", index=False, compression="snappy")
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Uploaded s3://{s3_path} ({size_kb:.1f} KB, {len(df):,} rows)")


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
        dedup_keys = ["symbol"]

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

    with fs.open(s3_path, "wb") as f:
        df_combined.to_parquet(f, engine="pyarrow", index=False, compression="snappy")
    file_size = fs.size(s3_path)
    size_kb = file_size / 1024 if file_size is not None else 0
    logger.info(f"✅ Appended → s3://{s3_path} ({size_kb:.1f} KB, {after:,} rows total)")


def parse_args():
    p = argparse.ArgumentParser(description="Fetch VNStock Equity Data to MinIO S3.")
    p.add_argument("--bucket", default=DEFAULT_BUCKET,    help="MinIO bucket name")
    p.add_argument("--prefix", default=DEFAULT_S3_PREFIX, help="Path prefix inside bucket")
    p.add_argument("--append", action="store_true",       help="Ghi tiếp vào file cũ, deduplicate theo symbol")
    return p.parse_args()


def main():
    args   = parse_args()
    prefix = f"{args.bucket}/{args.prefix}"

    print("=" * 80)
    print("Fetch VNStock Equity Data to MinIO S3")
    print("=" * 80)
    print(f"MinIO Endpoint : {MINIO_ENDPOINT}")
    print(f"Target prefix  : s3://{prefix}/equity/")
    print(f"Mode           : {'append' if args.append else 'write (skip if exists)'}")
    print(f"Run at         : {datetime.now(ICT).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 80)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    ref = Reference()

    # 1. EQUITY — equity.parquet (HOSE STOCK only)
    logger.info("Fetching equity list (HOSE, STOCK)...")
    try:
        df_equity_all = ref.equity.list_by_exchange()
        df_equity = (
            df_equity_all[
                (df_equity_all["exchange"] == "HOSE") &
                (df_equity_all["type"]     == "STOCK")
            ]
            .reset_index(drop=True)
        )
        equity_path = f"{prefix}/equity/equity.parquet"
        if args.append:
            append_parquet_to_s3(df_equity, fs, equity_path, dedup_keys=["symbol"])
        else:
            write_parquet_to_s3(df_equity, fs, equity_path)
        logger.info(f"  {len(df_equity)} HOSE STOCK symbols saved.")
    except Exception as e:
        logger.error(f"Error fetching equity: {e}")

    # 2. VN30 MEMBERS — vn30.parquet
    logger.info("Fetching VN30 index members...")
    try:
        df_vn30 = ref.index.members("VN30")
        if isinstance(df_vn30, pd.Series):
            df_vn30 = df_vn30.to_frame().reset_index(drop=True)
        vn30_path = f"{prefix}/equity/vn30.parquet"
        if args.append:
            append_parquet_to_s3(df_vn30, fs, vn30_path, dedup_keys=["symbol"])
        else:
            write_parquet_to_s3(df_vn30, fs, vn30_path)
        logger.info(f"  {len(df_vn30)} VN30 members saved.")
    except Exception as e:
        logger.error(f"Error fetching VN30 members: {e}")

    print("\n" + "=" * 80)
    logger.info("🎉 Equity ingestion complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
