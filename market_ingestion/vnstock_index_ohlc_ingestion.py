#!/usr/bin/env python3
"""
vnstock_index_ohlc_ingestion.py

Fetch historical OHLCV (daily) for a fixed set of benchmark indices using
vnstock_data Market().index(symbol).ohlcv():
VNINDEX, VN30, HNXINDEX, HNX30.

Target structure in MinIO:
  raw/market/
    index/ohlc.parquet

Behavior:
 - Default date range: 2025-01-01 → 2025-12-31 (cả năm 2025).
 - Mặc định bỏ qua nếu file đích đã tồn tại; dùng --append để gộp.
 - Bỏ qua mã trả về DataFrame rỗng.
 - Retry khi rate-limit / timeout (kể cả SystemExit như vnstock_ohlc_ingestion).
"""

import os
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import s3fs
from dotenv import load_dotenv

_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
    os.path.join(_script_dir, "..", "reference_ingestion", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

from vnstock_data import Market

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("index_ohlc_ingestion")
ICT = timezone(timedelta(hours=7))

vnstock_key = os.getenv("VNSTOCK_API_KEY")
if vnstock_key:
    os.environ["VNSTOCK_API_KEY"] = vnstock_key
    logger.info(f"VNStock API Key ('{vnstock_key[:4]}***') found and configured successfully (Sponsor tier active).")
else:
    logger.warning("No VNSTOCK_API_KEY found in environment or .env, running on Community tier.")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")
DEFAULT_MARKET_PREFIX = "raw/market"

# Chỉ lấy 4 chỉ số chính (không đọc từ index.parquet).
BENCHMARK_INDEX_SYMBOLS: tuple[str, ...] = ("VNINDEX", "VN30", "HNXINDEX", "HNX30")

OHLC_PER_REQ_DELAY = 0.33
WAIT_TIME_ON_ERROR = 65


def _add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(ICT)
    return df


def write_parquet_to_s3(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str) -> None:
    if df is None or df.empty:
        logger.warning(f"DataFrame is empty, skipping upload for {s3_path}")
        return
    if fs.exists(s3_path):
        logger.info(f"Exists, skipping: s3://{s3_path}  (use --append to merge)")
        return
    df = _add_ingested_at(df)
    with fs.open(s3_path, "wb") as f:
        df.to_parquet(f, engine="pyarrow", index=False, compression="snappy")
    file_size = fs.size(s3_path)
    size_kb = file_size / 1024 if file_size is not None else 0
    logger.info(f"✅ Uploaded s3://{s3_path} ({size_kb:.1f} KB, {len(df):,} rows)")


def append_parquet_to_s3(
    df_new: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    dedup_keys: list[str] | None = None,
) -> None:
    if df_new is None or df_new.empty:
        logger.warning(f"New DataFrame is empty, nothing to append for {s3_path}")
        return

    if dedup_keys is None:
        dedup_keys = ["symbol", "time"]

    df_new = _add_ingested_at(df_new)

    if fs.exists(s3_path):
        try:
            with fs.open(s3_path, "rb") as f:
                df_old = pd.read_parquet(f)
            logger.info(f"Read existing file: {len(df_old):,} rows from s3://{s3_path}")
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


def fetch_index_ohlcv_with_retry(idx_symbol: str, start: str, end: str) -> pd.DataFrame | None:
    """Gọi Market().index(idx_symbol).ohlcv(start, end) với retry."""
    while True:
        try:
            mkt = Market()
            df = mkt.index(idx_symbol).ohlcv(start=start, end=end)
            if df is not None and not df.empty:
                df = df.copy()
                if "symbol" not in df.columns:
                    df.insert(0, "symbol", idx_symbol)
                else:
                    blank = df["symbol"].isna() | (df["symbol"].astype(str).str.strip() == "")
                    df["symbol"] = df["symbol"].where(~blank, idx_symbol)
                    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
            return df
        except SystemExit:
            logger.warning(f"{idx_symbol} — Rate-limit (SystemExit). Waiting {WAIT_TIME_ON_ERROR}s before retry...")
            time.sleep(WAIT_TIME_ON_ERROR)
        except Exception as e:
            err_str = str(e).lower()
            if any(kw in err_str for kw in ("rate", "limit", "timeout", "429", "503")):
                logger.warning(f"{idx_symbol} — Rate-limit/Timeout: {e}. Waiting {WAIT_TIME_ON_ERROR}s before retry...")
                time.sleep(WAIT_TIME_ON_ERROR)
            else:
                logger.error(f"{idx_symbol} — Unexpected error: {e}. Skipping.")
                return None


def fetch_index_ohlcv_all(
    index_symbols: list[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    results: list[pd.DataFrame] = []
    logger.info(f"Fetching index OHLCV [{start} → {end}] for: {', '.join(index_symbols)}")

    for idx_symbol in index_symbols:
        df = fetch_index_ohlcv_with_retry(idx_symbol, start, end)
        if df is None:
            logger.error(f"{idx_symbol} — skipped due to error.")
        elif df.empty:
            logger.warning(f"{idx_symbol} — no data in range, skipped.")
        else:
            logger.info(f"{idx_symbol} — {len(df):,} rows fetched.")
            results.append(df)
        time.sleep(OHLC_PER_REQ_DELAY)

    if not results:
        return pd.DataFrame()

    df_all = pd.concat(results, ignore_index=True)
    logger.info(f"Total: {len(df_all):,} rows from {len(results)}/{len(index_symbols)} indices.")
    return df_all


def parse_args():
    p = argparse.ArgumentParser(description="Fetch VNStock index OHLCV history to MinIO S3.")
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket name")
    p.add_argument("--market-prefix", default=DEFAULT_MARKET_PREFIX, help="Prefix for output market data")
    p.add_argument("--start", default="2025-01-01", help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", default="2025-12-31", help="End date (YYYY-MM-DD)")
    p.add_argument("--append", action="store_true", help="Gộp file, deduplicate theo (symbol, time)")
    return p.parse_args()


def main():
    args = parse_args()

    market_prefix = f"{args.bucket}/{args.market_prefix}"
    ohlc_s3_path = f"{market_prefix}/index/ohlc.parquet"

    print("=" * 80)
    print("Fetch VNStock Index OHLCV to MinIO S3")
    print("=" * 80)
    print(f"MinIO Endpoint    : {MINIO_ENDPOINT}")
    print(f"Indices           : {', '.join(BENCHMARK_INDEX_SYMBOLS)}")
    print(f"Target output     : s3://{ohlc_s3_path}")
    print(f"Date range        : {args.start}  →  {args.end}")
    print(f"Mode              : {'append' if args.append else 'write (skip if exists)'}")
    print(f"Run at            : {datetime.now(ICT).strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 80)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    df_ohlc = fetch_index_ohlcv_all(list(BENCHMARK_INDEX_SYMBOLS), start=args.start, end=args.end)

    if args.append:
        append_parquet_to_s3(df_ohlc, fs, ohlc_s3_path)
    else:
        write_parquet_to_s3(df_ohlc, fs, ohlc_s3_path)

    print("\n" + "=" * 80)
    logger.info("🎉 Index OHLCV ingestion complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
