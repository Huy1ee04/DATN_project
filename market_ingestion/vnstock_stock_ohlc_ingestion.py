#!/usr/bin/env python3
"""
vnstock_ohlc_ingestion.py

Fetch historical OHLC (daily) data for all equity symbols listed in
raw/reference/equity/equity.parquet using vnstock_data Market().equity(symbol).ohlcv()
and save to MinIO S3 as a single Parquet file.

Target structure in MinIO:
  raw/market/
    equity/ohlc.parquet

Behavior:
 - By default does not overwrite existing parquet files.
 - Use --overwrite to force re-fetch + overwrite.
 - Default date range: 2026-01-01 → 2026-03-31 (Q1 2026).
 - Skips symbols that return empty data.
 - Retries on rate-limit / timeout errors with configurable backoff.
"""

import os
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import s3fs
from dotenv import load_dotenv

# --- TRƯỚC KHI IMPORT vnstock_data, PHẢI LOAD .ENV ---
# Tìm theo thứ tự: thư mục script → project root → reference_ingestion
_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, '.env'),                          # market_ingestion/.env
    os.path.join(_script_dir, '..', '.env'),                    # project root/.env
    os.path.join(_script_dir, '..', 'reference_ingestion', '.env'),  # reference_ingestion/.env
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

from vnstock_data import Market

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger('ohlc_ingestion')
ICT = timezone(timedelta(hours=7))

# Setup VNSTOCK Sponsor API Key
vnstock_key = os.getenv("VNSTOCK_API_KEY")
if vnstock_key:
    # Market() from vnstock_data 3.0.0 uses os.environ["VNSTOCK_API_KEY"] automatically.
    # Ensure it's available in the environment.
    os.environ["VNSTOCK_API_KEY"] = vnstock_key
    logger.info(f"VNStock API Key ('{vnstock_key[:4]}***') found and configured successfully (Sponsor tier active).")
else:
    logger.warning("No VNSTOCK_API_KEY found in environment or .env, running on Community tier.")

# ---------------- Default Config ----------------
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY     = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY     = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET        = os.getenv("MINIO_BUCKET", "stock-data")
DEFAULT_REF_PREFIX    = "raw/reference"
DEFAULT_MARKET_PREFIX = "raw/market"

# Rate-limit tuning
OHLC_PER_REQ_DELAY = 0.33      # ~240 req/min cho Sponsor tier
WAIT_TIME_ON_ERROR  = 65         # Seconds to wait on rate-limit / timeout
BATCH_LOG_SIZE      = 50         # Print progress every N symbols


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

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
    file_size = fs.size(s3_path)
    size_kb = file_size / 1024 if file_size is not None else 0
    logger.info(f"✅ Uploaded s3://{s3_path} ({size_kb:.1f} KB, {len(df):,} rows)")


def append_parquet_to_s3(
    df_new: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    dedup_keys: list[str] = None,
) -> None:
    """
    Append df_new vào file Parquet đã có trên MinIO.
    Nếu file chưa tồn tại → ghi mới bình thường.
    Deduplicate theo dedup_keys (mặc định: ['symbol', 'time']) để tránh trùng row.
    Row mới (df_new) được ưu tiên giữ lại khi trùng key.
    """
    if df_new is None or df_new.empty:
        logger.warning(f"New DataFrame is empty, nothing to append for {s3_path}")
        return

    if dedup_keys is None:
        dedup_keys = ["symbol", "time"]

    # Stamp ingested_at cho rows MỚI trước khi concat
    # → rows cũ sẽ giữ nguyên ingested_at gốc của chúng
    df_new = _add_ingested_at(df_new)

    # Đọc file cũ nếu tồn tại
    if fs.exists(s3_path):
        try:
            with fs.open(s3_path, "rb") as f:
                df_old = pd.read_parquet(f)
            logger.info(f"Read existing file: {len(df_old):,} rows from s3://{s3_path}")
            # Concat: new trước, old sau → drop_duplicates giữ new
            df_combined = pd.concat([df_new, df_old], ignore_index=True)
        except Exception as e:
            logger.error(f"Cannot read existing parquet ({e}), will overwrite with new data.")
            df_combined = df_new
    else:
        logger.info(f"File not found, creating new: s3://{s3_path}")
        df_combined = df_new

    # Deduplicate: giữ row đầu tiên (= row mới) khi trùng key
    before = len(df_combined)
    valid_keys = [k for k in dedup_keys if k in df_combined.columns]
    if valid_keys:
        df_combined = df_combined.drop_duplicates(subset=valid_keys, keep="first")
    after = len(df_combined)
    if before != after:
        logger.info(f"Dedup: {before - after:,} duplicate rows removed → {after:,} rows kept")

    # KHÔNG gọi _add_ingested_at ở đây — rows cũ giữ nguyên timestamp gốc
    with fs.open(s3_path, "wb") as f:
        df_combined.to_parquet(f, engine="pyarrow", index=False, compression="snappy")

    file_size = fs.size(s3_path)
    size_kb = file_size / 1024 if file_size is not None else 0
    logger.info(f"✅ Appended → s3://{s3_path} ({size_kb:.1f} KB, {after:,} rows total)")


def read_symbols_from_equity_parquet(fs: s3fs.S3FileSystem, equity_s3_path: str) -> list[str]:
    """Read symbol list from raw/reference/equity/equity.parquet on MinIO."""
    if not fs.exists(equity_s3_path):
        logger.error(f"Missing equity parquet: s3://{equity_s3_path}")
        return []

    try:
        with fs.open(equity_s3_path, "rb") as f:
            df_equity = pd.read_parquet(f)
    except Exception as e:
        logger.error(f"Cannot read equity parquet: {e}")
        return []

    if "symbol" not in df_equity.columns:
        logger.error("equity.parquet has no 'symbol' column.")
        return []

    symbols = (
        df_equity["symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    logger.info(f"Loaded {len(symbols)} symbols from s3://{equity_s3_path}")
    return symbols


# ------------------------------------------------------------------ #
# OHLC Fetching                                                       #
# ------------------------------------------------------------------ #

def fetch_ohlc_with_retry(symbol: str, start: str, end: str) -> pd.DataFrame | None:
    """
    Fetch ohlcv() for a symbol over [start, end] with infinite retry on
    rate-limit / timeout errors.
    Returns None if the symbol has no data (not an error).

    NOTE: vnstock gọi sys.exit() khi rate-limit → cần catch SystemExit.
    """
    while True:
        try:
            mkt = Market()
            df = mkt.equity(symbol).ohlcv(start=start, end=end)
            if df is not None and not df.empty:
                df.insert(0, "symbol", symbol)  # prepend symbol column
            return df
        except SystemExit:
            # vnstock calls sys.exit() on rate-limit exceeded → retry after backoff
            logger.warning(
                f"{symbol} — Rate-limit (SystemExit). "
                f"Waiting {WAIT_TIME_ON_ERROR}s before retry..."
            )
            time.sleep(WAIT_TIME_ON_ERROR)
        except Exception as e:
            err_str = str(e).lower()
            if any(kw in err_str for kw in ("rate", "limit", "timeout", "429", "503")):
                logger.warning(
                    f"{symbol} — Rate-limit/Timeout: {e}. "
                    f"Waiting {WAIT_TIME_ON_ERROR}s before retry..."
                )
                time.sleep(WAIT_TIME_ON_ERROR)
            else:
                logger.error(f"{symbol} — Unexpected error: {e}. Skipping.")
                return None


def fetch_ohlc_all_symbols(
    symbols: list[str],
    start: str,
    end: str,
) -> pd.DataFrame:
    """
    Iterate through symbols, fetch OHLC, concatenate into one DataFrame.
    Logs progress every BATCH_LOG_SIZE symbols.
    """
    total = len(symbols)
    results: list[pd.DataFrame] = []
    empty_count = 0
    error_count = 0

    logger.info(f"Fetching OHLC [{start} → {end}] for {total} symbols...")

    for i, symbol in enumerate(symbols, start=1):
        df = fetch_ohlc_with_retry(symbol, start, end)

        if df is None:
            error_count += 1
        elif df.empty:
            logger.debug(f"[{i}/{total}] {symbol} — no data in range, skipped.")
            empty_count += 1
        else:
            results.append(df)

        time.sleep(OHLC_PER_REQ_DELAY)

        if i % BATCH_LOG_SIZE == 0 or i == total:
            logger.info(
                f"Progress: {i}/{total} | "
                f"Collected: {len(results)} symbols | "
                f"Empty: {empty_count} | Errors: {error_count}"
            )

    if not results:
        return pd.DataFrame()

    df_all = pd.concat(results, ignore_index=True)
    logger.info(f"Total rows: {len(df_all):,} from {len(results)} symbols.")
    return df_all


# ------------------------------------------------------------------ #
# CLI                                                                 #
# ------------------------------------------------------------------ #

def parse_args():
    p = argparse.ArgumentParser(description="Fetch VNStock OHLC history to MinIO S3.")
    p.add_argument("--bucket",        default=DEFAULT_BUCKET,        help="MinIO bucket name")
    p.add_argument("--ref-prefix",    default=DEFAULT_REF_PREFIX,    help="Prefix for reference data (equity.parquet)")
    p.add_argument("--market-prefix", default=DEFAULT_MARKET_PREFIX, help="Prefix for output market data")
    p.add_argument("--start",         default="2026-01-01",          help="Start date (YYYY-MM-DD)")
    p.add_argument("--end",           default="2026-03-31",          help="End date (YYYY-MM-DD)")
    p.add_argument("--append",        action="store_true",           help="Ghi tiếp vào file cũ, deduplicate theo (symbol, time)")
    return p.parse_args()


def main():
    args = parse_args()

    ref_prefix    = f"{args.bucket}/{args.ref_prefix}"     # e.g. stock-data/raw/reference
    market_prefix = f"{args.bucket}/{args.market_prefix}"  # e.g. stock-data/raw/market
    now           = datetime.now(ICT)

    print("=" * 80)
    print("Fetch VNStock OHLC History to MinIO S3")
    print("=" * 80)
    print(f"MinIO Endpoint    : {MINIO_ENDPOINT}")
    print(f"Reference source  : s3://{ref_prefix}/equity/equity.parquet")
    print(f"Target output     : s3://{market_prefix}/equity/ohlc.parquet")
    print(f"Date range        : {args.start}  →  {args.end}")
    print(f"Mode              : {'append' if args.append else 'write (skip if exists)'}")
    print(f"Run at            : {now.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print("=" * 80)

    # --- Connect to MinIO via s3fs ---
    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    # --- Read symbols from reference parquet ---
    equity_s3_path = f"{ref_prefix}/equity/equity.parquet"
    symbols = read_symbols_from_equity_parquet(fs, equity_s3_path)

    if not symbols:
        logger.error("No symbols loaded — aborting.")
        return

    # --- Skip nếu file đã tồn tại và không dùng --append ---
    ohlc_s3_path = f"{market_prefix}/equity/ohlc.parquet"
    if not args.append and fs.exists(ohlc_s3_path):
        logger.info(f"Exists, skipping: s3://{ohlc_s3_path}  (use --append to merge)")
        return

    # --- Fetch OHLC ---
    df_ohlc = fetch_ohlc_all_symbols(symbols, start=args.start, end=args.end)

    # --- Upload ---
    if args.append:
        append_parquet_to_s3(df_ohlc, fs, ohlc_s3_path)
    else:
        write_parquet_to_s3(df_ohlc, fs, ohlc_s3_path)

    print("\n" + "=" * 80)
    logger.info("🎉 OHLC ingestion complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()
