#!/usr/bin/env python3
"""
vnstock_company_info_ingestion.py

Fetch Reference().company(symbol).info() for each symbol from Reference API,
concatenate rows, and save one Parquet file to:
raw/reference/company/info/info.parquet on MinIO.
"""

import os
import time
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
logger = logging.getLogger("company_info_ingestion")
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

COMPANY_BATCH_SIZE    = 50
COMPANY_PER_REQ_DELAY = 0.33
WAIT_TIME_ON_ERROR    = 65
MAX_RETRIES           = 5


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

    _write_parquet_and_log(df_combined, fs, s3_path, success_prefix="Appended ->")


def _normalize_symbols(series: pd.Series) -> list[str]:
    """Chuẩn hóa Series mã: strip, upper, bỏ NA/rỗng, dedup."""
    return (
        series.astype(str).str.strip().str.upper()
        .replace("", pd.NA).dropna().drop_duplicates().tolist()
    )


def fetch_exchange_symbols(
    ref: Reference,
    exchange: str = "HOSE",
    instrument_type: str = "STOCK",
) -> list[str]:
    """Lấy danh sách mã từ Reference API, lọc theo exchange + type."""
    try:
        df_all = ref.equity.list_by_exchange()
        if df_all is None or df_all.empty:
            logger.warning("list_by_exchange() returned empty DataFrame.")
            return []
        mask = pd.Series([True] * len(df_all), index=df_all.index)
        if "exchange" in df_all.columns:
            mask &= df_all["exchange"].astype(str).str.upper() == exchange.upper()
        else:
            logger.warning("Column 'exchange' not found; skipping exchange filter.")
        if "type" in df_all.columns:
            mask &= df_all["type"].astype(str).str.upper() == instrument_type.upper()
        else:
            logger.warning("Column 'type' not found; skipping type filter.")
        df_filtered = df_all[mask].reset_index(drop=True)
        if "symbol" not in df_filtered.columns:
            logger.error("Column 'symbol' not found in list_by_exchange() result.")
            return []
        symbols = _normalize_symbols(df_filtered["symbol"])
        logger.info(f"Fetched {len(symbols)} symbols from Reference API (exchange={exchange}, type={instrument_type})")
        return symbols
    except Exception as e:
        logger.error(f"fetch_exchange_symbols() failed: {e}")
        return []


def fetch_company_info_with_retry(
    ref: Reference,
    symbol: str,
    max_retries: int = MAX_RETRIES,
) -> pd.DataFrame:
    for attempt in range(1, max_retries + 1):
        try:
            return ref.company(symbol).info()
        except Exception as e:
            if attempt == max_retries:
                logger.error(
                    f"{symbol} - Failed after {max_retries} attempts: {e}. Skipping symbol."
                )
                break
            logger.warning(
                f"{symbol} - Attempt {attempt}/{max_retries} failed: {e}. "
                f"Waiting {WAIT_TIME_ON_ERROR} seconds before retry..."
            )
            time.sleep(WAIT_TIME_ON_ERROR)
    return pd.DataFrame()


def fetch_company_info_concat(
    ref: Reference,
    symbols: list[str],
    max_retries: int = MAX_RETRIES,
) -> pd.DataFrame:
    total = len(symbols)
    results: list[pd.DataFrame] = []
    empty_count = 0

    logger.info(f"Fetching company.info() for {total} symbols...")

    for i, symbol in enumerate(symbols, start=1):
        df = fetch_company_info_with_retry(ref, symbol, max_retries=max_retries)
        if df is not None and not df.empty:
            results.append(df)
        else:
            logger.warning(f"[{i}/{total}] {symbol} returned empty info(), skipped.")
            empty_count += 1

        time.sleep(COMPANY_PER_REQ_DELAY)

        if i % COMPANY_BATCH_SIZE == 0 or i == total:
            logger.info(
                f"Progress: {i}/{total} | Rows collected: {len(results)} | Empty: {empty_count}"
            )

    if not results:
        return pd.DataFrame()

    df_all = pd.concat(results, ignore_index=True)
    logger.info(f"Concatenated {len(df_all):,} rows from {len(results)} symbols.")
    return df_all


def log_run_info(args: argparse.Namespace, info_s3_path: str) -> None:
    separator = "=" * 80
    mode = "append" if args.append else "write (skip if exists)"
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nFetch VNStock Company Info to MinIO S3\n%s\n"
        "MinIO Endpoint    : %s\n"
        "Symbol source     : Reference API (exchange=%s, type=%s)\n"
        "Target output     : s3://%s\n"
        "Mode              : %s\n"
        "Run at            : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.exchange, args.instrument_type,
        info_s3_path,
        mode,
        run_at,
        separator,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch VNStock Company Info to MinIO S3.")
    parser.add_argument("--bucket",  default=DEFAULT_BUCKET,    help="MinIO bucket name")
    parser.add_argument("--prefix",  default=DEFAULT_S3_PREFIX, help="Path prefix inside bucket")
    parser.add_argument("--exchange", default="HOSE",           help="Sàn lọc từ Reference API: HOSE, HNX, UPCOM (default: HOSE)")
    parser.add_argument("--instrument-type", default="STOCK", dest="instrument_type",
                        help="Loại CK: STOCK, ETF, … (default: STOCK)")
    parser.add_argument("--append",  action="store_true",       help="Ghi tiếp vào file cũ, deduplicate theo symbol")
    args = parser.parse_args()

    prefix = f"{args.bucket}/{args.prefix}"
    info_s3_path = f"{prefix}/company/info/info.parquet"
    log_run_info(args, info_s3_path)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    ref = Reference()
    symbols = fetch_exchange_symbols(
        ref,
        exchange=args.exchange,
        instrument_type=args.instrument_type,
    )
    if not symbols:
        logger.error("No symbols loaded — aborting.")
        return

    df_company = fetch_company_info_concat(ref, symbols)
    if args.append:
        append_parquet_to_s3(df_company, fs, info_s3_path, dedup_keys=["symbol"])
    else:
        write_parquet_to_s3(df_company, fs, info_s3_path)

    separator = "=" * 80
    logger.info("\n%s\nCompany info ingestion complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
