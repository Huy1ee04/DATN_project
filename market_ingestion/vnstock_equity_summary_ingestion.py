#!/usr/bin/env python3
"""
vnstock_equity_summary_ingestion.py

Fetch Market().equity(symbol).summary() for all symbols in
raw/reference/equity/equity.parquet and save to MinIO S3 as one Parquet file.

Target structure in MinIO:
  raw/market/
    equity/summary.parquet

Behavior:
 - By default skips run if summary.parquet already exists (no --append).
 - Use --append to merge with existing file, deduplicated by symbol (new rows win).
 - Skips symbols that return empty / no data.
 - Retries on rate-limit / timeout with same backoff pattern as OHLC ingestion.
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
logger = logging.getLogger("equity_summary_ingestion")
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
DEFAULT_REF_PREFIX = "raw/reference"
DEFAULT_MARKET_PREFIX = "raw/market"

SUMMARY_PER_REQ_DELAY = 0.33
WAIT_TIME_ON_ERROR = 65
BATCH_LOG_SIZE = 50


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
    with fs.open(s3_path, "wb") as fh:
        df.to_parquet(fh, engine="pyarrow", index=False, compression="snappy")
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"{success_prefix} s3://{s3_path} ({size_kb:.1f} KB, {len(df):,} rows)")


def write_parquet_to_s3(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str) -> None:
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
    dedup_keys: list[str] | None = None,
) -> None:
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

    _write_parquet_and_log(df_combined, fs, s3_path, success_prefix="Appended ->")


def read_symbols_from_equity_parquet(fs: s3fs.S3FileSystem, equity_s3_path: str) -> list[str]:
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


def _normalize_summary_to_df(symbol: str, summary) -> pd.DataFrame | None:
    """
    Chuẩn hóa kết quả summary() thành DataFrame có cột ``symbol``.

    Kiểm tra thư viện (vnstock_data / KBS): ``summary()`` trả về ``pd.DataFrame([payload])``;
    mã thường gắn ở ``df.attrs['symbol']``, payload JSON có thể *không* có cột ``symbol``.
    Nếu API đã có cột ``symbol`` thì chỉ chuẩn hóa chữ hoa / điền ô trống bằng mã request.
    """
    if summary is None:
        return None
    if isinstance(summary, pd.Series):
        df = summary.to_frame().T.reset_index(drop=True)
    elif isinstance(summary, pd.DataFrame):
        if summary.empty:
            return None
        df = summary.copy().reset_index(drop=True)
    else:
        try:
            df = pd.DataFrame(summary)
        except (TypeError, ValueError):
            return None
        if df.empty:
            return None

    if "symbol" not in df.columns:
        attrs = getattr(df, "attrs", None) or {}
        attr_sym = attrs.get("symbol") if isinstance(attrs, dict) else None
        if attr_sym is not None and str(attr_sym).strip():
            use_sym = str(attr_sym).strip().upper()
        else:
            use_sym = str(symbol).strip().upper()
        df.insert(0, "symbol", use_sym)
    else:
        df["symbol"] = df["symbol"].where(df["symbol"].notna() & (df["symbol"].astype(str).str.strip() != ""), symbol)
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()

    return df


def fetch_summary_with_retry(symbol: str) -> pd.DataFrame | None:
    """
    Fetch equity(symbol).summary() with retry on rate-limit / timeout.
    Returns None if no data or hard error.
    """
    while True:
        try:
            mkt = Market()
            raw = mkt.equity(symbol).summary()
            return _normalize_summary_to_df(symbol, raw)
        except SystemExit:
            logger.warning(
                f"{symbol} — Rate-limit (SystemExit). Waiting {WAIT_TIME_ON_ERROR}s before retry..."
            )
            time.sleep(WAIT_TIME_ON_ERROR)
        except Exception as e:
            err_str = str(e).lower()
            if any(kw in err_str for kw in ("rate", "limit", "timeout", "429", "503")):
                logger.warning(
                    f"{symbol} — Rate-limit/Timeout: {e}. Waiting {WAIT_TIME_ON_ERROR}s before retry..."
                )
                time.sleep(WAIT_TIME_ON_ERROR)
            else:
                logger.error(f"{symbol} — Unexpected error: {e}. Skipping.")
                return None


def fetch_summary_all_symbols(symbols: list[str]) -> pd.DataFrame:
    total = len(symbols)
    results: list[pd.DataFrame] = []
    empty_count = 0
    error_count = 0

    logger.info(f"Fetching equity summary() for {total} symbols...")

    for i, symbol in enumerate(symbols, start=1):
        df = fetch_summary_with_retry(symbol)

        if df is None:
            error_count += 1
        elif df.empty:
            logger.debug(f"[{i}/{total}] {symbol} — empty summary, skipped.")
            empty_count += 1
        else:
            results.append(df)

        time.sleep(SUMMARY_PER_REQ_DELAY)

        if i % BATCH_LOG_SIZE == 0 or i == total:
            logger.info(
                f"Progress: {i}/{total} | Collected: {len(results)} | Empty: {empty_count} | Errors: {error_count}"
            )

    if not results:
        return pd.DataFrame()

    df_all = pd.concat(results, ignore_index=True)
    logger.info(f"Total rows: {len(df_all):,} from {len(results)} symbols.")
    return df_all


def parse_args():
    p = argparse.ArgumentParser(description="Fetch VNStock Market.equity().summary() to MinIO S3.")
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket name")
    p.add_argument("--ref-prefix", default=DEFAULT_REF_PREFIX, help="Prefix for reference data (equity.parquet)")
    p.add_argument("--market-prefix", default=DEFAULT_MARKET_PREFIX, help="Prefix for output market data")
    p.add_argument(
        "--append",
        action="store_true",
        help="Merge into existing summary.parquet; deduplicate by symbol (new wins)",
    )
    return p.parse_args()


def log_run_info(args: argparse.Namespace, ref_prefix: str, market_prefix: str) -> None:
    separator = "=" * 80
    mode = "append" if args.append else "write (skip if exists)"
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nFetch VNStock equity summary() to MinIO S3\n%s\n"
        "MinIO Endpoint    : %s\n"
        "Reference source  : s3://%s/equity/equity.parquet\n"
        "Target output     : s3://%s/equity/summary.parquet\n"
        "Mode              : %s\n"
        "Run at            : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        ref_prefix,
        market_prefix,
        mode,
        run_at,
        separator,
    )


def main():
    args = parse_args()
    ref_prefix = f"{args.bucket}/{args.ref_prefix}"
    market_prefix = f"{args.bucket}/{args.market_prefix}"
    log_run_info(args, ref_prefix, market_prefix)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    equity_s3_path = f"{ref_prefix}/equity/equity.parquet"
    symbols = read_symbols_from_equity_parquet(fs, equity_s3_path)

    if not symbols:
        logger.error("No symbols loaded — aborting.")
        return

    summary_s3_path = f"{market_prefix}/equity/summary.parquet"
    if not args.append and fs.exists(summary_s3_path):
        logger.info(f"Exists, skipping: s3://{summary_s3_path}  (use --append to merge)")
        return

    df_summary = fetch_summary_all_symbols(symbols)

    if args.append:
        append_parquet_to_s3(df_summary, fs, summary_s3_path, dedup_keys=["symbol"])
    else:
        write_parquet_to_s3(df_summary, fs, summary_s3_path)

    separator = "=" * 80
    logger.info("\n%s\nEquity summary ingestion complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
