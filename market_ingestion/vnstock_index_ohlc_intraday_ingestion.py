#!/usr/bin/env python3
"""
vnstock_index_ohlc_intraday_ingestion.py

Fetch intraday OHLCV for benchmark indices using:
  mkt.index(symbol).ohlcv(start, end, interval="1m")

Indices mặc định: VNINDEX, VN30, HNXINDEX, HNX30.

Target structure in MinIO (Hive-style partition theo ngày):
  raw/market/index/year=YYYY/month=MM/day=DD/ohlc.parquet

Behavior:
 - Default interval: 1m (minute-bar); configurable via --interval.
 - Default date range: today (ICT) → today.
 - Danh sách indices: dùng BENCHMARK_INDICES mặc định hoặc --indices flag.
 - Skip existing partition files unless --append.
 - Retry on rate-limit / timeout (including SystemExit from vnstock).
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
logger = logging.getLogger("index_ohlc_intraday_ingestion")
ICT = timezone(timedelta(hours=7))

vnstock_key = os.getenv("VNSTOCK_API_KEY")
if vnstock_key:
    os.environ["VNSTOCK_API_KEY"] = vnstock_key
    logger.info(f"VNStock API Key ('{vnstock_key[:4]}***') found and configured (Sponsor tier active).")
else:
    logger.warning("No VNSTOCK_API_KEY found in environment or .env, running on Community tier.")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")
DEFAULT_MARKET_PREFIX = "raw/market"

BENCHMARK_INDICES: tuple[str, ...] = ("VNINDEX", "VN30", "HNXINDEX", "HNX30")

OHLC_PER_REQ_DELAY = 0.5
WAIT_TIME_ON_ERROR = 65
BATCH_LOG_SIZE = 4


# ------------------------------------------------------------------ #
# Helpers                                                             #
# ------------------------------------------------------------------ #

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


def _df_with_partition_date(df: pd.DataFrame) -> pd.DataFrame | None:
    """Thêm cột _partition_date (date only) để Hive-style partition YYYY/MM/DD."""
    if df is None or df.empty:
        return None
    time_col = next((c for c in ("time", "date", "tradingDate") if c in df.columns), None)
    if time_col is None:
        logger.error("OHLCV DataFrame has no time/date column for partitioning.")
        return None
    out = df.copy()
    dt = pd.to_datetime(out[time_col], errors="coerce")
    if getattr(dt.dtype, "tz", None) is not None:
        dt = dt.dt.tz_convert(ICT)
    out["_partition_date"] = dt.dt.normalize()
    out = out.loc[out["_partition_date"].notna()]
    if out.empty:
        logger.warning("No rows with valid timestamps after parsing; nothing to write.")
        return None
    return out


def write_parquet_to_s3(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str) -> None:
    """Ghi mới parquet lên MinIO. Bỏ qua nếu file đã tồn tại (dùng --append để gộp)."""
    if df is None or df.empty:
        logger.warning(f"DataFrame empty, skipping: {s3_path}")
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
    """
    Gộp df_new vào file parquet đã có trên MinIO.
    Row mới (df_new) được ưu tiên giữ khi trùng dedup_keys.
    """
    if df_new is None or df_new.empty:
        logger.warning(f"New DataFrame empty, nothing to append for {s3_path}")
        return

    if dedup_keys is None:
        dedup_keys = ["symbol", "time"]

    df_new = _add_ingested_at(df_new)

    if fs.exists(s3_path):
        try:
            with fs.open(s3_path, "rb") as fh:
                df_old = pd.read_parquet(fh)
            logger.info(f"Read existing: {len(df_old):,} rows from s3://{s3_path}")
            df_combined = pd.concat([df_new, df_old], ignore_index=True)
        except Exception as e:
            logger.error(f"Cannot read existing parquet ({e}), overwriting with new data.")
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
        logger.info(f"Dedup: {before - after:,} rows removed → {after:,} kept")

    _write_parquet_and_log(df_combined, fs, s3_path, success_prefix="Appended ->")


def write_ohlc_partitioned_parquet(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    market_prefix: str,
    segment: str,
    *,
    append: bool,
) -> None:
    """
    Ghi parquet theo Hive-style partition:
      {market_prefix}/{segment}/year=YYYY/month=MM/day=DD/ohlc.parquet
    """
    prep = _df_with_partition_date(df)
    if prep is None:
        return
    dedup_keys = ["symbol", "time"]
    n_parts = 0
    for part_val, group in prep.groupby("_partition_date", sort=True):
        ts = pd.Timestamp(part_val)
        y, mo, d = ts.year, ts.month, ts.day
        s3_path = (
            f"{market_prefix}/{segment}"
            f"/year={y:04d}/month={mo:02d}/day={d:02d}/ohlc.parquet"
        )
        g = group.drop(columns=["_partition_date"])
        if append:
            append_parquet_to_s3(g, fs, s3_path, dedup_keys=dedup_keys)
        else:
            write_parquet_to_s3(g, fs, s3_path)
        n_parts += 1
    logger.info(
        f"Partition write done: {n_parts} day partition(s) "
        f"under {segment}/year=YYYY/month=MM/day=DD/ohlc.parquet"
    )


# ------------------------------------------------------------------ #
# Fetching                                                            #
# ------------------------------------------------------------------ #

def fetch_index_ohlcv_with_retry(
    symbol: str, start: str, end: str, interval: str
) -> pd.DataFrame | None:
    """
    Gọi mkt.index(symbol).ohlcv(start, end, interval) với retry vô hạn
    khi gặp rate-limit / timeout.
    """
    while True:
        try:
            mkt = Market()
            df = mkt.index(symbol).ohlcv(start=start, end=end, interval=interval)
            if df is not None and not df.empty:
                if "symbol" not in df.columns:
                    df = df.copy()
                    df.insert(0, "symbol", symbol)
                else:
                    df = df.copy()
                    df["symbol"] = df["symbol"].where(
                        df["symbol"].notna() & (df["symbol"].astype(str).str.strip() != ""),
                        symbol,
                    )
                    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
            return df
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


def fetch_index_ohlcv_all(
    indices: list[str],
    start: str,
    end: str,
    interval: str,
) -> pd.DataFrame:
    total = len(indices)
    results: list[pd.DataFrame] = []
    empty_count = 0
    error_count = 0

    logger.info(f"Fetching index OHLCV [{interval}] [{start} → {end}] for {total} indices: {indices}")

    for i, symbol in enumerate(indices, start=1):
        df = fetch_index_ohlcv_with_retry(symbol, start, end, interval)

        if df is None:
            error_count += 1
        elif df.empty:
            logger.debug(f"[{i}/{total}] {symbol} — no data in range, skipped.")
            empty_count += 1
        else:
            results.append(df)
            logger.info(f"[{i}/{total}] {symbol} — {len(df):,} rows fetched.")

        time.sleep(OHLC_PER_REQ_DELAY)

        if i % BATCH_LOG_SIZE == 0 or i == total:
            logger.info(
                f"Progress: {i}/{total} | Collected: {len(results)} | "
                f"Empty: {empty_count} | Errors: {error_count}"
            )

    if not results:
        return pd.DataFrame()

    df_all = pd.concat(results, ignore_index=True)
    logger.info(f"Total rows: {len(df_all):,} from {len(results)} indices.")
    return df_all


# ------------------------------------------------------------------ #
# CLI                                                                 #
# ------------------------------------------------------------------ #

def parse_args():
    today_ict = datetime.now(ICT).strftime("%Y-%m-%d")
    p = argparse.ArgumentParser(
        description="Fetch VNStock intraday index OHLCV → MinIO S3 (Parquet, partitioned by date)."
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket name")
    p.add_argument("--market-prefix", default=DEFAULT_MARKET_PREFIX, help="Prefix for output market data")
    p.add_argument("--start", default=today_ict, help="Start date (YYYY-MM-DD)")
    p.add_argument("--end", default=today_ict, help="End date (YYYY-MM-DD)")
    p.add_argument(
        "--interval", default="1m",
        help="OHLCV candle interval: 1m, 5m, 15m, 30m, 1h, 1D, … (default: 1m)"
    )
    p.add_argument(
        "--indices", nargs="+", default=list(BENCHMARK_INDICES),
        help=(
            "Danh sách chỉ số cần lấy (space-separated). "
            f"Mặc định: {' '.join(BENCHMARK_INDICES)}"
        ),
    )
    p.add_argument(
        "--append", action="store_true",
        help="Gộp vào parquet đã có theo từng ngày; deduplicate theo (symbol, time)."
    )
    return p.parse_args()


def log_run_info(args: argparse.Namespace, market_prefix: str, indices: list[str]) -> None:
    separator = "=" * 80
    mode = "append" if args.append else "write (skip if exists)"
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nFetch VNStock Index Intraday OHLCV → MinIO S3 (Parquet, partitioned)\n%s\n"
        "MinIO Endpoint    : %s\n"
        "Interval          : %s\n"
        "Date range        : %s  →  %s\n"
        "Indices           : %s\n"
        "Target output     : s3://%s/index/year=YYYY/month=MM/day=DD/ohlc.parquet\n"
        "Mode              : %s\n"
        "Run at            : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.interval,
        args.start, args.end,
        ", ".join(indices),
        market_prefix,
        mode,
        run_at,
        separator,
    )


def main():
    args = parse_args()
    market_prefix = f"{args.bucket}/{args.market_prefix}"
    indices = [s.strip().upper() for s in args.indices if s.strip()]
    log_run_info(args, market_prefix, indices)

    if not indices:
        logger.error("No indices specified — aborting.")
        return

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    df_ohlc = fetch_index_ohlcv_all(
        indices, start=args.start, end=args.end, interval=args.interval
    )

    write_ohlc_partitioned_parquet(
        df_ohlc,
        fs,
        market_prefix,
        "index",
        append=args.append,
    )

    separator = "=" * 80
    logger.info("\n%s\nIndex intraday OHLCV ingestion complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
