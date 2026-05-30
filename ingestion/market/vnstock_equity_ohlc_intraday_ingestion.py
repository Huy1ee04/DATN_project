#!/usr/bin/env python3
"""
vnstock_equity_ohlc_intraday_ingestion.py

Fetch intraday OHLCV for equity symbols using:
  mkt.equity(symbol).ohlcv(start, end, interval="1m")

Target structure in MinIO (partitioned by năm/tháng/ngày của cột ``time``):
  raw/market/equity/ohlc_{interval}/YYYY/MM/DD/ohlc.parquet

Behavior:
 - Default interval: 1m (minute-bar); configurable via --interval.
 - Default date range: today (ICT) → today.
 - Symbol source (theo thứ tự ưu tiên):
     1. --symbols flag (truyền thẳng qua CLI)
     2. Reference().equity.list_by_exchange() lọc theo --exchange / --instrument-type
     3. Fallback: raw/reference/equity/equity.csv trên MinIO (nếu API lỗi)
 - Skip existing partition files unless --append.
 - Retry on rate-limit / timeout (including SystemExit from vnstock).
"""

import os
import time
import logging
import argparse
from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
import s3fs
from dotenv import load_dotenv
from vtit_gx.polars.gx_schema_validity import gx_check_columns_to_match_set

_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
    os.path.join(_script_dir, "..", "reference", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

from vnstock_data import Market, Reference

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("equity_ohlc_intraday_ingestion")
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

# Intraday có nhiều row hơn → delay cao hơn một chút để tránh rate-limit
OHLC_PER_REQ_DELAY = 0.5
WAIT_TIME_ON_ERROR = 65
BATCH_LOG_SIZE = 20
OHLC_INTRADAY_EXPECTED_COLUMNS = (
    "symbol",
    "time",
    "open",
    "high",
    "low",
    "close",
    "volume",
)


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
    """Thêm cột _partition_date (date only) để hive-style partition YYYY/MM/DD."""
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


def _normalize_symbols(series: pd.Series) -> list[str]:
    """Chuẩn hóa Series mã chứng khoán: strip, upper, bỏ NA/rỗng, dedup."""
    return (
        series.astype(str)
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )


def fetch_exchange_symbols(exchange: str = "HOSE", instrument_type: str = "STOCK") -> list[str]:
    """
    Lấy danh sách mã từ Reference API, lọc theo exchange + type.
    Trả về list rỗng nếu API gặp lỗi.
    """
    try:
        df_all = Reference().equity.list_by_exchange()
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




# ------------------------------------------------------------------ #
# Fetching                                                            #
# ------------------------------------------------------------------ #

def fetch_ohlcv_intraday_with_retry(
    symbol: str, start: str, end: str, interval: str
) -> pd.DataFrame | None:
    """
    Gọi mkt.equity(symbol).ohlcv(start, end, interval) với retry vô hạn
    khi gặp rate-limit / timeout.
    """
    while True:
        try:
            mkt = Market()
            df = mkt.equity(symbol).ohlcv(start=start, end=end, interval=interval)
            if df is not None and not df.empty:
                df = df.copy()
                if "symbol" not in df.columns:
                    df.insert(0, "symbol", symbol)
                else:
                    blank = df["symbol"].isna() | (df["symbol"].astype(str).str.strip() == "")
                    df["symbol"] = df["symbol"].where(~blank, symbol)
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


def fetch_ohlcv_intraday_all_symbols(
    symbols: list[str],
    start: str,
    end: str,
    interval: str,
) -> pd.DataFrame:
    total = len(symbols)
    results: list[pd.DataFrame] = []
    empty_count = 0
    error_count = 0

    logger.info(f"Fetching intraday OHLCV [{interval}] [{start} → {end}] for {total} symbols...")

    for i, symbol in enumerate(symbols, start=1):
        df = fetch_ohlcv_intraday_with_retry(symbol, start, end, interval)

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
                f"Progress: {i}/{total} | Collected: {len(results)} | "
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
    today_ict = datetime.now(ICT).strftime("%Y-%m-%d")
    p = argparse.ArgumentParser(
        description="Fetch VNStock intraday equity OHLCV → MinIO S3 (Parquet, partitioned by date)."
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
        "--symbols", nargs="+",
        help="Override symbol list (space-separated). Skips API/equity.csv lookup if set."
    )
    p.add_argument(
        "--exchange", default="HOSE",
        help="Sàn giao dịch để lọc từ Reference API: HOSE, HNX, UPCOM (default: HOSE)."
    )
    p.add_argument(
        "--instrument-type", default="STOCK", dest="instrument_type",
        help="Loại chứng khoán: STOCK, ETF, … (default: STOCK)."
    )
    p.add_argument(
        "--append", action="store_true",
        help="Gộp vào parquet đã có theo từng ngày; deduplicate theo (symbol, time)."
    )
    return p.parse_args()


def log_run_info(args: argparse.Namespace, market_prefix: str) -> None:
    separator = "=" * 80
    symbol_source = (
        "--symbols flag" if args.symbols
        else f"Reference API (exchange={args.exchange}, type={args.instrument_type})"
    )
    mode = "append" if args.append else "write (skip if exists)"
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nFetch VNStock Equity Intraday OHLCV → MinIO S3 (Parquet, partitioned)\n%s\n"
        "MinIO Endpoint    : %s\n"
        "Interval          : %s\n"
        "Date range        : %s  →  %s\n"
        "Symbol source     : %s\n"
        "Target output     : s3://%s/equity/year=YYYY/month=MM/day=DD/ohlc.parquet\n"
        "Mode              : %s\n"
        "Run at            : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.interval,
        args.start, args.end,
        symbol_source,
        market_prefix,
        mode,
        run_at,
        separator,
    )


def main():
    args = parse_args()
    market_prefix = f"{args.bucket}/{args.market_prefix}"
    log_run_info(args, market_prefix)

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    if args.symbols:
        # Ưu tiên danh sách truyền thẳng qua CLI
        symbols = [s.strip().upper() for s in args.symbols if s.strip()]
        logger.info(
            f"Using {len(symbols)} symbol(s) from --symbols: "
            f"{symbols[:10]}{'...' if len(symbols) > 10 else ''}"
        )
    else:
        symbols = fetch_exchange_symbols(
            exchange=args.exchange,
            instrument_type=args.instrument_type,
        )

    if not symbols:
        logger.error("No symbols to process — aborting.")
        return

    df_ohlc = fetch_ohlcv_intraday_all_symbols(
        symbols, start=args.start, end=args.end, interval=args.interval
    )

    if not df_ohlc.empty:
        gx_check_columns_to_match_set(
            pl.from_pandas(df_ohlc),
            {"column_set": list(OHLC_INTRADAY_EXPECTED_COLUMNS), "exact_match": True},
        )

    write_ohlc_partitioned_parquet(
        df_ohlc,
        fs,
        market_prefix,
        "equity",
        append=args.append,
    )

    separator = "=" * 80
    logger.info("\n%s\nIntraday equity OHLCV ingestion complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
