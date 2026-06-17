#!/usr/bin/env python3
"""
vnstock_equity_summary_ingestion.py

Fetch Market().equity(symbol).summary() for HOSE/STOCK symbols and write to MinIO.

Target (partitioned by date, co-located with ohlc.parquet):
  raw/market/equity/year=YYYY/month=MM/day=DD/summary.parquet

Each row includes ``date`` = trading date (YYYY-MM-DD). When run from Airflow,
pass ``--date`` (typically ``next_ds``). Without ``--date``, defaults to today (ICT).
Uses --append to merge with existing file in the same partition, dedup by symbol.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import polars as pl
import s3fs
from dotenv import load_dotenv
from vnstock_data import Market
from vtit_gx.polars.gx_schema_validity import gx_check_columns_to_match_set

_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in (
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
    os.path.join(_script_dir, "..", "reference", ".env"),
):
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("hose_summary_daily_ingestion")
ICT = timezone(timedelta(hours=7))

vnstock_key = os.getenv("VNSTOCK_API_KEY")
if vnstock_key:
    os.environ["VNSTOCK_API_KEY"] = vnstock_key
    logger.info("VNStock API Key ('%s***') configured.", vnstock_key[:4])
else:
    logger.warning("No VNSTOCK_API_KEY — running on Community tier.")

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

SUMMARY_METRIC_COLUMNS = (
    "high_52w",
    "low_52w",
    "dividend",
    "beta",
    "eps",
    "bvps",
    "market_cap",
    "pe",
    "pb",
    "roe",
    "change_1m",
    "change_1y",
    "dividend_yield",
    "foreign_ownership_pct",
)
SUMMARY_DAILY_EXPECTED_COLUMNS = ("symbol", "date", *SUMMARY_METRIC_COLUMNS, "ingested_at")


def _decode_bytes(value):
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="ignore")
    return value


def _normalize_output_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for column in SUMMARY_DAILY_EXPECTED_COLUMNS:
        if column not in df.columns:
            df[column] = pd.NA

    df["symbol"] = df["symbol"].map(_decode_bytes).astype(str).str.strip().str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")

    for column in SUMMARY_METRIC_COLUMNS:
        df[column] = df[column].map(_decode_bytes)
        df[column] = pd.to_numeric(df[column], errors="coerce")

    # ingested_at sẽ được gán riêng trước khi write, giữ nguyên nếu đã có
    if "ingested_at" not in df.columns:
        df["ingested_at"] = pd.NaT

    return df.loc[:, SUMMARY_DAILY_EXPECTED_COLUMNS]


def _normalize_summary(symbol: str, summary) -> pd.DataFrame | None:
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
        use_sym = str(attr_sym).strip().upper() if attr_sym else str(symbol).strip().upper()
        df.insert(0, "symbol", use_sym)
    else:
        df["symbol"] = df["symbol"].where(
            df["symbol"].notna() & (df["symbol"].astype(str).str.strip() != ""),
            symbol,
        )
        df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()

    return df


def _fetch_summary(symbol: str, mkt: Market) -> pd.DataFrame | None:
    while True:
        try:
            raw = mkt.equity(symbol).summary()
            return _normalize_summary(symbol, raw)
        except SystemExit:
            logger.warning("%s — rate-limit, retry in %ss", symbol, WAIT_TIME_ON_ERROR)
            time.sleep(WAIT_TIME_ON_ERROR)
        except Exception as exc:
            err = str(exc).lower()
            if any(kw in err for kw in ("rate", "limit", "timeout", "429", "503")):
                logger.warning("%s — retry in %ss: %s", symbol, WAIT_TIME_ON_ERROR, exc)
                time.sleep(WAIT_TIME_ON_ERROR)
            else:
                logger.error("%s — skip: %s", symbol, exc)
                return None


def _read_hose_symbols(fs: s3fs.S3FileSystem, equity_s3_path: str) -> list[str]:
    if not fs.exists(equity_s3_path):
        logger.error("Missing equity parquet: s3://%s", equity_s3_path)
        return []

    with fs.open(equity_s3_path, "rb") as fh:
        df = pd.read_parquet(fh)

    for col in ("symbol", "exchange", "type"):
        if col not in df.columns:
            logger.error("equity.parquet missing column: %s", col)
            return []

    mask = (
        df["exchange"].astype(str).str.upper().eq("HOSE")
        & df["type"].astype(str).str.upper().eq("STOCK")
    )
    symbols = (
        df.loc[mask, "symbol"]
        .astype(str)
        .str.strip()
        .str.upper()
        .replace("", pd.NA)
        .dropna()
        .drop_duplicates()
        .tolist()
    )
    logger.info("Loaded %s HOSE/STOCK symbols from s3://%s", len(symbols), equity_s3_path)
    return symbols


def _fetch_all(symbols: list[str], run_date: date) -> pd.DataFrame:
    mkt = Market()
    rows: list[pd.DataFrame] = []
    empty_count = 0
    error_count = 0
    total = len(symbols)

    logger.info("Fetching summary() for %s HOSE symbols (date=%s)...", total, run_date)
    for i, symbol in enumerate(symbols, start=1):
        df = _fetch_summary(symbol, mkt)
        if df is None:
            error_count += 1
        elif df.empty:
            empty_count += 1
        else:
            df.insert(1, "date", run_date)
            rows.append(df)

        time.sleep(SUMMARY_PER_REQ_DELAY)
        if i % BATCH_LOG_SIZE == 0 or i == total:
            logger.info(
                "Progress %s/%s | rows=%s | empty=%s | errors=%s",
                i, total, len(rows), empty_count, error_count,
            )

    if not rows:
        return pd.DataFrame()
    out = pd.concat(rows, ignore_index=True)
    logger.info("Collected %s rows for date=%s", len(out), run_date)
    return out


# ── Partitioned write ────────────────────────────────────────────────────────

def _build_partition_path(market_prefix: str, run_date: date) -> str:
    """Build S3 path: {market_prefix}/equity/year=YYYY/month=MM/day=DD/summary.parquet"""
    return (
        f"{market_prefix}/equity/"
        f"year={run_date.year:04d}/month={run_date.month:02d}/day={run_date.day:02d}/"
        f"summary.parquet"
    )


def _write_partition(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    *,
    append: bool = False,
) -> None:
    """Write summary.parquet to a single day partition.

    If append=True and file exists, merge + dedup by symbol (new rows win).
    If append=False and file exists, skip.
    """
    if df.empty:
        logger.warning("Nothing to write for s3://%s", s3_path)
        return

    df = _normalize_output_schema(df)

    if fs.exists(s3_path):
        if not append:
            logger.info("Exists, skipping: s3://%s  (use --append to merge)", s3_path)
            return

        # Append mode: merge with existing
        size = fs.size(s3_path) or 0
        if size == 0:
            logger.warning(
                "Existing parquet is 0 bytes, treating as missing: s3://%s",
                s3_path,
            )
            combined = df
        else:
            try:
                with fs.open(s3_path, "rb") as fh:
                    old = pd.read_parquet(fh)
            except Exception as exc:
                logger.error(
                    "Cannot read existing parquet (%s) — aborting to preserve s3://%s",
                    exc,
                    s3_path,
                )
                raise
            old = _normalize_output_schema(old)
            combined = pd.concat([df, old], ignore_index=True)
            logger.info("Merged with existing file: %s + %s rows", len(df), len(old))
    else:
        combined = df
        logger.info("Creating new file: s3://%s", s3_path)

    # Gán ingested_at cho dữ liệu mới trước khi merge
    ingested_at = datetime.now(ICT).strftime("%Y-%m-%dT%H:%M:%S%z")
    combined["ingested_at"] = combined["ingested_at"].fillna(ingested_at)

    combined = _normalize_output_schema(combined)
    before = len(combined)
    combined = combined.drop_duplicates(subset=["symbol"], keep="first")
    if len(combined) != before:
        logger.info("Dedup (symbol): removed %s duplicate rows", before - len(combined))

    with fs.open(s3_path, "wb") as fh:
        combined.to_parquet(fh, engine="pyarrow", index=False, compression="snappy")
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Written s3://%s (%.1f KB, %s rows)", s3_path, size_kb, len(combined))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Fetch HOSE equity summary() daily → partitioned summary.parquet on MinIO.",
    )
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--ref-prefix", default=DEFAULT_REF_PREFIX)
    p.add_argument("--market-prefix", default=DEFAULT_MARKET_PREFIX)
    p.add_argument(
        "--date",
        default=None,
        help="Trading date YYYY-MM-DD for column ``date``. Default: today (ICT).",
    )
    p.add_argument(
        "--append",
        action="store_true",
        help="Merge into existing summary.parquet in the day partition; deduplicate by symbol (new wins).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    run_date = date.fromisoformat(args.date) if args.date else datetime.now(ICT).date()

    ref_prefix = f"{args.bucket}/{args.ref_prefix}"
    market_prefix = f"{args.bucket}/{args.market_prefix}"
    equity_path = f"{ref_prefix}/equity/equity.parquet"
    output_path = _build_partition_path(market_prefix, run_date)

    sep = "=" * 80
    mode = "append" if args.append else "write (skip if exists)"
    logger.info(
        "\n%s\nHOSE equity summary() daily ingestion (partitioned)\n%s\n"
        "MinIO     : %s\n"
        "Source    : s3://%s\n"
        "Target    : s3://%s\n"
        "Mode      : %s\n"
        "Run date  : %s\n%s",
        sep, sep, MINIO_ENDPOINT, equity_path, output_path, mode, run_date, sep,
    )

    fs = s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )

    symbols = _read_hose_symbols(fs, equity_path)
    if not symbols:
        logger.error("No HOSE symbols — aborting.")
        return

    df = _fetch_all(symbols, run_date)
    if df.empty:
        logger.warning("No summary data collected — aborting.")
        return

    df = _normalize_output_schema(df)

    gx_check_columns_to_match_set(
        pl.from_pandas(df),
        {"column_set": list(SUMMARY_DAILY_EXPECTED_COLUMNS), "exact_match": True},
    )

    _write_partition(df, fs, output_path, append=args.append)
    logger.info("\n%s\nHOSE summary daily ingestion complete.\n%s", sep, sep)


if __name__ == "__main__":
    main()
