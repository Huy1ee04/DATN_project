#!/usr/bin/env python3
"""
_company_ingest_common.py

Shared utilities for company-scoped reference ingestion scripts (events, news, ...).

Each caller provides:
  - DOMAIN folder name on MinIO (e.g. "events", "news")
  - FILENAME of the parquet file (e.g. "events.parquet")
  - DATE_COL used by --date filter (e.g. "public_date")
  - DEDUP_KEYS_PRIMARY / DEDUP_KEYS_FALLBACK
  - fetch_one(symbol) → DataFrame  (e.g. lambda s: ref.company(s).events())

This module owns: env loading, logging, MinIO config + S3FS factory,
symbol fetching, per-symbol fetch loop with retry, date filter,
write/append/append-only logic with atomic write + dedup-key safety check,
shared argparse, and run-info logger.

Không ép kiểu id/public_date trước Parquet — nếu concat trộn int/str gây lỗi PyArrow,
pipeline có thể tự thêm bước validate/chuẩn hoá riêng.
"""

from __future__ import annotations

import argparse
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Callable, Iterable, Sequence

import pandas as pd
import s3fs
from dotenv import load_dotenv


# ── Env loading (runs once on import) ────────────────────────────────────────

def _load_env_from_repo() -> None:
    here = os.path.dirname(os.path.abspath(__file__))
    for path in (os.path.join(here, ".env"), os.path.join(here, "..", ".env")):
        if os.path.isfile(path):
            load_dotenv(dotenv_path=path)
            break


_load_env_from_repo()


# ── Logging factory ──────────────────────────────────────────────────────────

ICT = timezone(timedelta(hours=7))
_LOG_FMT = "%(asctime)s [%(levelname)s] %(message)s"


def get_logger(name: str) -> logging.Logger:
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format=_LOG_FMT)
    return logging.getLogger(name)


# ── MinIO config ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class MinIOConfig:
    endpoint: str
    access_key: str
    secret_key: str
    default_bucket: str
    default_prefix: str = "raw/reference"


def load_minio_config() -> MinIOConfig:
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
    if not endpoint.startswith("http"):
        endpoint = f"http://{endpoint}"
    return MinIOConfig(
        endpoint=endpoint,
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio_access_key"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio_secret_key"),
        default_bucket=os.getenv("MINIO_BUCKET", "stock-data"),
    )


def build_s3fs(cfg: MinIOConfig) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=cfg.access_key,
        secret=cfg.secret_key,
        client_kwargs={"endpoint_url": cfg.endpoint},
    )


# ── VNSTOCK key announcement ─────────────────────────────────────────────────

def announce_vnstock_key(logger: logging.Logger) -> None:
    key = os.getenv("VNSTOCK_API_KEY")
    if key:
        os.environ["VNSTOCK_API_KEY"] = key
        logger.info(
            "VNStock API Key ('%s***') found and configured (Sponsor tier active).",
            key[:4],
        )
    else:
        logger.warning(
            "No VNSTOCK_API_KEY found in environment or .env, running on Community tier."
        )


# ── Defaults ─────────────────────────────────────────────────────────────────

COMPANY_BATCH_SIZE = 50
COMPANY_PER_REQ_DELAY = 0.5
WAIT_TIME_ON_ERROR = 65
MAX_RETRIES = 5
PUBLIC_DATE_FMT = "%Y-%m-%d"


# ── DataFrame helpers ────────────────────────────────────────────────────────

def add_ingested_at(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["ingested_at"] = datetime.now(ICT)
    return df


def coerce_int_column(
    df: pd.DataFrame,
    column: str,
    *,
    logger: logging.Logger,
    drop_invalid: bool = True,
) -> pd.DataFrame:
    """
    Ép cột số nguyên về int64.

    Một số batch API trả news_id (hoặc cột tương tự) lẫn int/str/float; concat nhiều
    symbol hoặc merge append khiến PyArrow không ghi được Parquet. Hàng không ép được
    sẽ bị loại khi drop_invalid=True (mặc định).
    """
    if df is None or df.empty or column not in df.columns:
        return df

    df = df.copy()
    before = len(df)
    numeric = pd.to_numeric(df[column], errors="coerce")
    is_whole = numeric.notna() & (numeric % 1 == 0)
    invalid_mask = ~is_whole
    invalid_count = int(invalid_mask.sum())

    if invalid_count:
        samples = df.loc[invalid_mask, column].head(5).tolist()
        logger.warning(
            "%s: %d/%d rows cannot coerce to int (samples: %s)",
            column, invalid_count, before, samples,
        )
        if drop_invalid:
            df = df.loc[~invalid_mask].reset_index(drop=True)
        else:
            df[column] = numeric.where(is_whole).astype("Int64")
            return df

    if df.empty:
        logger.warning("%s: all %d rows dropped after int coercion.", column, before)
        return df

    df[column] = pd.to_numeric(df[column], errors="coerce").astype("int64")
    if invalid_count:
        logger.info(
            "%s: kept %d/%d rows as int64 (%d dropped).",
            column, len(df), before, invalid_count,
        )
    return df


def resolve_dedup_keys(
    columns: Iterable[str],
    *,
    primary: Sequence[str],
    fallback: Sequence[str],
    logger: logging.Logger,
) -> list[str]:
    """Return primary if all present, else fallback. Never returns just ['symbol']."""
    cols = set(columns)
    if all(k in cols for k in primary):
        return list(primary)
    if all(k in cols for k in fallback):
        logger.warning(
            "Primary dedup keys %s incomplete; falling back to %s",
            list(primary), list(fallback),
        )
        return list(fallback)
    raise ValueError(
        f"Cannot dedup safely: need {list(primary)} or {list(fallback)}, "
        f"got columns={sorted(cols)}"
    )


# ── Date arg + filter ────────────────────────────────────────────────────────

def parse_date_arg(value: str) -> str:
    datetime.strptime(value, PUBLIC_DATE_FMT)
    return value


def filter_by_date_col(
    df: pd.DataFrame,
    *,
    min_date: str,
    date_col: str,
    logger: logging.Logger,
) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if date_col not in df.columns:
        logger.warning("Column '%s' not found; skipping date filter.", date_col)
        return df

    min_ts = pd.Timestamp(min_date)
    parsed = pd.to_datetime(df[date_col], errors="coerce")
    before = len(df)
    filtered = df.loc[parsed >= min_ts].reset_index(drop=True)
    logger.info(
        "Date filter %s >= %s: %s -> %s rows (%s dropped, %s missing/invalid)",
        date_col, min_date,
        f"{before:,}", f"{len(filtered):,}",
        f"{before - len(filtered):,}", f"{parsed.isna().sum():,}",
    )
    return filtered


# ── Symbol fetching ──────────────────────────────────────────────────────────

def _normalize_symbols(series: pd.Series) -> list[str]:
    return (
        series.astype(str).str.strip().str.upper()
        .replace("", pd.NA).dropna().drop_duplicates().tolist()
    )


def fetch_exchange_symbols(
    ref,
    *,
    exchange: str,
    instrument_type: str,
    logger: logging.Logger,
) -> list[str]:
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
        logger.info(
            "Fetched %d symbols from Reference API (exchange=%s, type=%s)",
            len(symbols), exchange, instrument_type,
        )
        return symbols
    except Exception as e:
        logger.error("fetch_exchange_symbols() failed: %s", e)
        return []


# ── Per-symbol fetch loop with retry ────────────────────────────────────────

def _fetch_one_with_retry(
    *,
    symbol: str,
    fetch_one: Callable[[str], pd.DataFrame],
    method_label: str,
    logger: logging.Logger,
    max_retries: int,
) -> pd.DataFrame:
    for attempt in range(1, max_retries + 1):
        try:
            return fetch_one(symbol)
        except Exception as e:
            if attempt == max_retries:
                logger.error(
                    "%s - Failed after %d attempts: %s. Skipping symbol.",
                    symbol, max_retries, e,
                )
                break
            logger.warning(
                "%s - Attempt %d/%d failed: %s. Waiting %ds before retry...",
                symbol, attempt, max_retries, e, WAIT_TIME_ON_ERROR,
            )
            time.sleep(WAIT_TIME_ON_ERROR)
    return pd.DataFrame()


def fetch_per_symbol_concat(
    *,
    symbols: list[str],
    fetch_one: Callable[[str], pd.DataFrame],
    method_label: str,
    logger: logging.Logger,
    max_retries: int = MAX_RETRIES,
    per_req_delay: float = COMPANY_PER_REQ_DELAY,
    batch_size: int = COMPANY_BATCH_SIZE,
) -> pd.DataFrame:
    total = len(symbols)
    results: list[pd.DataFrame] = []
    empty_count = 0

    logger.info("Fetching %s() for %d symbols...", method_label, total)
    for i, symbol in enumerate(symbols, start=1):
        df = _fetch_one_with_retry(
            symbol=symbol,
            fetch_one=fetch_one,
            method_label=method_label,
            logger=logger,
            max_retries=max_retries,
        )
        if df is not None and not df.empty:
            if "symbol" not in df.columns:
                df = df.copy()
                df["symbol"] = symbol
            results.append(df)
        else:
            logger.warning(
                "[%d/%d] %s returned empty %s(), skipped.",
                i, total, symbol, method_label,
            )
            empty_count += 1

        time.sleep(per_req_delay)
        if i % batch_size == 0 or i == total:
            logger.info(
                "Progress: %d/%d | Frames: %d | Empty: %d",
                i, total, len(results), empty_count,
            )

    if not results:
        return pd.DataFrame()

    df_all = pd.concat(results, ignore_index=True)
    logger.info("Concatenated %s rows from %d symbols.", f"{len(df_all):,}", len(results))
    return df_all


# ── Parquet I/O on MinIO ────────────────────────────────────────────────────

def _write_parquet_and_log(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    logger: logging.Logger,
    success_prefix: str,
) -> None:
    with fs.open(s3_path, "wb") as f:
        df.to_parquet(f, engine="pyarrow", index=False, compression="snappy")
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(
        "%s s3://%s (%.1f KB, %s rows)",
        success_prefix, s3_path, size_kb, f"{len(df):,}",
    )


def _write_parquet_atomic(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    logger: logging.Logger,
    success_prefix: str,
) -> None:
    """Write to .tmp then mv into place — protects the existing file on partial failure."""
    tmp_path = f"{s3_path}.tmp"
    _write_parquet_and_log(
        df, fs, tmp_path, logger, success_prefix=f"{success_prefix} (staging)"
    )
    if fs.exists(s3_path):
        fs.rm(s3_path)
    fs.mv(tmp_path, s3_path)
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(
        "%s s3://%s (%.1f KB, %s rows)",
        success_prefix, s3_path, size_kb, f"{len(df):,}",
    )


def write_parquet_to_s3(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    logger: logging.Logger,
) -> None:
    if df is None or df.empty:
        logger.warning("DataFrame is empty, skipping upload for %s", s3_path)
        return
    if fs.exists(s3_path):
        logger.info("Exists, skipping: s3://%s  (use --append to merge)", s3_path)
        return
    df = add_ingested_at(df)
    _write_parquet_and_log(df, fs, s3_path, logger, success_prefix="Uploaded")


def replace_parquet_on_s3(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    logger: logging.Logger,
) -> None:
    """
    Ghi đè toàn bộ file (snapshot mới thay thế dữ liệu cũ).

    - Object storage không có posix-unlink riêng: ghi file .tmp hoàn chỉnh rồi thay thế
      `info.parquet` — tương đương “xoá + ghi lại” mà không để file hỏng giữa chừng.
    - DataFrame rỗng → không ghi (giữ file cũ khi API lỗi toàn pipeline).
    """
    if df is None or df.empty:
        logger.warning(
            "DataFrame is empty — không ghi đè s3://%s; file cũ (nếu có) được giữ.",
            s3_path,
        )
        return
    df = add_ingested_at(df)
    _write_parquet_atomic(df, fs, s3_path, logger, success_prefix="Replaced ->")


def append_parquet_to_s3(
    df_new: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    *,
    primary_keys: Sequence[str],
    fallback_keys: Sequence[str],
    logger: logging.Logger,
    transform_df: Callable[[pd.DataFrame], pd.DataFrame] | None = None,
) -> None:
    """
    Merge df_new vào file Parquet gốc trên MinIO (read → concat → dedup → ghi đè).

    Đảm bảo không mất dữ liệu cũ:
    - df_new rỗng → giữ nguyên file cũ.
    - Đọc file cũ thất bại → abort, không ghi.
    - Dedup giữ bản fetch mới khi trùng khóa.
    - Abort nếu bất kỳ khóa cũ nào biến mất sau merge.
    - Ghi atomic qua .tmp.
    """
    if df_new is None or df_new.empty:
        logger.warning(
            "New DataFrame is empty — giữ nguyên file cũ, không ghi đè %s", s3_path
        )
        return

    df_new = add_ingested_at(df_new)
    df_old: pd.DataFrame | None = None

    if fs.exists(s3_path):
        try:
            with fs.open(s3_path, "rb") as f:
                df_old = pd.read_parquet(f)
        except Exception as e:
            logger.error(
                "Cannot read existing parquet (%s) — aborting append to preserve s3://%s",
                e, s3_path,
            )
            return
        logger.info("Read existing: %s rows from s3://%s", f"{len(df_old):,}", s3_path)
        df_combined = pd.concat([df_new, df_old], ignore_index=True)
    else:
        logger.info("File not found, creating new: s3://%s", s3_path)
        df_combined = df_new

    if transform_df is not None:
        df_combined = transform_df(df_combined)
        if df_combined is None or df_combined.empty:
            logger.warning(
                "transform_df returned empty after merge — aborting append to preserve s3://%s",
                s3_path,
            )
            return

    keys = resolve_dedup_keys(
        df_combined.columns,
        primary=primary_keys,
        fallback=fallback_keys,
        logger=logger,
    )

    before = len(df_combined)
    df_combined = df_combined.drop_duplicates(subset=keys, keep="first")
    after = len(df_combined)
    if before != after:
        logger.info(
            "Dedup on %s: %s -> %s rows (%s duplicates removed)",
            keys, f"{before:,}", f"{after:,}", f"{before - after:,}",
        )

    if df_old is not None:
        old_keys = {
            tuple(row) for row in df_old[keys].astype(str).fillna("").to_numpy()
        }
        kept_keys = {
            tuple(row) for row in df_combined[keys].astype(str).fillna("").to_numpy()
        }
        lost = old_keys - kept_keys
        if lost:
            logger.error(
                "Merge would drop %d existing key(s) — aborting write to s3://%s",
                len(lost), s3_path,
            )
            return
        logger.info(
            "Preserved all %s existing dedup keys (%s rows -> %s rows after merge)",
            f"{len(old_keys):,}", f"{len(df_old):,}", f"{after:,}",
        )

    _write_parquet_atomic(df_combined, fs, s3_path, logger, success_prefix="Appended ->")


def append_only_parquet_to_s3(
    df_new: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    *,
    primary_keys: Sequence[str],
    fallback_keys: Sequence[str],
    logger: logging.Logger,
) -> None:
    """Write new batch as a standalone parquet file; no read/merge of existing files."""
    if df_new is None or df_new.empty:
        logger.warning("DataFrame is empty, skipping upload for %s", s3_path)
        return

    df_new = add_ingested_at(df_new)
    try:
        keys = resolve_dedup_keys(
            df_new.columns, primary=primary_keys, fallback=fallback_keys, logger=logger
        )
    except ValueError:
        keys = []

    if keys:
        before = len(df_new)
        df_new = df_new.drop_duplicates(subset=keys, keep="first")
        if before != len(df_new):
            logger.info(
                "Intra-batch dedup: %s -> %s rows", f"{before:,}", f"{len(df_new):,}"
            )

    if fs.exists(s3_path):
        logger.info("Overwriting existing incremental file: s3://%s", s3_path)
    _write_parquet_and_log(df_new, fs, s3_path, logger, success_prefix="Append-only ->")


def incremental_s3_path(
    *,
    prefix: str,
    domain: str,
    label: str,
    min_date: str | None,
) -> str:
    """e.g. {prefix}/company/events/incremental/events_gte_2026-05-14.parquet"""
    base = f"{prefix}/company/{domain}/incremental"
    if min_date:
        return f"{base}/{label}_gte_{min_date}.parquet"
    stamp = datetime.now(ICT).strftime("%Y%m%d_%H%M%S")
    return f"{base}/{label}_{stamp}.parquet"


# ── Shared argparse + run-info logger ───────────────────────────────────────

def build_arg_parser(
    *,
    description: str,
    default_bucket: str,
    default_prefix: str,
    date_help: str,
) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("--bucket", default=default_bucket, help="MinIO bucket name")
    parser.add_argument("--prefix", default=default_prefix, help="Path prefix inside bucket")
    parser.add_argument(
        "--exchange", default="HOSE",
        help="Sàn lọc từ Reference API: HOSE, HNX, UPCOM (default: HOSE)",
    )
    parser.add_argument(
        "--instrument-type", default="STOCK", dest="instrument_type",
        help="Loại CK: STOCK, ETF, … (default: STOCK)",
    )
    parser.add_argument(
        "--append", action="store_true",
        help="Read file gốc → merge + dedup → ghi đè 1 file (chậm khi file lớn)",
    )
    parser.add_argument(
        "--append-only", action="store_true", dest="append_only",
        help="Ghi file parquet mới trong */incremental/ — không đọc file cũ",
    )
    parser.add_argument(
        "--date", type=parse_date_arg, default=None, metavar="YYYY-MM-DD",
        help=date_help,
    )
    return parser


def log_run_info(
    *,
    title: str,
    cfg: MinIOConfig,
    args: argparse.Namespace,
    output_s3_path: str,
    date_col: str,
    logger: logging.Logger,
) -> None:
    separator = "=" * 80
    if args.append_only:
        mode = "append-only (new parquet file, no read/merge)"
    elif args.append:
        mode = "append (read + merge + dedup → single file)"
    else:
        mode = "write (skip if exists)"
    date_filter = f"{date_col} >= {args.date}" if args.date else "(none — all rows)"
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\n%s\n%s\n"
        "MinIO Endpoint    : %s\n"
        "Symbol source     : Reference API (exchange=%s, type=%s)\n"
        "Target output     : s3://%s\n"
        "Date filter       : %s\n"
        "Mode              : %s\n"
        "Run at            : %s\n%s",
        separator, title, separator,
        cfg.endpoint,
        args.exchange, args.instrument_type,
        output_s3_path, date_filter, mode, run_at,
        separator,
    )
