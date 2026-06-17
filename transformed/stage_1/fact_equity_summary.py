#!/usr/bin/env python3
"""
fact_equity_summary.py

Transform dữ liệu summary equity (raw) thành bảng fact summary daily.

Source (MinIO) — partitioned:
  raw/market/equity/year=YYYY/month=MM/day=DD/summary.parquet

Output (MinIO):
  transformed/stage_1/fact/fact_equity_summary.parquet   (single consolidated file)

Modes:
  append  – (default) đọc partition raw của --run-date, transform, gộp vào file cũ, dedup.
  rebuild – đọc TẤT CẢ partition raw, transform, ghi lại file mới từ đầu.

Columns (output):
  symbol, trade_date, high_52w, low_52w, beta, eps, bvps,
  market_cap, pe, pb, roe, dividend_yield
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
    gx_check_columns_not_null,
    gx_check_table_row_count_between,
)

# ── Load .env ────────────────────────────────────────────────────────────────
_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("fact_equity_summary_transform")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PREFIX = "raw/market/equity"
DST_PREFIX = "transformed/stage_1/fact"
DST_FILENAME = "fact_equity_summary.parquet"

METRIC_COLUMNS = (
    "high_52w",
    "low_52w",
    "beta",
    "eps",
    "bvps",
    "market_cap",
    "pe",
    "pb",
    "roe",
    "dividend_yield",
)

OUTPUT_COLUMNS = [
    "symbol",
    "trade_date",
    *METRIC_COLUMNS,
]


def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def _build_daily_source_prefix(base_prefix: str, run_date: str | None) -> str:
    """
    Trả về source prefix cho 1 ngày cụ thể nếu có run_date.
    run_date format: YYYY-MM-DD.
    """
    if not run_date:
        return base_prefix

    dt = datetime.strptime(run_date, "%Y-%m-%d")
    return (
        f"{base_prefix}/"
        f"year={dt.year:04d}/month={dt.month:02d}/day={dt.day:02d}"
    )


def read_partition(fs: s3fs.S3FileSystem, bucket: str, prefix: str) -> pl.DataFrame:
    """Đọc tất cả file summary.parquet trong prefix."""
    # Tìm summary.parquet (không lấy ohlc.parquet)
    pattern = f"{bucket}/{prefix}/**/summary.parquet"
    flat_pattern = f"{bucket}/{prefix}/summary.parquet"
    paths = list(dict.fromkeys(fs.glob(pattern) + fs.glob(flat_pattern)))

    if not paths:
        logger.warning(f"No summary.parquet files found at s3://{bucket}/{prefix}")
        return pl.DataFrame()

    logger.info(f"Reading {len(paths)} summary.parquet file(s) from s3://{bucket}/{prefix} ...")
    frames: list[pl.DataFrame] = []
    for path in paths:
        try:
            with fs.open(path, "rb") as f:
                raw = f.read()
            df = pl.read_parquet(io.BytesIO(raw))
            frames.append(df)
            logger.info(f"  ✓ {path} → {df.shape[0]:,} rows × {df.shape[1]} cols")
        except Exception as exc:
            logger.error(f"  ✗ Cannot read {path}: {exc}")

    if not frames:
        return pl.DataFrame()

    # Normalize cột date về cùng type trước khi concat
    # (một số file là Datetime('ns'), số khác là String)
    aligned = []
    for fr in frames:
        if "date" in fr.columns and fr["date"].dtype != pl.Utf8:
            fr = fr.with_columns(pl.col("date").cast(pl.Utf8, strict=False))
        aligned.append(fr)

    combined = pl.concat(aligned, how="diagonal")
    logger.info(f"  → Combined: {combined.shape[0]:,} rows × {combined.shape[1]} cols")
    return combined


def transform(df: pl.DataFrame) -> pl.DataFrame:
    """
    Chuẩn hóa dữ liệu summary:
    - Rename date → trade_date, cast về pl.Date
    - Cast tất cả metric columns về Float64
    - Normalize symbol (uppercase, strip)
    - Bỏ ingested_at, thêm year/month/day
    - Dedup theo (symbol, trade_date)
    """
    required_cols = {"symbol", "date"}
    missing_cols = sorted(required_cols - set(df.columns))
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Bỏ cột ingested_at nếu có
    drop_cols = [c for c in df.columns if c == "ingested_at" or c.startswith("ingested_at_")]
    if drop_cols:
        df = df.drop(drop_cols)

    # Rename date → trade_date
    df = df.rename({"date": "trade_date"})

    # Parse trade_date về Date
    dtype = df["trade_date"].dtype
    if dtype == pl.Date:
        pass
    elif dtype in (pl.Int64, pl.Int32, pl.UInt64, pl.UInt32):
        df = df.with_columns(
            pl.from_epoch(pl.col("trade_date"), time_unit="ms").cast(pl.Date)
        )
    elif dtype in (pl.Datetime, pl.Datetime("ms"), pl.Datetime("us"), pl.Datetime("ns")):
        df = df.with_columns(pl.col("trade_date").cast(pl.Date))
    elif dtype in (pl.Utf8, pl.String):
        df = df.with_columns(
            pl.col("trade_date")
            .cast(pl.Utf8, strict=False)
            .str.slice(0, 10)
            .str.to_date(format="%Y-%m-%d", strict=False)
        )

    # Normalize symbol
    df = df.with_columns(
        pl.col("symbol")
        .cast(pl.Utf8, strict=False)
        .str.strip_chars()
        .str.to_uppercase()
        .alias("symbol")
    )

    # Cast metric columns về Float64
    cast_exprs = []
    for col in METRIC_COLUMNS:
        if col in df.columns:
            cast_exprs.append(pl.col(col).cast(pl.Float64, strict=False).alias(col))
        else:
            cast_exprs.append(pl.lit(None).cast(pl.Float64).alias(col))
    df = df.with_columns(cast_exprs)

    # Drop null symbol/trade_date
    df = df.drop_nulls(["symbol", "trade_date"]).filter(pl.col("symbol") != "")

    # Select đúng thứ tự output, dedup
    result = df.select(OUTPUT_COLUMNS).unique(subset=["symbol", "trade_date"], keep="first")
    result = result.sort(["symbol", "trade_date"])

    logger.info(f"Transformed: {result.shape[0]:,} rows × {result.shape[1]} cols")
    return result


def _read_existing_single_file(
    fs: s3fs.S3FileSystem, s3_path: str
) -> pl.DataFrame:
    """Đọc file consolidated đã có trên MinIO (nếu tồn tại)."""
    if not fs.exists(s3_path):
        logger.info(f"No existing file at s3://{s3_path} — will create new.")
        return pl.DataFrame()
    try:
        with fs.open(s3_path, "rb") as f:
            raw = f.read()
        size_kb = len(raw) / 1024
        df = pl.read_parquet(io.BytesIO(raw))
        logger.info(
            f"Read existing s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)"
        )
        return df
    except Exception as exc:
        logger.error(f"Cannot read existing file s3://{s3_path}: {exc}")
        return pl.DataFrame()


def _write_single_parquet(
    df: pl.DataFrame, fs: s3fs.S3FileSystem, s3_path: str
) -> None:
    """Ghi DataFrame vào 1 file parquet duy nhất trên MinIO."""
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info(f"Saved s3://{s3_path} ({size_kb:.1f} KB, {df.shape[0]:,} rows)")


def write_single_file(
    df_new: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
    *,
    mode: str = "append",
) -> None:
    """
    Ghi dữ liệu summary đã transform vào 1 file parquet duy nhất.

    Modes:
      append  – gộp df_new vào file cũ, dedup theo (symbol, trade_date),
                row mới được ưu tiên giữ khi trùng.
      rebuild – ghi df_new trực tiếp (bỏ qua file cũ).
    """
    if df_new.is_empty():
        logger.warning("No rows to write.")
        return

    if mode == "rebuild":
        logger.info("Mode=rebuild → ghi mới toàn bộ, bỏ qua file cũ.")
        df_final = df_new
    else:
        # Mode append: đọc file cũ → concat → dedup
        df_old = _read_existing_single_file(fs, s3_path)
        if df_old.is_empty():
            df_final = df_new
        else:
            # Align schema: chỉ lấy các cột chung
            common_cols = sorted(set(df_new.columns) & set(df_old.columns))
            if set(df_new.columns) != set(df_old.columns):
                logger.warning(
                    f"Schema mismatch — new cols: {sorted(df_new.columns)}, "
                    f"old cols: {sorted(df_old.columns)}. Using common: {common_cols}"
                )
                df_old = df_old.select(common_cols)
                df_new = df_new.select(common_cols)

            before = df_old.shape[0] + df_new.shape[0]
            # df_new đặt trước để khi dedup (keep="first") → row mới được ưu tiên
            df_combined = pl.concat([df_new, df_old], how="diagonal")
            df_final = df_combined.unique(
                subset=["symbol", "trade_date"], keep="first"
            )
            after = df_final.shape[0]
            if before != after:
                logger.info(
                    f"Dedup (symbol, trade_date): {before - after:,} duplicate rows removed"
                )
            logger.info(
                f"Append: {df_old.shape[0]:,} old + {df_new.shape[0]:,} new "
                f"→ {after:,} final rows"
            )

    # Sort cho dễ đọc và tối ưu predicate pushdown
    df_final = df_final.sort(["symbol", "trade_date"])
    _write_single_parquet(df_final, fs, s3_path)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform partitioned equity summary → fact_equity_summary (single file)."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    parser.add_argument("--source-prefix", default=SRC_PREFIX, help="Source prefix")
    parser.add_argument("--destination-prefix", default=DST_PREFIX, help="Destination prefix")
    parser.add_argument("--destination-filename", default=DST_FILENAME, help="Output filename")
    parser.add_argument(
        "--run-date",
        default="",
        help="Ngày xử lý YYYY-MM-DD. Nếu truyền, source-prefix sẽ tự thu hẹp về đúng partition ngày đó.",
    )
    parser.add_argument(
        "--mode",
        choices=["append", "rebuild"],
        default="append",
        help=(
            "append (default): gộp dữ liệu mới vào file cũ, dedup (symbol, trade_date). "
            "rebuild: đọc toàn bộ partition raw, ghi lại file từ đầu."
        ),
    )
    return parser.parse_args()


def log_run_info(args: argparse.Namespace, source_prefix: str, dst_path: str) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nTransform: raw equity summary → fact equity summary (single file)\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s/%s/ (summary.parquet only)\n"
        "Destination    : s3://%s\n"
        "Mode           : %s\n"
        "Run date       : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        args.bucket, source_prefix,
        dst_path,
        args.mode,
        args.run_date or "(all dates in source-prefix)",
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    fs = _build_fs()

    # Khi mode=rebuild, đọc toàn bộ raw (không lọc theo run_date)
    if args.mode == "rebuild":
        source_prefix = args.source_prefix
    else:
        source_prefix = _build_daily_source_prefix(
            args.source_prefix, args.run_date or None
        )

    dst_path = f"{args.bucket}/{args.destination_prefix}/{args.destination_filename}"
    log_run_info(args, source_prefix, dst_path)

    df_raw = read_partition(fs, args.bucket, source_prefix)
    if df_raw.is_empty():
        logger.error("Source is empty — aborting.")
        return

    df_summary = transform(df_raw)

    # ── GX Gate: Basic quality check after transform ─────────────────
    logger.info("Running GX validation (Stage 1: BK not-null + Volume)...")
    gx_check_columns_not_null(df_summary, {"columns": ["symbol", "trade_date"]})
    gx_check_table_row_count_between(df_summary, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_single_file(
        df_summary,
        fs=fs,
        s3_path=dst_path,
        mode=args.mode,
    )

    separator = "=" * 80
    logger.info("\n%s\nfact_equity_summary transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
