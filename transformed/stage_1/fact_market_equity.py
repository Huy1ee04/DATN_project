#!/usr/bin/env python3
"""
fact_market_equity.py

Transform dữ liệu intraday equity (raw) thành dữ liệu daily OHLCV theo symbol.

Source (MinIO):
  raw/market/equity/year=YYYY/month=MM/day=DD/ohlc.parquet

Output (MinIO):
  transformed/stage_1/fact/fact_market_equity.parquet   (single consolidated file)

Modes:
  append  – (default) đọc partition raw của --run-date, transform, gộp vào file cũ, dedup.
  rebuild – đọc TẤT CẢ partition raw, transform, ghi lại file mới từ đầu.
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
    gx_check_columns_to_match_set,
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
logger = logging.getLogger("fact_market_equity_transform")
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
DST_FILENAME = "fact_market_equity.parquet"


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
    """Đọc tất cả file ohlc.parquet trong prefix (bỏ qua summary.parquet)."""
    pattern = f"{bucket}/{prefix}/**/*.parquet"
    flat_pattern = f"{bucket}/{prefix}/*.parquet"
    all_paths = list(dict.fromkeys(fs.glob(pattern) + fs.glob(flat_pattern)))
    # Chỉ lấy ohlc.parquet — summary.parquet do fact_equity_summary.py xử lý
    paths = [p for p in all_paths if p.endswith("ohlc.parquet")]

    if not paths:
        logger.warning(f"No ohlc.parquet files found at s3://{bucket}/{prefix}")
        return pl.DataFrame()

    logger.info(f"Reading {len(paths)} ohlc file(s) from s3://{bucket}/{prefix} ...")
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

    combined = pl.concat(frames, how="diagonal")
    logger.info(f"  → Combined: {combined.shape[0]:,} rows × {combined.shape[1]} cols")
    return combined


def transform(df: pl.DataFrame) -> pl.DataFrame:
    required_cols = {"symbol", "time", "open", "high", "low", "close", "volume"}
    missing_cols = sorted(required_cols - set(df.columns))
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Parse time về Datetime, sau đó tách trade_date để aggregate theo ngày.
    df = df.with_columns(
        pl.col("time").str.to_datetime(strict=False).alias("time")
        if df["time"].dtype in (pl.Utf8, pl.String)
        else pl.col("time").cast(pl.Datetime, strict=False).alias("time")
    )

    df = (
        df.drop_nulls(["symbol", "time"])
        .with_columns(
            pl.col("time").dt.date().alias("trade_date"),
            pl.col("volume").cast(pl.Int64, strict=False).fill_null(0),
            pl.col("open").cast(pl.Float64, strict=False),
            pl.col("high").cast(pl.Float64, strict=False),
            pl.col("low").cast(pl.Float64, strict=False),
            pl.col("close").cast(pl.Float64, strict=False),
        )
        .sort(["symbol", "trade_date", "time"])
    )

    result = (
        df.group_by(["symbol", "trade_date"], maintain_order=True)
        .agg(
            pl.col("open").first().alias("open"),
            pl.col("high").max().alias("high"),
            pl.col("low").min().alias("low"),
            pl.col("close").last().alias("close"),
            pl.col("volume").sum().alias("volume"),
        )
        .select(
            "symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        )
    )
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
    Ghi dữ liệu daily đã transform vào 1 file parquet duy nhất.

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
        description="Transform intraday equity OHLCV thành dữ liệu daily theo symbol."
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
        "\n%s\nTransform: raw equity intraday → fact equity daily (single file)\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s/%s/\n"
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

    df_daily = transform(df_raw)

    # ── GX Gate: Input Quality ────────────────────────────────────────────
    logger.info("Running GX validation (Stage 1: Schema + Completeness + Volume)...")
    gx_check_columns_to_match_set(df_daily, {
        "column_set": ["symbol", "trade_date", "open", "high", "low", "close", "volume"],
    })
    gx_check_columns_not_null(df_daily, {"columns": ["symbol", "trade_date"]})
    gx_check_table_row_count_between(df_daily, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_single_file(
        df_daily,
        fs=fs,
        s3_path=dst_path,
        mode=args.mode,
    )

    separator = "=" * 80
    logger.info("\n%s\nfact_market_equity transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
