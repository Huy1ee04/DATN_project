#!/usr/bin/env python3
"""
fact_market_index_daily.py

Transform dữ liệu intraday index (raw) thành dữ liệu daily OHLCV theo symbol.

Source (MinIO):
  raw/market/index/year=YYYY/month=MM/day=DD/ohlc.parquet

Output (MinIO):
  transformed/fact/market/index_daily/year=YYYY/month=MM/day=DD/ohlc.parquet
"""

import io
import os
import logging
import argparse
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

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
logger = logging.getLogger("fact_market_index_daily_transform")
ICT = timezone(timedelta(hours=7))

# ── Config ───────────────────────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_PREFIX = "raw/market/index"
DST_PREFIX = "transformed/fact/market/index_daily"


def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_partition(fs: s3fs.S3FileSystem, bucket: str, prefix: str) -> pl.DataFrame:
    """Đọc tất cả file parquet trong prefix."""
    pattern = f"{bucket}/{prefix}/**/*.parquet"
    flat_pattern = f"{bucket}/{prefix}/*.parquet"
    paths = list(dict.fromkeys(fs.glob(pattern) + fs.glob(flat_pattern)))

    if not paths:
        logger.warning(f"No parquet files found at s3://{bucket}/{prefix}")
        return pl.DataFrame()

    logger.info(f"Reading {len(paths)} file(s) from s3://{bucket}/{prefix} ...")
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
        .with_columns(
            pl.col("trade_date").dt.year().cast(pl.Int32).alias("year"),
            pl.col("trade_date").dt.month().cast(pl.Int32).alias("month"),
            pl.col("trade_date").dt.day().cast(pl.Int32).alias("day"),
            pl.lit(datetime.now(ICT).strftime("%Y-%m-%dT%H:%M:%S%z")).alias("updated_at"),
        )
        .select(
            "symbol",
            "trade_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "year",
            "month",
            "day",
            "updated_at",
        )
    )
    logger.info(f"Transformed: {result.shape[0]:,} rows × {result.shape[1]} cols")
    return result


def write_partitioned_daily(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    bucket: str,
    dst_prefix: str,
    overwrite: bool = False,
) -> None:
    if df.is_empty():
        logger.warning("No rows to write.")
        return

    day_partitions = (
        df.select(["year", "month", "day"])
        .unique()
        .sort(["year", "month", "day"])
        .iter_rows(named=True)
    )

    written = 0
    for part in day_partitions:
        year = int(part["year"])
        month = int(part["month"])
        day = int(part["day"])

        partition_df = df.filter(
            (pl.col("year") == year) & (pl.col("month") == month) & (pl.col("day") == day)
        ).drop(["year", "month", "day"])

        dst_path = (
            f"{bucket}/{dst_prefix}/"
            f"year={year:04d}/month={month:02d}/day={day:02d}/ohlc.parquet"
        )

        if fs.exists(dst_path) and not overwrite:
            logger.info(f"Exists, skipping: s3://{dst_path} (dùng --overwrite để ghi đè)")
            continue

        buf = io.BytesIO()
        partition_df.write_parquet(buf, compression="snappy")
        buf.seek(0)
        with fs.open(dst_path, "wb") as f:
            f.write(buf.read())

        written += 1
        size_kb = (fs.size(dst_path) or 0) / 1024
        logger.info(
            f"Saved s3://{dst_path} ({size_kb:.1f} KB, {partition_df.shape[0]:,} rows)"
        )

    logger.info(f"Done writing {written} daily partition file(s).")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Transform intraday index OHLCV thành dữ liệu daily theo symbol."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    parser.add_argument("--source-prefix", default=SRC_PREFIX, help="Source prefix")
    parser.add_argument("--destination-prefix", default=DST_PREFIX, help="Destination prefix")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Ghi đè output nếu đã tồn tại",
    )
    return parser.parse_args()


def log_run_info(args: argparse.Namespace) -> None:
    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nTransform: raw index intraday → fact index daily\n%s\n"
        "MinIO Endpoint : %s\n"
        "Bucket         : %s\n"
        "Source         : s3://%s/%s/\n"
        "Destination    : s3://%s/%s/year=YYYY/month=MM/day=DD/\n"
        "Overwrite      : %s\n"
        "Run at         : %s\n%s",
        separator, separator,
        MINIO_ENDPOINT,
        args.bucket,
        args.bucket, args.source_prefix,
        args.bucket, args.destination_prefix,
        args.overwrite,
        run_at,
        separator,
    )


def main() -> None:
    args = parse_args()
    fs = _build_fs()
    log_run_info(args)

    df_raw = read_partition(fs, args.bucket, args.source_prefix)
    if df_raw.is_empty():
        logger.error("Source is empty — aborting.")
        return

    df_daily = transform(df_raw)
    write_partitioned_daily(
        df_daily,
        fs=fs,
        bucket=args.bucket,
        dst_prefix=args.destination_prefix,
        overwrite=args.overwrite,
    )

    separator = "=" * 80
    logger.info("\n%s\nfact_market_index_daily transform complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
