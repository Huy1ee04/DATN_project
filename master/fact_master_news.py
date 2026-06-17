#!/usr/bin/env python3
"""
fact_master_news.py (master) — Resolve surrogate keys.

Đọc fact_stock_news từ stage_2, resolve FK chuẩn Kimball.

Source (MinIO):
  transformed/stage_2/fact/fact_stock_news.parquet
  master/dimension/dim_master_stock.parquet   → symbol → stock_key

Destination (MinIO):
  master/fact/fact_master_news.parquet

Output schema:
  stock_key          (Int32) — FK → dim_master_stock
  date_key           (Int32) — FK → dim_master_event (yyyymmdd)
  symbol             (Utf8)  — natural key (audit)
  public_date        (Date)  — natural key (audit)
  news_title         (Utf8)  — measure
  news_short_content (Utf8)  — measure
  news_image_url     (Utf8)  — measure
  news_source_link   (Utf8)  — measure
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

_script_dir = os.path.dirname(os.path.abspath(__file__))
for _env_path in [
    os.path.join(_script_dir, ".env"),
    os.path.join(_script_dir, "..", ".env"),
]:
    if os.path.isfile(_env_path):
        load_dotenv(dotenv_path=_env_path)
        break

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fact_master_news_publish")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

SRC_FACT_PATH = "transformed/stage_2/fact/fact_stock_news.parquet"
SRC_DIM_STOCK_PATH = "master/dimension/dim_master_stock.parquet"
DST_PREFIX = "master/fact"
DST_FILENAME = "fact_master_news.parquet"


def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY, secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs, s3_path):
    logger.info("Reading s3://%s ...", s3_path)
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def write_parquet(df, fs, s3_path, overwrite=False):
    if fs.exists(s3_path) and not overwrite:
        logger.info("Exists, skipping: s3://%s", s3_path)
        return
    buf = io.BytesIO()
    df.write_parquet(buf, compression="snappy")
    buf.seek(0)
    with fs.open(s3_path, "wb") as f:
        f.write(buf.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


def resolve_fk(df_fact, df_dim_stock):
    """Resolve symbol → stock_key, public_date → public_date_key."""
    # stock_key
    stock_lookup = df_dim_stock.select(["symbol", "stock_key"]).unique(subset=["symbol"])
    df_fact = df_fact.join(stock_lookup, on="symbol", how="left")

    n_orphan = df_fact.filter(pl.col("stock_key").is_null()).shape[0]
    if n_orphan > 0:
        samples = df_fact.filter(pl.col("stock_key").is_null()).select("symbol").unique().to_series().to_list()[:10]
        logger.warning("%d rows no stock_key (orphans: %s) — dropping.", n_orphan, samples)
        df_fact = df_fact.filter(pl.col("stock_key").is_not_null())

    # public_date_key = yyyymmdd (Role-Playing Dimension: ngày đăng tin)
    df_fact = df_fact.with_columns(
        (pl.col("public_date").dt.year().cast(pl.Int32) * 10000
         + pl.col("public_date").dt.month().cast(pl.Int32) * 100
         + pl.col("public_date").dt.day().cast(pl.Int32)
        ).alias("public_date_key")
    )

    logger.info("FK: %d/%d stock_key resolved, public_date_key computed.",
                df_fact.shape[0] - n_orphan, df_fact.shape[0])

    # Reorder: FK first → measures (bỏ natural keys theo Kimball)
    measure_cols = [c for c in df_fact.columns
                    if c not in ("stock_key", "public_date_key", "symbol", "public_date")]
    output = ["stock_key", "public_date_key", *measure_cols]
    return df_fact.select([c for c in output if c in df_fact.columns]).sort(["stock_key", "public_date_key"])


def parse_args():
    p = argparse.ArgumentParser(description="Master: fact_stock_news with FK.")
    p.add_argument("--bucket", default=DEFAULT_BUCKET)
    p.add_argument("--overwrite", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    bucket = args.bucket
    fact_path = f"{bucket}/{SRC_FACT_PATH}"
    stock_path = f"{bucket}/{SRC_DIM_STOCK_PATH}"
    dst = f"{bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nMaster: fact_stock_news → fact_master_news (resolve FK)\n%s\n"
        "Fact source : s3://%s\nDim stock   : s3://%s\nDest        : s3://%s\nRun at      : %s\n%s",
        separator, separator, fact_path, stock_path, dst, run_at, separator)

    fs = _build_fs()
    for label, path in [("Fact (stage_2)", fact_path), ("Dim stock", stock_path)]:
        if not fs.exists(path):
            logger.error("%s not found: s3://%s", label, path)
            return

    df_fact = read_parquet(fs, fact_path)
    df_dim_stock = read_parquet(fs, stock_path)
    if df_fact.is_empty():
        logger.error("Fact source is empty — aborting.")
        return

    df_result = resolve_fk(df_fact, df_dim_stock)
    logger.info("Final: %s rows, schema: %s", f"{df_result.shape[0]:,}", df_result.schema)

    # ── GX Gate: Referential Integrity ─────────────────────────────
    logger.info("Running GX validation (Master: FK not null)...")
    gx_check_columns_not_null(df_result, {"columns": ["stock_key", "public_date_key"]})
    gx_check_table_row_count_between(df_result, {"min_value": 1})
    logger.info("GX validation passed ✓")

    write_parquet(df_result, fs, dst, overwrite=args.overwrite)
    logger.info("\n%s\nfact_master_news publish complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
