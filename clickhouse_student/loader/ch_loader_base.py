#!/usr/bin/env python3
"""
ch_loader_base.py

Base utilities cho việc load dữ liệu từ MinIO (Parquet) vào ClickHouse.

Pattern: TRUNCATE local table → INSERT batch via clickhouse-connect.
Replication tự động qua ReplicatedReplacingMergeTree.
"""

import io
import os
import logging
from datetime import datetime, timedelta, timezone

import polars as pl
import s3fs
import clickhouse_connect
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

ICT = timezone(timedelta(hours=7))

# ── MinIO config ─────────────────────────────────────────────────────────────

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

# ── ClickHouse config ────────────────────────────────────────────────────────

CH_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CH_PORT = int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123"))
CH_USER = os.getenv("CLICKHOUSE_USER", "default")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "default")
CH_DATABASE = "stock_data"

# ── Batch size for INSERT ────────────────────────────────────────────────────

INSERT_BATCH_SIZE = 50_000


# ── Helpers ──────────────────────────────────────────────────────────────────

def build_s3fs() -> s3fs.S3FileSystem:
    """Build s3fs client for MinIO."""
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def build_ch_client() -> clickhouse_connect.driver.Client:
    """Build ClickHouse HTTP client."""
    return clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )


def read_parquet_from_minio(
    fs: s3fs.S3FileSystem,
    s3_path: str,
    logger: logging.Logger,
) -> pl.DataFrame:
    """Đọc parquet từ MinIO, trả về Polars DataFrame."""
    logger.info("Reading s3://%s ...", s3_path)
    if not fs.exists(s3_path):
        logger.error("File not found: s3://%s", s3_path)
        return pl.DataFrame()
    with fs.open(s3_path, "rb") as f:
        df = pl.read_parquet(io.BytesIO(f.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def load_to_clickhouse(
    df: pl.DataFrame,
    ch_table_local: str,
    ch_table_distributed: str,
    logger: logging.Logger,
) -> None:
    """
    Full refresh: TRUNCATE local table → INSERT batch.

    Args:
        df: Polars DataFrame chứa dữ liệu cần load.
        ch_table_local: Tên bảng local (e.g., "dim_stock_local").
        ch_table_distributed: Tên bảng Distributed (e.g., "dim_stock") — dùng để verify.
        logger: Logger instance.
    """
    if df.is_empty():
        logger.warning("DataFrame is empty — skip load for %s.", ch_table_local)
        return

    client = build_ch_client()

    # 1. TRUNCATE local table (replica sẽ tự sync)
    logger.info("TRUNCATE TABLE %s.%s ...", CH_DATABASE, ch_table_local)
    client.command(f"TRUNCATE TABLE {CH_DATABASE}.{ch_table_local}")

    # 2. Get column names from ClickHouse table schema
    ch_columns_info = client.query(
        f"SELECT name FROM system.columns "
        f"WHERE database = '{CH_DATABASE}' AND table = '{ch_table_local}' "
        f"ORDER BY position"
    )
    ch_columns = [row[0] for row in ch_columns_info.result_rows]
    logger.info("CH columns: %s", ch_columns)

    # 3. Align DataFrame columns to CH schema
    # Chỉ lấy các cột có trong cả DataFrame VÀ CH table
    common_cols = [c for c in ch_columns if c in df.columns]
    missing_in_df = [c for c in ch_columns if c not in df.columns]
    extra_in_df = [c for c in df.columns if c not in ch_columns]

    if missing_in_df:
        logger.warning("Columns in CH but not in DataFrame (will be NULL): %s", missing_in_df)
    if extra_in_df:
        logger.info("Columns in DataFrame but not in CH (skipped): %s", extra_in_df)

    df_aligned = df.select(common_cols)

    # 4. Convert to Pandas for clickhouse-connect insert
    #    Pandas nanosecond timestamps overflow for dates > 2262-04-11.
    #    ClickHouse Date (UInt16) max = 2149-06-06.
    #    Workaround: clamp + convert Date → String → Python date.
    import datetime as _dt

    CH_DATE_MAX = _dt.date(2149, 6, 6)
    CH_DATE_MIN = _dt.date(1970, 1, 1)

    def _safe_date(x: str) -> _dt.date | None:
        if not isinstance(x, str) or not x:
            return None
        d = _dt.date.fromisoformat(x)
        if d > CH_DATE_MAX:
            return CH_DATE_MAX
        if d < CH_DATE_MIN:
            return CH_DATE_MIN
        return d

    date_cols = [c for c in df_aligned.columns if df_aligned[c].dtype == pl.Date]
    if date_cols:
        df_for_pandas = df_aligned.with_columns([
            pl.col(c).cast(pl.String).alias(c) for c in date_cols
        ])
        pdf = df_for_pandas.to_pandas()
        for c in date_cols:
            pdf[c] = pdf[c].apply(_safe_date)
    else:
        pdf = df_aligned.to_pandas()

    # 5. INSERT in batches
    total_rows = len(pdf)
    inserted = 0

    for start in range(0, total_rows, INSERT_BATCH_SIZE):
        end = min(start + INSERT_BATCH_SIZE, total_rows)
        batch = pdf.iloc[start:end]
        client.insert_df(
            table=f"{CH_DATABASE}.{ch_table_local}",
            df=batch,
            column_names=common_cols,
        )
        inserted += len(batch)
        logger.info("  Inserted %s/%s rows", f"{inserted:,}", f"{total_rows:,}")

    # 6. Verify via Distributed table
    result = client.query(f"SELECT count() FROM {CH_DATABASE}.{ch_table_distributed}")
    ch_count = result.result_rows[0][0]
    logger.info(
        "Verify: %s.%s has %s rows (expected %s)",
        CH_DATABASE, ch_table_distributed, f"{ch_count:,}", f"{total_rows:,}",
    )

    if ch_count != total_rows:
        logger.warning(
            "Row count mismatch! CH=%s vs Source=%s (may need SYSTEM SYNC REPLICA)",
            ch_count, total_rows,
        )

    client.close()
