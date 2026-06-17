#!/usr/bin/env python3
"""
bridge_stock_index.py (stage_2) — SCD Type 2 merge.

Nhận snapshot từ stage_1 (trạng thái hiện tại) và merge với lịch sử SCD Type 2
đã có từ stage_2 trước đó.

Source (MinIO):
  transformed/stage_1/dimension/bridge_stock_index_snapshot.parquet  → snapshot hiện tại
  transformed/stage_2/dimension/bridge_stock_index.parquet           → lịch sử SCD2 (self-read)

Destination (MinIO):
  transformed/stage_2/dimension/bridge_stock_index.parquet

SCD Type 2 Logic:
  Nếu file stage_2 cũ đã tồn tại:
    - Bản ghi CŨ còn hiệu lực + VẪN CÒN trong snapshot → giữ nguyên effective_from/to, is_current=1.
    - Bản ghi CŨ còn hiệu lực + BỊ LOẠI khỏi snapshot → đóng: effective_to = run_date - 1, is_current=0.
    - Bản ghi MỚI (chưa có trong lịch sử active)       → mở: effective_from = run_date, effective_to = 9999-12-31, is_current=1.
    - Bản ghi CŨ đã đóng (effective_to < 9999-12-31)   → giữ nguyên (lịch sử).
  Nếu file chưa tồn tại:
    - Tất cả rows → effective_from = run_date, effective_to = 9999-12-31, is_current=1.

Output schema:
  symbol         (Utf8)  — mã chứng khoán
  index_id       (Int64) — ID chỉ số
  effective_from (Date)  — ngày bắt đầu hiệu lực
  effective_to   (Date)  — ngày kết thúc (9999-12-31 = still active)
  is_current     (Int8)  — 1 = active, 0 = historical
"""

import argparse
import io
import logging
import os
from datetime import date, datetime, timedelta, timezone

import polars as pl
import s3fs
from dotenv import load_dotenv

from vtit_gx.polars import (
    gx_check_columns_not_null,
    gx_check_compound_columns_unique,
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
logger = logging.getLogger("bridge_stock_index_stage2")
ICT = timezone(timedelta(hours=7))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9100")
if not MINIO_ENDPOINT.startswith("http"):
    MINIO_ENDPOINT = f"http://{MINIO_ENDPOINT}"

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio_access_key")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio_secret_key")
DEFAULT_BUCKET = os.getenv("MINIO_BUCKET", "stock-data")

# stage_1 snapshot (input)
SRC_SNAPSHOT_PATH = "transformed/stage_1/dimension/bridge_stock_index_snapshot.parquet"

# stage_2 SCD2 history (self-read for merge + output)
DST_PREFIX = "transformed/stage_2/dimension"
DST_FILENAME = "bridge_stock_index.parquet"

MERGE_KEY = ["symbol", "index_id"]
OPEN_END_DATE = date(9999, 12, 31)


# ── Helpers ──────────────────────────────────────────────────────────────────

def _build_fs() -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=MINIO_ACCESS_KEY,
        secret=MINIO_SECRET_KEY,
        client_kwargs={"endpoint_url": MINIO_ENDPOINT},
    )


def read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    logger.info("Reading s3://%s ...", s3_path)
    with fs.open(s3_path, "rb") as file:
        df = pl.read_parquet(io.BytesIO(file.read()))
    logger.info("  → %s rows × %s cols", f"{df.shape[0]:,}", df.shape[1])
    return df


def try_read_parquet(fs: s3fs.S3FileSystem, s3_path: str) -> pl.DataFrame:
    """Đọc parquet nếu tồn tại, trả về DataFrame rỗng nếu không (first run)."""
    if not fs.exists(s3_path):
        logger.info("Stage_2 history not found (first run?): s3://%s", s3_path)
        return pl.DataFrame()
    try:
        return read_parquet(fs, s3_path)
    except Exception as exc:
        logger.warning("Cannot read s3://%s: %s", s3_path, exc)
        return pl.DataFrame()


def write_parquet(
    df: pl.DataFrame,
    fs: s3fs.S3FileSystem,
    s3_path: str,
) -> None:
    """SCD2 luôn overwrite — vì kết quả đã bao gồm toàn bộ lịch sử."""
    buffer = io.BytesIO()
    df.write_parquet(buffer, compression="snappy")
    buffer.seek(0)
    with fs.open(s3_path, "wb") as file:
        file.write(buffer.read())
    size_kb = (fs.size(s3_path) or 0) / 1024
    logger.info("Saved s3://%s (%.1f KB, %s rows)", s3_path, size_kb, f"{df.shape[0]:,}")


# ── Validate ─────────────────────────────────────────────────────────────────

def validate_snapshot(df: pl.DataFrame) -> None:
    """Kiểm tra snapshot từ stage_1 có đúng schema."""
    required = {"symbol", "index_id"}
    actual = set(df.columns)
    missing = required - actual
    if missing:
        raise ValueError(
            f"stage_1 snapshot thiếu cột: {sorted(missing)}. "
            f"Cột thực tế: {sorted(actual)}"
        )
    logger.info("Snapshot schema validation passed ✓")


# ── SCD Type 2 merge ────────────────────────────────────────────────────────

def merge_scd2(
    df_existing: pl.DataFrame,
    df_snapshot: pl.DataFrame,
    run_date: date,
) -> pl.DataFrame:
    """
    Merge snapshot hiện tại với lịch sử SCD Type 2.

    Args:
        df_existing: File stage_2 hiện tại trên MinIO (có thể rỗng = first run).
        df_snapshot: Snapshot từ stage_1 (symbol + index_id, trạng thái hiện tại).
        run_date: Ngày chạy transform.

    Returns:
        DataFrame đã merge: historical + closed + active.
    """
    # First run: tất cả rows là mới
    if df_existing.is_empty():
        n = df_snapshot.shape[0]
        logger.info("First run — all %s rows are new.", f"{n:,}")
        return (
            df_snapshot.with_columns(
                pl.lit(run_date).alias("effective_from"),
                pl.lit(OPEN_END_DATE).alias("effective_to"),
                pl.lit(1).cast(pl.Int8).alias("is_current"),
            )
            .select(["symbol", "index_id", "effective_from", "effective_to", "is_current"])
        )

    # Đảm bảo cột SCD đúng kiểu
    for col in ("effective_from", "effective_to"):
        if col in df_existing.columns:
            df_existing = df_existing.with_columns(pl.col(col).cast(pl.Date, strict=False))
    if "is_current" in df_existing.columns:
        df_existing = df_existing.with_columns(pl.col("is_current").cast(pl.Int8, strict=False))
    else:
        # Backward compat: nếu file cũ không có is_current, infer từ effective_to
        df_existing = df_existing.with_columns(
            (pl.col("effective_to") == OPEN_END_DATE).cast(pl.Int8).alias("is_current")
        )

    # 1. Tách bản ghi cũ: đã đóng (lịch sử) vs còn hiệu lực
    df_history = df_existing.filter(pl.col("is_current") == 0)
    df_active_old = df_existing.filter(pl.col("is_current") == 1)

    logger.info(
        "Existing stage_2: %s historical (closed) + %s active (open).",
        f"{df_history.shape[0]:,}",
        f"{df_active_old.shape[0]:,}",
    )

    # 2. Xác định tập hợp key
    new_keys = df_snapshot.select(MERGE_KEY).unique()
    old_active_keys = df_active_old.select(MERGE_KEY).unique()

    # 3. Bản ghi VẪN CÒN trong snapshot → giữ nguyên effective_from, is_current=1
    df_unchanged = df_active_old.join(new_keys, on=MERGE_KEY, how="inner")

    # 4. Bản ghi BỊ LOẠI → đóng: effective_to = run_date - 1, is_current = 0
    close_date = run_date - timedelta(days=1)
    df_removed = (
        df_active_old.join(new_keys, on=MERGE_KEY, how="anti")
        .with_columns(
            pl.lit(close_date).alias("effective_to"),
            pl.lit(0).cast(pl.Int8).alias("is_current"),
        )
    )

    # 5. Bản ghi MỚI (chưa có trong active cũ) → mở mới
    df_added = (
        df_snapshot.join(old_active_keys, on=MERGE_KEY, how="anti")
        .with_columns(
            pl.lit(run_date).alias("effective_from"),
            pl.lit(OPEN_END_DATE).alias("effective_to"),
            pl.lit(1).cast(pl.Int8).alias("is_current"),
        )
    )

    logger.info(
        "SCD2 merge: %s unchanged, %s closed, %s new.",
        f"{df_unchanged.shape[0]:,}",
        f"{df_removed.shape[0]:,}",
        f"{df_added.shape[0]:,}",
    )

    # 6. Ghép tất cả: lịch sử + đã đóng + giữ nguyên + mới
    output_cols = ["symbol", "index_id", "effective_from", "effective_to", "is_current"]
    result = pl.concat(
        [
            df_history.select(output_cols),
            df_removed.select(output_cols),
            df_unchanged.select(output_cols),
            df_added.select(output_cols),
        ],
        how="vertical",
    )
    return result.unique().sort(["index_id", "symbol", "effective_from"])


# ── Main ─────────────────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stage 2: SCD Type 2 merge for bridge_stock_index."
    )
    parser.add_argument("--bucket", default=DEFAULT_BUCKET, help="MinIO bucket")
    parser.add_argument(
        "--date",
        default=None,
        help="Ngày hiệu lực (effective_from) YYYY-MM-DD. Mặc định: today (ICT).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_date = (
        date.fromisoformat(args.date) if args.date else datetime.now(ICT).date()
    )
    fs = _build_fs()

    snapshot_path = f"{args.bucket}/{SRC_SNAPSHOT_PATH}"
    dst_path = f"{args.bucket}/{DST_PREFIX}/{DST_FILENAME}"

    separator = "=" * 80
    run_at = datetime.now(ICT).strftime("%Y-%m-%d %H:%M:%S %Z")
    logger.info(
        "\n%s\nStage 2: SCD Type 2 merge — bridge_stock_index\n%s\n"
        "MinIO Endpoint : %s\n"
        "Snapshot (stg1): s3://%s\n"
        "History (stg2) : s3://%s\n"
        "Destination    : s3://%s\n"
        "Run date       : %s\n"
        "Run at         : %s\n%s",
        separator,
        separator,
        MINIO_ENDPOINT,
        snapshot_path,
        dst_path,
        dst_path,
        run_date,
        run_at,
        separator,
    )

    # 1. Đọc snapshot từ stage_1
    if not fs.exists(snapshot_path):
        logger.error("Snapshot not found: s3://%s — run stage_1 first.", snapshot_path)
        return

    df_snapshot = read_parquet(fs, snapshot_path)
    if df_snapshot.is_empty():
        logger.error("Snapshot is empty — aborting.")
        return

    validate_snapshot(df_snapshot)

    # 2. Đọc lịch sử SCD2 hiện tại (nếu có)
    df_existing = try_read_parquet(fs, dst_path)

    # 3. Apply SCD Type 2 merge
    df_result = merge_scd2(df_existing, df_snapshot, run_date)

    # 4. Log thống kê
    n_active = df_result.filter(pl.col("is_current") == 1).shape[0]
    n_historical = df_result.filter(pl.col("is_current") == 0).shape[0]
    logger.info(
        "Result: %s total rows (%s active, %s historical).",
        f"{df_result.shape[0]:,}",
        f"{n_active:,}",
        f"{n_historical:,}",
    )

    # ── GX Gate: Business Logic ─────────────────────────────────────────
    logger.info("Running GX validation (Stage 2: SCD2 integrity)...")
    gx_check_columns_not_null(df_result, {
        "columns": ["symbol", "index_id", "effective_from", "effective_to", "is_current"]
    })
    # Active records: (symbol, index_id) must be unique
    df_active = df_result.filter(pl.col("is_current") == 1)
    if df_active.shape[0] > 0:
        gx_check_compound_columns_unique(df_active, {"column_list": ["symbol", "index_id"]})
    gx_check_table_row_count_between(df_result, {"min_value": 1})
    logger.info("GX validation passed ✓")

    # 5. Ghi kết quả (luôn overwrite vì kết quả bao gồm toàn bộ lịch sử)
    write_parquet(df_result, fs, dst_path)

    logger.info("\n%s\nbridge_stock_index stage_2 (SCD2) complete!\n%s", separator, separator)


if __name__ == "__main__":
    main()
