#!/usr/bin/env python3
"""Load dim_master_sector.parquet → stock_data.dim_sector."""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ch_loader_base import (
    DEFAULT_BUCKET, build_s3fs, read_parquet_from_minio, load_to_clickhouse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_dim_sector")

MINIO_PATH = "master/dimension/dim_master_sector.parquet"
CH_LOCAL = "dim_sector_local"
CH_DISTRIBUTED = "dim_sector"


def main() -> None:
    logger.info("=== Load dim_sector → ClickHouse ===")
    fs = build_s3fs()
    s3_path = f"{DEFAULT_BUCKET}/{MINIO_PATH}"
    df = read_parquet_from_minio(fs, s3_path, logger)
    if df.is_empty():
        return
    load_to_clickhouse(df, CH_LOCAL, CH_DISTRIBUTED, logger)
    logger.info("=== dim_sector load complete! ===")


if __name__ == "__main__":
    main()
