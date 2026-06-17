#!/usr/bin/env python3
"""Load bridge_master_stock_index.parquet → stock_data.bridge_stock_index."""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ch_loader_base import (
    DEFAULT_BUCKET, build_s3fs, read_parquet_from_minio, load_to_clickhouse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_bridge_stock_index")

MINIO_PATH = "master/dimension/bridge_master_stock_index.parquet"
CH_LOCAL = "bridge_stock_index_local"
CH_DISTRIBUTED = "bridge_stock_index"


def main() -> None:
    logger.info("=== Load bridge_stock_index → ClickHouse ===")
    fs = build_s3fs()
    s3_path = f"{DEFAULT_BUCKET}/{MINIO_PATH}"
    df = read_parquet_from_minio(fs, s3_path, logger)
    if df.is_empty():
        return
    load_to_clickhouse(df, CH_LOCAL, CH_DISTRIBUTED, logger)
    logger.info("=== bridge_stock_index load complete! ===")


if __name__ == "__main__":
    main()
