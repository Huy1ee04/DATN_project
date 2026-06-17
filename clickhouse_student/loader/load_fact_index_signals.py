#!/usr/bin/env python3
"""Load fact_master_index_signals.parquet → stock_data.fact_index_signals."""

import logging
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from ch_loader_base import (
    DEFAULT_BUCKET, build_s3fs, read_parquet_from_minio, load_to_clickhouse,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("load_fact_index_signals")

MINIO_PATH = "master/fact/fact_master_index_signals.parquet"
CH_LOCAL = "fact_index_signals_local"
CH_DISTRIBUTED = "fact_index_signals"


def main() -> None:
    logger.info("=== Load fact_index_signals → ClickHouse ===")
    fs = build_s3fs()
    s3_path = f"{DEFAULT_BUCKET}/{MINIO_PATH}"
    df = read_parquet_from_minio(fs, s3_path, logger)
    if df.is_empty():
        return
    load_to_clickhouse(df, CH_LOCAL, CH_DISTRIBUTED, logger)
    logger.info("=== fact_index_signals load complete! ===")


if __name__ == "__main__":
    main()
