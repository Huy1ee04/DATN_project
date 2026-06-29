"""
config.py — Application settings from environment variables.
"""
import os
from functools import lru_cache


class Settings:
    """Simple settings from env vars — no Hydra/OmegaConf needed."""

    CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "localhost")
    CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
    CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "default")
    CLICKHOUSE_DATABASE: str = os.getenv("CLICKHOUSE_DATABASE", "stock_data")

    API_TITLE: str = os.getenv("API_TITLE", "DATN Stock Dashboard API")
    API_VERSION: str = os.getenv("API_VERSION", "1.0.0")


@lru_cache()
def get_settings() -> Settings:
    return Settings()
