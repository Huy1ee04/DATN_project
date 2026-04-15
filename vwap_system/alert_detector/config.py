"""Cấu hình cho Alert Detector — đọc từ .env"""
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))


class Config:
    CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_HTTP_PORT: int = int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123))
    CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', 'default')
    CLICKHOUSE_DB: str = os.getenv('CLICKHOUSE_DB', 'vwap')

    ALERT_THRESHOLD_PCT: float = float(os.getenv('ALERT_THRESHOLD_PCT', 1.5))
    POLL_INTERVAL_SEC: int = int(os.getenv('POLL_INTERVAL_SEC', 10))

    SYMBOLS: list = os.getenv('SYMBOLS', 'HPG,SSI,VNM,VCB,TCB').split(',')
