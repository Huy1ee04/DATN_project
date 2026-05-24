"""Cấu hình cho Alert Detector — đọc từ .env"""
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))


class Config:
    # --- ClickHouse ---
    CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_HTTP_PORT: int = int(os.getenv('CLICKHOUSE_HTTP_PORT', 8123))
    CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', 'default')
    CLICKHOUSE_DB: str = os.getenv('CLICKHOUSE_DB', 'vwap')

    # --- VWAP Bands ---
    # Nếu ALERT_BAND_MODE='pct' thì dùng ngưỡng phần trăm |price-vwap|/vwap.
    ALERT_THRESHOLD_PCT: float = float(os.getenv('ALERT_THRESHOLD_PCT', 1.5))
    # Nếu ALERT_BAND_MODE='sigma' thì dùng bands chuẩn:
    #   upper = vwap + k*sigma
    #   lower = vwap - k*sigma
    ALERT_BAND_MODE: str = os.getenv('ALERT_BAND_MODE', 'sigma')  # 'pct' | 'sigma'
    BAND_SIGMA_MULTIPLIER: float = float(os.getenv('BAND_SIGMA_MULTIPLIER', 2.0))

    # --- RSI ---
    RSI_PERIOD: int = int(os.getenv('RSI_PERIOD', 14))
    RSI_OVERBOUGHT: float = float(os.getenv('RSI_OVERBOUGHT', 70))
    RSI_OVERSOLD: float = float(os.getenv('RSI_OVERSOLD', 30))

    # --- Volume Spike ---
    VOLUME_LOOKBACK: int = int(os.getenv('VOLUME_LOOKBACK', 20))
    VOLUME_SPIKE_RATIO: float = float(os.getenv('VOLUME_SPIKE_RATIO', 3.0))

    # --- Runtime ---
    POLL_INTERVAL_SEC: int = int(os.getenv('POLL_INTERVAL_SEC', 10))
    ALERT_COOLDOWN_SEC: int = int(os.getenv('ALERT_COOLDOWN_SEC', 300))
    CANDLE_BUFFER_SIZE: int = int(os.getenv('CANDLE_BUFFER_SIZE', 50))

    SYMBOLS: list = os.getenv('SYMBOLS', 'HPG,SSI,VNM,VCB,TCB').split(',')
