"""Cấu hình cho Trade Producer — đọc từ .env"""
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))


class Config:
    # DNSE WebSocket API
    DNSE_API_KEY: str = os.getenv('DNSE_API_KEY', 'your-api-key')
    DNSE_API_SECRET: str = os.getenv('DNSE_API_SECRET', 'your-api-secret')
    DNSE_WS_URL: str = os.getenv('DNSE_WS_URL', 'wss://ws-openapi.dnse.com.vn')
    DNSE_ENCODING: str = os.getenv('DNSE_ENCODING', 'msgpack')

    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TRADES_TOPIC: str = os.getenv('KAFKA_TOPIC', 'dnse.trades')
    KAFKA_OHLC_TOPIC: str = os.getenv('KAFKA_OHLC_TOPIC', 'dnse.ohlc')

    # Resolution cho OHLC (DNSE WebSocket: "1" ~ 1 phút)
    OHLC_RESOLUTION: str = os.getenv('OHLC_RESOLUTION', '1')

    # Danh sách mã chứng khoán cần theo dõi
    SYMBOLS: list = os.getenv('SYMBOLS', 'HPG,SSI,VNM,VCB,TCB').split(',')
