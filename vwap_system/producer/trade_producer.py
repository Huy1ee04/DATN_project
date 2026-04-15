"""
Trade Producer — DNSE WebSocket → Kafka

Subscribe real-time trade ticks từ DNSE và publish lên Kafka topic.
Chạy: uv run trade_producer.py
"""

import asyncio
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone, timedelta

# Thêm path đến trading_websocket SDK
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'dnse_sdk', 'trading_websocket'))

from kafka import KafkaProducer
from trading_websocket import TradingClient
from trading_websocket.models import Trade
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('trade_producer')

ICT = timezone(timedelta(hours=7))


def create_kafka_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        acks='all',
        retries=5,
        retry_backoff_ms=500,
        linger_ms=5,          # batch nhỏ để giảm latency
        batch_size=16384,
    )


async def run(config: Config) -> None:
    producer = create_kafka_producer(config.KAFKA_BOOTSTRAP_SERVERS)
    logger.info(f"Kafka producer ready → {config.KAFKA_BOOTSTRAP_SERVERS}")

    client = TradingClient(
        api_key=config.DNSE_API_KEY,
        api_secret=config.DNSE_API_SECRET,
        base_url=config.DNSE_WS_URL,
        encoding=config.DNSE_ENCODING,
        auto_reconnect=True,
        max_retries=10,
    )

    stats = {'sent': 0, 'errors': 0}

    def handle_trade(trade: Trade) -> None:
        received_at = datetime.now(ICT).strftime('%Y-%m-%dT%H:%M:%S.%f')
        message = {
            'received_at': received_at,
            'symbol': trade.symbol,
            'price': float(trade.price),
            'quantity': int(trade.quantity),
            'total_volume': int(trade.totalVolumeTraded),
            'board_id': int(trade.boardId),
            'market_id': int(trade.marketId),
        }
        try:
            producer.send(config.KAFKA_TOPIC, value=message)
            stats['sent'] += 1
            if stats['sent'] % 200 == 0:
                logger.info(
                    f"[{trade.symbol}] price={trade.price:.2f} "
                    f"qty={trade.quantity:,} | total_sent={stats['sent']:,}"
                )
        except Exception as exc:
            stats['errors'] += 1
            logger.error(f"Kafka send error: {exc}")

    await client.connect()
    logger.info(f"Connected to DNSE. Subscribing: {config.SYMBOLS}")

    await client.subscribe_trades(
        symbols=config.SYMBOLS,
        on_trade=handle_trade,
        encoding=config.DNSE_ENCODING,
    )

    logger.info("Producer running — Ctrl+C để dừng")

    try:
        while True:
            await asyncio.sleep(60)
            logger.info(
                f"Heartbeat | sent={stats['sent']:,} errors={stats['errors']}"
            )
    except asyncio.CancelledError:
        logger.info("Shutting down gracefully...")
    finally:
        await client.disconnect()
        producer.flush(timeout=15)
        producer.close()
        logger.info(f"Done. Total sent: {stats['sent']:,}")


def main() -> None:
    config = Config()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    task = loop.create_task(run(config))

    def _shutdown(sig, _frame):
        logger.info(f"Signal {sig.name} received")
        task.cancel()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        loop.run_until_complete(task)
    except asyncio.CancelledError:
        pass
    finally:
        loop.close()


if __name__ == '__main__':
    main()
