"""
VWAP Alert Detector

Polls ClickHouse cho ticks mới, cập nhật VWAP state,
và ghi cảnh báo vào bảng alerts khi giá lệch ngưỡng.
Chạy: uv run detector.py
"""

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

import clickhouse_connect

from vwap import VWAPCalculator, ICT
from config import Config

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('detector')


class AlertDetector:

    def __init__(self, config: Config):
        self.config = config
        self.ch = clickhouse_connect.get_client(
            host=config.CLICKHOUSE_HOST,
            port=config.CLICKHOUSE_HTTP_PORT,
            username=config.CLICKHOUSE_USER,
            password=config.CLICKHOUSE_PASSWORD,
            database=config.CLICKHOUSE_DB,
        )
        self.calc = VWAPCalculator()
        self._last_ts: Optional[datetime] = None
        self._warm_up()

    # ------------------------------------------------------------------

    def _warm_up(self) -> None:
        """Nạp ticks hôm nay để VWAP có trạng thái ban đầu chính xác."""
        logger.info("Warming up VWAP state with today's ticks...")
        today = datetime.now(ICT).strftime('%Y-%m-%d')
        rows = self.ch.query(
            f"SELECT received_at, symbol, price, quantity "
            f"FROM trades_raw "
            f"WHERE toDate(received_at) = '{today}' "
            f"ORDER BY received_at ASC"
        ).result_rows

        for ts, symbol, price, quantity in rows:
            self.calc.update(symbol, float(price), int(quantity), ts)

        if rows:
            self._last_ts = rows[-1][0]
        logger.info(f"Warm-up done: {len(rows):,} ticks loaded")

    def _fetch_new_ticks(self):
        if self._last_ts:
            where = (
                f"received_at > '{self._last_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
            )
        else:
            today = datetime.now(ICT).strftime('%Y-%m-%d')
            where = f"toDate(received_at) = '{today}'"

        return self.ch.query(
            f"SELECT received_at, symbol, price, quantity "
            f"FROM trades_raw WHERE {where} "
            f"ORDER BY received_at ASC LIMIT 5000"
        ).result_rows

    def _fire(
        self,
        symbol: str,
        alert_type: str,
        price: float,
        vwap: float,
        deviation_pct: float,
    ) -> None:
        now = datetime.now(ICT)
        logger.warning(
            f"🚨 {alert_type} | {symbol} "
            f"price={price:.2f} vwap={vwap:.2f} dev={deviation_pct:+.2f}%"
        )
        self.ch.insert(
            'alerts',
            [[now, symbol, alert_type, price, vwap, deviation_pct]],
            column_names=[
                'alert_time', 'symbol', 'alert_type', 'price',
                'vwap', 'deviation_pct',
            ],
        )

    def _check(self, symbol: str, price: float, ts: datetime) -> None:
        threshold = self.config.ALERT_THRESHOLD_PCT

        # --- Session VWAP ---
        s_vwap = self.calc.get_session_vwap(symbol, ts)
        if s_vwap and s_vwap > 0:
            dev = (price - s_vwap) / s_vwap * 100
            if dev > threshold:
                self._fire(symbol, 'BREAKOUT_UP', price, s_vwap, dev)
            elif dev < -threshold:
                self._fire(symbol, 'BREAKDOWN', price, s_vwap, dev)

    def run(self) -> None:
        logger.info(
            f"Detector running | threshold=±{self.config.ALERT_THRESHOLD_PCT}% "
            f"| poll every {self.config.POLL_INTERVAL_SEC}s"
        )
        while True:
            try:
                ticks = self._fetch_new_ticks()
                if ticks:
                    for ts, symbol, price, quantity in ticks:
                        self.calc.update(symbol, float(price), int(quantity), ts)
                        self._check(symbol, float(price), ts)
                    self._last_ts = ticks[-1][0]
                    logger.debug(f"Processed {len(ticks)} ticks")

                # Dọn dẹp anchor cũ mỗi ngày
                self.calc.cleanup_old_anchors(cutoff_days=1)

            except KeyboardInterrupt:
                logger.info("Detector stopped by user.")
                break
            except Exception as exc:
                logger.error(f"Detector error: {exc}", exc_info=True)

            time.sleep(self.config.POLL_INTERVAL_SEC)


if __name__ == '__main__':
    AlertDetector(Config()).run()
