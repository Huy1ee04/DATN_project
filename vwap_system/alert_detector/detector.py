"""
VWAP Alert Detector

Polls ClickHouse cho OHLCV nến mới (1 phút), cập nhật session VWAP state,
và ghi cảnh báo vào bảng alerts khi giá lệch ngưỡng.
Chạy: uv run detector.py
"""

import logging
import time
from datetime import datetime
from pathlib import Path

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
        self._ensure_schema()
        self.calc = VWAPCalculator()
        self._warm_up()

    def _ensure_schema(self) -> None:
        """Make sure required OHLC tables/MVs exist.

        Note: ClickHouse init.sql chỉ chạy lần đầu khi container volume rỗng,
        nên detector cần tự đảm bảo schema khi `ohlc_raw` chưa tồn tại.
        """
        db = self.config.CLICKHOUSE_DB
        try:
            exists = self.ch.query(
                f"SELECT count() FROM system.tables WHERE database = '{db}' AND name = 'ohlc_raw'"
            ).result_rows
            if exists and exists[0][0] > 0:
                return
        except Exception as exc:
            logger.error(f"Failed to check schema existence: {exc}", exc_info=True)

        init_sql_path = (
            Path(__file__).resolve().parents[1] / 'clickhouse' / 'init.sql'
        )
        if not init_sql_path.exists():
            logger.error(f"Missing init sql file: {init_sql_path}")
            return

        logger.warning(
            "ohlc_raw not found. Ensuring ClickHouse schema by running init.sql..."
        )
        sql_text = init_sql_path.read_text(encoding='utf-8')

        # Simple statement splitter: drop comment lines, then split by ';'
        # (Works for this init.sql format.)
        cleaned_lines: list[str] = []
        for line in sql_text.splitlines():
            stripped = line.strip()
            if stripped.startswith('--') or stripped == '':
                continue
            cleaned_lines.append(line)
        cleaned = '\n'.join(cleaned_lines)

        statements = [s.strip() for s in cleaned.split(';') if s.strip()]
        for stmt in statements:
            try:
                if hasattr(self.ch, 'command'):
                    self.ch.command(stmt)
                else:
                    self.ch.query(stmt)
            except Exception as exc:
                # Tối giản: cứ chạy tiếp để idempotent.
                logger.debug(f"Init stmt failed (ignored): {exc}")

        logger.info("ClickHouse schema ensured (init.sql executed).")

    # ------------------------------------------------------------------

    def _warm_up(self) -> None:
        """Nạp OHLCV hôm nay để VWAP có trạng thái ban đầu chính xác."""
        logger.info("Warming up VWAP state with today's OHLCV candles...")
        today = datetime.now(ICT).strftime('%Y-%m-%d')
        symbols_sql = ','.join([f"'{s.strip()}'" for s in self.config.SYMBOLS])
        rows = self.ch.query(
            f"SELECT candle_time, symbol, high, low, close, volume "
            f"FROM ohlc_raw "
            f"WHERE toDate(candle_time) = '{today}' AND symbol IN ({symbols_sql}) "
            f"ORDER BY candle_time ASC"
        ).result_rows

        last_seen_ts_by_symbol: dict[str, datetime] = {}
        for ts, symbol, high, low, close, volume in rows:
            # Deduplicate: cùng candle_time có thể được publish nhiều lần.
            if last_seen_ts_by_symbol.get(symbol) == ts:
                continue
            self.calc.update(
                symbol=symbol,
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                ts=ts,
            )
            last_seen_ts_by_symbol[symbol] = ts

        # last candle_time theo từng symbol để không bị bỏ lỡ khi poll.
        self._last_ts_by_symbol = {}
        for ts, symbol, *_ in rows:
            self._last_ts_by_symbol[symbol] = ts

        logger.info(f"Warm-up done: {len(rows):,} candles loaded")

    def _fetch_new_ohlc(self, symbol: str):
        last_ts = self._last_ts_by_symbol.get(symbol)
        today = datetime.now(ICT).strftime('%Y-%m-%d')
        if last_ts:
            where = (
                f"symbol = '{symbol}' "
                f"AND toDate(candle_time) = '{today}' "
                f"AND candle_time > '{last_ts.strftime('%Y-%m-%d %H:%M:%S.%f')}'"
            )
        else:
            where = f"symbol = '{symbol}' AND toDate(candle_time) = '{today}'"

        return self.ch.query(
            f"SELECT candle_time, symbol, high, low, close, volume "
            f"FROM ohlc_raw WHERE {where} "
            f"ORDER BY candle_time ASC LIMIT 5000"
        ).result_rows

    def _fire(
        self,
        symbol: str,
        alert_type: str,
        price: float,
        vwap: float,
        deviation_pct: float,
        ts: datetime,
    ) -> None:
        logger.warning(
            f"🚨 {alert_type} | {symbol} "
            f"price={price:.2f} vwap={vwap:.2f} dev={deviation_pct:+.2f}%"
        )
        self.ch.insert(
            'alerts',
            [[ts, symbol, alert_type, price, vwap, deviation_pct]],
            column_names=[
                'alert_time', 'symbol', 'alert_type', 'price',
                'vwap', 'deviation_pct',
            ],
        )

    def _check(self, symbol: str, price: float, ts: datetime) -> None:
        s_vwap, s_sigma = self.calc.get_session_vwap_and_sigma(symbol, ts)
        if not s_vwap or s_vwap <= 0:
            return

        # deviation_pct vẫn lưu để dashboard không phải thay đổi nhiều.
        deviation_pct = (price - s_vwap) / s_vwap * 100

        mode = getattr(self.config, 'ALERT_BAND_MODE', 'sigma')
        if mode == 'pct':
            threshold = self.config.ALERT_THRESHOLD_PCT
            if deviation_pct > threshold:
                self._fire(symbol, 'BREAKOUT_UP', price, s_vwap, deviation_pct, ts)
            elif deviation_pct < -threshold:
                self._fire(symbol, 'BREAKDOWN', price, s_vwap, deviation_pct, ts)
            return

        # mode = 'sigma' (bands chuẩn)
        k = float(getattr(self.config, 'BAND_SIGMA_MULTIPLIER', 2.0))
        if not s_sigma or s_sigma <= 0:
            return

        upper = s_vwap + k * s_sigma
        lower = s_vwap - k * s_sigma
        if price > upper:
            self._fire(symbol, 'BREAKOUT_UP', price, s_vwap, deviation_pct, ts)
        elif price < lower:
            self._fire(symbol, 'BREAKDOWN', price, s_vwap, deviation_pct, ts)

    def run(self) -> None:
        logger.info(
            f"Detector running | mode={self.config.ALERT_BAND_MODE} "
            f"| pct_threshold=±{self.config.ALERT_THRESHOLD_PCT}% "
            f"| sigma_k={self.config.BAND_SIGMA_MULTIPLIER} "
            f"| poll every {self.config.POLL_INTERVAL_SEC}s"
        )
        while True:
            try:
                total_processed = 0
                for symbol in self.config.SYMBOLS:
                    candles = self._fetch_new_ohlc(symbol)
                    if not candles:
                        continue

                    prev_ts: datetime | None = None
                    for ts, sym, high, low, close, volume in candles:
                        # Deduplicate theo candle_time.
                        if prev_ts == ts:
                            continue
                        self.calc.update(
                            symbol=sym,
                            high=float(high),
                            low=float(low),
                            close=float(close),
                            volume=int(volume),
                            ts=ts,
                        )
                        self._check(sym, float(close), ts)

                    self._last_ts_by_symbol[symbol] = prev_ts if prev_ts else candles[-1][0]
                    total_processed += len(candles)

                if total_processed:
                    logger.debug(f"Processed {total_processed} candles")

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
