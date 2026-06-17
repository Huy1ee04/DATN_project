"""
Multi-Signal Alert Detector

Polls ClickHouse cho OHLCV nến mới (1 phút), cập nhật VWAP + candle buffer,
chạy tất cả alert rules (VWAP, RSI, Volume Spike), ghi cảnh báo vào alerts_v2.
Chạy: uv run detector.py
"""

import logging
import time
from datetime import datetime
from pathlib import Path

import clickhouse_connect

from vwap import VWAPCalculator, ICT
from candle_buffer import CandleBuffer, Candle
from models import Alert
from config import Config
from slack_notifier import SlackNotifier

# Cảnh báo đơn lẻ (tạm comment — dùng Combined rule thay thế)
# from rules.vwap_rule import VWAPRule
# from rules.rsi_rule import RSIRule
# from rules.volume_spike_rule import VolumeSpikeRule

# Cảnh báo kết hợp đa tín hiệu (≥ 2 chỉ số đồng thuận mới fire)
from rules.combined_rule import CombinedSignalRule

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
        self.buffer = CandleBuffer(maxlen=config.CANDLE_BUFFER_SIZE)

        # Đăng ký alert rules
        # --- Cảnh báo đơn lẻ (tạm comment — dùng Combined rule) ---
        # self.rules = [
        #     VWAPRule(config, self.calc),
        #     RSIRule(config),
        #     VolumeSpikeRule(config),
        # ]

        # --- Cảnh báo kết hợp (chỉ fire khi ≥ 2 tín hiệu đồng thuận) ---
        self.rules = [
            CombinedSignalRule(config, self.calc),
        ]
        rule_names = [r.RULE_NAME for r in self.rules]
        logger.info(f"Registered rules: {rule_names}")

        # Slack notifier (chỉ gửi CRITICAL)
        self.slack = SlackNotifier()

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
                # Kiểm tra thêm alerts_v2
                self._ensure_alerts_v2()
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
        self._ensure_alerts_v2()

    def _ensure_alerts_v2(self) -> None:
        """Tạo bảng alerts_v2 nếu chưa tồn tại."""
        try:
            stmt = """
            CREATE TABLE IF NOT EXISTS alerts_v2 (
                alert_time      DateTime64(3, 'Asia/Ho_Chi_Minh'),
                symbol          LowCardinality(String),
                rule_name       LowCardinality(String),
                alert_type      String,
                severity        LowCardinality(String),
                price           Float64,
                indicator_value Float64,
                threshold       Float64,
                deviation_pct   Float64,
                message         String
            ) ENGINE = MergeTree()
            ORDER BY (alert_time, symbol, rule_name)
            TTL toDate(alert_time) + INTERVAL 90 DAY
            """
            if hasattr(self.ch, 'command'):
                self.ch.command(stmt)
            else:
                self.ch.query(stmt)
            logger.info("alerts_v2 table ensured.")
        except Exception as exc:
            logger.debug(f"alerts_v2 creation (ignored): {exc}")

    # ------------------------------------------------------------------

    def _warm_up(self) -> None:
        """Nạp OHLCV hôm nay để VWAP + buffer có trạng thái ban đầu chính xác.

        Dùng argMax để lấy bản cập nhật mới nhất cho mỗi cây nến.
        """
        logger.info("Warming up VWAP + candle buffer with today's OHLCV candles...")
        today = datetime.now(ICT).strftime('%Y-%m-%d')
        symbols_sql = ','.join([f"'{s.strip()}'" for s in self.config.SYMBOLS])

        # Lấy bản mới nhất của mỗi nến (deduplicate bằng argMax)
        rows = self.ch.query(
            f"SELECT "
            f"  candle_time, "
            f"  symbol, "
            f"  argMax(open, received_at) AS open, "
            f"  argMax(high, received_at) AS high, "
            f"  argMax(low, received_at) AS low, "
            f"  argMax(close, received_at) AS close, "
            f"  argMax(volume, received_at) AS volume, "
            f"  max(received_at) AS last_received "
            f"FROM ohlc_raw "
            f"WHERE toDate(candle_time) = '{today}' AND symbol IN ({symbols_sql}) "
            f"GROUP BY candle_time, symbol "
            f"ORDER BY candle_time ASC"
        ).result_rows

        # Watermark = received_at lớn nhất trong warm-up
        self._last_received_at: dict[str, datetime] = {}

        for ts, symbol, open_, high, low, close, volume, last_recv in rows:
            self.calc.update(
                symbol=symbol,
                high=float(high),
                low=float(low),
                close=float(close),
                volume=int(volume),
                ts=ts,
            )
            self.buffer.push(symbol, Candle(
                ts=ts, open=float(open_), high=float(high),
                low=float(low), close=float(close), volume=int(volume),
            ))
            self._last_received_at[symbol] = last_recv

        for sym in self.config.SYMBOLS:
            s = sym.strip()
            logger.info(
                f"  {s}: buffer={self.buffer.size(s)} candles, "
                f"vwap={self.calc.get_session_vwap(s)}"
            )

        logger.info(f"Warm-up done: {len(rows):,} candles loaded")

    def _fetch_new_ohlc(self, symbol: str):
        """Lấy nến mới từ ClickHouse.

        Watermark dựa trên received_at (thời điểm nhận message),
        argMax deduplicate lấy bản cập nhật mới nhất cho mỗi candle_time.
        """
        last_recv = self._last_received_at.get(symbol)
        today = datetime.now(ICT).strftime('%Y-%m-%d')

        if last_recv:
            recv_str = last_recv.strftime('%Y-%m-%d %H:%M:%S.%f')
            where = (
                f"symbol = '{symbol}' "
                f"AND toDate(candle_time) = '{today}' "
                f"AND received_at > '{recv_str}'"
            )
        else:
            where = f"symbol = '{symbol}' AND toDate(candle_time) = '{today}'"

        return self.ch.query(
            f"SELECT "
            f"  candle_time, symbol, "
            f"  argMax(open, received_at) AS open, "
            f"  argMax(high, received_at) AS high, "
            f"  argMax(low, received_at) AS low, "
            f"  argMax(close, received_at) AS close, "
            f"  argMax(volume, received_at) AS volume, "
            f"  max(received_at) AS last_received "
            f"FROM ohlc_raw WHERE {where} "
            f"GROUP BY candle_time, symbol "
            f"ORDER BY candle_time ASC "
            f"LIMIT 5000"
        ).result_rows

    def _fire_alert(self, alert: Alert) -> None:
        """Ghi alert vào ClickHouse alerts_v2 + log."""
        logger.warning(
            f"🚨 [{alert.rule_name}] {alert.alert_type} | {alert.symbol} "
            f"price={alert.price:.2f} indicator={alert.indicator_value:.2f} "
            f"severity={alert.severity} | {alert.message}"
        )
        self.ch.insert(
            'alerts_v2',
            [[
                alert.alert_time, alert.symbol, alert.rule_name,
                alert.alert_type, alert.severity, alert.price,
                alert.indicator_value, alert.threshold,
                alert.deviation_pct, alert.message,
            ]],
            column_names=[
                'alert_time', 'symbol', 'rule_name', 'alert_type',
                'severity', 'price', 'indicator_value', 'threshold',
                'deviation_pct', 'message',
            ],
        )
        # Tương thích ngược: ghi vào bảng alerts cũ nếu là VWAP rule
        if alert.rule_name == 'VWAP':
            try:
                self.ch.insert(
                    'alerts',
                    [[
                        alert.alert_time, alert.symbol,
                        alert.alert_type.replace('VWAP_', ''),
                        alert.price, alert.indicator_value, alert.deviation_pct,
                    ]],
                    column_names=[
                        'alert_time', 'symbol', 'alert_type',
                        'price', 'vwap', 'deviation_pct',
                    ],
                )
            except Exception:
                pass  # alerts cũ không quan trọng nếu fail

        # Gửi Slack nếu CRITICAL
        self.slack.send_alert(alert)

    def _process_candle(
        self, symbol: str, ts: datetime,
        open_: float, high: float, low: float,
        close: float, volume: int,
    ) -> None:
        """Xử lý 1 nến mới: cập nhật VWAP + buffer + chạy tất cả rules."""
        # Cập nhật VWAP
        self.calc.update(
            symbol=symbol, high=high, low=low,
            close=close, volume=volume, ts=ts,
        )

        # Đẩy vào candle buffer (cho RSI / Volume Spike)
        self.buffer.push(symbol, Candle(
            ts=ts, open=open_, high=high,
            low=low, close=close, volume=volume,
        ))

        # Chạy tất cả rules
        for rule in self.rules:
            try:
                alert = rule.evaluate(symbol, close, ts, self.buffer)
                if alert:
                    self._fire_alert(alert)
            except Exception as exc:
                logger.error(
                    f"Rule {rule.RULE_NAME} error for {symbol}: {exc}",
                    exc_info=True,
                )

    def run(self) -> None:
        logger.info(
            f"Detector running | rules={[r.RULE_NAME for r in self.rules]} "
            f"| vwap_mode={self.config.ALERT_BAND_MODE} "
            f"| sigma_k={self.config.BAND_SIGMA_MULTIPLIER} "
            f"| rsi_period={self.config.RSI_PERIOD} "
            f"| vol_lookback={self.config.VOLUME_LOOKBACK} "
            f"| cooldown={self.config.ALERT_COOLDOWN_SEC}s "
            f"| poll every {self.config.POLL_INTERVAL_SEC}s"
        )
        while True:
            try:
                total_processed = 0
                for symbol in self.config.SYMBOLS:
                    symbol = symbol.strip()
                    candles = self._fetch_new_ohlc(symbol)
                    if not candles:
                        continue

                    for ts, sym, open_, high, low, close, volume, last_recv in candles:
                        self._process_candle(
                            symbol=sym,
                            ts=ts,
                            open_=float(open_),
                            high=float(high),
                            low=float(low),
                            close=float(close),
                            volume=int(volume),
                        )

                    # Cập nhật watermark = received_at lớn nhất trong batch
                    last_recv_in_batch = candles[-1][7]  # cột last_received
                    self._last_received_at[symbol] = last_recv_in_batch
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
