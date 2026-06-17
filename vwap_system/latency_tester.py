"""
Latency Tester — Đo độ trễ end-to-end của pipeline streaming

Đo latency giữa thời điểm DNSE tạo nến (candle_time) và thời điểm
ClickHouse nhận được message (received_at), tính p50/p95/p99.

Chạy: uv run latency_tester.py
      uv run latency_tester.py --interval 10 --window 30
"""

import argparse
import logging
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import clickhouse_connect
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / '.env')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
)
logger = logging.getLogger('latency_tester')

ICT = timezone(timedelta(hours=7))

# ── ClickHouse config ────────────────────────────────────────
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_HTTP_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'default')
CLICKHOUSE_DB = os.getenv('CLICKHOUSE_DB', 'vwap')


def get_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DB,
    )


# ── SQL Queries ──────────────────────────────────────────────

SUMMARY_SQL = """
SELECT
    count()                                                                      AS total_messages,
    round(avg(date_diff('millisecond', candle_time, received_at)), 1)             AS avg_latency_ms,
    round(quantile(0.50)(date_diff('millisecond', candle_time, received_at)), 1)  AS p50_ms,
    round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1)  AS p95_ms,
    round(quantile(0.99)(date_diff('millisecond', candle_time, received_at)), 1)  AS p99_ms,
    round(min(date_diff('millisecond', candle_time, received_at)), 1)             AS min_ms,
    round(max(date_diff('millisecond', candle_time, received_at)), 1)             AS max_ms
FROM ohlc_raw
WHERE toDate(candle_time) = today()
  AND candle_time >= now() - INTERVAL {window} MINUTE
"""

LATEST_SQL = """
SELECT
    symbol,
    candle_time,
    received_at,
    date_diff('millisecond', candle_time, received_at) AS latency_ms
FROM ohlc_raw
WHERE toDate(candle_time) = today()
ORDER BY received_at DESC
LIMIT 1
"""

TIMESERIES_SQL = """
SELECT
    toStartOfMinute(received_at)                                                  AS minute,
    count()                                                                       AS msg_count,
    round(avg(date_diff('millisecond', candle_time, received_at)), 1)              AS avg_latency_ms,
    round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1)   AS p95_ms
FROM ohlc_raw
WHERE toDate(candle_time) = today()
  AND candle_time >= now() - INTERVAL {window} MINUTE
GROUP BY minute
ORDER BY minute ASC
"""

DISTRIBUTION_SQL = """
SELECT
    multiIf(
        latency_ms < 500,   '<500ms',
        latency_ms < 1000,  '500-1000ms',
        latency_ms < 1500,  '1000-1500ms',
        latency_ms < 2000,  '1500-2000ms',
        latency_ms < 3000,  '2000-3000ms',
                            '>3000ms'
    ) AS bucket,
    count() AS cnt
FROM (
    SELECT date_diff('millisecond', candle_time, received_at) AS latency_ms
    FROM ohlc_raw
    WHERE toDate(candle_time) = today()
      AND candle_time >= now() - INTERVAL {window} MINUTE
)
GROUP BY bucket
ORDER BY bucket ASC
"""


# ── Display helpers ──────────────────────────────────────────

def color(val: float, warn: float = 1500, crit: float = 2000) -> str:
    """ANSI color based on latency threshold."""
    if val < warn:
        return f"\033[92m{val:.1f}ms\033[0m"  # green
    elif val < crit:
        return f"\033[93m{val:.1f}ms\033[0m"  # yellow
    else:
        return f"\033[91m{val:.1f}ms\033[0m"  # red


def print_header():
    print("\033[1m")
    print("=" * 72)
    print("  📡 VWAP Streaming Latency Tester")
    print("  DNSE WebSocket → Kafka → ClickHouse (ohlc_raw)")
    print("=" * 72)
    print("\033[0m")


def print_summary(ch, window: int):
    rows = ch.query(SUMMARY_SQL.format(window=window)).result_rows
    if not rows or rows[0][0] == 0:
        print("  ⚠️  Không có dữ liệu trong cửa sổ thời gian. "
              "Kiểm tra producer + Kafka + ClickHouse.")
        return

    total, avg, p50, p95, p99, min_l, max_l = rows[0]

    print(f"\n  📊 Tổng hợp ({window} phút gần nhất, {total:,} messages)")
    print(f"  ├── Avg Latency  : {color(avg)}")
    print(f"  ├── p50 Latency  : {color(p50)}")
    print(f"  ├── p95 Latency  : {color(p95)}")
    print(f"  ├── p99 Latency  : {color(p99)}")
    print(f"  ├── Min Latency  : {color(min_l)}")
    print(f"  └── Max Latency  : {color(max_l)}")


def print_latest(ch):
    rows = ch.query(LATEST_SQL).result_rows
    if not rows:
        return
    symbol, candle_time, received_at, latency_ms = rows[0]
    print(f"\n  🕐 Message gần nhất: [{symbol}] latency={color(latency_ms)}")
    print(f"     candle_time  = {candle_time}")
    print(f"     received_at  = {received_at}")


def print_distribution(ch, window: int):
    rows = ch.query(DISTRIBUTION_SQL.format(window=window)).result_rows
    if not rows:
        return
    print(f"\n  📈 Phân bố Latency:")
    max_cnt = max(r[1] for r in rows) if rows else 1
    for bucket, cnt in rows:
        bar_len = int(cnt / max_cnt * 30) if max_cnt > 0 else 0
        bar = "█" * bar_len
        print(f"     {bucket:>12s} | {bar} ({cnt:,})")


def print_timeseries(ch, window: int):
    rows = ch.query(TIMESERIES_SQL.format(window=window)).result_rows
    if not rows:
        return
    print(f"\n  📉 Latency theo phút (gần nhất):")
    print(f"     {'Phút':>8s}  {'Msgs':>6s}  {'Avg(ms)':>10s}  {'p95(ms)':>10s}")
    print(f"     {'─' * 8}  {'─' * 6}  {'─' * 10}  {'─' * 10}")
    # Show last 10 rows
    for minute, msg_count, avg_ms, p95_ms in rows[-10:]:
        min_str = minute.strftime('%H:%M') if hasattr(minute, 'strftime') else str(minute)
        print(f"     {min_str:>8s}  {msg_count:>6,}  {avg_ms:>10.1f}  {p95_ms:>10.1f}")


# ── Main loop ────────────────────────────────────────────────

def run(interval: int, window: int):
    ch = get_client()
    logger.info(
        f"Connected to ClickHouse {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/{CLICKHOUSE_DB}"
    )
    print_header()
    print(f"  ⚙️  Poll interval: {interval}s | Window: {window} phút")
    print(f"  ⏹  Ctrl+C để dừng\n")

    iteration = 0
    try:
        while True:
            iteration += 1
            now_str = datetime.now(ICT).strftime('%H:%M:%S')
            print(f"\n{'─' * 72}")
            print(f"  🔄 Iteration #{iteration} — {now_str} ICT")

            try:
                print_latest(ch)
                print_summary(ch, window)
                print_distribution(ch, window)
                print_timeseries(ch, window)
            except Exception as exc:
                logger.error(f"Query error: {exc}", exc_info=True)

            time.sleep(interval)

    except KeyboardInterrupt:
        print(f"\n\n  ✅ Dừng sau {iteration} iterations.")


def main():
    parser = argparse.ArgumentParser(
        description='VWAP Streaming Latency Tester'
    )
    parser.add_argument(
        '--interval', type=int, default=10,
        help='Khoảng thời gian giữa mỗi lần đo (giây, mặc định: 10)',
    )
    parser.add_argument(
        '--window', type=int, default=10,
        help='Cửa sổ thời gian phân tích (phút, mặc định: 10)',
    )
    args = parser.parse_args()
    run(interval=args.interval, window=args.window)


if __name__ == '__main__':
    main()
