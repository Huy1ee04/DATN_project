"""
vwap.py — Service layer for VWAP streaming data.

Queries ClickHouse database `vwap` via cross-database queries
(backend connects to `stock_data`, uses fully-qualified `vwap.table_name`).
"""
import logging
from typing import Dict, Optional

from app.db.clickhouse import ClickHouseDB

logger = logging.getLogger(__name__)


class VWAPService:
    """Business logic for VWAP real-time streaming queries."""

    async def get_ohlc_with_indicators(
        self,
        symbol: str,
        date: Optional[str] = None,
        start_time: str = "09:00:00",
        limit: int = 300,
    ) -> Dict:
        """
        Get intraday OHLCV 1-minute candles with running VWAP + σ-bands.
        Port of Streamlit dashboard `load_ohlc_with_indicators()`.
        """
        date_expr = f"'{date}'" if date else "today()"

        query = f"""
            SELECT * FROM (
                SELECT
                    candle_time AS time,
                    open, high, low,
                    close AS price,
                    volume AS quantity,
                    vwap,
                    sigma
                FROM (
                    SELECT
                        candle_time, open, high, low, close, volume,
                        (
                            sum(((high + low + close) / 3.0) * volume)
                                OVER (ORDER BY candle_time
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                            /
                            nullIf(
                                sum(volume) OVER (ORDER BY candle_time
                                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                                0
                            )
                        ) AS vwap,
                        sqrt(greatest(
                            (
                                sum(
                                    (((high + low + close) / 3.0) * ((high + low + close) / 3.0)) * volume
                                )
                                    OVER (ORDER BY candle_time
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                /
                                nullIf(
                                    sum(volume) OVER (ORDER BY candle_time
                                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                                    0
                                )
                            )
                            -
                            (
                                (
                                    sum(((high + low + close) / 3.0) * volume)
                                        OVER (ORDER BY candle_time
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                    /
                                    nullIf(
                                        sum(volume) OVER (ORDER BY candle_time
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                                        0
                                    )
                                )
                                *
                                (
                                    sum(((high + low + close) / 3.0) * volume)
                                        OVER (ORDER BY candle_time
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                                    /
                                    nullIf(
                                        sum(volume) OVER (ORDER BY candle_time
                                            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
                                        0
                                    )
                                )
                            ),
                            0
                        )) AS sigma
                    FROM vwap.ohlc_raw
                    WHERE symbol = '{symbol}'
                      AND toDate(candle_time) = {date_expr}
                ) t
                WHERE formatDateTime(candle_time, '%H:%M:%S') >= '{start_time}'
                ORDER BY time DESC
                LIMIT {limit}
            )
            ORDER BY time ASC
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_alerts(
        self,
        symbol: Optional[str] = None,
        date: Optional[str] = None,
        rule_filter: str = "ALL",
        severity: Optional[str] = None,
        limit: int = 50,
    ) -> Dict:
        """Get alerts from vwap.alerts_v2."""
        date_expr = f"'{date}'" if date else "today()"
        conditions = [f"toDate(alert_time) = {date_expr}"]

        if symbol:
            conditions.append(f"symbol = '{symbol}'")
        if rule_filter != "ALL":
            conditions.append(f"rule_name = '{rule_filter}'")
        if severity:
            conditions.append(f"severity = '{severity}'")

        where = " AND ".join(conditions)

        query = f"""
            SELECT alert_time, symbol, rule_name, alert_type, severity,
                   price, indicator_value, threshold, deviation_pct, message
            FROM vwap.alerts_v2
            WHERE {where}
            ORDER BY alert_time DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_summary(self, date: Optional[str] = None) -> Dict:
        """Get summary stats: candle count, alert count."""
        date_expr = f"'{date}'" if date else "today()"

        candle_query = f"""
            SELECT count() AS total
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = {date_expr}
        """
        alert_query = f"""
            SELECT count() AS total
            FROM vwap.alerts_v2
            WHERE toDate(alert_time) = {date_expr}
        """

        candle_data, _ = await ClickHouseDB.execute(candle_query)
        alert_data, timing = await ClickHouseDB.execute(alert_query)

        return {
            "success": True,
            "data": {
                "candles": candle_data[0]["total"] if candle_data else 0,
                "alerts": alert_data[0]["total"] if alert_data else 0,
            },
            "timing": timing,
        }

    async def get_last_price(
        self, symbol: str, date: Optional[str] = None
    ) -> Dict:
        """Get latest close price for a symbol."""
        date_expr = f"'{date}'" if date else "today()"
        query = f"""
            SELECT close, candle_time
            FROM vwap.ohlc_raw
            WHERE symbol = '{symbol}' AND toDate(candle_time) = {date_expr}
            ORDER BY candle_time DESC
            LIMIT 1
        """
        data, timing = await ClickHouseDB.execute(query)
        return {
            "success": True,
            "data": data[0] if data else None,
            "timing": timing,
        }

    async def get_latency(self, window: int = 30) -> Dict:
        """Get pipeline latency stats."""
        summary_query = f"""
            SELECT
                count()                                                                      AS total,
                round(avg(date_diff('millisecond', candle_time, received_at)), 1)             AS avg_ms,
                round(quantile(0.50)(date_diff('millisecond', candle_time, received_at)), 1)  AS p50_ms,
                round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1)  AS p95_ms,
                round(quantile(0.99)(date_diff('millisecond', candle_time, received_at)), 1)  AS p99_ms
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = today()
              AND candle_time >= now() - INTERVAL {window} MINUTE
        """

        timeseries_query = f"""
            SELECT
                toStartOfMinute(received_at)                                                  AS minute,
                count()                                                                       AS msg_count,
                round(avg(date_diff('millisecond', candle_time, received_at)), 1)              AS avg_ms,
                round(quantile(0.95)(date_diff('millisecond', candle_time, received_at)), 1)   AS p95_ms
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = today()
              AND candle_time >= now() - INTERVAL {window} MINUTE
            GROUP BY minute
            ORDER BY minute ASC
        """

        current_query = """
            SELECT
                symbol,
                candle_time,
                received_at,
                date_diff('millisecond', candle_time, received_at) AS latency_ms
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = today()
            ORDER BY received_at DESC
            LIMIT 1
        """

        throughput_query = """
            SELECT count() / 60.0 AS mps
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = today()
              AND received_at >= now() - INTERVAL 1 MINUTE
        """

        total_query = """
            SELECT count() AS total
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = today()
        """

        summary_data, _ = await ClickHouseDB.execute(summary_query)
        timeseries_data, _ = await ClickHouseDB.execute(timeseries_query)
        current_data, _ = await ClickHouseDB.execute(current_query)
        throughput_data, _ = await ClickHouseDB.execute(throughput_query)
        total_data, timing = await ClickHouseDB.execute(total_query)

        summary = summary_data[0] if summary_data else {
            "total": 0, "avg_ms": 0, "p50_ms": 0, "p95_ms": 0, "p99_ms": 0
        }

        return {
            "success": True,
            "data": {
                "summary": summary,
                "timeseries": timeseries_data,
                "current": current_data[0] if current_data else None,
                "throughput": round(throughput_data[0]["mps"], 1) if throughput_data else 0,
                "total_today": total_data[0]["total"] if total_data else 0,
            },
            "timing": timing,
        }

    async def get_latency_distribution(self, window: int = 30) -> Dict:
        """Get latency distribution by buckets."""
        query = f"""
            SELECT
                multiIf(
                    lat < 500,   '<500ms',
                    lat < 1000,  '500–1000ms',
                    lat < 1500,  '1000–1500ms',
                    lat < 2000,  '1500–2000ms',
                    lat < 3000,  '2000–3000ms',
                                 '>3000ms'
                ) AS bucket,
                count() AS cnt
            FROM (
                SELECT date_diff('millisecond', candle_time, received_at) AS lat
                FROM vwap.ohlc_raw
                WHERE toDate(candle_time) = today()
                  AND candle_time >= now() - INTERVAL {window} MINUTE
            )
            GROUP BY bucket
            ORDER BY bucket ASC
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "timing": timing}

    async def get_symbols(self) -> Dict:
        """Get list of symbols that have OHLC data today."""
        query = """
            SELECT DISTINCT symbol
            FROM vwap.ohlc_raw
            WHERE toDate(candle_time) = today()
            ORDER BY symbol
        """
        data, timing = await ClickHouseDB.execute(query)
        symbols = [r["symbol"] for r in data] if data else []
        return {"success": True, "data": symbols, "timing": timing}

