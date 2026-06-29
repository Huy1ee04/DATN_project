"""
market.py — Service layer for market/index data.
"""
import logging
from typing import Dict, Optional
from app.db.clickhouse import ClickHouseDB

logger = logging.getLogger(__name__)


class MarketService:
    """Business logic for market index queries."""

    async def get_indices(self) -> Dict:
        """Get all indices with latest data."""
        query = """
            SELECT
                i.index_symbol,
                i.index_name,
                d.trade_date,
                f.close,
                f.price_change_pct,
                f.total_volume,
                f.sma_20,
                f.sma_50,
                f.rsi_14,
                f.macd,
                sig.signal_market_trend,
                sig.signal_market_rsi
            FROM fact_market_index f
            INNER JOIN dim_index i ON f.index_key = i.index_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            LEFT JOIN fact_index_signals sig
                ON f.index_key = sig.index_key AND f.date_key = sig.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_index f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
            ORDER BY i.index_symbol
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_index_ohlcv(
        self,
        symbol: str,
        limit: int = 90,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        """Get OHLCV + indicators for an index."""
        conditions = [f"i.index_symbol = '{symbol}'"]
        if start_date:
            conditions.append(f"d.trade_date >= '{start_date}'")
        if end_date:
            conditions.append(f"d.trade_date <= '{end_date}'")

        where = " AND ".join(conditions)

        query = f"""
            SELECT
                i.index_symbol AS symbol,
                d.trade_date,
                f.open, f.high, f.low, f.close,
                f.total_volume,
                f.price_change_pct,
                f.sma_20, f.sma_50, f.ema_12, f.ema_26,
                f.rsi_14, f.macd,
                sig.signal_market_trend,
                sig.signal_market_rsi
            FROM fact_market_index f
            INNER JOIN dim_index i ON f.index_key = i.index_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            LEFT JOIN fact_index_signals sig
                ON f.index_key = sig.index_key AND f.date_key = sig.date_key
            WHERE {where}
            ORDER BY d.trade_date DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_overview(self) -> Dict:
        """Dashboard overview: latest stats for all indices + top movers."""
        # Latest index data with signals
        indices_query = """
            SELECT
                i.index_symbol AS index_symbol,
                i.index_name AS index_name,
                d.trade_date AS trade_date,
                f.open AS open,
                f.high AS high,
                f.low AS low,
                f.close AS close,
                f.total_volume AS total_volume,
                f.price_change_pct AS price_change_pct,
                sig.signal_market_trend AS signal_market_trend,
                sig.signal_market_rsi AS signal_market_rsi
            FROM fact_market_index f
            INNER JOIN dim_index i ON f.index_key = i.index_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            LEFT JOIN fact_index_signals sig
                ON f.index_key = sig.index_key AND f.date_key = sig.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_index f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
            ORDER BY i.index_symbol
        """
        # Top gainers
        gainers_query = """
            SELECT s.symbol, f.close, f.price_change_pct
            FROM fact_market_equity f
            INNER JOIN dim_stock s ON f.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_equity f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
              AND f.price_change_pct IS NOT NULL
            ORDER BY f.price_change_pct DESC
            LIMIT 10
        """
        # Top losers
        losers_query = """
            SELECT s.symbol, f.close, f.price_change_pct
            FROM fact_market_equity f
            INNER JOIN dim_stock s ON f.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_equity f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
              AND f.price_change_pct IS NOT NULL
            ORDER BY f.price_change_pct ASC
            LIMIT 10
        """
        # Sector top gainers
        sector_gainers_query = """
            SELECT
                s.sector AS sector,
                f.price_change_pct AS price_change_pct,
                f.total_market_cap AS total_market_cap,
                f.total_trade_value AS total_trade_value
            FROM fact_market_sector f
            INNER JOIN dim_sector s ON f.sector_key = s.sector_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_sector f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
              AND f.price_change_pct IS NOT NULL
            ORDER BY f.price_change_pct DESC
            LIMIT 5
        """
        # Sector top losers
        sector_losers_query = """
            SELECT
                s.sector AS sector,
                f.price_change_pct AS price_change_pct,
                f.total_market_cap AS total_market_cap,
                f.total_trade_value AS total_trade_value
            FROM fact_market_sector f
            INNER JOIN dim_sector s ON f.sector_key = s.sector_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_sector f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
              AND f.price_change_pct IS NOT NULL
            ORDER BY f.price_change_pct ASC
            LIMIT 5
        """

        indices, t1 = await ClickHouseDB.execute(indices_query)
        gainers, t2 = await ClickHouseDB.execute(gainers_query)
        losers, t3 = await ClickHouseDB.execute(losers_query)
        sec_gainers, t4 = await ClickHouseDB.execute(sector_gainers_query)
        sec_losers, t5 = await ClickHouseDB.execute(sector_losers_query)

        return {
            "success": True,
            "data": {
                "indices": indices,
                "top_gainers": gainers,
                "top_losers": losers,
                "sector_top_gainers": sec_gainers,
                "sector_top_losers": sec_losers,
            },
            "timing": {
                "indices": t1["total_time"],
                "gainers": t2["total_time"],
                "losers": t3["total_time"],
                "sector_gainers": t4["total_time"],
                "sector_losers": t5["total_time"],
            },
        }

    async def get_calendar_events(self, month: int, year: int) -> Dict:
        """Get calendar events for a specific month/year from dim_date_event."""
        query = f"""
            SELECT
                trade_date,
                event_name,
                is_day_off
            FROM dim_date_event
            WHERE cal_year = {year}
              AND cal_month = {month}
              AND (event_name IS NOT NULL AND event_name != '' OR is_day_off = 1)
            ORDER BY trade_date ASC
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

