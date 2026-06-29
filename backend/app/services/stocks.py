"""
stocks.py — Service layer for stock data.
"""
import logging
from typing import Dict, List, Optional
from app.db.clickhouse import ClickHouseDB

logger = logging.getLogger(__name__)


class StockService:
    """Business logic for stock queries."""

    async def get_stocks(
        self,
        limit: int = 100,
        offset: int = 0,
        search: Optional[str] = None,
        sector: Optional[str] = None,
        exchange: Optional[str] = None,
    ) -> Dict:
        """Get list of stocks from dim_stock."""
        conditions = ["1=1"]
        if search:
            conditions.append(f"(symbol ILIKE '%{search}%' OR name ILIKE '%{search}%' OR organ_short_name ILIKE '%{search}%')")
        if sector:
            conditions.append(f"sector = '{sector}'")
        if exchange:
            conditions.append(f"exchange = '{exchange}'")

        where = " AND ".join(conditions)

        query = f"""
            SELECT stock_key, symbol, name, sector, exchange,
                   organ_short_name, organ_name, listing_date, issued_share, profile
            FROM dim_stock
            WHERE {where}
            ORDER BY symbol
            LIMIT {limit} OFFSET {offset}
        """
        count_query = f"SELECT count() as total FROM dim_stock WHERE {where}"

        data, timing = await ClickHouseDB.execute(query)
        count_result, _ = await ClickHouseDB.execute(count_query)
        total = count_result[0]["total"] if count_result else 0

        return {
            "success": True,
            "data": data,
            "count": len(data),
            "total": total,
            "timing": timing,
        }

    async def get_sectors(self) -> Dict:
        """Get distinct sectors."""
        query = """
            SELECT DISTINCT sector
            FROM dim_stock
            WHERE sector IS NOT NULL AND sector != ''
            ORDER BY sector
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": [r["sector"] for r in data], "timing": timing}

    async def get_stock_detail(self, symbol: str) -> Dict:
        """Get full stock info from dim_stock."""
        query = f"""
            SELECT stock_key, symbol, name, sector, exchange,
                   organ_short_name, organ_name, listing_date,
                   issued_share, profile, type
            FROM dim_stock
            WHERE symbol = '{symbol}'
            LIMIT 1
        """
        data, timing = await ClickHouseDB.execute(query)
        return {
            "success": True,
            "data": data[0] if data else None,
            "timing": timing,
        }

    async def get_ohlcv(
        self,
        symbol: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 90,
    ) -> Dict:
        """Get OHLCV + indicators for a stock."""
        conditions = [f"s.symbol = '{symbol}'"]
        if start_date:
            conditions.append(f"d.trade_date >= '{start_date}'")
        if end_date:
            conditions.append(f"d.trade_date <= '{end_date}'")

        where = " AND ".join(conditions)

        query = f"""
            SELECT
                s.symbol,
                d.trade_date,
                f.open, f.high, f.low, f.close,
                f.total_volume,
                f.price_change_pct,
                f.sma_20, f.sma_50, f.ema_12, f.ema_26,
                f.rsi_14, f.macd, f.vwap,
                f.pe, f.pb, f.roe, f.eps, f.bvps,
                f.market_cap, f.dividend_yield,
                f.high_52w, f.low_52w, f.beta
            FROM fact_market_equity f
            INNER JOIN dim_stock s ON f.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE {where}
            ORDER BY d.trade_date DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_signals(
        self,
        symbol: str,
        limit: int = 30,
    ) -> Dict:
        """Get signal labels for a stock."""
        query = f"""
            SELECT
                s.symbol,
                d.trade_date,
                sig.signal_rsi, sig.signal_trend, sig.signal_macd,
                sig.signal_dividend, sig.signal_roe,
                sig.signal_pe, sig.signal_pb, sig.signal_price_pos,
                sig.label_stock_class, sig.label_trading_action
            FROM fact_stock_signals sig
            INNER JOIN dim_stock s ON sig.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON sig.date_key = d.date_key
            WHERE s.symbol = '{symbol}'
            ORDER BY d.trade_date DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_stock_news(
        self,
        symbol: str,
        limit: int = 20,
    ) -> Dict:
        """Get news for a stock."""
        query = f"""
            SELECT
                s.symbol,
                d.trade_date,
                n.news_title,
                n.news_short_content,
                n.news_image_url,
                n.news_source_link
            FROM fact_stock_news n
            INNER JOIN dim_stock s ON n.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON n.public_date_key = d.date_key
            WHERE s.symbol = '{symbol}'
            ORDER BY d.trade_date DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_stock_events(
        self,
        symbol: str,
        limit: int = 20,
    ) -> Dict:
        """Get events for a stock."""
        query = f"""
            SELECT
                s.symbol,
                d.trade_date,
                ev.event_name_vi,
                ev.event_title_vi,
                ev.event_code
            FROM fact_stock_events ev
            INNER JOIN dim_stock s ON ev.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON ev.public_date_key = d.date_key
            WHERE s.symbol = '{symbol}'
            ORDER BY d.trade_date DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_screener(
        self,
        sector: Optional[str] = None,
        exchange: Optional[str] = None,
        signal: Optional[str] = None,
        pe_min: Optional[float] = None,
        pe_max: Optional[float] = None,
        pb_min: Optional[float] = None,
        pb_max: Optional[float] = None,
        rsi_min: Optional[float] = None,
        rsi_max: Optional[float] = None,
        market_cap_min: Optional[float] = None,
        sort_by: str = "market_cap",
        sort_order: str = "DESC",
        limit: int = 50,
        offset: int = 0,
    ) -> Dict:
        """Screen stocks based on fundamental & technical criteria."""
        conditions = ["1=1"]
        if sector:
            conditions.append(f"s.sector = '{sector}'")
        if exchange:
            conditions.append(f"s.exchange = '{exchange}'")
        if pe_min is not None:
            conditions.append(f"f.pe >= {pe_min}")
        if pe_max is not None:
            conditions.append(f"f.pe <= {pe_max}")
        if pb_min is not None:
            conditions.append(f"f.pb >= {pb_min}")
        if pb_max is not None:
            conditions.append(f"f.pb <= {pb_max}")
        if rsi_min is not None:
            conditions.append(f"f.rsi_14 >= {rsi_min}")
        if rsi_max is not None:
            conditions.append(f"f.rsi_14 <= {rsi_max}")
        if market_cap_min is not None:
            conditions.append(f"f.market_cap >= {market_cap_min}")
        if signal:
            conditions.append(f"sig.label_trading_action ILIKE '%{signal}%'")

        where = " AND ".join(conditions)

        # Validate sort_by to prevent injection
        valid_sort_cols = {
            "symbol", "close", "price_change_pct", "pe", "pb",
            "roe", "eps", "market_cap", "rsi_14", "dividend_yield", "beta",
        }
        if sort_by not in valid_sort_cols:
            sort_by = "market_cap"
        if sort_order.upper() not in ("ASC", "DESC"):
            sort_order = "DESC"

        query = f"""
            SELECT
                s.symbol,
                s.organ_short_name,
                s.sector,
                s.exchange,
                d.trade_date,
                f.close,
                f.price_change_pct,
                f.pe, f.pb, f.roe, f.eps,
                f.market_cap,
                f.dividend_yield,
                f.rsi_14, f.macd,
                f.beta,
                f.high_52w, f.low_52w,
                sig.label_stock_class,
                sig.label_trading_action
            FROM fact_market_equity f
            INNER JOIN dim_stock s ON f.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            LEFT JOIN fact_stock_signals sig
                ON f.stock_key = sig.stock_key AND f.date_key = sig.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_equity f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
              AND {where}
            ORDER BY {sort_by} {sort_order}
            LIMIT {limit} OFFSET {offset}
        """

        count_query = f"""
            SELECT count() as total
            FROM fact_market_equity f
            INNER JOIN dim_stock s ON f.stock_key = s.stock_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_equity f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
              AND {where}
        """

        data, timing = await ClickHouseDB.execute(query)
        count_result, _ = await ClickHouseDB.execute(count_query)
        total = count_result[0]["total"] if count_result else 0

        return {
            "success": True,
            "data": data,
            "count": len(data),
            "total": total,
            "timing": timing,
        }
