"""
sectors.py — Service layer for sector data.
"""
import logging
from typing import Dict, Optional
from app.db.clickhouse import ClickHouseDB

logger = logging.getLogger(__name__)


class SectorService:
    """Business logic for sector queries."""

    async def get_all_sectors(self) -> Dict:
        """Get all sectors with latest market data."""
        query = """
            SELECT
                s.sector_key AS sector_key,
                s.sector AS sector,
                d.trade_date AS trade_date,
                f.price_change_pct AS price_change_pct,
                f.total_trade_value AS total_trade_value,
                f.total_market_cap AS total_market_cap,
                f.avg_pe AS avg_pe,
                f.avg_pb AS avg_pb,
                f.avg_eps AS avg_eps,
                f.stock_count AS stock_count
            FROM fact_market_sector f
            INNER JOIN dim_sector s ON f.sector_key = s.sector_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_sector f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
            )
            ORDER BY f.total_market_cap DESC
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}

    async def get_sector_detail(self, sector_key: int) -> Dict:
        """
        Get sector detail: basic info + latest metrics + top stocks in sector.
        """
        # 1. Sector info + latest metrics
        info_query = f"""
            SELECT
                s.sector_key AS sector_key,
                s.sector AS sector,
                d.trade_date AS trade_date,
                f.price_change_pct AS price_change_pct,
                f.total_trade_value AS total_trade_value,
                f.total_market_cap AS total_market_cap,
                f.avg_pe AS avg_pe,
                f.avg_pb AS avg_pb,
                f.avg_eps AS avg_eps,
                f.stock_count AS stock_count
            FROM fact_market_sector f
            INNER JOIN dim_sector s ON f.sector_key = s.sector_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE s.sector_key = {sector_key}
              AND d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_sector f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
                WHERE f2.sector_key = {sector_key}
              )
            LIMIT 1
        """

        # 2. Top stocks in this sector (by market_cap desc)
        stocks_query = f"""
            SELECT
                st.symbol AS symbol,
                st.organ_short_name AS organ_short_name,
                d.trade_date AS trade_date,
                eq.close AS close,
                eq.price_change_pct AS price_change_pct,
                eq.market_cap AS market_cap,
                eq.pe AS pe,
                eq.pb AS pb,
                eq.eps AS eps
            FROM fact_market_equity eq
            INNER JOIN dim_stock st ON eq.stock_key = st.stock_key
            INNER JOIN dim_date_event d ON eq.date_key = d.date_key
            WHERE st.sector = (
                SELECT sector FROM dim_sector WHERE sector_key = {sector_key} LIMIT 1
            )
              AND d.trade_date = (
                SELECT max(d2.trade_date)
                FROM fact_market_equity f2
                INNER JOIN dim_date_event d2 ON f2.date_key = d2.date_key
              )
            ORDER BY eq.market_cap DESC
            LIMIT 15
        """

        info, t1 = await ClickHouseDB.execute(info_query)
        stocks, t2 = await ClickHouseDB.execute(stocks_query)

        return {
            "success": True,
            "data": {
                "info": info[0] if info else None,
                "top_stocks": stocks,
            },
            "timing": {
                "info": t1["total_time"],
                "stocks": t2["total_time"],
            },
        }

    async def get_sector_history(
        self,
        sector_key: int,
        limit: int = 90,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict:
        """Get sector time series for chart."""
        conditions = [f"f.sector_key = {sector_key}"]
        if start_date:
            conditions.append(f"d.trade_date >= '{start_date}'")
        if end_date:
            conditions.append(f"d.trade_date <= '{end_date}'")

        where = " AND ".join(conditions)

        query = f"""
            SELECT
                s.sector AS sector_name,
                d.trade_date,
                f.price_change_pct,
                f.total_trade_value,
                f.total_market_cap,
                f.avg_pe,
                f.avg_pb,
                f.avg_eps,
                f.stock_count
            FROM fact_market_sector f
            INNER JOIN dim_sector s ON f.sector_key = s.sector_key
            INNER JOIN dim_date_event d ON f.date_key = d.date_key
            WHERE {where}
            ORDER BY d.trade_date DESC
            LIMIT {limit}
        """
        data, timing = await ClickHouseDB.execute(query)
        return {"success": True, "data": data, "count": len(data), "timing": timing}
