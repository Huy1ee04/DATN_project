"""
market.py — API endpoints for market/index data.
"""
import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, status

from app.services.market import MarketService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/overview")
async def get_market_overview():
    """Dashboard overview: indices + top gainers/losers."""
    try:
        service = MarketService()
        return await service.get_overview()
    except Exception as e:
        logger.error("Error fetching overview: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/indices")
async def get_indices():
    """Get all indices with latest data."""
    try:
        service = MarketService()
        return await service.get_indices()
    except Exception as e:
        logger.error("Error fetching indices: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/indices/{symbol}/ohlcv")
async def get_index_ohlcv(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 90,
):
    """Get OHLCV + indicators for an index."""
    try:
        service = MarketService()
        return await service.get_index_ohlcv(
            symbol=symbol.upper(),
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except Exception as e:
        logger.error("Error fetching index OHLCV for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/calendar")
async def get_calendar_events(month: int = 0, year: int = 0):
    """
    Get calendar events (holidays, special dates) for a month.

    - **month**: 1-12 (default: current month)
    - **year**: 4-digit year (default: current year)
    """
    from datetime import date

    if month == 0:
        month = date.today().month
    if year == 0:
        year = date.today().year

    try:
        service = MarketService()
        return await service.get_calendar_events(month=month, year=year)
    except Exception as e:
        logger.error("Error fetching calendar: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
