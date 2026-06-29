"""
stocks.py — API endpoints for stock data.
"""
import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, status

from app.services.stocks import StockService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/")
async def get_stocks(
    limit: int = 100,
    offset: int = 0,
    search: Optional[str] = None,
    sector: Optional[str] = None,
    exchange: Optional[str] = None,
):
    """
    Get list of stocks.

    - **limit**: Max records (default 100)
    - **offset**: Pagination offset
    - **search**: Search by symbol or name
    - **sector**: Filter by sector
    - **exchange**: Filter by exchange (HOSE, HNX, UPCOM)
    """
    try:
        service = StockService()
        return await service.get_stocks(
            limit=limit, offset=offset, search=search,
            sector=sector, exchange=exchange,
        )
    except Exception as e:
        logger.error("Error fetching stocks: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/sectors")
async def get_sectors():
    """Get list of distinct sectors."""
    try:
        service = StockService()
        return await service.get_sectors()
    except Exception as e:
        logger.error("Error fetching sectors: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/screener")
async def get_screener(
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
):
    """
    Screen stocks by fundamental & technical criteria.

    - **sector**: Filter by sector
    - **exchange**: Filter by exchange
    - **signal**: Filter by trading signal (Mua, Nắm giữ, Bán)
    - **pe_min/pe_max**: P/E range
    - **pb_min/pb_max**: P/B range
    - **rsi_min/rsi_max**: RSI(14) range
    - **market_cap_min**: Minimum market cap
    - **sort_by**: Column to sort (symbol, close, pe, pb, roe, rsi_14, market_cap...)
    - **sort_order**: ASC or DESC
    """
    try:
        service = StockService()
        return await service.get_screener(
            sector=sector, exchange=exchange, signal=signal,
            pe_min=pe_min, pe_max=pe_max,
            pb_min=pb_min, pb_max=pb_max,
            rsi_min=rsi_min, rsi_max=rsi_max,
            market_cap_min=market_cap_min,
            sort_by=sort_by, sort_order=sort_order,
            limit=limit, offset=offset,
        )
    except Exception as e:
        logger.error("Error in screener: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{symbol}/detail")
async def get_stock_detail(symbol: str):
    """Get full company info for a stock."""
    try:
        service = StockService()
        return await service.get_stock_detail(symbol=symbol.upper())
    except Exception as e:
        logger.error("Error fetching detail for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{symbol}/ohlcv")
async def get_stock_ohlcv(
    symbol: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 90,
):
    """Get OHLCV + technical indicators for a stock."""
    try:
        service = StockService()
        return await service.get_ohlcv(
            symbol=symbol.upper(),
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except Exception as e:
        logger.error("Error fetching OHLCV for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{symbol}/signals")
async def get_stock_signals(symbol: str, limit: int = 30):
    """Get signal labels for a stock."""
    try:
        service = StockService()
        return await service.get_signals(symbol=symbol.upper(), limit=limit)
    except Exception as e:
        logger.error("Error fetching signals for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{symbol}/news")
async def get_stock_news(symbol: str, limit: int = 20):
    """Get news for a stock."""
    try:
        service = StockService()
        return await service.get_stock_news(symbol=symbol.upper(), limit=limit)
    except Exception as e:
        logger.error("Error fetching news for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{symbol}/events")
async def get_stock_events(symbol: str, limit: int = 20):
    """Get events for a stock."""
    try:
        service = StockService()
        return await service.get_stock_events(symbol=symbol.upper(), limit=limit)
    except Exception as e:
        logger.error("Error fetching events for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
