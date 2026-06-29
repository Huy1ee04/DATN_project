"""
vwap.py — API endpoints for VWAP real-time streaming data.

Data source: ClickHouse database `vwap` (cross-database query from `stock_data`).
"""
import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, status

from app.services.vwap import VWAPService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("/symbols")
async def get_vwap_symbols():
    """Get list of symbols with streaming data today."""
    try:
        service = VWAPService()
        return await service.get_symbols()
    except Exception as e:
        logger.error("Error fetching VWAP symbols: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/ohlc/{symbol}")
async def get_vwap_ohlc(
    symbol: str,
    date: Optional[str] = None,
    start_time: str = "09:00:00",
    limit: int = 300,
):
    """
    Get intraday 1-min OHLCV with running VWAP + σ-bands.

    - **symbol**: Stock symbol (e.g., HPG)
    - **date**: Date string YYYY-MM-DD (default: today)
    - **start_time**: Start time HH:MM:SS (default: 09:00:00)
    - **limit**: Max candles (default: 300)
    """
    try:
        service = VWAPService()
        return await service.get_ohlc_with_indicators(
            symbol=symbol.upper(),
            date=date,
            start_time=start_time,
            limit=limit,
        )
    except Exception as e:
        logger.error("Error fetching VWAP OHLC for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/alerts")
async def get_vwap_alerts(
    symbol: Optional[str] = None,
    date: Optional[str] = None,
    rule: str = "ALL",
    severity: Optional[str] = None,
    limit: int = 50,
):
    """
    Get alerts from alerts_v2.

    - **symbol**: Filter by symbol (optional)
    - **date**: Date string YYYY-MM-DD (default: today)
    - **rule**: Filter by rule_name: ALL, COMBINED, VWAP, RSI, VOLUME_SPIKE
    - **severity**: Filter by severity: CRITICAL, WARNING, INFO
    - **limit**: Max alerts (default: 50)
    """
    try:
        service = VWAPService()
        return await service.get_alerts(
            symbol=symbol.upper() if symbol else None,
            date=date,
            rule_filter=rule,
            severity=severity,
            limit=limit,
        )
    except Exception as e:
        logger.error("Error fetching VWAP alerts: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/summary")
async def get_vwap_summary(date: Optional[str] = None):
    """Get summary: candle count + alert count for a date."""
    try:
        service = VWAPService()
        return await service.get_summary(date=date)
    except Exception as e:
        logger.error("Error fetching VWAP summary: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/price/{symbol}")
async def get_vwap_last_price(symbol: str, date: Optional[str] = None):
    """Get latest close price for a symbol."""
    try:
        service = VWAPService()
        return await service.get_last_price(symbol=symbol.upper(), date=date)
    except Exception as e:
        logger.error("Error fetching last price for %s: %s", symbol, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/latency")
async def get_vwap_latency(window: int = 30):
    """
    Get pipeline latency stats.

    - **window**: Analysis window in minutes (default: 30)
    """
    try:
        service = VWAPService()
        return await service.get_latency(window=window)
    except Exception as e:
        logger.error("Error fetching latency: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/latency/distribution")
async def get_vwap_latency_distribution(window: int = 30):
    """
    Get latency distribution by buckets.

    - **window**: Analysis window in minutes (default: 30)
    """
    try:
        service = VWAPService()
        return await service.get_latency_distribution(window=window)
    except Exception as e:
        logger.error("Error fetching latency distribution: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
