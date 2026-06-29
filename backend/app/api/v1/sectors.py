"""
sectors.py — API endpoints for sector data.
"""
import logging
from typing import Optional
from fastapi import APIRouter, HTTPException, status

from app.services.sectors import SectorService

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get("")
async def get_sectors():
    """Get all sectors with latest market data."""
    try:
        service = SectorService()
        return await service.get_all_sectors()
    except Exception as e:
        logger.error("Error fetching sectors: %s", e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{sector_key}")
async def get_sector_detail(sector_key: int):
    """Get detail for a specific sector including top stocks."""
    try:
        service = SectorService()
        return await service.get_sector_detail(sector_key)
    except Exception as e:
        logger.error("Error fetching sector %s: %s", sector_key, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )


@router.get("/{sector_key}/history")
async def get_sector_history(
    sector_key: int,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 365,
):
    """Get sector history time series for charts."""
    try:
        service = SectorService()
        return await service.get_sector_history(
            sector_key=sector_key,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
        )
    except Exception as e:
        logger.error("Error fetching sector history %s: %s", sector_key, e, exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)
        )
