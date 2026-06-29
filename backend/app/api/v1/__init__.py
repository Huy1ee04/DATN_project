from fastapi import APIRouter
from app.api.v1.stocks import router as stocks_router
from app.api.v1.market import router as market_router
from app.api.v1.vwap import router as vwap_router
from app.api.v1.sectors import router as sectors_router

router = APIRouter()
router.include_router(stocks_router, prefix="/stocks", tags=["Stocks"])
router.include_router(market_router, prefix="/market", tags=["Market"])
router.include_router(vwap_router, prefix="/vwap", tags=["VWAP Streaming"])
router.include_router(sectors_router, prefix="/sectors", tags=["Sectors"])
