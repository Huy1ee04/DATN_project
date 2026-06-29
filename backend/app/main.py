"""
main.py — FastAPI application entry point.
"""
import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import get_settings
from app.api.v1 import router as api_v1_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

settings = get_settings()

app = FastAPI(
    title=settings.API_TITLE,
    version=settings.API_VERSION,
    description="DATN Stock Data Dashboard — ClickHouse Backend",
)

# CORS — allow frontend (localhost:3000) to call backend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
    max_age=3600,
)


@app.get("/")
async def root():
    """Public root endpoint."""
    return {
        "message": "DATN Stock Dashboard API",
        "version": settings.API_VERSION,
        "docs": "/docs",
        "endpoints": {
            "stocks": "/api/v1/stocks",
            "market_overview": "/api/v1/market/overview",
            "indices": "/api/v1/market/indices",
            "vwap_streaming": "/api/v1/vwap",
        },
    }


@app.get("/api/v1/health")
async def health():
    """Health check."""
    return {"status": "healthy"}


# Include API v1 routes
app.include_router(api_v1_router, prefix="/api/v1")
