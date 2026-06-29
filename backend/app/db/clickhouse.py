"""
clickhouse.py — ClickHouse database client.
"""
import logging
import time
import asyncio
from typing import Dict, List, Optional, Tuple

from clickhouse_connect import get_client
from app.core.config import get_settings

logger = logging.getLogger(__name__)


def get_clickhouse_client():
    """Create a new ClickHouse client for each query (thread-safe)."""
    s = get_settings()
    return get_client(
        host=s.CLICKHOUSE_HOST,
        port=s.CLICKHOUSE_PORT,
        username=s.CLICKHOUSE_USER,
        password=s.CLICKHOUSE_PASSWORD,
        database=s.CLICKHOUSE_DATABASE,
        settings={
            "max_execution_time": 30,
            "connect_timeout": 10,
        },
    )


class ClickHouseDB:
    """Handles ClickHouse database operations."""

    @staticmethod
    async def execute(
        query: str, params: Optional[Dict] = None
    ) -> Tuple[List[Dict], Dict[str, float]]:
        """
        Execute query, return (results, timing_info).
        Runs blocking I/O in thread pool for concurrency.
        """
        try:
            connect_start = time.time()
            client = get_clickhouse_client()
            connect_time = time.time() - connect_start

            def _run():
                query_start = time.time()
                result = client.query(query, parameters=params or {})
                query_time = time.time() - query_start
                client.close()
                return list(result.named_results()), query_time

            result, query_time = await asyncio.to_thread(_run)

            timing = {
                "connection_time": round(connect_time, 4),
                "query_time": round(query_time, 4),
                "total_time": round(connect_time + query_time, 4),
            }
            return result, timing
        except Exception as e:
            logger.error("ClickHouse query error: %s | Query: %s", e, query)
            raise
