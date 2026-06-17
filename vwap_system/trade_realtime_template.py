"""
Market data subscription example.

Demonstrates:
- Subscribing to OHLCV updates

This example shows how to receive real-time market data for multiple symbols.
"""

import asyncio
from datetime import datetime

from trading_websocket import TradingClient
from trading_websocket.models import Ohlc


async def main():
    # Initialize client
    encoding = "msgpack"  # json or msgpack
    client = TradingClient(
        api_key="eyJvcmciOiJkbnNlIiwiaWQiOiI3ODFjM2U0OTJkMzA0YjM0YWIyODhmYzhjOGIzMTUxNyIsImgiOiJtdXJtdXIxMjgifQ==",
        api_secret="g1VWXtkJX0sdkenHyqexiwET7E8PsH4TT1NxkYViLL11cQq94eGUdkl85i_dT-rLwlVt78lf5X1rNU7dBHzp1w",
        base_url="wss://ws-openapi.dnse.com.vn",
        encoding=encoding,
    )

    def handle_ohlc(ohlc: Ohlc):
        # Ohlc model currently does not expose receivedAt; use time/lastUpdated.
        ts = ohlc.lastUpdated or ohlc.time
        received_at = datetime.fromtimestamp(ts).strftime("%H:%M:%S.%f")[:-3] if ts else "N/A"
        print(f"[{received_at}] OHLC: {ohlc}")

    # Connect to gateway
    print("Connecting to WebSocket gateway...")
    await client.connect()
    print(f"Connected! Session ID: {client._session_id}\n")

    print("Subscribing to ohlc for SSI...")
    # internal 1 3 5 15 30 1H 1D 1W
    await client.subscribe_ohlc_closed(
        ["SSI"],
        resolution="1",
        on_ohlc=handle_ohlc,
        encoding=encoding,
    )

    print("\nReceiving market data (will run for 1 hour)...\n")

    # Run for 8H to collect data
    # In a real application, you might run indefinitely or until a specific condition
    await asyncio.sleep(8 * 60 * 60)

    # Disconnect gracefully
    print("\n\nDisconnecting...")
    await client.disconnect()
    print("Disconnected!")


if __name__ == "__main__":
    asyncio.run(main())