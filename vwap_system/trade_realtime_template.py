"""
Market data subscription example.

Demonstrates:
- Subscribing to trade updates

This example shows how to receive real-time market data for multiple symbols.
"""

import asyncio

from trading_websocket import TradingClient
from trading_websocket.models import Trade


async def main():
    # Initialize client
    encoding = "msgpack"  # json or msgpack
    client = TradingClient(
        api_key="eyJvcmciOiJkbnNlIiwiaWQiOiI3ODFjM2U0OTJkMzA0YjM0YWIyODhmYzhjOGIzMTUxNyIsImgiOiJtdXJtdXIxMjgifQ==",
        api_secret="g1VWXtkJX0sdkenHyqexiwET7E8PsH4TT1NxkYViLL11cQq94eGUdkl85i_dT-rLwlVt78lf5X1rNU7dBHzp1w",
        base_url="wss://ws-openapi.dnse.com.vn",
        encoding=encoding,
    )

    def handle_trade(trade: Trade):
        print(f"TRADE: {trade}")

    # Connect to gateway
    print("Connecting to WebSocket gateway...")
    await client.connect()
    print(f"Connected! Session ID: {client._session_id}\n")

    print("Subscribing to trades for SSI...")
    await client.subscribe_trades(["SSI"], on_trade=handle_trade, encoding=encoding, board_id="G1")

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