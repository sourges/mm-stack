"""Where to connect and what to subscribe to. Public market data only (no keys)."""

import json

KRAKEN_BOOK_DEPTH = 10  # Kraken's checksum covers the top 10 levels


def kraken(symbols):
    """Kraken spot WebSocket v2: order book (depth 10), trades, instrument info."""
    subs = [
        {"method": "subscribe",
         "params": {"channel": "book", "symbol": symbols, "depth": KRAKEN_BOOK_DEPTH},
         "req_id": 1},
        {"method": "subscribe",
         "params": {"channel": "trade", "symbol": symbols},
         "req_id": 2},
        # Sizes and price ticks for every pair (min order size, price increment).
        {"method": "subscribe",
         "params": {"channel": "instrument"},
         "req_id": 3},
    ]
    return {
        "name": "kraken",
        "host": "ws.kraken.com",
        "port": 443,
        "path": "/v2",
        "subscribe": [json.dumps(s, separators=(",", ":")) for s in subs],
    }


def coinbase(products):
    """Coinbase Advanced Trade market data: heartbeats + level2 book + trades.

    Heartbeats keep the connection open; most channels close after 60-90 s
    without updates otherwise.
    """
    subs = [
        {"type": "subscribe", "channel": "heartbeats"},
        {"type": "subscribe", "channel": "level2", "product_ids": products},
        {"type": "subscribe", "channel": "market_trades", "product_ids": products},
    ]
    return {
        "name": "coinbase",
        "host": "advanced-trade-ws.coinbase.com",
        "port": 443,
        "path": "/",
        "subscribe": [json.dumps(s, separators=(",", ":")) for s in subs],
    }
