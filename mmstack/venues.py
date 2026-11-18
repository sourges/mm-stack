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


BINANCEUS_DEPTH = 20  # levels in each full-book message


def binanceus(symbols):
    """Binance.US public streams for each symbol (e.g. "BTCUSD"), on one
    combined-stream connection (messages arrive as {"stream": ..., "data": ...};
    the book messages don't name their symbol, the stream name does):
      <sym>@depth20@100ms  the whole top 20 levels, every 100 ms
      <sym>@trade          every trade
      <sym>@bookTicker     best bid/ask on every change (used as a cross-check)
    The streams are chosen in the URL, so nothing is sent after connecting.
    """
    streams = []
    for sym in symbols:
        low = sym.lower()
        streams += [f"{low}@depth{BINANCEUS_DEPTH}@100ms", f"{low}@trade", f"{low}@bookTicker"]
    return {
        "name": "binanceus",
        "host": "stream.binance.us",
        "port": 9443,
        "path": "/stream?streams=" + "/".join(streams),
        "subscribe": [],
    }
