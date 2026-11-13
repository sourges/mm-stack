"""Builders for fake exchange messages, in the exchanges' real formats."""

import json
from decimal import Decimal as D

from mmstack.books import Book, kraken_checksum
from mmstack.venues import KRAKEN_BOOK_DEPTH

T0 = 1_763_000_000_000_000_000  # some fixed time in ns


def event(seq, conn, type_, data=None, t=None):
    return {"seq": seq, "t": T0 + seq * 1_000_000 if t is None else t,
            "conn": conn, "type": type_, "data": data or {}}


class KrakenFeed:
    """Keeps a shadow book so every message carries the checksum Kraken would send.

    Prices/qtys are written as raw JSON numbers (like Kraken does), keeping
    trailing zeros, e.g. {"price":45283.5,"qty":0.10000000}.
    """

    def __init__(self, symbol="BTC/USD"):
        self.symbol = symbol
        self.book = Book()

    def _levels(self, levels):
        return "[" + ",".join(f'{{"price":{p},"qty":{q}}}' for p, q in levels) + "]"

    def _apply(self, bids, asks):
        for p, q in bids:
            self.book.bids.set(D(p), D(q))
        for p, q in asks:
            self.book.asks.set(D(p), D(q))
        self.book.bids.truncate(KRAKEN_BOOK_DEPTH)
        self.book.asks.truncate(KRAKEN_BOOK_DEPTH)

    def message(self, type_, bids=(), asks=(), checksum=None):
        if type_ == "snapshot":
            self.book = Book()
        self._apply(bids, asks)
        cs = kraken_checksum(self.book) if checksum is None else checksum
        return ('{"channel":"book","type":"%s","data":[{"symbol":"%s","bids":%s,"asks":%s,'
                '"checksum":%d,"timestamp":"2026-11-12T22:00:00.000000Z"}]}'
                % (type_, self.symbol, self._levels(bids), self._levels(asks), cs))


def coinbase_l2(seq, type_, product, updates):
    """updates: list of (side, price, qty) with side 'bid' or 'offer'."""
    return json.dumps({
        "channel": "l2_data",
        "client_id": "",
        "timestamp": "2026-11-12T22:00:00.000000Z",
        "sequence_num": seq,
        "events": [{
            "type": type_,
            "product_id": product,
            "updates": [{"side": s, "event_time": "2026-11-12T22:00:00.000000Z",
                         "price_level": p, "new_quantity": q} for s, p, q in updates],
        }],
    })


def coinbase_heartbeat(seq):
    return json.dumps({"channel": "heartbeats", "client_id": "", "timestamp": "2026-11-12T22:00:00Z",
                       "sequence_num": seq, "events": [{"heartbeat_counter": seq}]})


def sample_session():
    """A small realistic session: both venues connect, books build and move,
    one Kraken checksum failure, one Coinbase sequence gap, a disconnect."""
    k = KrakenFeed("BTC/USD")
    evs = []
    n = 0

    def add(conn, type_, data=None):
        nonlocal n
        n += 1
        evs.append(event(n, conn, type_, data))

    add("kraken", "conn_open", {"host": "ws.kraken.com"})
    add("kraken", "ws_open")
    add("kraken", "ws_in", {"text": '{"channel":"status","type":"update","data":[{"system":"online"}]}'})
    add("kraken", "ws_in", {"text": k.message("snapshot",
        bids=[("45283.5", "0.10000000"), ("45283.4", "1.54582015")],
        asks=[("45285.2", "0.00100000"), ("45286.4", "1.54571953")])})
    add("coinbase", "conn_open", {"host": "advanced-trade-ws.coinbase.com"})
    add("coinbase", "ws_open")
    add("coinbase", "ws_in", {"text": coinbase_heartbeat(0)})
    add("coinbase", "ws_in", {"text": coinbase_l2(1, "snapshot", "BTC-USD",
        [("bid", "45280.01", "0.5"), ("bid", "45279.00", "1.2"), ("offer", "45281.50", "0.3")])})
    add("kraken", "ws_in", {"text": k.message("update", bids=[("45284.0", "0.25000000")])})
    add("kraken", "ws_in", {"text": '{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","price":45284.0,"qty":0.1}]}'})
    add("coinbase", "ws_in", {"text": coinbase_l2(2, "update", "BTC-USD", [("offer", "45281.00", "0.1")])})
    add("coinbase", "ws_in", {"text": coinbase_heartbeat(3)})
    add("kraken", "ws_in", {"text": k.message("update", asks=[("45285.2", "0")])})
    add("kraken", "ws_in", {"text": k.message("update", bids=[("45284.0", "0")], checksum=12345)})
    add("kraken", "ws_in", {"text": k.message("update", bids=[("45283.9", "1.00000000")])})  # skipped: invalid
    add("kraken", "ws_in", {"text": k.message("snapshot",
        bids=[("45283.5", "0.10000000")], asks=[("45286.4", "1.54571953")])})
    add("coinbase", "ws_in", {"text": coinbase_l2(5, "update", "BTC-USD", [("bid", "45280.50", "1")])})  # gap 3->5
    add("coinbase", "conn_close", {"reason": "eof from server"})
    return evs


# ---------- private feed (our orders) ----------
INSTRUMENT_BTC = ('{"channel":"instrument","type":"snapshot","data":{"assets":[],"pairs":['
                  '{"symbol":"BTC/USD","base":"BTC","quote":"USD","status":"online",'
                  '"qty_precision":8,"qty_increment":0.00000001,"price_precision":1,'
                  '"price_increment":0.1,"qty_min":0.00005,"cost_min":0.5}]}}')


def executions(type_, orders):
    """orders: list of dicts with cl_ord_id, exec_type, order_status (+ optional reason)."""
    data = [dict({"order_id": f"O{i}", "symbol": "BTC/USD", "side": "buy",
                  "order_type": "limit", "timestamp": "2026-11-13T17:00:00Z"}, **o)
            for i, o in enumerate(orders)]
    return json.dumps({"channel": "executions", "type": type_, "data": data, "sequence": 1})


def ack(method, success=True, error=None, req_id=1):
    msg = {"method": method, "req_id": req_id, "success": success,
           "time_in": "2026-11-13T17:00:00Z", "time_out": "2026-11-13T17:00:00Z"}
    if success:
        msg["result"] = {"order_id": "O1"}
    else:
        msg["error"] = error
    return json.dumps(msg)
