"""The state machine: events in -> state updated -> output lines.

Rules that keep it deterministic (same log in = same output out):
  - "now" comes only from the event's own timestamp, never the system clock
  - prices are Decimals, never floats
  - no randomness, no threads, no I/O in here
update(event) returns a list of output lines; the caller prints them.
"""

import json
from datetime import datetime, timezone
from decimal import Decimal

from .books import Book, kraken_checksum
from .venues import KRAKEN_BOOK_DEPTH


def iso(t_ns):
    """Nanosecond timestamp -> '2026-11-12T22:04:01.123456Z' (integer maths only)."""
    sec, ns = divmod(t_ns, 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns // 1000:06d}Z"


class StateMachine:
    def __init__(self):
        self.now = None  # ns, from the last event
        self.conns = {}  # conn name -> "connecting" / "open" / "closed"
        self.books = {}  # (venue, symbol) -> Book
        self.valid = {}  # (venue, symbol) -> bool (False until a good snapshot)
        self.last_top = {}  # (venue, symbol) -> (bid, ask) last printed
        self.cb_last_seq = None  # Coinbase sequence_num on the current connection
        self.stats = {}

    # ---------- helpers ----------
    def _count(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n

    def _line(self, text):
        return f"{iso(self.now)} {text}"

    def _invalidate_venue(self, venue):
        for key in self.books:
            if key[0] == venue:
                self.valid[key] = False
        for key in list(self.last_top):
            if key[0] == venue:
                del self.last_top[key]

    def _top_line(self, venue, symbol):
        """Print a line only when best bid or best ask changed."""
        key = (venue, symbol)
        if not self.valid.get(key):
            return None
        book = self.books[key]
        bid, ask = book.best_bid(), book.best_ask()
        if bid is None or ask is None:
            return None
        if self.last_top.get(key) == (bid, ask):
            return None
        self.last_top[key] = (bid, ask)
        return self._line(f"{venue} {symbol} bid={bid} ask={ask} mid={book.mid()}")

    # ---------- main entry ----------
    def update(self, event):
        self.now = event["t"]
        type_ = event["type"]
        conn = event["conn"]
        self._count(f"events.{type_}")

        if type_ == "conn_open":
            self.conns[conn] = "connecting"
            return []
        if type_ == "ws_open":
            self.conns[conn] = "open"
            self._invalidate_venue(conn)  # wait for fresh snapshots
            if conn == "coinbase":
                self.cb_last_seq = None  # sequence numbers restart per connection
            return [self._line(f"{conn} connected")]
        if type_ == "conn_close":
            self.conns[conn] = "closed"
            self._invalidate_venue(conn)
            return [self._line(f"{conn} disconnected: {event['data'].get('reason', '')}")]
        if type_ == "ws_in":
            msg = json.loads(event["data"]["text"], parse_float=Decimal)
            if conn == "kraken":
                return self._kraken(msg)
            if conn == "coinbase":
                return self._coinbase(msg)
        return []

    # ---------- Kraken ----------
    def _kraken(self, msg):
        out = []
        channel = msg.get("channel")
        if channel == "book":
            for d in msg.get("data", []):
                symbol = d["symbol"]
                key = ("kraken", symbol)
                if msg.get("type") == "snapshot":
                    self.books[key] = Book()
                    self.valid[key] = True
                    self.last_top.pop(key, None)
                elif not self.valid.get(key):
                    self._count("kraken.updates_skipped_invalid_book")
                    continue
                book = self.books[key]
                for lvl in d.get("bids", []):
                    book.bids.set(lvl["price"], lvl["qty"])
                for lvl in d.get("asks", []):
                    book.asks.set(lvl["price"], lvl["qty"])
                # Levels pushed past our depth are NOT sent as qty 0; trim them.
                book.bids.truncate(KRAKEN_BOOK_DEPTH)
                book.asks.truncate(KRAKEN_BOOK_DEPTH)
                if kraken_checksum(book) != d["checksum"]:
                    self._count("kraken.checksum_bad")
                    self.valid[key] = False
                    self.last_top.pop(key, None)
                    out.append(self._line(f"kraken {symbol} CHECKSUM MISMATCH - book invalid until next snapshot"))
                    continue
                self._count("kraken.checksum_ok")
                line = self._top_line("kraken", symbol)
                if line:
                    out.append(line)
        elif channel == "trade":
            self._count("kraken.trades", len(msg.get("data", [])))
        elif msg.get("method") == "subscribe" and msg.get("success") is False:
            out.append(self._line(f"kraken subscribe error: {msg.get('error')}"))
        return out

    # ---------- Coinbase ----------
    def _coinbase(self, msg):
        out = []
        seq = msg.get("sequence_num")
        if seq is not None:
            if self.cb_last_seq is not None and seq != self.cb_last_seq + 1:
                self._count("coinbase.seq_gaps")
                out.append(self._line(
                    f"coinbase SEQ GAP {self.cb_last_seq} -> {seq} - books invalid until resubscribe"))
                self._invalidate_venue("coinbase")
            self.cb_last_seq = seq

        channel = msg.get("channel")
        if channel == "l2_data":
            changed = []
            for ev in msg.get("events", []):
                symbol = ev["product_id"]
                key = ("coinbase", symbol)
                if ev.get("type") == "snapshot":
                    self.books[key] = Book()
                    self.valid[key] = True
                    self.last_top.pop(key, None)
                elif not self.valid.get(key):
                    self._count("coinbase.updates_skipped_invalid_book")
                    continue
                book = self.books[key]
                for u in ev.get("updates", []):
                    side = book.bids if u["side"] == "bid" else book.asks
                    side.set(Decimal(u["price_level"]), Decimal(u["new_quantity"]))
                if symbol not in changed:
                    changed.append(symbol)
            for symbol in changed:
                line = self._top_line("coinbase", symbol)
                if line:
                    out.append(line)
        elif channel == "market_trades":
            for ev in msg.get("events", []):
                self._count("coinbase.trades", len(ev.get("trades", [])))
        elif msg.get("type") == "error":
            out.append(self._line(f"coinbase error: {msg.get('message')}"))
        return out

    # ---------- end of run ----------
    def summary(self):
        return [f"summary {k}={self.stats[k]}" for k in sorted(self.stats)]
