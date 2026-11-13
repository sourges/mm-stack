"""The state machine: events in -> state updated -> output lines + actions.

Rules that keep it deterministic (same log in = same output and actions out):
  - "now" comes only from the event's own timestamp, never the system clock
  - prices are Decimals, never floats
  - order ids are built from the log itself (first event time + a counter)
  - no randomness, no threads, no I/O in here

update(event) returns output lines. Actions it wants (reconnect, place,
cancel) are collected and handed over by take_actions(). The caller logs each
action to the event log and only then sends it (live) or drops it (replay).

When the logged "action" events come back through update(), we check them
against what we produced. In replay that is the proof that the state machine
makes exactly the same decisions it made live.
"""

import json
from datetime import datetime, timezone
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from .books import Book, kraken_checksum
from .venues import KRAKEN_BOOK_DEPTH

TEST_ORDER_DISCOUNT = Decimal("0.05")  # test buy sits 5% below the best bid
TEST_ORDER_HOLD_NS = 5_000_000_000  # cancel 5 s after Kraken confirms it


def iso(t_ns):
    """Nanosecond timestamp -> '2026-11-12T22:04:01.123456Z' (integer maths only)."""
    sec, ns = divmod(t_ns, 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns // 1000:06d}Z"


def round_to(value, increment, rounding):
    """Round value to a multiple of increment (Decimals), e.g. price ticks."""
    return (value / increment).to_integral_value(rounding=rounding) * increment


class StateMachine:
    def __init__(self, test_order_symbol=None, pretend_private_ready=False):
        self.now = None  # ns, from the last event
        self.first_t = None  # ns of the first event; used to build order ids
        self.conns = {}  # conn name -> "connecting" / "open" / "closed"
        self.books = {}  # (venue, symbol) -> Book
        self.valid = {}  # (venue, symbol) -> bool (False until a good snapshot)
        self.last_top = {}  # (venue, symbol) -> (bid, ask) last printed
        self.cb_last_seq = None  # Coinbase sequence_num on the current connection
        self.instruments = {}  # Kraken symbol -> {"price_increment": D, "qty_min": D, ...}
        self.stats = {}

        # actions
        self._new_actions = []  # produced, not yet handed to the caller
        self._expected = []  # produced, not yet seen back in the log
        self._action_seq = 0
        self._reconnect_pending = set()  # conns we asked to reconnect, until ws_open

        # private / orders
        self.private_ready = False  # executions snapshot received
        self.pretend_private_ready = pretend_private_ready  # replay dry-run only
        self.orders = {}  # cl_ord_id -> {"status":..., "symbol":..., "open_t":...}
        self.test_symbol = test_order_symbol
        self.test_phase = "off" if test_order_symbol is None else "waiting"
        self.test_cl_ord_id = None

    # ---------- helpers ----------
    def _count(self, key, n=1):
        self.stats[key] = self.stats.get(key, 0) + n

    def _line(self, text):
        return f"{iso(self.now)} {text}"

    def _act(self, out, **action):
        """Queue an action. Prices/qtys are strings so the log holds them exactly."""
        self._action_seq += 1
        action = {"id": self._action_seq, **action}
        self._new_actions.append(action)
        self._expected.append(action)
        self._count(f"actions.{action['action']}")
        details = " ".join(f"{k}={v}" for k, v in action.items() if k not in ("id", "action"))
        out.append(self._line(f"ACTION {action['action']} {details}"))
        return action

    def take_actions(self):
        actions, self._new_actions = self._new_actions, []
        return actions

    def _invalidate_venue(self, venue):
        for key in self.books:
            if key[0] == venue:
                self.valid[key] = False
        for key in list(self.last_top):
            if key[0] == venue:
                del self.last_top[key]

    def _request_reconnect(self, out, conn, reason):
        if conn in self._reconnect_pending:
            return
        self._reconnect_pending.add(conn)
        self._act(out, action="reconnect", conn=conn, reason=reason)

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
        if self.first_t is None:
            self.first_t = event["t"]
        type_ = event["type"]
        conn = event["conn"]
        self._count(f"events.{type_}")
        out = []

        if type_ == "conn_open":
            self.conns[conn] = "connecting"
        elif type_ == "ws_open":
            self.conns[conn] = "open"
            self._reconnect_pending.discard(conn)
            self._invalidate_venue(conn)  # wait for fresh snapshots
            if conn == "coinbase":
                self.cb_last_seq = None  # sequence numbers restart per connection
            out.append(self._line(f"{conn} connected"))
        elif type_ == "conn_close":
            self.conns[conn] = "closed"
            self._invalidate_venue(conn)
            if conn == "kraken_priv":
                self.private_ready = False
            out.append(self._line(f"{conn} disconnected: {event['data'].get('reason', '')}"))
        elif type_ == "ws_in":
            msg = json.loads(event["data"]["text"], parse_float=Decimal)
            if conn == "kraken":
                self._kraken(msg, out)
            elif conn == "coinbase":
                self._coinbase(msg, out)
            elif conn == "kraken_priv":
                self._kraken_private(msg, out)
        elif type_ == "session":
            # Settings recorded by live mode; replay picks them up from the log.
            sym = event["data"].get("test_order")
            if sym and self.test_phase == "off":
                self.test_symbol, self.test_phase = sym, "waiting"
        elif type_ == "action":
            self._check_logged_action(event["data"], out)
        elif type_ == "action_rejected":
            self._action_rejected(event["data"], out)

        self._test_order_step(out)
        return out

    # ---------- action bookkeeping ----------
    def _check_logged_action(self, logged, out):
        self._count("actions.logged")
        if not self._expected:
            self._count("actions.mismatch")
            out.append(self._line(f"ACTION MISMATCH: log has {logged}, state machine produced nothing"))
            return
        mine = self._expected.pop(0)
        if mine != logged:
            self._count("actions.mismatch")
            out.append(self._line(f"ACTION MISMATCH: log has {logged}, state machine produced {mine}"))

    def _action_rejected(self, data, out):
        out.append(self._line(f"action {data.get('id')} rejected: {data.get('reason')}"))
        if self.test_phase in ("placing", "cancelling") and data.get("id") == self._test_action_id:
            self.test_phase = "done"
            out.append(self._line(f"TEST ORDER FAILED: {data.get('reason')}"))

    # ---------- Kraken public ----------
    def _kraken(self, msg, out):
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
                    out.append(self._line(f"kraken {symbol} CHECKSUM MISMATCH - reconnecting"))
                    self._request_reconnect(out, "kraken", f"checksum mismatch {symbol}")
                    continue
                self._count("kraken.checksum_ok")
                line = self._top_line("kraken", symbol)
                if line:
                    out.append(line)
        elif channel == "trade":
            self._count("kraken.trades", len(msg.get("data", [])))
        elif channel == "instrument":
            for pair in (msg.get("data") or {}).get("pairs", []):
                self.instruments[pair["symbol"]] = {
                    k: pair.get(k) for k in ("price_increment", "qty_increment", "qty_min",
                                             "cost_min", "status")}
        elif msg.get("method") == "subscribe" and msg.get("success") is False:
            out.append(self._line(f"kraken subscribe error: {msg.get('error')}"))

    # ---------- Coinbase ----------
    def _coinbase(self, msg, out):
        seq = msg.get("sequence_num")
        if seq is not None:
            if self.cb_last_seq is not None and seq != self.cb_last_seq + 1:
                self._count("coinbase.seq_gaps")
                out.append(self._line(f"coinbase SEQ GAP {self.cb_last_seq} -> {seq} - reconnecting"))
                self._invalidate_venue("coinbase")
                self._request_reconnect(out, "coinbase", f"sequence gap {self.cb_last_seq}->{seq}")
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

    # ---------- Kraken private (our orders) ----------
    def _kraken_private(self, msg, out):
        method = msg.get("method")
        if method in ("add_order", "cancel_order"):
            if msg.get("success"):
                self._count(f"kraken_priv.{method}_ok")
            else:
                self._count(f"kraken_priv.{method}_error")
                out.append(self._line(f"kraken {method} error: {msg.get('error')}"))
                if self.test_phase == "placing" and method == "add_order":
                    self.test_phase = "done"
                    out.append(self._line(f"TEST ORDER FAILED: {msg.get('error')}"))
            return
        if method == "subscribe" and msg.get("success") is False:
            out.append(self._line(f"kraken_priv subscribe error: {msg.get('error')}"))
            return
        if msg.get("channel") != "executions":
            return
        if msg.get("type") == "snapshot":
            self.private_ready = True
            open_orders = len(msg.get("data", []))
            out.append(self._line(f"kraken_priv executions ready ({open_orders} open orders on the account)"))
        for d in msg.get("data", []):
            cl = d.get("cl_ord_id")
            if cl is None or cl not in self.orders:
                continue  # not ours (e.g. the spread bot's orders)
            order = self.orders[cl]
            exec_type = d.get("exec_type")
            order["status"] = d.get("order_status", order["status"])
            out.append(self._line(f"order {cl} {exec_type} status={order['status']}"
                                  + (f" reason={d.get('reason')}" if d.get("reason") else "")))
            if exec_type == "new":
                order["open_t"] = self.now
            if exec_type in ("trade", "filled"):
                self._count("kraken_priv.fills")

    # ---------- step 5: one test order ----------
    _test_action_id = None

    def _test_order_step(self, out):
        phase = self.test_phase
        if phase in ("off", "done"):
            return
        sym = self.test_symbol
        if phase == "waiting":
            inst = self.instruments.get(sym)
            key = ("kraken", sym)
            ready = self.private_ready or self.pretend_private_ready
            if not (ready and inst and self.valid.get(key)):
                return
            if inst.get("status") not in (None, "online"):
                return
            book = self.books[key]
            bid = book.best_bid()
            if bid is None:
                return
            price = round_to(bid * (1 - TEST_ORDER_DISCOUNT), inst["price_increment"], ROUND_FLOOR)
            qty = inst["qty_min"]
            if inst.get("cost_min") and price * qty < inst["cost_min"]:
                qty = round_to(inst["cost_min"] / price, inst["qty_increment"], ROUND_CEILING)
            qty = round_to(qty, inst["qty_increment"], ROUND_CEILING)
            cl = f"mm{(self.first_t // 1_000_000_000) % 10**10}t1"  # <= 18 chars, unique per session
            self.test_cl_ord_id = cl
            self.orders[cl] = {"status": "sent", "symbol": sym, "open_t": None}
            action = self._act(out, action="place", conn="kraken_priv", symbol=sym, side="buy",
                               qty=format(qty, "f"), price=format(price, "f"), cl_ord_id=cl)
            self._test_action_id = action["id"]
            self.test_phase = "placing"
            return
        order = self.orders[self.test_cl_ord_id]
        if phase == "placing":
            if order["status"] in ("canceled", "expired"):
                self.test_phase = "done"
                out.append(self._line("TEST ORDER FAILED: canceled by Kraken before it opened"))
            elif order["status"] in ("partially_filled", "filled"):
                self.test_phase = "done"
                out.append(self._line("TEST ORDER WARNING: it filled - check your Kraken balance"))
            elif order["open_t"] is not None and self.now - order["open_t"] >= TEST_ORDER_HOLD_NS:
                action = self._act(out, action="cancel", conn="kraken_priv",
                                   cl_ord_id=self.test_cl_ord_id)
                self._test_action_id = action["id"]
                self.test_phase = "cancelling"
        elif phase == "cancelling":
            if order["status"] == "canceled":
                self.test_phase = "done"
                out.append(self._line("TEST ORDER COMPLETE: placed, confirmed, cancelled"))
            elif order["status"] in ("partially_filled", "filled"):
                self.test_phase = "done"
                out.append(self._line("TEST ORDER WARNING: it filled - check your Kraken balance"))

    # ---------- end of run ----------
    def summary(self):
        lines = [f"summary {k}={self.stats[k]}" for k in sorted(self.stats)]
        if self._expected:
            lines.append(f"summary actions.not_in_log={len(self._expected)}")
        if self.test_phase != "off":
            lines.append(f"summary test_order={self.test_phase}")
        return lines
