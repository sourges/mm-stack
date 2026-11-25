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
from .paper_mm import PaperMarketMaker
from .venues import BINANCEUS_DEPTH, KRAKEN_BOOK_DEPTH

TEST_ORDER_DISCOUNT = Decimal("0.05")  # test buy sits 5% below the best bid
TEST_ORDER_HOLD_NS = 5_000_000_000  # cancel 5 s after Kraken confirms it


def iso(t_ns):
    """Nanosecond timestamp -> '2026-11-12T22:04:01.123456Z' (integer maths only)."""
    sec, ns = divmod(t_ns, 1_000_000_000)
    dt = datetime.fromtimestamp(sec, tz=timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S") + f".{ns // 1000:06d}Z"


def bn_decimal(text):
    """Binance.US pads numbers ("62310.42000000"); drop the padding so prices
    print cleanly and the price tick can be read off them (0.01, not 1e-8)."""
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return Decimal(text)


def round_to(value, increment, rounding):
    """Round value to a multiple of increment (Decimals), e.g. price ticks."""
    return (value / increment).to_integral_value(rounding=rounding) * increment


class StateMachine:
    def __init__(self, test_order_symbol=None, pretend_private_ready=False, paper_mm=None,
                 paper_from_ns=None, paper_to_ns=None):
        self.now = None  # ns, from the last event
        self.first_t = None  # ns of the first event; used to build order ids
        self.conns = {}  # conn name -> "connecting" / "open" / "closed"
        self.books = {}  # (venue, symbol) -> Book
        self.valid = {}  # (venue, symbol) -> bool (False until a good snapshot)
        self.last_top = {}  # (venue, symbol) -> (bid, ask) last printed
        self.cb_last_seq = None  # Coinbase sequence_num on the current connection
        # Binance.US, per symbol, reset on every (re)connect
        # Binance.US and Binance, per (venue, symbol), reset on every (re)connect
        self.bn_last_id = {}  # last book lastUpdateId seen
        self.bn_last_trade = {}  # last trade id seen
        self.bn_ticker = {}  # latest bookTicker: (update id, bid, ask)
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

        # step 10 (paper): {"symbols": [...], "spread": "0.0005", ...} or None
        self.paper_config = None
        self.paper = {}  # symbol -> PaperMarketMaker
        self.paper_venue = "kraken"
        self.paper_ref = "coinbase"  # whose price is the reference (fair price signal, alarm)
        # Replay window (runner): before paper_from_ns the books build but the paper
        # market maker doesn't exist yet; it starts on the first event at or after it.
        # paper_to_ns is only reported here; the caller stops reading the log there.
        self.paper_from_ns = paper_from_ns
        self.paper_to_ns = paper_to_ns
        self.paper_start_t = None  # ns the paper market maker started (summary time base)
        self._paper_pending = None  # config waiting for paper_from_ns
        if paper_mm:
            self._start_paper(paper_mm)

    def _start_paper(self, cfg):
        """Start the paper market maker now, or hold it until the window opens."""
        if self.paper_from_ns is not None and (self.now is None or self.now < self.paper_from_ns):
            self._paper_pending = cfg
        else:
            self._enable_paper(cfg)

    def _enable_paper(self, cfg):
        self._paper_pending = None
        self.paper_start_t = self.now
        self.paper_config = cfg
        self.paper_venue = cfg.get("venue", "kraken")  # whose book and trades we quote on
        self.paper_ref = cfg.get("ref", "coinbase")  # whose price the signal and alarm watch
        on_binance = self.paper_venue == "binanceus"
        for sym in cfg["symbols"]:
            mm = PaperMarketMaker(sym, spread=Decimal(cfg["spread"]),
                                  max_inv_multiple=Decimal(str(cfg.get("max_inv", 5))),
                                  requote=Decimal(str(cfg.get("requote", "0.1"))),
                                  latency_ns=int(cfg.get("latency_ms", 150)) * 1_000_000,
                                  fills=cfg.get("fills", "queue"),
                                  skew=cfg.get("skew", True),
                                  ref_weight=Decimal(str(cfg.get("coinbase", "0"))),
                                  rate_tier=None if on_binance else cfg.get("rate_tier", "none"),
                                  rate_margin=Decimal(str(cfg.get("rate_margin", "0"))),
                                  alarm_bps=Decimal(str(cfg.get("alarm_bps", "0"))),
                                  alarm_window_ns=int(cfg.get("alarm_window_ms", 100)) * 1_000_000,
                                  alarm_hold_ns=int(cfg.get("alarm_hold_ms", 1000)) * 1_000_000,
                                  alarm_max_hold_ns=int(cfg.get("alarm_max_hold_ms", 0)) * 1_000_000,
                                  alarm_vol=Decimal(str(cfg.get("alarm_vol", "0"))),
                                  alarm_vol_tau_ns=int(cfg.get("alarm_vol_tau_s", 600)) * 1_000_000_000,
                                  book_depth=BINANCEUS_DEPTH if on_binance else KRAKEN_BOOK_DEPTH,
                                  kraken_limits=not on_binance,
                                  imb_threshold=Decimal(str(cfg.get("imb_alarm", "0"))),
                                  imb_hold_ns=int(cfg.get("imb_hold_ms", 1000)) * 1_000_000,
                                  imb_reading=cfg.get("imb_reading", "weighted"),
                                  imb_levels=int(cfg.get("imb_levels", 10)),
                                  imb_bps=Decimal(str(cfg.get("imb_bps", "5"))))
            if sym in self.instruments and not on_binance:
                mm.set_instrument(self.instruments[sym])
            self.paper[sym] = mm

    def _paper_lines(self, lines):
        return [self._line(x) for x in lines]

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
        if venue == self.paper_ref:  # the paper market maker must stop using that venue's price
            for mm in self.paper.values():
                mm.on_reference(self.now, None)
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
        if self._paper_pending is not None and self.now >= self.paper_from_ns:
            self._enable_paper(self._paper_pending)
        type_ = event["type"]
        conn = event["conn"]
        self._count(f"events.{type_}")
        out = []

        if type_ == "conn_open":
            self.conns[conn] = "connecting"
        elif type_ == "ws_open":
            if conn == self.paper_venue:
                for mm in self.paper.values():
                    out.extend(self._paper_lines(mm.on_disconnect(self.now)))
            self.conns[conn] = "open"
            self._reconnect_pending.discard(conn)
            self._invalidate_venue(conn)  # wait for fresh snapshots
            if conn == "coinbase":
                self.cb_last_seq = None  # sequence numbers restart per connection
            if conn in self.BINANCE_VENUES:
                self._binance_reset(conn)
            out.append(self._line(f"{conn} connected"))
        elif type_ == "conn_close":
            if conn == self.paper_venue:
                for mm in self.paper.values():
                    out.extend(self._paper_lines(mm.on_disconnect(self.now)))
            self.conns[conn] = "closed"
            self._invalidate_venue(conn)
            if conn == "kraken_priv":
                self.private_ready = False
            if conn in self.BINANCE_VENUES:
                self._binance_reset(conn)
            out.append(self._line(f"{conn} disconnected: {event['data'].get('reason', '')}"))
        elif type_ == "ws_in":
            msg = json.loads(event["data"]["text"], parse_float=Decimal)
            if conn == "kraken":
                self._kraken(msg, out)
            elif conn == "coinbase":
                self._coinbase(msg, out)
            elif conn in self.BINANCE_VENUES:
                self._binance(conn, msg, out)
            elif conn == "kraken_priv":
                self._kraken_private(msg, out)
        elif type_ == "session":
            # Settings recorded by live mode; replay picks them up from the log.
            sym = event["data"].get("test_order")
            if sym and self.test_phase == "off":
                self.test_symbol, self.test_phase = sym, "waiting"
            if event["data"].get("paper_mm") and not self.paper and self._paper_pending is None:
                self._start_paper(event["data"]["paper_mm"])
        elif type_ == "action":
            self._check_logged_action(event["data"], out)
        elif type_ == "action_rejected":
            self._action_rejected(event["data"], out)
        elif type_ == "timer":
            for mm in self.paper.values():
                out.extend(self._paper_lines(mm.on_timer(self.now)))

        # Time-based decisions run after EVERY event, timer ticks included.
        self._test_order_step(out)
        return out

    def _window_line(self):
        """What part of the log the paper market maker actually saw."""
        def when(t):
            return iso(t) if t is not None else "-"
        start = self.paper_start_t if self.paper_start_t is not None else self.first_t  # --to only
        hours = (Decimal(self.now - start) / 3_600_000_000_000) if (start is not None and self.now) else Decimal(0)
        return (f"summary window from={when(self.paper_from_ns)} to={when(self.paper_to_ns)} "
                f"paper_start={when(start)} last_event={when(self.now)} hours={hours:.2f}")

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
                    if symbol in self.paper and self.paper_venue == "kraken":
                        out.extend(self._paper_lines(self.paper[symbol].on_book(self.now, book, False)))
                    continue
                self._count("kraken.checksum_ok")
                line = self._top_line("kraken", symbol)
                if line:
                    out.append(line)
                if symbol in self.paper and self.paper_venue == "kraken":
                    out.extend(self._paper_lines(self.paper[symbol].on_book(self.now, book, True)))
        elif channel == "trade":
            self._count("kraken.trades", len(msg.get("data", [])))
            if msg.get("type") == "update" and self.paper_venue == "kraken":  # never old trades
                for tr in msg.get("data", []):
                    mm = self.paper.get(tr.get("symbol"))
                    if mm:
                        out.extend(self._paper_lines(
                            mm.on_trade(self.now, tr["side"], tr["price"], tr["qty"])))
        elif channel == "instrument":
            for pair in (msg.get("data") or {}).get("pairs", []):
                self.instruments[pair["symbol"]] = {
                    k: pair.get(k) for k in ("price_increment", "qty_increment", "qty_min",
                                             "cost_min", "status")}
                if pair["symbol"] in self.paper and self.paper_venue == "kraken":
                    self.paper[pair["symbol"]].set_instrument(self.instruments[pair["symbol"]])
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
                mm = self.paper.get(symbol.replace("-", "/"))  # BTC-USD -> Kraken's BTC/USD
                if mm is not None and self.paper_ref == "coinbase":
                    key = ("coinbase", symbol)
                    mm.on_reference(self.now, self.books[key].mid() if self.valid.get(key) else None)
        elif channel == "market_trades":
            for ev in msg.get("events", []):
                self._count("coinbase.trades", len(ev.get("trades", [])))
        elif msg.get("type") == "error":
            out.append(self._line(f"coinbase error: {msg.get('message')}"))

    # ---------- Binance.US and Binance (same stream format) ----------
    BINANCE_VENUES = ("binanceus", "binance")

    def _binance_reset(self, conn):
        """A new connection: ids may jump over what we missed while away."""
        for d in (self.bn_last_id, self.bn_last_trade, self.bn_ticker):
            for k in [k for k in d if k[0] == conn]:
                del d[k]

    def _binance(self, conn, msg, out):
        stream = msg.get("stream")
        if stream is None:
            if "code" in msg or "error" in msg:
                out.append(self._line(f"{conn} error: {msg}"))
            return
        data = msg.get("data") or {}
        sym, _, kind = stream.partition("@")
        sym = sym.upper()
        if kind.startswith("depth"):
            self._binance_book(conn, sym, data, out)
        elif kind == "trade":
            self._binance_trade(conn, sym, data, out)
        elif kind == "bookTicker":
            self._count(f"{conn}.tickers")
            self.bn_ticker[(conn, sym)] = (data["u"], bn_decimal(data["b"]), bn_decimal(data["a"]))
        if conn == "binanceus" and self.paper_venue == "binanceus":
            # any message proves the feed is alive; Binance.US only sends a
            # book when it changes, so an unchanged valid book is still current
            for symbol, mm in self.paper.items():
                if self.valid.get((conn, symbol.replace("/", ""))):
                    out.extend(self._paper_lines(mm.on_feed_alive(self.now)))

    def _bn_paper(self, conn, sym):
        """The paper market maker quoting this Binance.US pair, if any
        (BTCUSD -> the BTC/USD one), only when quoting on Binance.US."""
        if conn != "binanceus" or self.paper_venue != "binanceus" or not sym.endswith("USD"):
            return None
        return self.paper.get(sym[:-3] + "/USD")

    def _bn_reference(self, conn, sym):
        """The paper market maker that uses this Binance pair as its reference
        price (BTCUSDT -> the BTC/USD one), only with --mm-ref binance."""
        if conn != "binance" or self.paper_ref != "binance" or not sym.endswith("USDT"):
            return None
        return self.paper.get(sym[:-4] + "/USD")

    def _binance_book(self, conn, sym, data, out):
        """Each message is the whole top 20, so it replaces our copy outright:
        no checksum needed, and a missed message can't leave the book wrong."""
        key = (conn, sym)
        uid = data["lastUpdateId"]
        last = self.bn_last_id.get(key)
        if last is not None and uid < last:
            self._count(f"{conn}.books_out_of_order")  # older than what we have: ignore
            return
        self.bn_last_id[key] = uid
        book = Book()
        for price, qty in data.get("bids", []):
            book.bids.set(bn_decimal(price), bn_decimal(qty))
        for price, qty in data.get("asks", []):
            book.asks.set(bn_decimal(price), bn_decimal(qty))
        mm = self._bn_paper(conn, sym)
        ref = self._bn_reference(conn, sym)
        bid, ask = book.best_bid(), book.best_ask()
        if bid is None or ask is None or bid >= ask:
            self._count(f"{conn}.books_bad")  # empty side or crossed: don't trust it
            if self.valid.get(key):
                out.append(self._line(f"{conn} {sym} BAD BOOK bid={bid} ask={ask} - not used"))
            self.valid[key] = False
            self.last_top.pop(key, None)
            if mm is not None:
                out.extend(self._paper_lines(mm.on_book(self.now, book, False)))
            if ref is not None:
                out.extend(self._paper_lines(ref.on_reference(self.now, None)))
            return
        self.books[key] = book
        self.valid[key] = True
        self._count(f"{conn}.books_ok")
        ticker = self.bn_ticker.get(key)
        if ticker is not None and ticker[0] == uid:  # same moment: tops must agree
            self._count(f"{conn}.top_check_ok" if ticker[1:] == (bid, ask)
                        else f"{conn}.top_check_differs")
        line = self._top_line(conn, sym)
        if line:
            out.append(line)
        if mm is not None:
            out.extend(self._paper_lines(mm.on_book(self.now, book, True)))
        if ref is not None:
            out.extend(self._paper_lines(ref.on_reference(self.now, book.mid())))

    def _binance_trade(self, conn, sym, data, out):
        """Trade ids count up by one per symbol, so a jump means we missed trades."""
        self._count(f"{conn}.trades")
        key = (conn, sym)
        tid = data["t"]
        last = self.bn_last_trade.get(key)
        if last is not None:
            if tid <= last:
                self._count(f"{conn}.trades_repeated")
                return
            if tid != last + 1:
                self._count(f"{conn}.trade_gaps")
                self._count(f"{conn}.trades_missed", tid - last - 1)
                out.append(self._line(f"{conn} {sym} TRADE GAP {last} -> {tid}"))
        self.bn_last_trade[key] = tid
        mm = self._bn_paper(conn, sym)
        if mm is not None:
            # "m" = the buyer was the maker, so the taker sold
            side = "sell" if data["m"] else "buy"
            out.extend(self._paper_lines(mm.on_trade(self.now, side, bn_decimal(data["p"]),
                                                     bn_decimal(data["q"]))))

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
    @staticmethod
    def _imb_setting(cfg):
        """The imbalance-alarm part of the settings line ('' when off)."""
        if not Decimal(str(cfg.get("imb_alarm", "0"))):
            return ""
        reading = cfg.get("imb_reading", "weighted")
        levels = {"touch": "", "depth": f"{cfg.get('imb_bps', '5')}bps"}.get(reading, str(cfg.get("imb_levels", 10)))
        return f" imb_alarm={cfg['imb_alarm']}/{reading}{levels}/{cfg.get('imb_hold_ms', 1000)}ms"

    @staticmethod
    def _alarm_setting(cfg):
        """The alarm part of the paper settings line ('' when the alarm is off)."""
        bps = Decimal(str(cfg.get("alarm_bps", "0")))
        vol = Decimal(str(cfg.get("alarm_vol", "0")))
        timing = f"/{cfg.get('alarm_window_ms', 100)}ms/{cfg.get('alarm_hold_ms', 1000)}ms"
        if int(cfg.get("alarm_max_hold_ms", 0)):
            timing += f"_up_to_{cfg['alarm_max_hold_ms']}ms"
        if vol:
            return (f" alarm={vol}x_normal_move{timing} avg_over={cfg.get('alarm_vol_tau_s', 600)}s"
                    + (f" floor={bps}bps" if bps else ""))
        if bps:
            return f" alarm={cfg['alarm_bps']}bps{timing}"
        return ""

    def summary(self):
        lines = [f"summary {k}={self.stats[k]}" for k in sorted(self.stats)]
        if self._expected:
            lines.append(f"summary actions.not_in_log={len(self._expected)}")
        if self.test_phase != "off":
            lines.append(f"summary test_order={self.test_phase}")
        if self.paper_from_ns is not None or self.paper_to_ns is not None:
            lines.append(self._window_line())
        if self.paper:
            # Without a window the time base stays the first event, as it always was.
            start = self.paper_start_t if self.paper_from_ns is not None else self.first_t
            total = (self.now - start) if self.now is not None else 0
            cfg = self.paper_config
            lines.append(f"summary paper settings spread={Decimal(cfg['spread']) * 100}% "
                         f"max_inv={cfg.get('max_inv', 5)}x requote={cfg.get('requote', '0.1')} "
                         f"latency={cfg.get('latency_ms', 150)}ms "
                         f"fills={cfg.get('fills', 'queue')} "
                         f"skew={'on' if cfg.get('skew', True) else 'off'} "
                         f"coinbase={cfg.get('coinbase', '0')} "
                         + (f"venue=binanceus rate_limit=not_modeled"
                            if self.paper_venue == "binanceus"
                            else f"rate_limit={cfg.get('rate_tier', 'none')}"
                            + (f"_margin_{cfg['rate_margin']}" if cfg.get("rate_margin") else ""))
                         + (f" ref={self.paper_ref}" if self.paper_ref != "coinbase" else "")
                         + self._alarm_setting(cfg)
                         + self._imb_setting(cfg))
            for sym in sorted(self.paper):
                lines.extend(self.paper[sym].summary_lines(total))
        return lines
