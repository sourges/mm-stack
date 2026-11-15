"""Step 10, paper mode: a simple market maker with SIMULATED orders.

Nothing is sent to an exchange. It lives inside the state machine, so it is
deterministic: replaying a log gives exactly the same quotes, fills and PnL.
That also means it can run over any recorded log, e.g. both spread settings
on the same day of data.

What it does, per symbol (Kraken):
  - quotes a bid and an ask at `spread` (a fraction, 0.0005 = 0.05%) each
    side of mid, rounded away from mid to the price tick
  - inventory skew from the guide (x = |inventory| / max_inventory):
        side that ADDS to inventory:   distance + spread * x / (1 - x)
        side that REDUCES inventory:   distance - spread * x
    at x >= 1 the side that adds is not quoted at all (hard cap)
  - only re-quotes when the wanted price moved at least `requote` x distance
    (each re-quote would cost queue position in real life)
  - a new quote only becomes live after `latency_ns` (we measured ~110 ms to
    Kraken from home)
  - FILLS: only when a real trade prints THROUGH our price (strictly better
    than our quote). A trade exactly at our price doesn't count: we'd be at
    the back of the queue (Moallemi-Yuan).
  - stale-data guard: no quotes while the book is invalid or hasn't updated
    for `stale_ns`
  - measures: PnL marked to mid, PnL after several fee levels, break-even fee,
    and MARKOUTS: after each fill, how far mid moved for/against us after
    1 s, 5 s and 30 s (negative = we got picked off = adverse selection)

Inventory is relative to a starting holding, so it can go negative (on spot
that means "below what we started with").
"""

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

FEE_LEVELS = (Decimal("0"), Decimal("0.001"), Decimal("0.004"))  # 0%, 0.10%, 0.40% (your tier)
MARKOUT_HORIZONS_NS = (1_000_000_000, 5_000_000_000, 30_000_000_000)
BPS = Decimal(10000)


def _round(value, tick, rounding):
    return (value / tick).to_integral_value(rounding=rounding) * tick


def tick_from_prices(prices):
    """Fallback price tick when Kraken's instrument data isn't in the log:
    the smallest decimal step seen in the book's prices (68337.3 -> 0.1)."""
    places = max((-p.as_tuple().exponent for p in prices if p.as_tuple().exponent < 0), default=0)
    return Decimal(1).scaleb(-places)


class PaperQuote:
    def __init__(self, price, size, live_at):
        self.price = price
        self.size = size  # remaining
        self.live_at = live_at


class PaperMarketMaker:
    def __init__(self, symbol, spread=Decimal("0.0005"), max_inv_multiple=5,
                 requote=Decimal("0.1"), latency_ns=150_000_000, stale_ns=10_000_000_000,
                 size=None):
        self.symbol = symbol
        self.spread = Decimal(spread)
        self.max_inv_multiple = Decimal(max_inv_multiple)
        self.requote = Decimal(requote)
        self.latency_ns = latency_ns
        self.stale_ns = stale_ns
        self.fixed_size = Decimal(size) if size is not None else None

        self.size = self.fixed_size  # order size; if None, decided once we know prices
        self.tick = None
        self.inventory = Decimal(0)
        self.cash = Decimal(0)
        self.notional = Decimal(0)
        self.bid = None  # PaperQuote or None
        self.ask = None
        self.last_mid = None
        self._book = None
        self.last_book_t = None
        self.book_ok = False
        self.pulled = True  # quotes off (stale/invalid) at the start
        self.fills = []  # dicts
        self._pending_markouts = []  # (fill index, horizon index, due_t)
        self.markouts = {h: [] for h in MARKOUT_HORIZONS_NS}  # horizon -> list of bps
        self.stats = {"requotes": 0, "stale_pulls": 0, "buys": 0, "sells": 0,
                      "quoted_ns": 0}
        self._last_t = None

    # ---------- setup ----------
    def set_instrument(self, inst):
        if inst and inst.get("price_increment"):
            self.tick = inst["price_increment"]
            if self.fixed_size is None and inst.get("qty_min"):
                self.size = inst["qty_min"]

    def _ensure_size(self, book):
        if self.tick is None:
            self.tick = tick_from_prices(list(book.bids.levels) + list(book.asks.levels))
        if self.size is None:
            if self.fixed_size is not None:
                self.size = self.fixed_size
            else:  # no instrument data in this log: about $5 worth
                self.size = _round(Decimal(5) / book.mid(), Decimal("0.00000001"), ROUND_CEILING)

    @property
    def max_inventory(self):
        return self.size * self.max_inv_multiple

    # ---------- time ----------
    def _advance(self, now):
        """Book-keeping that depends only on time: quoted time, markouts.
        Runs BEFORE an event is applied, so a markout due at time T uses the
        mid that was in effect at T (not a mid that arrived later)."""
        if self._last_t is not None and not self.pulled and (self.bid or self.ask):
            self.stats["quoted_ns"] += now - self._last_t
        self._last_t = now
        if self.last_mid is None:
            return
        still = []
        for fill_i, h, due in self._pending_markouts:
            if now >= due:
                f = self.fills[fill_i]
                sign = 1 if f["side"] == "buy" else -1
                self.markouts[h].append(sign * (self.last_mid - f["price"]) / f["price"] * BPS)
            else:
                still.append((fill_i, h, due))
        self._pending_markouts = still

    # ---------- inputs ----------
    def on_book(self, now, book, valid):
        """Called after every book update for this symbol (valid = checksum OK)."""
        self._advance(now)
        self.book_ok = valid and book.mid() is not None
        if not self.book_ok:
            return self._pull(now)
        self.last_mid = book.mid()
        self._book = book
        self.last_book_t = now
        self._ensure_size(book)
        return self._quote(now, book)

    def on_timer(self, now):
        self._advance(now)
        if not self.pulled and (self.last_book_t is None or now - self.last_book_t > self.stale_ns):
            return self._pull(now)
        return []

    def on_trade(self, now, side, price, qty):
        """A real trade printed. side is the TAKER's side."""
        self._advance(now)
        if self.pulled:
            return []
        out = []
        if side == "sell" and self.bid and now >= self.bid.live_at and price < self.bid.price:
            out += self._fill(now, "buy", self.bid, min(self.bid.size, qty))
        elif side == "buy" and self.ask and now >= self.ask.live_at and price > self.ask.price:
            out += self._fill(now, "sell", self.ask, min(self.ask.size, qty))
        if out and self.book_ok:
            self._quote(now, self._book)  # re-quote at the new skew straight away
        return out

    def on_disconnect(self, now):
        self._advance(now)
        self.book_ok = False
        return self._pull(now)

    # ---------- logic ----------
    def wanted_quotes(self, mid):
        """(bid_price or None, ask_price or None) for the current inventory."""
        d = self.spread * mid
        x = min(abs(self.inventory) / self.max_inventory, Decimal(1))
        bid_d = ask_d = d
        if self.inventory > 0:  # long: we were hit on the bid
            bid_d = None if x >= 1 else d + d * x / (1 - x)
            ask_d = d - d * x
        elif self.inventory < 0:  # short: we were lifted on the ask
            ask_d = None if x >= 1 else d + d * x / (1 - x)
            bid_d = d - d * x
        bid = None if bid_d is None else _round(mid - bid_d, self.tick, ROUND_FLOOR)
        ask = None if ask_d is None else _round(mid + ask_d, self.tick, ROUND_CEILING)
        return bid, ask

    def _quote(self, now, book):
        out = []
        if self.pulled:
            self.pulled = False
        bid_p, ask_p = self.wanted_quotes(self.last_mid)
        # never cross the book (we only ever want to be a maker)
        if bid_p is not None and book.best_ask() is not None and bid_p >= book.best_ask():
            bid_p = book.best_ask() - self.tick
        if ask_p is not None and book.best_bid() is not None and ask_p <= book.best_bid():
            ask_p = book.best_bid() + self.tick
        threshold = self.requote * self.spread * self.last_mid
        self.bid = self._maybe_requote(now, self.bid, bid_p, threshold)
        self.ask = self._maybe_requote(now, self.ask, ask_p, threshold)
        return out

    def _maybe_requote(self, now, quote, price, threshold):
        if price is None:
            if quote is not None:
                self.stats["requotes"] += 1
            return None
        if quote is not None and abs(quote.price - price) < threshold and quote.size == self.size:
            return quote  # close enough: keep our place
        self.stats["requotes"] += 1
        return PaperQuote(price, self.size, now + self.latency_ns)

    def _pull(self, now):
        if self.pulled:
            return []
        self.pulled = True
        self.bid = self.ask = None
        self.stats["stale_pulls"] += 1
        return [f"PAPER {self.symbol} quotes pulled (book stale or invalid)"]

    def _fill(self, now, side, quote, qty):
        quote.size -= qty
        value = qty * quote.price
        if side == "buy":
            self.inventory += qty
            self.cash -= value
            self.stats["buys"] += 1
        else:
            self.inventory -= qty
            self.cash += value
            self.stats["sells"] += 1
        self.notional += value
        self.fills.append({"t": now, "side": side, "price": quote.price, "qty": qty})
        i = len(self.fills) - 1
        for h in MARKOUT_HORIZONS_NS:
            self._pending_markouts.append((i, h, now + h))
        # inventory changed: both quotes must move to the new skew
        self.bid = self.ask = None
        return [f"PAPER FILL {self.symbol} {side} {qty:f} @ {quote.price:f} "
                f"inv={self.inventory:f} pnl={self.pnl():.4f}"]

    # ---------- results ----------
    def pnl(self, fee=Decimal(0)):
        mark = self.inventory * self.last_mid if self.last_mid is not None else Decimal(0)
        return self.cash + mark - self.notional * fee

    def summary_lines(self, total_ns):
        s = self.symbol
        n = len(self.fills)
        lines = [f"summary paper {s} fills={n} buys={self.stats['buys']} sells={self.stats['sells']} "
                 f"volume={self.notional:.2f} inventory={self.inventory:f}"]
        fees = " ".join(f"fee{fee * 100:.2f}%={self.pnl(fee):.4f}" for fee in FEE_LEVELS)
        breakeven = (self.pnl() / self.notional * 100) if self.notional else Decimal(0)
        lines.append(f"summary paper {s} pnl {fees} breakeven_fee={breakeven:.3f}%")
        marks = []
        for h in MARKOUT_HORIZONS_NS:
            vals = self.markouts[h]
            avg = (sum(vals) / len(vals)) if vals else None
            marks.append(f"{h // 1_000_000_000}s=" + ("n/a" if avg is None else f"{avg:.2f}bps"))
        lines.append(f"summary paper {s} markout {' '.join(marks)} (n={len(self.markouts[MARKOUT_HORIZONS_NS[0]])})")
        quoted = (Decimal(self.stats["quoted_ns"]) / total_ns * 100) if total_ns else Decimal(0)
        lines.append(f"summary paper {s} requotes={self.stats['requotes']} "
                     f"stale_pulls={self.stats['stale_pulls']} quoted_time={quoted:.1f}%")
        return lines
