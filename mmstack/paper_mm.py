"""Step 10, paper mode: a simple market maker with SIMULATED orders.

Nothing is sent to an exchange. It lives inside the state machine, so it is
deterministic: replaying a log gives exactly the same quotes, fills and PnL.
That also means it can run over any recorded log, e.g. several settings on
the same day of data.

What it does, per symbol (Kraken):
  - quotes a bid and an ask at `spread` (a fraction, 0.0005 = 0.05%) each
    side of mid, rounded away from mid to the price tick
  - inventory skew from the guide (x = |inventory| / max_inventory), unless
    skew is switched off:
        side that ADDS to inventory:   distance + spread * x / (1 - x)
        side that REDUCES inventory:   distance - spread * x
    at x >= 1 the side that adds is not quoted at all (hard cap, always on)
  - only re-quotes when the wanted price moved at least `requote` x distance

Order timing (`latency_ns`, we measured ~110 ms to Kraken from home):
  - a new quote is live only after the latency
  - a quote we decide to move or pull STAYS LIVE for the latency and can
    still be filled ("stale fill": the slow maker's main loss, Menkveld 2016)
  - one change in flight per side at a time

Fills, mode "queue" (default):
  - when a quote goes live we note the size already resting at its price:
    the queue AHEAD of us (Moallemi-Yuan 2017)
  - a trade AT our price uses up the queue ahead first; what is left fills us
  - a trade THROUGH our price fills us completely
  - size that leaves our level without a trade is a cancel; we assume cancels
    are spread evenly through the queue. Size added joins behind us.
  - Kraken's book and trade feeds arrive in either order, a fraction of a
    millisecond apart, so the two are matched to avoid counting the same
    size twice.
  - we only record the top 10 levels. If our price is outside them the queue
    is unknown: no at-price fills, and when the level comes into view we
    assume ALL of it is ahead of us (worst case).
Fills, mode "through" (the old rule, kept for comparison): only trades
strictly through our price fill, for at most the trade's size.

Also reported: PnL split into spread earned vs inventory drift, markouts
(mid move for/against us 1 s, 5 s, 30 s after each fill; negative = picked
off), re-quotes per minute, and how much of the order flow Kraken's rate
limits would have blocked. The rate limits are only measured, not enforced.

Inventory is relative to a starting holding, so it can go negative.
"""

from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from .venues import KRAKEN_BOOK_DEPTH

FEE_LEVELS = (Decimal("0"), Decimal("0.001"), Decimal("0.004"))  # 0%, 0.10%, 0.40% (your tier)
MARKOUT_HORIZONS_NS = (1_000_000_000, 5_000_000_000, 30_000_000_000)
BPS = Decimal(10000)
MATCH_NS = 250_000_000  # a trade and its book change arrive within this of each other
NS = Decimal(1_000_000_000)

# Kraken spot trading rate limits, per pair (docs: "Spot Trading Limits").
# name -> (threshold, decay per second)
RATE_TIERS = {"starter": (Decimal(60), Decimal(1)),
              "intermediate": (Decimal(125), Decimal("2.34")),
              "pro": (Decimal(180), Decimal("3.75"))}
ADD_COST = Decimal(1)
CANCEL_COST_BY_AGE = ((5, 8), (10, 6), (15, 5), (45, 4), (90, 2), (300, 1))  # (age < s, points)


def cancel_cost(age_ns):
    for limit_s, points in CANCEL_COST_BY_AGE:
        if age_ns < limit_s * 1_000_000_000:
            return Decimal(points)
    return Decimal(0)


class RateCounter:
    """Kraken's per-pair rate counter for one account tier. An action that
    would push the counter over the threshold is counted as blocked (and, as
    on Kraken, a rejected order adds nothing)."""

    def __init__(self, threshold, decay):
        self.threshold, self.decay = threshold, decay
        self.counter = Decimal(0)
        self.last_t = None
        self.actions = 0
        self.blocked = 0
        self.peak = Decimal(0)

    def charge(self, now, cost):
        if self.last_t is not None:
            self.counter = max(Decimal(0), self.counter - self.decay * (now - self.last_t) / NS)
        self.last_t = now
        self.actions += 1
        if self.counter + cost > self.threshold:
            self.blocked += 1
            return
        self.counter += cost
        self.peak = max(self.peak, self.counter)


def _round(value, tick, rounding):
    return (value / tick).to_integral_value(rounding=rounding) * tick


def _qty(value):
    """A size for display: at most 8 decimals, no trailing zeros (0.4, not 0.40000000)."""
    text = f"{value:.8f}".rstrip("0").rstrip(".")
    return "0" if text in ("", "-0") else text


def tick_from_prices(prices):
    """Fallback price tick when Kraken's instrument data isn't in the log:
    the smallest decimal step seen in the book's prices (68337.3 -> 0.1)."""
    places = max((-p.as_tuple().exponent for p in prices if p.as_tuple().exponent < 0), default=0)
    return Decimal(1).scaleb(-places)


def level_visible(side, price):
    """Can we see this price level in the recorded book (top 10 levels)?"""
    if price in side.levels or len(side) < KRAKEN_BOOK_DEPTH:
        return True
    worst = side.top(KRAKEN_BOOK_DEPTH)[-1][0]
    return price >= worst if side.is_bid else price <= worst


class PaperQuote:
    def __init__(self, price, size, placed_t, live_at):
        self.price = price
        self.size = size  # remaining
        self.placed_t = placed_t
        self.live_at = live_at
        self.dead_at = None  # set when we decide to cancel/move it
        # queue tracking
        self.checked = False  # looked for our level when the quote went live
        self.ahead = None  # size ahead of us; None = unknown (level not visible yet)
        self.level_size = None  # last seen size at our level; None = out of view
        self.credits = []  # [t, qty]: trades at our price not yet seen in the book
        self.debits = []  # [t, qty, ahead_reduction]: book drops treated as cancels

    def is_live(self, now):
        return self.size > 0 and now >= self.live_at and (self.dead_at is None or now < self.dead_at)


class PaperMarketMaker:
    def __init__(self, symbol, spread=Decimal("0.0005"), max_inv_multiple=5,
                 requote=Decimal("0.1"), latency_ns=150_000_000, stale_ns=10_000_000_000,
                 size=None, fills="queue", skew=True):
        if fills not in ("queue", "through"):
            raise ValueError(f"fills must be 'queue' or 'through', not {fills!r}")
        self.symbol = symbol
        self.spread = Decimal(spread)
        self.max_inv_multiple = Decimal(max_inv_multiple)
        self.requote = Decimal(requote)
        self.latency_ns = latency_ns
        self.stale_ns = stale_ns
        self.fixed_size = Decimal(size) if size is not None else None
        self.fill_mode = fills
        self.skew = skew

        self.size = self.fixed_size  # order size; if None, decided once we know prices
        self.tick = None
        self.inventory = Decimal(0)
        self.cash = Decimal(0)
        self.notional = Decimal(0)
        self.spread_earned = Decimal(0)
        self.cur = {"bid": None, "ask": None}  # the quote on the book (may be dying)
        self.nxt = {"bid": None, "ask": None}  # its replacement, live when cur dies
        self.last_mid = None
        self._book = None
        self.last_book_t = None
        self.book_ok = False
        self.pulled = True  # quotes off (stale/invalid) at the start
        self.fills = []  # dicts
        self._pending_markouts = []  # (fill index, horizon, due_t)
        self.markouts = {h: [] for h in MARKOUT_HORIZONS_NS}  # horizon -> [(fill index, bps)]
        self.stats = {"requotes": 0, "places": 0, "cancels": 0, "stale_pulls": 0,
                      "buys": 0, "sells": 0, "quoted_ns": 0,
                      "queue_seen": 0, "queue_unseen": 0}
        self.rate = {name: RateCounter(*cfg) for name, cfg in RATE_TIERS.items()}
        self._last_t = None

    # kept for convenience (and tests)
    @property
    def bid(self):
        return self.cur["bid"]

    @property
    def ask(self):
        return self.cur["ask"]

    # ---------- setup ----------
    def set_instrument(self, inst):
        if inst and inst.get("price_increment"):
            self.tick = inst["price_increment"]
            if self.fixed_size is None and inst.get("qty_min"):
                self.size = inst["qty_min"]

    def _ensure_size(self, book):
        if self.tick is None:
            self.tick = tick_from_prices(list(book.bids.levels) + list(book.asks.levels))
        if self.size is None:  # no instrument data in this log: about $5 worth
            self.size = _round(Decimal(5) / book.mid(), Decimal("0.00000001"), ROUND_CEILING)

    @property
    def max_inventory(self):
        return self.size * self.max_inv_multiple

    # ---------- time ----------
    def _advance(self, now):
        """Book-keeping that depends only on time: quoted time, markouts,
        quotes going live or dying. Runs BEFORE an event is applied, so a
        markout due at time T uses the mid that was in effect at T."""
        if self._last_t is not None and any(q and q.is_live(self._last_t) for q in self.cur.values()):
            self.stats["quoted_ns"] += now - self._last_t
        self._last_t = now
        if self.last_mid is not None and self._pending_markouts:
            still = []
            for fill_i, h, due in self._pending_markouts:
                if now >= due:
                    f = self.fills[fill_i]
                    sign = 1 if f["side"] == "buy" else -1
                    self.markouts[h].append(
                        (fill_i, sign * (self.last_mid - f["price"]) / f["price"] * BPS))
                else:
                    still.append((fill_i, h, due))
            self._pending_markouts = still
        for side in ("bid", "ask"):
            q = self.cur[side]
            if q is not None and q.dead_at is not None and now >= q.dead_at:
                self.cur[side], self.nxt[side] = self.nxt[side], None
        if self.fill_mode == "queue" and self._book is not None and self.book_ok:
            self._first_look(now, self._book)

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
        if self.fill_mode == "queue":
            self._first_look(now, book)
            self._track_queues(now, book)
        self.pulled = False
        self._quote(now, book)
        return []

    def on_timer(self, now):
        self._advance(now)
        if self.pulled:
            return []
        if self.last_book_t is None or now - self.last_book_t > self.stale_ns:
            return self._pull(now)
        if self.book_ok:
            self._quote(now, self._book)  # e.g. a change that had to wait for one in flight
        return []

    def on_trade(self, now, side, price, qty):
        """A real trade printed. side is the TAKER's side."""
        self._advance(now)
        out = []
        if side == "sell":  # a taker sell hits bids
            q, our_side = self.cur["bid"], "buy"
            through = q is not None and price < q.price
        else:  # a taker buy lifts asks
            q, our_side = self.cur["ask"], "sell"
            through = q is not None and price > q.price
        if q is not None and q.is_live(now):
            if through:
                amount = q.size if self.fill_mode == "queue" else min(q.size, qty)
                out += self._fill(now, our_side, q, amount, "through")
            elif price == q.price and self.fill_mode == "queue" and q.ahead is not None:
                amount = min(q.size, self._use_queue(now, q, qty))
                if amount > 0:
                    out += self._fill(now, our_side, q, amount, "queue")
        if out and self.book_ok and not self.pulled:
            self._quote(now, self._book)  # inventory changed: move to the new skew
        return out

    def on_disconnect(self, now):
        self._advance(now)
        self.book_ok = False
        return self._pull(now)

    # ---------- queue position ----------
    def _first_look(self, now, book):
        """When a quote goes live, look for the size already at its price."""
        for side, book_side in (("bid", book.bids), ("ask", book.asks)):
            q = self.cur[side]
            if q is None or q.checked or now < q.live_at:
                continue
            q.checked = True
            if level_visible(book_side, q.price):
                q.level_size = book_side.levels.get(q.price, Decimal(0))
                q.ahead = q.level_size
                self.stats["queue_seen"] += 1
            else:
                self.stats["queue_unseen"] += 1

    def _track_queues(self, now, book):
        for side, book_side in (("bid", book.bids), ("ask", book.asks)):
            q = self.cur[side]
            if q is None or not q.checked:
                continue
            if not level_visible(book_side, q.price):
                q.level_size = None  # out of view: we can't see what happens there
                continue
            new = book_side.levels.get(q.price, Decimal(0))
            if q.ahead is None:
                q.ahead = new  # first time in view: worst case, all of it is ahead
            elif q.level_size is None:
                q.ahead = min(q.ahead, new)  # back in view: can't be more than what's there
            elif new < q.level_size:
                self._level_dropped(now, q, q.level_size - new)
            q.level_size = new

    def _level_dropped(self, now, q, drop):
        """Our level lost `drop` size. Part may be trades we already counted;
        the rest is cancels, spread evenly through the queue."""
        level = q.level_size
        q.credits = [c for c in q.credits if now - c[0] <= MATCH_NS and c[1] > 0]
        for c in q.credits:
            m = min(drop, c[1])
            c[1] -= m
            drop -= m
            level -= m
        if drop <= 0:
            return
        # `level` is now the size at our price just before the cancels
        reduction = min(q.ahead, drop * q.ahead / level) if level > 0 else Decimal(0)
        q.ahead -= reduction
        q.debits.append([now, drop, reduction])

    def _use_queue(self, now, q, qty):
        """A trade of `qty` printed at our price. Returns how much reaches us
        after the queue ahead is used up."""
        reached = Decimal(0)
        # Part of this trade may already have shown up as a book drop that we
        # treated as a cancel: undo that, and count it as a trade instead.
        q.debits = [d for d in q.debits if now - d[0] <= MATCH_NS and d[1] > 0]
        for d in q.debits:
            m = min(qty, d[1])
            if m <= 0:
                continue
            undo = d[2] * m / d[1]
            q.ahead += undo
            d[2] -= undo
            d[1] -= m
            qty -= m
            take = min(q.ahead, m)
            q.ahead -= take
            reached += m - take
        if qty > 0:
            take = min(q.ahead, qty)
            q.ahead -= take
            reached += qty - take
            q.credits.append([now, qty])  # the book will show this drop shortly
        return reached

    # ---------- quoting ----------
    def wanted_quotes(self, mid):
        """(bid_price or None, ask_price or None) for the current inventory."""
        d = self.spread * mid
        x = min(abs(self.inventory) / self.max_inventory, Decimal(1))
        add_d = None if x >= 1 else (d + d * x / (1 - x) if self.skew else d)
        reduce_d = d - d * x if self.skew else d
        bid_d = ask_d = d
        if self.inventory > 0:  # long: the bid adds, the ask reduces
            bid_d, ask_d = add_d, reduce_d
        elif self.inventory < 0:  # short: the ask adds, the bid reduces
            ask_d, bid_d = add_d, reduce_d
        bid = None if bid_d is None else _round(mid - bid_d, self.tick, ROUND_FLOOR)
        ask = None if ask_d is None else _round(mid + ask_d, self.tick, ROUND_CEILING)
        return bid, ask

    def _quote(self, now, book):
        bid_p, ask_p = self.wanted_quotes(self.last_mid)
        # never cross the book (we only ever want to be a maker)
        if bid_p is not None and book.best_ask() is not None and bid_p >= book.best_ask():
            bid_p = book.best_ask() - self.tick
        if ask_p is not None and book.best_bid() is not None and ask_p <= book.best_bid():
            ask_p = book.best_bid() + self.tick
        threshold = self.requote * self.spread * self.last_mid
        self._move(now, "bid", bid_p, threshold)
        self._move(now, "ask", ask_p, threshold)

    def _charge(self, now, cost):
        for counter in self.rate.values():
            counter.charge(now, cost)

    def _new_quote(self, now, price):
        self.stats["places"] += 1
        self._charge(now, ADD_COST)
        return PaperQuote(price, self.size, now, now + self.latency_ns)

    def _cancel(self, now, q):
        self.stats["cancels"] += 1
        self._charge(now, cancel_cost(now - q.placed_t))
        q.dead_at = now + self.latency_ns

    def _move(self, now, side, price, threshold):
        q = self.cur[side]
        if q is None:
            if price is not None:
                self.cur[side] = self._new_quote(now, price)
            return
        if q.dead_at is not None or now < q.live_at:
            return  # one change in flight per side
        if price is None:
            self._cancel(now, q)
            return
        if abs(q.price - price) < threshold and q.size == self.size:
            return  # close enough: keep our place in the queue
        self.stats["requotes"] += 1
        self._cancel(now, q)
        self.nxt[side] = self._new_quote(now, price)

    def _pull(self, now):
        if self.pulled:
            return []
        self.pulled = True
        for side in ("bid", "ask"):
            q = self.cur[side]
            self.nxt[side] = None
            if q is not None and q.dead_at is None:
                self._cancel(now, q)
        self.stats["stale_pulls"] += 1
        return [f"PAPER {self.symbol} quotes pulled (book stale or invalid)"]

    def _fill(self, now, side, quote, qty, kind):
        stale = quote.dead_at is not None
        quote.size -= qty
        value = qty * quote.price
        if side == "buy":
            self.inventory += qty
            self.cash -= value
            self.stats["buys"] += 1
            self.spread_earned += (self.last_mid - quote.price) * qty
        else:
            self.inventory -= qty
            self.cash += value
            self.stats["sells"] += 1
            self.spread_earned += (quote.price - self.last_mid) * qty
        self.notional += value
        self.fills.append({"t": now, "side": side, "price": quote.price, "qty": qty,
                           "kind": kind, "stale": stale})
        i = len(self.fills) - 1
        for h in MARKOUT_HORIZONS_NS:
            self._pending_markouts.append((i, h, now + h))
        if quote.size == 0 and quote.dead_at is None:
            self.cur["bid" if side == "buy" else "ask"] = None
        tag = kind + (" stale" if stale else "")
        return [f"PAPER FILL {self.symbol} {side} {_qty(qty)} @ {quote.price:f} ({tag}) "
                f"inv={_qty(self.inventory)} pnl={self.pnl():.4f}"]

    # ---------- results ----------
    def pnl(self, fee=Decimal(0)):
        mark = self.inventory * self.last_mid if self.last_mid is not None else Decimal(0)
        return self.cash + mark - self.notional * fee

    def _avg_markout(self, horizon, keep=lambda f: True):
        vals = [v for i, v in self.markouts[horizon] if keep(self.fills[i])]
        return "n/a" if not vals else f"{sum(vals) / len(vals):.2f}bps"

    def summary_lines(self, total_ns):
        s = self.symbol
        n = len(self.fills)
        at_price = sum(1 for f in self.fills if f["kind"] == "queue")
        stale = sum(1 for f in self.fills if f["stale"])
        lines = [f"summary paper {s} fills={n} (at_price={at_price} through={n - at_price} "
                 f"stale={stale}) buys={self.stats['buys']} sells={self.stats['sells']} "
                 f"volume={self.notional:.2f} inventory={_qty(self.inventory)}"]
        fees = " ".join(f"fee{fee * 100:.2f}%={self.pnl(fee):.4f}" for fee in FEE_LEVELS)
        breakeven = (self.pnl() / self.notional * 100) if self.notional else Decimal(0)
        lines.append(f"summary paper {s} pnl {fees} breakeven_fee={breakeven:.3f}%")
        lines.append(f"summary paper {s} pnl_split spread_earned={self.spread_earned:.4f} "
                     f"inventory_drift={self.pnl() - self.spread_earned:.4f}")
        marks = " ".join(f"{h // 1_000_000_000}s={self._avg_markout(h)}" for h in MARKOUT_HORIZONS_NS)
        lines.append(f"summary paper {s} markout {marks} (n={len(self.markouts[MARKOUT_HORIZONS_NS[0]])})")
        h1 = MARKOUT_HORIZONS_NS[0]
        lines.append(f"summary paper {s} markout_1s_by_type "
                     f"at_price={self._avg_markout(h1, lambda f: f['kind'] == 'queue')} "
                     f"through={self._avg_markout(h1, lambda f: f['kind'] == 'through')} "
                     f"stale={self._avg_markout(h1, lambda f: f['stale'])}")
        quoted = (Decimal(self.stats["quoted_ns"]) / total_ns * 100) if total_ns else Decimal(0)
        minutes = Decimal(total_ns) / NS / 60 if total_ns else Decimal(0)
        per_min = (self.stats["requotes"] / minutes) if minutes else Decimal(0)
        looked = self.stats["queue_seen"] + self.stats["queue_unseen"]
        seen = f"{Decimal(self.stats['queue_seen']) / looked * 100:.0f}%" if looked else "n/a"
        lines.append(f"summary paper {s} requotes={self.stats['requotes']} per_min={per_min:.1f} "
                     f"stale_pulls={self.stats['stale_pulls']} quoted_time={quoted:.1f}% "
                     f"queue_visible={seen}")
        blocked = " ".join(
            f"{name}={(Decimal(c.blocked) / c.actions * 100) if c.actions else Decimal(0):.1f}%"
            for name, c in self.rate.items())
        lines.append(f"summary paper {s} rate_limit_blocked {blocked} "
                     f"(order actions={self.stats['places'] + self.stats['cancels']})")
        return lines
