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

Coinbase-adjusted fair price (`ref_weight`, 0 = off): the two exchanges follow
each other's jumps over about a second, Coinbase a little ahead. We track the
normal gap between Coinbase's and Kraken's mid (a slow average, ~60 s) and
centre our quotes on
    kraken_mid + ref_weight * (coinbase_mid - kraken_mid - normal_gap)
so when Coinbase has just moved and Kraken hasn't, the quotes lean that way.

Kraken's rate limit (`rate_tier`): with a tier set, the bot tracks Kraken's
per-pair rate counter itself and holds back any order it couldn't afford,
always keeping enough room to cancel what it has resting.

Also reported: PnL split into spread earned vs inventory drift, markouts
(mid move for/against us 1 s, 5 s, 30 s after each fill; negative = picked
off), re-quotes per minute, and how much of the order flow Kraken's rate
limits would have blocked.

Inventory is relative to a starting holding, so it can go negative.
"""

from collections import deque
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
MAX_CANCEL_COST = Decimal(8)  # room we keep per resting order so it can always be cancelled
REF_TAU_NS = 60_000_000_000  # the "normal gap" to the reference price averages over ~60 s
REF_UPDATE_NS = 1_000_000_000  # ...and is updated once a second
REF_STALE_NS = 2_000_000_000  # ignore a reference price older than this
VOL_WARMUP_NS = 60_000_000_000  # volatility alarm stays silent for its first 60 s of data
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

    def level(self, now):
        """The counter right now, after decay."""
        if self.last_t is not None:
            self.counter = max(Decimal(0), self.counter - self.decay * (now - self.last_t) / NS)
        self.last_t = now
        return self.counter

    def charge(self, now, cost):
        self.level(now)
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


def level_visible(side, price, depth=KRAKEN_BOOK_DEPTH):
    """Can we see this price level in the recorded book (top `depth` levels:
    10 on Kraken, 20 on Binance.US)?"""
    if price in side.levels or len(side) < depth:
        return True
    worst = side.top(depth)[-1][0]
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
                 size=None, fills="queue", skew=True, ref_weight=Decimal(0), rate_tier=None,
                 alarm_bps=Decimal(0), alarm_window_ns=100_000_000, alarm_hold_ns=1_000_000_000,
                 alarm_vol=Decimal(0), alarm_vol_tau_ns=600_000_000_000,
                 book_depth=KRAKEN_BOOK_DEPTH, kraken_limits=True):
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
        self.ref_weight = Decimal(ref_weight)
        if rate_tier not in (None, "none") and rate_tier not in RATE_TIERS:
            raise ValueError(f"unknown rate tier {rate_tier!r}")
        self.rate_tier = None if rate_tier in (None, "none") else rate_tier
        self.alarm_bps = Decimal(alarm_bps)  # fixed jump size; with alarm_vol it is the floor
        self.alarm_vol = Decimal(alarm_vol)  # jump size in units of Coinbase's normal move
        self.alarm_vol_tau_ns = alarm_vol_tau_ns
        self.book_depth = book_depth  # levels the recorded book shows
        self.kraken_limits = kraken_limits  # False on Binance.US: Kraken's rate counter means nothing there
        self.alarm_on = bool(self.alarm_bps or self.alarm_vol)
        self.alarm_window_ns = alarm_window_ns
        self.alarm_hold_ns = alarm_hold_ns

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
        self._held = {"bid": False, "ask": False}  # currently holding back this side's order
        self.stats.update({"held_back": 0, "cancel_blocked": 0, "quotes_with_ref": 0, "quote_calcs": 0})
        # reference (Coinbase) price
        self.ref_mid = None
        self.ref_t = None
        self.ref_gap = None  # slow average of (ref_mid - our mid)
        self._gap_t = None
        self._last_t = None
        # alarm: a fast jump on the reference exchange takes one side off for a while
        self._ref_hist = deque()  # (time, reference mid) inside the alarm window
        self.alarm_until = {"bid": 0, "ask": 0}  # that side stays off until this time
        self.alarm_off_ns = {"bid": 0, "ask": 0}
        self.stats.update({"alarms_up": 0, "alarms_down": 0, "alarm_fills": 0})
        # volatility alarm: running average of the squared move per alarm window (bps^2)
        self.vol_var = None
        self._vol_t = None  # start of the current window on the move grid
        self._vol_p = None  # reference price at that start
        self._vol_seen_ns = 0  # how much data the average is built on
        self._thr = {"n": 0, "sum": Decimal(0), "min": None, "max": None}

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
            self._cancel_all(now)  # in case the rate limit blocked a cancel earlier
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

    def on_reference(self, now, ref_mid):
        """The other exchange's mid changed (None = its book can't be trusted)."""
        if not self.ref_weight and not self.alarm_on:
            return []  # both off: behave exactly as if Coinbase weren't there
        self._advance(now)
        self.ref_mid = ref_mid
        self.ref_t = now if ref_mid is not None else None
        fired = False
        if ref_mid is None:
            self.ref_gap = self._gap_t = None  # start the average again when it comes back
            self._ref_hist.clear()  # a jump across an outage is not a jump
            self._vol_t = self._vol_p = None  # ...and not a move for the average
        elif self.alarm_on:
            fired = self._check_alarm(now, ref_mid)
            if self.alarm_vol:
                self._update_vol(now, ref_mid)  # after the check: a jump never raises its own bar
        # with only the alarm on, Coinbase ticks must not cause extra re-quotes
        if (self.ref_weight or fired) and self.book_ok and not self.pulled:
            self._quote(now, self._book)
        return []

    def alarm_threshold(self):
        """Jump size in bps that rings the alarm, or None while the volatility
        alarm is still warming up. Fixed mode: alarm_bps. Volatility mode:
        alarm_vol x the normal move per window, never below alarm_bps."""
        if not self.alarm_vol:
            return self.alarm_bps
        if self.vol_var is None or self._vol_seen_ns < VOL_WARMUP_NS:
            return None
        return max(self.alarm_vol * self.vol_var.sqrt(), self.alarm_bps)

    def _update_vol(self, now, ref_mid):
        """Average of the squared Coinbase move per alarm window, over ~tau.
        A quiet gap of n windows counts as n windows sharing the move."""
        if self._vol_t is None:
            self._vol_t, self._vol_p = now, ref_mid
            return
        span = now - self._vol_t
        if span < self.alarm_window_ns:
            return
        n = span // self.alarm_window_ns
        step = n * self.alarm_window_ns
        r = (ref_mid - self._vol_p) / self._vol_p * BPS
        sample = r * r / n
        if self.vol_var is None:
            self.vol_var = sample
        else:
            self.vol_var += (sample - self.vol_var) * step / (self.alarm_vol_tau_ns + step)
        self._vol_seen_ns += step
        self._vol_t += step
        self._vol_p = ref_mid

    def _check_alarm(self, now, ref_mid):
        """Has the reference price jumped the alarm threshold or more within the
        window? Up: our ask is about to be bought too cheaply, so it comes off.
        Down: the bid comes off. Returns True if an alarm fired."""
        hist = self._ref_hist
        while hist and now - hist[0][0] > self.alarm_window_ns:
            hist.popleft()
        fired = False
        thr = self.alarm_threshold()
        if thr is not None and self.alarm_vol:
            t = self._thr
            t["n"] += 1
            t["sum"] += thr
            t["min"] = thr if t["min"] is None else min(t["min"], thr)
            t["max"] = thr if t["max"] is None else max(t["max"], thr)
        if hist and thr is not None:
            low = min(p for _, p in hist)
            high = max(p for _, p in hist)
            up = (ref_mid - low) / low * BPS
            down = (high - ref_mid) / high * BPS
            # "> 0": in a dead-flat market the volatility threshold can reach 0,
            # and a price that has not moved is never a jump
            if up > 0 and up >= thr:
                fired = self._alarm(now, "ask")
            elif down > 0 and down >= thr:
                fired = self._alarm(now, "bid")
        if fired:
            hist.clear()  # the move must go a further alarm_bps to ring again
        hist.append((now, ref_mid))
        return fired

    def _alarm(self, now, side):
        until = now + self.alarm_hold_ns
        old = self.alarm_until[side]
        if old <= now:  # a new alarm, not an extension of one still running
            self.stats["alarms_up" if side == "ask" else "alarms_down"] += 1
        self.alarm_off_ns[side] += until - max(old, now)
        self.alarm_until[side] = until
        return True

    def _update_gap(self, now):
        """Slow average of the normal price gap between the two exchanges."""
        if self.ref_mid is None or self.last_mid is None:
            return
        gap = self.ref_mid - self.last_mid
        if self.ref_gap is None:
            self.ref_gap, self._gap_t = gap, now
            return
        dt = now - self._gap_t
        if dt >= REF_UPDATE_NS:
            self.ref_gap += (gap - self.ref_gap) * dt / (REF_TAU_NS + dt)
            self._gap_t = now

    def fair_price(self, now):
        """Centre for our quotes: our mid, leaned toward where the reference
        exchange says the price is heading."""
        self.stats["quote_calcs"] += 1
        if (not self.ref_weight or self.ref_mid is None or self.ref_gap is None
                or now - self.ref_t > REF_STALE_NS):
            return self.last_mid
        self.stats["quotes_with_ref"] += 1
        return self.last_mid + self.ref_weight * (self.ref_mid - self.last_mid - self.ref_gap)

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
            if level_visible(book_side, q.price, self.book_depth):
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
            if not level_visible(book_side, q.price, self.book_depth):
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
        if self.ref_weight:
            self._update_gap(now)
        bid_p, ask_p = self.wanted_quotes(self.fair_price(now))
        if now < self.alarm_until["bid"]:
            bid_p = None  # alarm: stay off this side
        if now < self.alarm_until["ask"]:
            ask_p = None
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

    def _open_orders(self):
        """Orders we may still have to cancel (ones already being cancelled are paid for)."""
        resting = sum(1 for q in self.cur.values() if q is not None and q.dead_at is None)
        return resting + sum(1 for q in self.nxt.values() if q is not None)

    def _affordable(self, now, side, cost, orders_after):
        """Rate limit check for a NEW order (or a re-quote). We only send it if,
        afterwards, there is still room to cancel every order we have resting."""
        if self.rate_tier is None:
            return True
        counter = self.rate[self.rate_tier]
        if counter.level(now) + cost + MAX_CANCEL_COST * orders_after <= counter.threshold:
            self._held[side] = False
            return True
        if not self._held[side]:  # count each hold-back once, not every time we re-check
            self._held[side] = True
            self.stats["held_back"] += 1
        return False

    def _new_quote(self, now, price):
        self.stats["places"] += 1
        self._charge(now, ADD_COST)
        return PaperQuote(price, self.size, now, now + self.latency_ns)

    def _cancel(self, now, q):
        """Returns False if the rate limit refuses the cancel (the quote stays live)."""
        cost = cancel_cost(now - q.placed_t)
        if self.rate_tier is not None:
            counter = self.rate[self.rate_tier]
            if counter.level(now) + cost > counter.threshold:
                self.stats["cancel_blocked"] += 1
                return False
        self.stats["cancels"] += 1
        self._charge(now, cost)
        q.dead_at = now + self.latency_ns
        return True

    def _move(self, now, side, price, threshold):
        q = self.cur[side]
        if q is None:
            if price is not None and self._affordable(now, side, ADD_COST, self._open_orders() + 1):
                self.cur[side] = self._new_quote(now, price)
            return
        if q.dead_at is not None or now < q.live_at:
            return  # one change in flight per side
        if price is None:
            self._cancel(now, q)
            return
        if abs(q.price - price) < threshold and q.size == self.size:
            return  # close enough: keep our place in the queue
        cost = cancel_cost(now - q.placed_t) + ADD_COST
        if not self._affordable(now, side, cost, self._open_orders()):
            return  # can't afford to move it yet: it stays where it is
        self.stats["requotes"] += 1
        self._cancel(now, q)
        self.nxt[side] = self._new_quote(now, price)

    def _cancel_all(self, now):
        for side in ("bid", "ask"):
            q = self.cur[side]
            self.nxt[side] = None
            if q is not None and q.dead_at is None:
                self._cancel(now, q)

    def _pull(self, now):
        if self.pulled:
            return []
        self.pulled = True
        self._cancel_all(now)
        self.stats["stale_pulls"] += 1
        return [f"PAPER {self.symbol} quotes pulled (book stale or invalid)"]

    def _fill(self, now, side, quote, qty, kind):
        stale = quote.dead_at is not None
        if now < self.alarm_until["bid" if side == "buy" else "ask"]:
            self.stats["alarm_fills"] += 1  # hit before our cancel landed
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
        if self.kraken_limits:
            lines.append(f"summary paper {s} rate_limit_blocked {blocked} "
                         f"(order actions={self.stats['places'] + self.stats['cancels']})")
        else:
            lines.append(f"summary paper {s} rate_limit not modeled "
                         f"(order actions={self.stats['places'] + self.stats['cancels']})")
        if self.rate_tier is not None:
            lines.append(f"summary paper {s} rate_limit enforced={self.rate_tier} "
                         f"held_back={self.stats['held_back']} "
                         f"cancel_blocked={self.stats['cancel_blocked']} "
                         f"peak={self.rate[self.rate_tier].peak:.0f}/{self.rate[self.rate_tier].threshold}")
        if self.ref_weight:
            calcs = self.stats["quote_calcs"]
            used = f"{Decimal(self.stats['quotes_with_ref']) / calcs * 100:.0f}%" if calcs else "n/a"
            lines.append(f"summary paper {s} coinbase_signal weight={self.ref_weight} used={used}")
        if self.alarm_on:
            off = {side: (Decimal(ns) / total_ns * 100) if total_ns else Decimal(0)
                   for side, ns in self.alarm_off_ns.items()}
            if self.alarm_vol:
                t = self._thr
                if t["n"]:
                    level = (f"(avg {t['sum'] / t['n']:.2f}bps min {t['min']:.2f} "
                             f"max {t['max']:.2f})")
                else:
                    level = "(never warmed up)"
                jump = (f"jump>={self.alarm_vol}x_normal_move {level}"
                        + (f" floor={self.alarm_bps}bps" if self.alarm_bps else ""))
            else:
                jump = f"jump>={self.alarm_bps}bps"
            lines.append(f"summary paper {s} coinbase_alarm {jump} "
                         f"within={self.alarm_window_ns // 1_000_000}ms "
                         f"hold={self.alarm_hold_ns // 1_000_000}ms "
                         f"alarms up={self.stats['alarms_up']} down={self.stats['alarms_down']} "
                         f"bid_off={off['bid']:.2f}% ask_off={off['ask']:.2f}% "
                         f"fills_during_alarm={self.stats['alarm_fills']}")
        return lines
