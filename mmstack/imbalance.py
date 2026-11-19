"""Does the order book's lean predict the next mid-price move?  (Research tool.)

The test of the VisualHFT 2026 paper on our own logs. Replays a log through
the normal state machine and, for each book we ask about:

- every time the mid price changes, takes a reading of the book just after
  the change (the "event"), and
- records which way the NEXT mid change goes and how long it took
  (kept if it comes within 30 s and the book stays valid until then).

Two readings, both in [-1, 1], > 0 = more size on the bid side:
  touch:    (b1 - a1) / (b1 + a1), the best bid and ask sizes only
  weighted: the paper's form over the top N levels, each level weighted
            1 / (1 + d / S)^2, d = its distance from its own side's best
            price, S = the spread at the reading (at least one tick)

A reading of exactly 0 makes no call. The hit rate is the share of calls that
point the way the next change went; the base rate is what a reading with no
information would score given how often it calls each way and how often the
price goes each way: P(call up) P(up) + P(call down) P(down).

This is analysis, not part of the deterministic trading core, so it uses
plain floats.
"""

from .paper_mm import tick_from_prices
from .state import StateMachine

MAX_WAIT_NS = 30_000_000_000  # next change must come within 30 s
HORIZONS_NS = ((1_000_000_000, "1s"), (5_000_000_000, "5s"))  # fixed-time moves after a reading
SPREAD_BUCKETS = ((1, 1, "1 tick"), (2, 2, "2 ticks"), (3, 5, "3-5 ticks"), (6, None, "6+ ticks"))
TIME_BUCKETS_MS = ((0, 50, "under 50 ms"), (50, 150, "50-150 ms"),
                   (150, 1000, "150 ms-1 s"), (1000, 30000, "1-30 s"))
READING_BINS = (-1.0, -0.6, -0.2, 0.2, 0.6, 1.0000001)


def touch_reading(book):
    bids, asks = book.bids.top(1), book.asks.top(1)
    b, a = float(bids[0][1]), float(asks[0][1])
    return (b - a) / (b + a) if b + a > 0 else 0.0


def weighted_reading(book, levels, tick):
    """The paper's distance-weighted imbalance over the top `levels` per side."""
    bids, asks = book.bids.top(levels), book.asks.top(levels)
    best_bid, best_ask = float(bids[0][0]), float(asks[0][0])
    scale = max(best_ask - best_bid, float(tick))
    b = sum(float(q) / (1 + (best_bid - float(p)) / scale) ** 2 for p, q in bids)
    a = sum(float(q) / (1 + (float(p) - best_ask) / scale) ** 2 for p, q in asks)
    return (b - a) / (b + a) if b + a > 0 else 0.0


def collect(tailer, keys, levels=10):
    """keys: list of (venue, symbol). Returns {key: {"events": [...], "changes": n,
    "tick": tick}}, each event a dict with touch, weighted, up (bool),
    wait_ns, spread_ticks, move_bps, half_spread_bps, and move_1s_bps /
    move_5s_bps (mid move after a fixed time, when the book stayed valid)."""
    sm = StateMachine()
    out = {k: {"events": [], "changes": 0, "tick": None} for k in keys}
    last_mid = {k: None for k in keys}
    pending = {k: None for k in keys}  # the reading waiting for its outcome
    horizon = {k: [] for k in keys}  # readings waiting for their 1 s / 5 s mid
    fallback_tick = {}
    wanted = {k[0] for k in keys}
    while True:
        events = tailer.read_available()
        if not events:
            break
        for ev in events:
            sm.update(ev)
            if ev["conn"] not in wanted:
                continue
            t = ev["t"]
            for key in keys:
                if key[0] != ev["conn"]:
                    continue
                book = sm.books.get(key)
                bid = ask = None
                if book is not None and sm.valid.get(key):
                    bid, ask = book.best_bid(), book.best_ask()
                if bid is None or ask is None:
                    last_mid[key] = pending[key] = None  # start again once the book is back
                    horizon[key] = []
                    continue
                old = last_mid[key]
                # fixed-time moves: the mid in effect at t0 + h is the one before this event
                waiting = []
                for e, due in horizon[key]:
                    while due and t >= e["t0"] + due[0][0]:
                        h, label = due.pop(0)
                        e[f"move_{label}_bps"] = float((old - e["mid0"]) / e["mid0"]) * 10000
                    if due:
                        waiting.append((e, due))
                horizon[key] = waiting
                mid = (bid + ask) / 2
                last_mid[key] = mid
                if old is None or mid == old:
                    continue
                # the mid changed: this settles the waiting reading, and opens a new one
                out[key]["changes"] += 1
                p = pending[key]
                if p is not None and t - p["t"] <= MAX_WAIT_NS:
                    p["up"] = mid > p["mid"]
                    p["wait_ns"] = t - p["t"]
                    p["move_bps"] = float((mid - p["mid"]) / p["mid"]) * 10000
                    out[key]["events"].append(p)
                tick = _tick(sm, key, book, fallback_tick)
                out[key]["tick"] = tick
                pending[key] = {
                    "t": t, "mid": mid,
                    "touch": touch_reading(book),
                    "weighted": weighted_reading(book, levels, tick),
                    "spread_ticks": int(round((ask - bid) / tick)),
                    "half_spread_bps": float((ask - bid) / 2 / mid) * 10000,
                }
                # the same event dict gets its 1 s / 5 s moves filled in later
                pending[key]["t0"], pending[key]["mid0"] = t, mid
                horizon[key].append((pending[key], list(HORIZONS_NS)))
    return out


def _tick(sm, key, book, fallback):
    venue, symbol = key
    if venue == "kraken":
        inc = (sm.instruments.get(symbol) or {}).get("price_increment")
        if inc:
            return inc
    if key not in fallback:  # read it off the prices once, like the paper market maker does
        fallback[key] = tick_from_prices(list(book.bids.levels) + list(book.asks.levels))
    return fallback[key]


def score(events, reading):
    """Hit rate, base rate and number of calls for one reading ("touch" or "weighted")."""
    calls = [e for e in events if e[reading] != 0]
    n = len(calls)
    if n == 0:
        return {"calls": 0, "no_call": len(events)}
    hits = sum(1 for e in calls if (e[reading] > 0) == e["up"])
    call_up = sum(1 for e in calls if e[reading] > 0) / n
    up = sum(1 for e in calls if e["up"]) / n
    base = call_up * up + (1 - call_up) * (1 - up)
    signed = sum(e["move_bps"] * (1 if e[reading] > 0 else -1) for e in calls) / n
    half = sum(e["half_spread_bps"] for e in calls) / n
    result = {"calls": n, "no_call": len(events) - n, "hit": hits / n, "base": base,
              "diff": hits / n - base, "signed_move_bps": signed, "half_spread_bps": half}
    for _, label in HORIZONS_NS:
        key = f"move_{label}_bps"
        done = [e for e in calls if key in e]
        result[f"signed_{label}_bps"] = (sum(e[key] * (1 if e[reading] > 0 else -1) for e in done)
                                         / len(done)) if done else None
    return result


def _fmt(s):
    if not s["calls"]:
        return "no calls"
    return (f"hit {s['hit'] * 100:5.1f}%  base {s['base'] * 100:5.1f}%  "
            f"diff {s['diff'] * 100:+5.1f} pts  (n={s['calls']})")


def report(name, data):
    """Text report for one book, as a list of lines."""
    ev = data["events"]
    lines = [f"imbalance {name}: {data['changes']} mid changes, {len(ev)} events "
             f"(next change within 30 s), tick {data['tick']}"]
    if len(ev) < 100:
        return lines + [f"imbalance {name}: not enough data"]
    for reading in ("touch", "weighted"):
        s = score(ev, reading)
        lines.append(f"imbalance {name} {reading:8s} {_fmt(s)}")
    for reading in ("touch", "weighted"):
        s = score(ev, reading)
        if s["calls"]:
            later = "  ".join(f"after {label} {s[f'signed_{label}_bps']:+.3f}"
                              for _, label in HORIZONS_NS if s[f"signed_{label}_bps"] is not None)
            lines.append(f"imbalance {name} {reading:8s} mid move signed by the reading (bps): "
                         f"next change {s['signed_move_bps']:+.3f}  {later}  "
                         f"(half spread {s['half_spread_bps']:.3f})")
    lines.append(f"imbalance {name} by spread (touch | weighted):")
    for lo, hi, label in SPREAD_BUCKETS:
        sub = [e for e in ev if e["spread_ticks"] >= lo and (hi is None or e["spread_ticks"] <= hi)]
        if sub:
            lines.append(f"imbalance {name}   {label:10s} {_fmt(score(sub, 'touch'))} | "
                         f"{_fmt(score(sub, 'weighted'))}")
    lines.append(f"imbalance {name} by time to the next change (touch | weighted):")
    for lo, hi, label in TIME_BUCKETS_MS:
        sub = [e for e in ev if lo * 1_000_000 <= e["wait_ns"] < hi * 1_000_000]
        if sub:
            lines.append(f"imbalance {name}   {label:11s} {_fmt(score(sub, 'touch'))} | "
                         f"{_fmt(score(sub, 'weighted'))}")
    slow = [e for e in ev if e["wait_ns"] >= 150_000_000]
    if slow:
        lines.append(f"imbalance {name}   150 ms+     {_fmt(score(slow, 'touch'))} | "
                     f"{_fmt(score(slow, 'weighted'))}  <-- what a 150 ms bot could use")
    lines.append(f"imbalance {name} share of next changes up, by weighted reading:")
    for lo, hi in zip(READING_BINS, READING_BINS[1:]):
        sub = [e for e in ev if lo <= e["weighted"] < hi]
        if sub:
            up = sum(1 for e in sub if e["up"]) / len(sub)
            later = ""
            for _, label in HORIZONS_NS:
                done = [e[f"move_{label}_bps"] for e in sub if f"move_{label}_bps" in e]
                if done:
                    later += f"  mid after {label} {sum(done) / len(done):+.3f} bps"
            lines.append(f"imbalance {name}   [{lo:+.1f}, {min(hi, 1.0):+.1f})  "
                         f"up {up * 100:5.1f}%{later}  (n={len(sub)})")
    return lines
