"""Watch one Hyperliquid account's public data.  (Research tool.)

Hyperliquid publishes every account's open orders, positions and fills
through its public info API (POST https://api.hyperliquid.xyz/info, no key).
`hlwatch` polls one address and logs what it sees, in our usual event-log
format, so `hlreport` can describe how that account trades:

  every --every seconds (default 5):
    orders     frontendOpenOrders  the account's resting orders
    positions  clearinghouseState  its perp positions and margin
    mids       allMids             mid prices, kept only for coins the
                                   account has shown up in (keeps files small)
  every --fills-every seconds (default 60):
    fills      userFills           its latest fills (the API returns at most
                                   2000); only fills not seen before are logged

Read-only, public, no account or key needed. Polling misses anything that
lives for less than the polling interval: we see the resting pattern, not
every quote update. Markouts compare the exchange's fill time with mids
stamped by OUR clock every few seconds, so they use 1 and 5 minute horizons,
where a few seconds of polling gap or clock error don't matter much.

This is analysis, not part of the deterministic trading core, so it uses
plain floats.
"""

import json
import re
import time
import urllib.request
from collections import defaultdict

INFO_URL = "https://api.hyperliquid.xyz/info"
ADDRESS = re.compile(r"^0x[0-9a-fA-F]{40}$")
TIMEOUT_S = 10


def http_post(body):
    """POST JSON to Hyperliquid's info endpoint, return the parsed reply."""
    req = urllib.request.Request(INFO_URL, data=json.dumps(body).encode(),
                                 headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=TIMEOUT_S) as resp:
        return json.loads(resp.read())


def check_address(address):
    if not ADDRESS.match(address):
        raise ValueError(f"not a Hyperliquid address: {address!r} (expected 0x + 40 hex digits)")
    return address.lower()


def fill_key(fill):
    """A fill's identity: its trade id, plus the order id and time to be safe."""
    return (fill.get("tid"), fill.get("oid"), fill.get("time"))


class Watcher:
    """One polling session. `post` and `writer` are passed in so tests can fake them."""

    def __init__(self, address, writer, post=http_post, fills_every=12):
        self.address = check_address(address)
        self.writer = writer
        self.post = post
        self.fills_every = fills_every  # in polls
        self.polls = 0
        self.seen_fills = set()
        self.coins = set()  # coins this account has appeared in
        self.errors = 0

    def _ask(self, kind, body):
        try:
            return self.post(body)
        except Exception as exc:  # network trouble: log it and carry on next poll
            self.errors += 1
            self.writer.write("hlwatch", "error", {"request": kind, "error": f"{type(exc).__name__}: {exc}"})
            return None

    def poll(self):
        """One round. Returns a short status string."""
        self.polls += 1
        user = self.address
        orders = self._ask("orders", {"type": "frontendOpenOrders", "user": user})
        if orders is not None:
            self.writer.write("hlwatch", "orders", {"orders": orders})
            self.coins.update(o.get("coin") for o in orders if o.get("coin"))
        state = self._ask("positions", {"type": "clearinghouseState", "user": user})
        if state is not None:
            self.writer.write("hlwatch", "positions", state)
            for p in state.get("assetPositions", []):
                coin = (p.get("position") or {}).get("coin")
                if coin:
                    self.coins.add(coin)
        new = 0
        if self.polls == 1 or self.polls % self.fills_every == 0:
            fills = self._ask("fills", {"type": "userFills", "user": user})
            if fills is not None:
                fresh = [f for f in fills if fill_key(f) not in self.seen_fills]
                for f in fresh:
                    self.seen_fills.add(fill_key(f))
                    if f.get("coin"):
                        self.coins.add(f["coin"])
                if fresh:
                    self.writer.write("hlwatch", "fills", {"fills": fresh, "returned": len(fills)})
                new = len(fresh)
        mids = self._ask("mids", {"type": "allMids"})
        if mids is not None and self.coins:
            self.writer.write("hlwatch", "mids", {c: mids[c] for c in sorted(self.coins) if c in mids})
        n_orders = len(orders) if orders is not None else "?"
        return f"poll {self.polls}: {n_orders} open orders, {new} new fills, {len(self.coins)} coins, {self.errors} errors"


def run(watcher, every_s, clock=time.monotonic, sleep=time.sleep, should_stop=lambda: False, say=print):
    """Poll every `every_s` seconds until should_stop() (or Ctrl+C in real use)."""
    while not should_stop():
        start = clock()
        say(watcher.poll())
        left = every_s - (clock() - start)
        if left > 0:
            sleep(left)


# ---------------- report ----------------

def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def load(events):
    """Split a hlwatch log into lists by type."""
    out = defaultdict(list)
    for ev in events:
        if ev.get("conn") == "hlwatch":
            out[ev["type"]].append(ev)
    return out


def mid_at(mid_series, t_ns):
    """Last mid known at or before t_ns from a sorted [(t_ns, mid)] list, else None."""
    lo, hi = 0, len(mid_series)
    while lo < hi:
        m = (lo + hi) // 2
        if mid_series[m][0] <= t_ns:
            lo = m + 1
        else:
            hi = m
    return mid_series[lo - 1][1] if lo else None


def _median(xs):
    xs = sorted(xs)
    if not xs:
        return None
    n = len(xs)
    return xs[n // 2] if n % 2 else (xs[n // 2 - 1] + xs[n // 2]) / 2


def report(events, markout_s=(60, 300)):
    """Text report, as a list of lines."""
    data = load(events)
    lines = []
    polls = data["orders"]
    if polls:
        span_h = (polls[-1]["t"] - polls[0]["t"]) / 3.6e12
        lines.append(f"hlreport {len(polls)} polls over {span_h:.2f} hours, "
                     f"{len(data['error'])} errors")
    # mids per coin, by our receive time
    mids = defaultdict(list)
    for ev in data["mids"]:
        for coin, px in ev["data"].items():
            v = _f(px)
            if v:
                mids[coin].append((ev["t"], v))

    # ---- fills ----
    fills = [f for ev in data["fills"] for f in ev["data"]["fills"]]
    by_coin = defaultdict(list)
    for f in fills:
        by_coin[f.get("coin")].append(f)
    lines.append(f"hlreport fills: {len(fills)} new fills logged")
    for coin in sorted(by_coin, key=lambda c: -sum(_f(f["px"]) * _f(f["sz"]) for f in by_coin[c])):
        fs = by_coin[coin]
        notional = sum(_f(f["px"]) * _f(f["sz"]) for f in fs)
        maker = sum(1 for f in fs if not f.get("crossed"))
        fees = sum(_f(f.get("fee")) or 0.0 for f in fs)
        pnl = sum(_f(f.get("closedPnl")) or 0.0 for f in fs)
        fee_bps = fees / notional * 10000 if notional else 0.0
        line = (f"hlreport   {coin}: {len(fs)} fills, ${notional:,.0f} traded, "
                f"maker {maker / len(fs) * 100:.0f}%, fees ${fees:+,.2f} ({fee_bps:+.2f} bps, - = rebate), "
                f"realized pnl ${pnl:+,.2f} ({pnl / notional * 10000 if notional else 0:+.2f} bps)")
        lines.append(line)
        # markouts: mid later vs fill price, signed so + = good for the account
        ms = sorted(mids.get(coin, []))
        if ms:
            parts = []
            for h in markout_s:
                vals = []
                for f in fs:
                    px = _f(f["px"])
                    t_ns = int(f["time"]) * 1_000_000  # exchange ms
                    later = mid_at(ms, t_ns + h * 1_000_000_000)
                    if later is None or t_ns + h * 1_000_000_000 > ms[-1][0]:
                        continue
                    sign = 1 if f.get("side") == "B" else -1
                    vals.append(sign * (later - px) / px * 10000)
                if vals:
                    parts.append(f"{h}s {sum(vals) / len(vals):+.2f} bps (n={len(vals)})")
            if parts:
                lines.append(f"hlreport     markout (mid later vs fill price, + = good): " + "  ".join(parts))

    # ---- quotes ----
    lines.append("hlreport quotes (median over polls):")
    dist = defaultdict(lambda: {"bid": [], "ask": [], "levels_bid": [], "levels_ask": [],
                                "usd_bid": [], "usd_ask": []})
    for ev in polls:
        per = defaultdict(lambda: {"B": [], "A": []})
        for o in ev["data"]["orders"]:
            per[o.get("coin")][o.get("side")].append(o)
        for coin, sides in per.items():
            mid = mid_at(sorted(mids.get(coin, [])), ev["t"])
            for side, key in (("B", "bid"), ("A", "ask")):
                os_ = sides[side]
                d = dist[coin]
                d[f"levels_{key}"].append(len({o.get("limitPx") for o in os_}))
                d[f"usd_{key}"].append(sum(_f(o["limitPx"]) * _f(o["sz"]) for o in os_))
                if os_ and mid:
                    best = max(_f(o["limitPx"]) for o in os_) if side == "B" else min(_f(o["limitPx"]) for o in os_)
                    d[key].append((mid - best) / mid * 10000 if side == "B" else (best - mid) / mid * 10000)
    for coin in sorted(dist, key=lambda c: -(_median(dist[c]["usd_bid"]) or 0) - (_median(dist[c]["usd_ask"]) or 0)):
        d = dist[coin]
        bb, ba = _median(d["bid"]), _median(d["ask"])
        lines.append(f"hlreport   {coin}: best bid {'n/a' if bb is None else f'{bb:.2f} bps'} below mid, "
                     f"best ask {'n/a' if ba is None else f'{ba:.2f} bps'} above, "
                     f"levels {_median(d['levels_bid'])}/{_median(d['levels_ask'])}, "
                     f"resting ${_median(d['usd_bid']):,.0f} bid / ${_median(d['usd_ask']):,.0f} ask")

    # ---- positions ----
    lines.append("hlreport positions (by poll):")
    pos = defaultdict(list)
    for ev in data["positions"]:
        for p in ev["data"].get("assetPositions", []):
            q = p.get("position") or {}
            v = _f(q.get("positionValue"))
            szi = _f(q.get("szi"))
            if q.get("coin") and v is not None and szi is not None:
                pos[q["coin"]].append(v if szi >= 0 else -v)
    n_polls = max(1, len(data["positions"]))
    for coin in sorted(pos, key=lambda c: -max(abs(x) for x in pos[c])):
        xs = pos[coin]
        flips = sum(1 for a, b in zip(xs, xs[1:]) if (a > 0) != (b > 0))
        lines.append(f"hlreport   {coin}: held in {len(xs) / n_polls * 100:.0f}% of polls, "
                     f"median |position| ${_median([abs(x) for x in xs]):,.0f}, "
                     f"largest ${max(abs(x) for x in xs):,.0f}, long/short flips {flips}")
    acct = [ev["data"].get("marginSummary", {}) for ev in data["positions"]]
    vals = [_f(a.get("accountValue")) for a in acct if _f(a.get("accountValue")) is not None]
    if vals:
        lines.append(f"hlreport account value: first ${vals[0]:,.0f}, last ${vals[-1]:,.0f}, "
                     f"change ${vals[-1] - vals[0]:+,.0f}")
    return lines
