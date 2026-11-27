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

HIP-3 dexes (e.g. "xyz", the stock perps): userFills already includes their
fills (coin names like "xyz:XYZ100"), but orders, positions and mids are per
dex. hlwatch asks for each extra dex too and merges the answers into the same
events, coin names prefixed "dex:". A dex is added when the account shows up
in one of its coins (usually through a fill) or with --dex. Each extra dex
adds about 24 request weight per poll (Hyperliquid allows 1200 a minute).

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


def on_dex(coin, dex):
    """A coin name as seen from one dex: 'XYZ100' on xyz -> 'xyz:XYZ100'."""
    return coin if not coin or ":" in coin else f"{dex}:{coin}"


def fill_key(fill):
    """A fill's identity: its trade id, plus the order id and time to be safe."""
    return (fill.get("tid"), fill.get("oid"), fill.get("time"))


class Watcher:
    """One polling session. `post` and `writer` are passed in so tests can fake them."""

    def __init__(self, address, writer, post=http_post, fills_every=12, dexes=()):
        self.address = check_address(address)
        self.writer = writer
        self.post = post
        self.fills_every = fills_every  # in polls
        self.polls = 0
        self.seen_fills = set()
        self.coins = set()  # coins this account has appeared in
        self.errors = 0
        self.dexes = []  # extra HIP-3 dexes polled besides the main one
        for d in dexes:
            self._add_dex(d, "asked (--dex)")

    def _add_dex(self, dex, why):
        if dex and dex not in self.dexes:
            self.dexes.append(dex)
            self.writer.write("hlwatch", "dex", {"dex": dex, "why": why})

    def _saw(self, coin):
        if coin:
            self.coins.add(coin)
            if ":" in coin:
                self._add_dex(coin.split(":", 1)[0], f"seen in {coin}")

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
        dexes = list(self.dexes)  # this poll's set; a dex found now is asked from the next poll

        orders = self._ask("orders", {"type": "frontendOpenOrders", "user": user})
        if orders is not None:
            partial = []
            for d in dexes:
                more = self._ask(f"orders {d}", {"type": "frontendOpenOrders", "user": user, "dex": d})
                if more is None:
                    partial.append(d)
                    continue
                orders = orders + [dict(o, coin=on_dex(o.get("coin"), d)) for o in more]
            data = {"orders": orders}
            if partial:
                data["missing_dexes"] = partial
            self.writer.write("hlwatch", "orders", data)
            for o in orders:
                self._saw(o.get("coin"))

        state = self._ask("positions", {"type": "clearinghouseState", "user": user})
        if state is not None:
            state = dict(state)
            positions = list(state.get("assetPositions", []))
            margins = {}
            for d in dexes:
                more = self._ask(f"positions {d}", {"type": "clearinghouseState", "user": user, "dex": d})
                if more is None:
                    continue
                for p in more.get("assetPositions", []):
                    q = dict(p.get("position") or {})
                    q["coin"] = on_dex(q.get("coin"), d)
                    positions.append(dict(p, position=q))
                margins[d] = more.get("marginSummary", {})
            state["assetPositions"] = positions
            if margins:
                state["dex_margin"] = margins  # each HIP-3 dex keeps its own margin account
            self.writer.write("hlwatch", "positions", state)
            for p in positions:
                self._saw((p.get("position") or {}).get("coin"))

        new = 0
        if self.polls == 1 or self.polls % self.fills_every == 0:
            fills = self._ask("fills", {"type": "userFills", "user": user})
            if fills is not None:
                fresh = [f for f in fills if fill_key(f) not in self.seen_fills]
                for f in fresh:
                    self.seen_fills.add(fill_key(f))
                    self._saw(f.get("coin"))
                if fresh:
                    self.writer.write("hlwatch", "fills", {"fills": fresh, "returned": len(fills)})
                new = len(fresh)

        mids = self._ask("mids", {"type": "allMids"})
        if mids is not None:
            mids = dict(mids)
            for d in dexes:
                more = self._ask(f"mids {d}", {"type": "allMids", "dex": d})
                if more is not None:
                    mids.update({on_dex(c, d): px for c, px in more.items()})
            if self.coins:
                self.writer.write("hlwatch", "mids", {c: mids[c] for c in sorted(self.coins) if c in mids})
        n_orders = len(orders) if orders is not None else "?"
        extra = f", dexes: {','.join(self.dexes)}" if self.dexes else ""
        return (f"poll {self.polls}: {n_orders} open orders, {new} new fills, "
                f"{len(self.coins)} coins{extra}, {self.errors} errors")


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
    """Read a hlwatch log ONCE (any iterable, e.g. a stream from the file) and
    keep only the small pieces the report needs, not the raw events:
      polls      [(t, [(coin, (best, levels, $) bid, (best, levels, $) ask)])]
                 one per orders event (what the quotes section needs, nothing more)
      mids       {coin: [(t, mid)]}   sorted once, ready for mid_at
      fills      [fill dict]          (few)
      positions  [(t, [(coin, signed value)], accountValue, {dex: accountValue})]
      errors     count;  dex  [(dex, why)]
    Multi-GB logs then fit in memory and the report needs no re-sorting."""
    out = {"polls": [], "mids": defaultdict(list), "fills": [], "positions": [],
           "errors": 0, "dex": []}
    for ev in events:
        if ev.get("conn") != "hlwatch":
            continue
        kind, t, d = ev["type"], ev["t"], ev["data"]
        if kind == "orders":
            per = defaultdict(lambda: {"B": [], "A": []})
            for o in d["orders"]:
                per[o.get("coin")][o.get("side")].append((_f(o.get("limitPx")), _f(o.get("sz"))))
            summary = []  # per coin: (coin, (best, levels, usd) for bid, the same for ask)
            for coin, sides in per.items():
                both = []
                for side in ("B", "A"):
                    os_ = sides[side]
                    best = None
                    if os_:
                        best = max(px for px, _ in os_) if side == "B" else min(px for px, _ in os_)
                    both.append((best, len({px for px, _ in os_}), sum(px * sz for px, sz in os_)))
                summary.append((coin, both[0], both[1]))
            out["polls"].append((t, summary))
        elif kind == "mids":
            for coin, px in d.items():
                v = _f(px)
                if v:
                    out["mids"][coin].append((t, v))
        elif kind == "fills":
            out["fills"].extend(d["fills"])
        elif kind == "positions":
            held = []
            for p_ in d.get("assetPositions", []):
                q = p_.get("position") or {}
                v, szi = _f(q.get("positionValue")), _f(q.get("szi"))
                if q.get("coin") and v is not None and szi is not None:
                    held.append((q["coin"], v if szi >= 0 else -v))
            acct = _f((d.get("marginSummary") or {}).get("accountValue"))
            dexes = {dex: _f((m or {}).get("accountValue")) for dex, m in (d.get("dex_margin") or {}).items()}
            out["positions"].append((t, held, acct, dexes))
        elif kind == "error":
            out["errors"] += 1
        elif kind == "dex":
            out["dex"].append((d["dex"], d["why"]))
    for series in out["mids"].values():
        series.sort()  # once, here (it used to be re-sorted for every poll x coin)
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
    """Text report, as a list of lines. `events` is read once."""
    data = load(events)
    lines = []
    polls = data["polls"]
    mids = data["mids"]
    if polls:
        span_h = (polls[-1][0] - polls[0][0]) / 3.6e12
        lines.append(f"hlreport {len(polls)} polls over {span_h:.2f} hours, "
                     f"{data['errors']} errors")

    # ---- fills ----
    fills = data["fills"]
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
        ms = mids.get(coin, [])
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
    for t, summary in polls:
        for coin, bid, ask in summary:
            mid = mid_at(mids.get(coin, []), t)
            d = dist[coin]
            for (best, levels, usd), side, key in ((bid, "B", "bid"), (ask, "A", "ask")):
                d[f"levels_{key}"].append(levels)
                d[f"usd_{key}"].append(usd)
                if best is not None and mid:
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
    for _, held, _, _ in data["positions"]:
        for coin, signed in held:
            pos[coin].append(signed)
    n_polls = max(1, len(data["positions"]))
    for coin in sorted(pos, key=lambda c: -max(abs(x) for x in pos[c])):
        xs = pos[coin]
        flips = sum(1 for a, b in zip(xs, xs[1:]) if (a > 0) != (b > 0))
        lines.append(f"hlreport   {coin}: held in {len(xs) / n_polls * 100:.0f}% of polls, "
                     f"median |position| ${_median([abs(x) for x in xs]):,.0f}, "
                     f"largest ${max(abs(x) for x in xs):,.0f}, long/short flips {flips}")
    vals = [acct for _, _, acct, _ in data["positions"] if acct is not None]
    if vals:
        lines.append(f"hlreport account value: first ${vals[0]:,.0f}, last ${vals[-1]:,.0f}, "
                     f"change ${vals[-1] - vals[0]:+,.0f}")
    dex_vals = defaultdict(list)  # HIP-3 dexes keep separate margin accounts
    for _, _, _, dexes in data["positions"]:
        for dex, v in dexes.items():
            if v is not None:
                dex_vals[dex].append(v)
    for dex in sorted(dex_vals):
        vs = dex_vals[dex]
        lines.append(f"hlreport account value on {dex}: first ${vs[0]:,.0f}, last ${vs[-1]:,.0f}, "
                     f"change ${vs[-1] - vs[0]:+,.0f}")
    for dex, why in data["dex"]:
        lines.append(f"hlreport dex {dex} added: {why}")
    return lines


def report_file(path):
    """The report for a log file, read one batch at a time, so a multi-GB log
    is never all in memory at once."""
    from .eventlog import EventTailer
    tailer = EventTailer(path)

    def stream():
        while True:
            batch = tailer.read_available()
            if not batch:
                return
            yield from batch
    try:
        return report(stream())
    finally:
        tailer.close()


if __name__ == "__main__":
    # python -m mmstack.hlwatch report <log> [--out file]   (streams the log; for big files)
    import argparse
    ap = argparse.ArgumentParser(prog="python -m mmstack.hlwatch")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rp = sub.add_parser("report", help="summarize an hlwatch log, reading it as a stream")
    rp.add_argument("log")
    rp.add_argument("--out", help="also write the report to this file")
    a = ap.parse_args()
    lines = report_file(a.log)
    for line in lines:
        print(line)
    if a.out:
        with open(a.out, "w", encoding="utf-8") as f:
            f.write("\n".join(lines) + "\n")
