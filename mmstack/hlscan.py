"""Find the accounts that market-make on Hyperliquid.  (Research tool.)

Hyperliquid's public trade feed names both accounts on every trade:
`users` is [buyer, seller], and `side` says who crossed the spread (the
taker): "B" means the buyer took, "A" means the seller took. The other one
provided the resting order: the maker. So the feed alone tells us who makes
markets, on which coins, and how it goes for them.

  python -m mmstack.hlscan record [--data-dir E:\\hldata] [--until-midnight]
      One WebSocket connection, trades for every market picked below, written
      to our usual event log (one file per run; with --until-midnight a fresh
      file each New York midnight, like mm-stack's recorder). Public, no key.

  python -m mmstack.hlscan report <log file or folder> [--out file]
      Per account AND per coin (an account can be a taker on one coin and a
      market maker on another): maker share, buys vs sells, hours active,
      markouts, net position change. Flags the likely market makers and
      ranks them.

Markets: a fixed core list (big, middle and smaller perps, the xyz stock
perps, three spot pairs) plus the main-dex perps ranked 20th-40th by 24 h
volume at start (the "middle of the pack", refreshed every new file).

Limits, honestly:
  - Fills only: no resting quotes, no cancels (hlwatch shows those for one
    account at a time).
  - One firm may use many addresses; vaults show up as makers too.
  - Markouts use the last TRADE price at the horizon, not the mid (the feed
    has no book). A trade price bounces between bid and ask, so a single
    markout is noisy by about half a spread; averages over many fills are
    what to read.
  - The feed's first message after subscribing repeats recent trades; the
    report drops repeats.

This is analysis, not part of the deterministic trading core: plain floats.
"""

import argparse
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime

from .eventlog import EventTailer, EventWriter
from .hlwatch import http_post
from .net import Reactor, WsClient

WS_HOST = "api.hyperliquid.xyz"
WS_PATH = "/ws"
CONN = "hlscan"
PING_EVERY_S = 50.0  # Hyperliquid closes a connection that's quiet for 60 s

# big, middle and smaller markets (see the 2026-11-27 volume lists)
CORE_PERPS = [
    "BTC", "ETH", "HYPE", "xyz:XYZ100",
    "ZEC", "SOL", "xyz:SKHX", "xyz:SP500", "xyz:SOXL", "xyz:NVDA", "xyz:MU", "xyz:SNDK",
    "xyz:GOOGL", "xyz:BRENTOIL", "xyz:TSLA", "xyz:GOLD", "xyz:SILVER", "xyz:CL", "XMR", "BIGTIME",
]
CORE_SPOT = ["HYPE/USDC", "UBTC/USDC", "UZEC/USDC"]
AUTO_RANKS = (20, 40)  # main-dex perps by 24 h $ volume, 1 = biggest; both ends included

# "likely market maker" on a coin (all must hold)
MIN_MAKER_FILLS = 50
MIN_MAKER_SHARE = 0.80  # of its $ volume on that coin, as maker
MIN_BALANCE = 0.80  # smaller side / bigger side of its maker buys vs sells
MIN_ACTIVE = 0.50  # share of the covered hours with at least one maker fill
MARKOUT_S = (60, 300)
TOP_N = 20


# ---------------- picking markets ----------------

def spot_names(spot_meta):
    """{"HYPE/USDC": "@107", "PURR/USDC": "PURR/USDC", ...}: the name each spot
    pair goes by in the trade feed, from the info API's spotMeta reply."""
    tokens = {t["index"]: t["name"] for t in spot_meta.get("tokens", [])}
    out = {}
    for pair in spot_meta.get("universe", []):
        base, quote = (tokens.get(i) for i in pair.get("tokens", [None, None])[:2])
        if base and quote:
            out[f"{base}/{quote}"] = pair["name"]
    return out


def auto_perps(meta_and_ctxs, ranks=AUTO_RANKS):
    """Main-dex perps ranked ranks[0]..ranks[1] (1 = biggest) by 24 h volume,
    from the info API's metaAndAssetCtxs reply; delisted ones skipped."""
    meta, ctxs = meta_and_ctxs[0], meta_and_ctxs[1]
    rows = []
    for asset, ctx in zip(meta.get("universe", []), ctxs):
        if asset.get("isDelisted"):
            continue
        try:
            vol = float(ctx.get("dayNtlVlm") or 0)
        except (TypeError, ValueError):
            vol = 0.0
        rows.append((-vol, asset["name"]))
    rows.sort()
    lo, hi = ranks
    return [name for _, name in rows[lo - 1:hi]]


def pick_markets(post=http_post, core_perps=None, core_spot=None, ranks=None):
    """[(feed name, label)] to subscribe to, plus notes on anything that failed.
    If the info API can't be reached, the core perps still go ahead.
    (Lists default to the module's, read at call time.)"""
    core_perps = CORE_PERPS if core_perps is None else core_perps
    core_spot = CORE_SPOT if core_spot is None else core_spot
    ranks = AUTO_RANKS if ranks is None else ranks
    picked, notes = [], []
    seen = set()

    def add(name, label, why):
        if name not in seen:
            seen.add(name)
            picked.append({"coin": name, "label": label, "why": why})

    for c in core_perps:
        add(c, c, "core")
    try:
        for c in auto_perps(post({"type": "metaAndAssetCtxs"}), ranks):
            add(c, c, f"volume rank {ranks[0]}-{ranks[1]}")
    except Exception as e:  # noqa: BLE001 - carry on with what we have
        notes.append(f"volume ranking unavailable: {e!r}")
    if core_spot:
        try:
            names = spot_names(post({"type": "spotMeta"}))
            for pair in core_spot:
                if pair in names:
                    add(names[pair], pair, "core spot")
                else:
                    notes.append(f"spot pair {pair} not found")
        except Exception as e:  # noqa: BLE001
            notes.append(f"spot names unavailable: {e!r}")
    return picked, notes


def subscribe_messages(coins):
    return [json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": c}},
                       separators=(",", ":")) for c in coins]


class HlClient(WsClient):
    """The trade-feed connection; sends a ping every PING_EVERY_S so a quiet
    spell never gets it closed."""

    def __init__(self, coins, log, host=WS_HOST, port=443, path=WS_PATH, use_tls=True, **kw):
        super().__init__(CONN, host, port, path, subscribe_messages(coins), log, use_tls=use_tls, **kw)
        self._last_ping = None

    def on_open(self, now):
        self._last_ping = now
        super().on_open(now)

    def tick(self, now):
        super().tick(now)
        if self.state == "open" and self._last_ping is not None and now - self._last_ping >= PING_EVERY_S:
            self._last_ping = now
            self.send_text('{"method":"ping"}')


# ---------------- recording ----------------

STOPPED_BY_USER = 3  # same as mm-stack's recorder: hlscan.bat stops looping


def record(data_dir, stop_at=None, post=http_post, host=WS_HOST, port=443, use_tls=True,
           should_stop=lambda: False, say=print, status_every=60.0):
    """Record until stop_at (seconds since 1970), should_stop() or Ctrl+C.
    Returns the log path."""
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, "hlscan-" + datetime.now().strftime("%Y%m%d-%H%M%S") + ".jsonl")
    writer = EventWriter(path)
    markets, notes = pick_markets(post)
    writer.write(CONN, "session", {"markets": markets, "notes": notes})
    for n in notes:
        say(f"note: {n}")
    say(f"logging to {path}")
    say(f"{len(markets)} markets: " + ", ".join(m["label"] for m in markets))
    client = HlClient([m["coin"] for m in markets], writer, host=host, port=port, use_tls=use_tls)
    reactor = Reactor([client])
    stop = threading.Event()
    net = threading.Thread(target=reactor.run, args=(stop,), name="hlscan-net", daemon=True)
    net.start()
    next_status = time.time() + status_every
    try:
        while not (should_stop() or (stop_at is not None and time.time() >= stop_at) or not net.is_alive()):
            time.sleep(0.2)
            if time.time() >= next_status:
                next_status = time.time() + status_every
                size = os.path.getsize(path) / 1e6
                say(f"[{datetime.now().strftime('%H:%M:%S')}] {client.state}, log {size:,.0f} MB")
    finally:
        stop.set()
        net.join(timeout=10)
        writer.close()
    return path


# ---------------- report ----------------

class _Stats:
    __slots__ = ("maker_n", "maker_usd", "maker_buy", "maker_sell", "taker_n", "taker_usd",
                 "net_sz", "hours", "mo_sum", "mo_n")

    def __init__(self):
        self.maker_n = self.taker_n = 0
        self.maker_usd = self.maker_buy = self.maker_sell = self.taker_usd = 0.0
        self.net_sz = 0.0
        self.hours = set()
        self.mo_sum = [0.0] * len(MARKOUT_S)
        self.mo_n = [0] * len(MARKOUT_S)


def _f(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def scan(events):
    """Read hlscan events ONCE (any iterable, e.g. a stream from files) and
    return the tallies. Memory grows with accounts x coins, not with trades."""
    stats = defaultdict(_Stats)  # (address, coin) -> _Stats
    labels = {}  # feed name -> label (spot "@107" -> "HYPE/USDC")
    coin_info = defaultdict(lambda: {"trades": 0, "usd": 0.0, "maker_usd": defaultdict(float)})
    last_seen = {}  # coin -> (time ms, set of tids at that time): drops the feed's repeats
    last_px = {}  # coin -> last trade price
    pending = defaultdict(deque)  # (coin, horizon index) -> (due ms, key, sign, px), in due order
    first_t = last_t = None
    repeats = 0
    files = 0
    for ev in events:
        if ev.get("conn") != CONN:
            continue
        if ev["type"] == "session":
            files += 1
            for m in ev["data"].get("markets", []):
                labels[m["coin"]] = m["label"]
            continue
        if ev["type"] != "ws_in":
            continue
        try:
            msg = json.loads(ev["data"]["text"])
        except (ValueError, KeyError, TypeError):
            continue
        if not isinstance(msg, dict) or msg.get("channel") != "trades":
            continue
        for tr in msg.get("data") or []:
            coin, t, tid = tr.get("coin"), tr.get("time"), tr.get("tid")
            px, sz = _f(tr.get("px")), _f(tr.get("sz"))
            users = tr.get("users") or []
            if coin is None or t is None or px is None or sz is None or len(users) != 2:
                continue
            seen = last_seen.get(coin)
            if seen is not None and (t < seen[0] or (t == seen[0] and tid in seen[1])):
                repeats += 1
                continue
            if seen is None or t > seen[0]:
                last_seen[coin] = (t, {tid})
            else:
                seen[1].add(tid)
            # markouts that came due before this trade: priced at the last trade before it
            for h in range(len(MARKOUT_S)):
                q = pending.get((coin, h))
                while q and q[0][0] <= t:
                    _, key, sign, fpx = q.popleft()
                    s = stats[key]
                    s.mo_sum[h] += sign * (last_px[coin] - fpx) / fpx * 10000
                    s.mo_n[h] += 1
            last_px[coin] = px
            first_t = t if first_t is None else min(first_t, t)
            last_t = t if last_t is None else max(last_t, t)

            buyer, seller = users
            side = tr.get("side")
            if side == "B":
                taker, maker, maker_buys = buyer, seller, False
            elif side == "A":
                taker, maker, maker_buys = seller, buyer, True
            else:
                continue
            usd = px * sz
            ci = coin_info[coin]
            ci["trades"] += 1
            ci["usd"] += usd
            ci["maker_usd"][maker] += usd

            m = stats[(maker, coin)]
            m.maker_n += 1
            m.maker_usd += usd
            if maker_buys:
                m.maker_buy += usd
            else:
                m.maker_sell += usd
            m.net_sz += sz if maker_buys else -sz
            m.hours.add(t // 3_600_000)
            sign = 1 if maker_buys else -1
            for h, secs in enumerate(MARKOUT_S):
                pending[(coin, h)].append((t + secs * 1000, (maker, coin), sign, px))

            k = stats[(taker, coin)]
            k.taker_n += 1
            k.taker_usd += usd
            k.net_sz += -sz if maker_buys else sz
    # anything still pending had no trade after its horizon: no markout for it
    hours = (last_t - first_t) / 3_600_000 if first_t is not None else 0.0
    return {"stats": stats, "labels": labels, "coins": coin_info, "hours": hours,
            "repeats": repeats, "files": files, "last_px": last_px}


def _verdict(s, hours):
    maker_total = s.maker_usd + s.taker_usd
    share = s.maker_usd / maker_total if maker_total else 0.0
    big, small = max(s.maker_buy, s.maker_sell), min(s.maker_buy, s.maker_sell)
    balance = small / big if big else 0.0
    covered = max(1, int(hours) + 1)
    active = min(1.0, len(s.hours) / covered)
    ok = (s.maker_n >= MIN_MAKER_FILLS and share >= MIN_MAKER_SHARE
          and balance >= MIN_BALANCE and active >= MIN_ACTIVE)
    return ok, share, balance, active


def _mo(s, h):
    return f"{s.mo_sum[h] / s.mo_n[h]:+.2f}" if s.mo_n[h] else "n/a"


def report(events, top_n=TOP_N):
    """Text report, as a list of lines. `events` is read once."""
    data = scan(events)
    stats, labels, coins, hours = data["stats"], data["labels"], data["coins"], data["hours"]
    lab = lambda c: labels.get(c, c)  # noqa: E731
    total_trades = sum(ci["trades"] for ci in coins.values())
    lines = [f"hlscan {total_trades:,} trades on {len(coins)} markets over {hours:.1f} hours "
             f"({data['files']} file(s), {data['repeats']:,} repeated trades dropped)",
             f"hlscan likely market maker on a coin = {MIN_MAKER_FILLS}+ maker fills, "
             f"{MIN_MAKER_SHARE:.0%}+ of its $ as maker, buys/sells within {1 - MIN_BALANCE:.0%}, "
             f"maker fills in {MIN_ACTIVE:.0%}+ of the hours"]

    flagged = []  # (maker $, address, coin, share, balance, active, stats)
    per_coin_flags = defaultdict(int)
    for (addr, coin), s in stats.items():
        if not s.maker_n:
            continue
        ok, share, balance, active = _verdict(s, hours)
        if ok:
            flagged.append((s.maker_usd, addr, coin, share, balance, active, s))
            per_coin_flags[coin] += 1

    lines.append("hlscan markets (by $ volume):")
    for coin in sorted(coins, key=lambda c: -coins[c]["usd"]):
        ci = coins[coin]
        makers = sorted(ci["maker_usd"].values(), reverse=True)
        top5 = sum(makers[:5]) / ci["usd"] * 100 if ci["usd"] else 0.0
        lines.append(f"hlscan   {lab(coin)}: {ci['trades']:,} trades, ${ci['usd']:,.0f}, "
                     f"{len(makers):,} makers, top 5 makers {top5:.0f}% of volume, "
                     f"{per_coin_flags[coin]} likely market maker(s)")

    lines.append(f"hlscan likely market makers, top {top_n} by maker $ "
                 f"(markout = trade price later vs fill, bps, + = good for the maker):")
    flagged.sort(key=lambda r: -r[0])
    for usd, addr, coin, share, balance, active, s in flagged[:top_n]:
        coin_share = usd / coins[coin]["usd"] * 100 if coins[coin]["usd"] else 0.0
        net_usd = s.net_sz * data["last_px"].get(coin, 0.0)
        lines.append(f"hlscan   {addr} {lab(coin)}: maker ${usd:,.0f} ({coin_share:.1f}% of the coin), "
                     f"{s.maker_n:,} fills, maker {share:.0%}, balance {balance:.2f}, "
                     f"active {active:.0%}, markout {MARKOUT_S[0]}s {_mo(s, 0)} "
                     f"{MARKOUT_S[1]}s {_mo(s, 1)} (n={s.mo_n[0]}), net change ${net_usd:+,.0f}")
    if not flagged:
        lines.append("hlscan   none yet")

    by_addr = defaultdict(list)
    for usd, addr, coin, *_ in flagged:
        by_addr[addr].append((usd, coin))
    multi = sorted(((sum(u for u, _ in v), a, v) for a, v in by_addr.items() if len(v) > 1), reverse=True)
    lines.append("hlscan accounts making markets on 2+ coins:")
    for total, addr, v in multi[:top_n]:
        lines.append(f"hlscan   {addr}: ${total:,.0f} as maker on "
                     + ", ".join(f"{lab(c)} (${u:,.0f})" for u, c in sorted(v, reverse=True)))
    if not multi:
        lines.append("hlscan   none yet")
    return lines


def _paths(target):
    if os.path.isdir(target):
        names = sorted(n for n in os.listdir(target)
                       if n.startswith("hlscan-") and (n.endswith(".jsonl") or n.endswith(".jsonl.gz")))
        return [os.path.join(target, n) for n in names]
    return [target]


def stream_files(paths):
    """Events from several logs in order, one batch at a time."""
    for p in paths:
        tailer = EventTailer(p)
        try:
            while True:
                batch = tailer.read_available()
                if not batch:
                    break
                yield from batch
        finally:
            tailer.close()


def report_paths(target):
    return report(stream_files(_paths(target)))


# ---------------- command line ----------------

def main(argv=None):
    ap = argparse.ArgumentParser(prog="python -m mmstack.hlscan")
    sub = ap.add_subparsers(dest="cmd", required=True)
    rec = sub.add_parser("record", help="record Hyperliquid trades (both accounts on each)")
    rec.add_argument("--data-dir", default=os.path.join("E:\\", "hldata"))
    rec.add_argument("--until-midnight", action="store_true",
                     help="stop at the next New York midnight (hlscan.bat starts the next file)")
    rep = sub.add_parser("report", help="rank the likely market makers in hlscan logs")
    rep.add_argument("target", help="one log file, or a folder of them")
    rep.add_argument("--out", help="also write the report to this file")
    a = ap.parse_args(argv)
    if a.cmd == "report":
        lines = report_paths(a.target)
        for line in lines:
            print(line)
        if a.out:
            with open(a.out, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
        return 0
    stop_at = None
    if a.until_midnight:
        from .days import new_york
        from .main import next_midnight
        stop_at = next_midnight(time.time(), new_york())
    presses = []
    signal.signal(signal.SIGINT, lambda *_: presses.append(1))
    if hasattr(signal, "SIGBREAK"):
        signal.signal(signal.SIGBREAK, lambda *_: presses.append(1))
    path = record(a.data_dir, stop_at=stop_at, should_stop=lambda: bool(presses))
    print(f"log saved: {path}")
    return STOPPED_BY_USER if presses else 0


if __name__ == "__main__":
    sys.exit(main())
