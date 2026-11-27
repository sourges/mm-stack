"""hlreport must be fast and lean on multi-GB logs, and give EXACTLY the
same report as before (2026-11-27: the old version re-sorted a coin's whole
price history for every poll x coin, and held every raw event in memory; a
1.87 GB log ran for over an hour without finishing).

old_report below is a frozen copy of the report before the change: the
reference the new one must match line for line."""

import contextlib
import io
import os
import random
import tempfile
import time
import tracemalloc
import unittest
from collections import defaultdict

from mmstack import hlwatch
from mmstack.eventlog import EventWriter
from mmstack.hlwatch import _f, _median, mid_at
from mmstack.main import main


def old_load(events):
    out = defaultdict(list)
    for ev in events:
        if ev.get("conn") == "hlwatch":
            out[ev["type"]].append(ev)
    return out


def old_report(events, markout_s=(60, 300)):
    """Text report, as a list of lines."""
    data = old_load(events)
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
    dex_vals = defaultdict(list)  # HIP-3 dexes keep separate margin accounts
    for ev in data["positions"]:
        for dex, margin in (ev["data"].get("dex_margin") or {}).items():
            v = _f((margin or {}).get("accountValue"))
            if v is not None:
                dex_vals[dex].append(v)
    for dex in sorted(dex_vals):
        vs = dex_vals[dex]
        lines.append(f"hlreport account value on {dex}: first ${vs[0]:,.0f}, last ${vs[-1]:,.0f}, "
                     f"change ${vs[-1] - vs[0]:+,.0f}")
    for ev in data["dex"]:
        lines.append(f"hlreport dex {ev['data']['dex']} added: {ev['data']['why']}")
    return lines


SEC = 1_000_000_000


def random_log(polls, coins=("BTC", "ETH", "xyz:XYZ100", "SOL"), seed=7):
    """A realistic-looking hlwatch log: each poll has orders, positions
    (with a dex margin), and mids; fills now and then; a dex event; errors."""
    rnd = random.Random(seed)
    px = {c: 100.0 + 50 * i for i, c in enumerate(coins)}
    evs = [{"seq": 1, "t": 0, "conn": "hlwatch", "type": "session", "data": {"address": "0x" + "a" * 40}}]
    t = 0
    fid = 0
    for p in range(polls):
        t += 5 * SEC + rnd.randint(-SEC // 10, SEC // 10)
        orders = []
        for c in coins:
            if rnd.random() < 0.8:
                for side in "BA":
                    for _ in range(rnd.randint(0, 3)):
                        off = rnd.uniform(0.0005, 0.003) * px[c]
                        orders.append({"coin": c, "side": side,
                                       "limitPx": f"{px[c] - off if side == 'B' else px[c] + off:.4f}",
                                       "sz": f"{rnd.uniform(0.1, 3):.3f}", "oid": rnd.randint(1, 10**9)})
        evs.append({"seq": 0, "t": t, "conn": "hlwatch", "type": "orders", "data": {"orders": orders}})
        aps = []
        for c in coins:
            if rnd.random() < 0.6:
                szi = rnd.uniform(-2, 2)
                aps.append({"position": {"coin": c, "szi": f"{szi:.4f}",
                                         "positionValue": f"{abs(szi) * px[c]:.2f}"}})
        evs.append({"seq": 0, "t": t + 1000, "conn": "hlwatch", "type": "positions",
                    "data": {"assetPositions": aps,
                             "marginSummary": {"accountValue": f"{10000 + rnd.uniform(-50, 50):.2f}"},
                             "dex_margin": {"xyz": {"accountValue": f"{500 + rnd.uniform(-5, 5):.2f}"}}}})
        if p % 7 == 3:
            fs = []
            for _ in range(rnd.randint(1, 4)):
                fid += 1
                c = rnd.choice(coins)
                fs.append({"coin": c, "side": rnd.choice("BA"), "px": f"{px[c]:.4f}",
                           "sz": f"{rnd.uniform(0.1, 2):.3f}", "crossed": rnd.random() < 0.3,
                           "fee": f"{rnd.uniform(-0.01, 0.02):.5f}", "closedPnl": f"{rnd.uniform(-1, 1):.4f}",
                           "time": (t - rnd.randint(0, 4) * SEC) // 1_000_000, "tid": fid, "oid": fid})
            evs.append({"seq": 0, "t": t + 2000, "conn": "hlwatch", "type": "fills",
                        "data": {"fills": fs, "returned": len(fs)}})
        if p == polls // 3:
            evs.append({"seq": 0, "t": t + 2500, "conn": "hlwatch", "type": "dex",
                        "data": {"dex": "xyz", "why": "fill in xyz:XYZ100"}})
        if p % 97 == 50:
            evs.append({"seq": 0, "t": t + 2600, "conn": "hlwatch", "type": "error",
                        "data": {"request": "orders", "error": "timeout"}})
        for c in coins:
            px[c] *= 1 + rnd.gauss(0, 0.0005)
        mt = t + 3000
        if p % 211 == 100:
            mt -= 7 * SEC  # a clock step: this mid is stamped before the previous ones
        evs.append({"seq": 0, "t": mt, "conn": "hlwatch", "type": "mids",
                    "data": {c: f"{px[c]:.4f}" for c in coins if rnd.random() < 0.95}})
    return evs


class TestSameReport(unittest.TestCase):
    def test_identical_to_the_old_report(self):
        for seed in (1, 2, 3):
            evs = random_log(1500, seed=seed)
            with self.subTest(seed=seed):
                self.assertEqual(hlwatch.report(iter(evs)), old_report(evs))

    def test_identical_on_tiny_and_empty_logs(self):
        for evs in ([], random_log(1), random_log(2), random_log(8)):
            self.assertEqual(hlwatch.report(iter(evs)), old_report(evs))

    def test_accepts_a_one_pass_stream(self):
        """run_hlreport feeds a generator: the report must read it only once."""
        evs = random_log(300)
        self.assertEqual(hlwatch.report(e for e in evs), old_report(evs))


class TestFastAndLean(unittest.TestCase):
    def test_many_polls_quickly(self):
        evs = random_log(20000)  # ~28 h of 5 s polls
        t0 = time.perf_counter()
        hlwatch.report(iter(evs))
        self.assertLess(time.perf_counter() - t0, 15)  # the old one: many minutes

    def test_keeps_only_small_pieces(self):
        """Reading a stream (as from the file), peak memory must stay far
        below what holding every decoded event costs."""
        import json
        lines = [json.dumps(e) for e in random_log(3000)]

        def stream():
            for line in lines:
                yield json.loads(line)  # a fresh dict each time, like the file reader
        tracemalloc.start()
        everything = [json.loads(line) for line in lines]
        held, _ = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        del everything
        tracemalloc.start()
        hlwatch.report(stream())
        _, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        self.assertLess(peak, held / 3)


class TestCommand(unittest.TestCase):
    def test_report_file_hands_the_report_a_stream_not_a_list(self):
        seen = {}
        real = hlwatch.report

        def spy(events):
            seen["type"] = type(events)
            return real(events)
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "hl.jsonl")
            w = EventWriter(path)
            w.write("hlwatch", "orders", {"orders": []})
            w.close()
            hlwatch.report = spy
            try:
                hlwatch.report_file(path)
            finally:
                hlwatch.report = real
        self.assertNotIn(seen["type"], (list, tuple))

    def test_command_line_entry(self):
        import subprocess
        import sys
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "hl.jsonl")
            out = os.path.join(d, "r.txt")
            evs = random_log(50)
            w = EventWriter(path)
            for e in evs:
                w.write(e["conn"], e["type"], e["data"])
            w.close()
            root = os.path.dirname(os.path.dirname(os.path.abspath(hlwatch.__file__)))
            r = subprocess.run([sys.executable, "-m", "mmstack.hlwatch", "report", path, "--out", out],
                               cwd=root, capture_output=True, text=True, timeout=120)
            self.assertEqual(r.returncode, 0, r.stderr)
            with open(out, encoding="utf-8") as f:
                self.assertEqual(f.read().splitlines(), r.stdout.splitlines())
            self.assertEqual(r.stdout.splitlines(), hlwatch.report_file(path))

    def test_hlreport_streams_the_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "hl.jsonl")
            evs = random_log(200)
            w = EventWriter(path)
            for e in evs:
                w.write(e["conn"], e["type"], e["data"])
            w.close()
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["hlreport", path])
            from mmstack.eventlog import EventTailer
            t = EventTailer(path)
            logged = []
            while True:
                b = t.read_available()
                if not b:
                    break
                logged += b
            t.close()
            self.assertEqual(buf.getvalue().splitlines(), old_report(logged))


if __name__ == "__main__":
    unittest.main()
