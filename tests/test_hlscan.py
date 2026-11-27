"""hlscan: who market-makes on Hyperliquid (research tool)."""

import json
import os
import tempfile
import threading
import time
import unittest
from unittest import mock

from wsproto.events import TextMessage

from mmstack import hlscan
from mmstack.eventlog import EventTailer, EventWriter
from tests.test_net import Server, listener

M, T1, T2, X, H = ("0x" + c * 40 for c in "mabxh")


def trade(coin, t_ms, side, px, sz, buyer, seller, tid):
    return {"coin": coin, "side": side, "px": str(px), "sz": str(sz), "time": t_ms,
            "users": [buyer, seller], "tid": tid, "hash": "0x0"}


def feed(*trades):
    """One logged ws_in event carrying a trades message."""
    msg = {"channel": "trades", "data": list(trades)}
    return {"seq": 0, "t": 0, "conn": "hlscan", "type": "ws_in", "data": {"text": json.dumps(msg)}}


def session(markets=()):
    return {"seq": 0, "t": 0, "conn": "hlscan", "type": "session",
            "data": {"markets": [{"coin": c, "label": l, "why": "core"} for c, l in markets], "notes": []}}


class Loose:
    """Small thresholds so hand-worked examples can be flagged."""

    def __enter__(self):
        self.p = mock.patch.multiple(hlscan, MIN_MAKER_FILLS=2, MIN_ACTIVE=0.0)
        self.p.start()

    def __exit__(self, *a):
        self.p.stop()


# ---------------- picking markets ----------------

SPOT_META = {"tokens": [{"name": "USDC", "index": 0}, {"name": "PURR", "index": 1},
                        {"name": "HYPE", "index": 150}, {"name": "UBTC", "index": 197}],
             "universe": [{"name": "PURR/USDC", "tokens": [1, 0], "index": 0},
                          {"name": "@107", "tokens": [150, 0], "index": 107},
                          {"name": "@142", "tokens": [197, 0], "index": 142}]}


def meta_and_ctxs(n=45, delisted=()):
    uni = [{"name": f"C{i}", **({"isDelisted": True} if i in delisted else {})} for i in range(1, n + 1)]
    ctxs = [{"dayNtlVlm": str(1_000_000 * (n + 1 - i))} for i in range(1, n + 1)]  # C1 biggest
    return [{"universe": uni}, ctxs]


class TestMarkets(unittest.TestCase):
    def test_spot_names(self):
        self.assertEqual(hlscan.spot_names(SPOT_META),
                         {"PURR/USDC": "PURR/USDC", "HYPE/USDC": "@107", "UBTC/USDC": "@142"})

    def test_ranks_20_to_40_by_volume(self):
        self.assertEqual(hlscan.auto_perps(meta_and_ctxs()), [f"C{i}" for i in range(20, 41)])

    def test_delisted_skipped(self):
        got = hlscan.auto_perps(meta_and_ctxs(delisted=(5,)))
        self.assertEqual(got, [f"C{i}" for i in range(21, 42)])  # everything moves up one

    def test_pick_core_then_auto_then_spot_without_repeats(self):
        def post(body):
            return {"metaAndAssetCtxs": meta_and_ctxs(), "spotMeta": SPOT_META}[body["type"]]
        picked, notes = hlscan.pick_markets(post, core_perps=["BTC", "C25"], core_spot=["HYPE/USDC", "NOPE/USDC"])
        coins = [m["coin"] for m in picked]
        self.assertEqual(coins[:2], ["BTC", "C25"])
        self.assertEqual(coins.count("C25"), 1)  # core and ranked: listed once
        self.assertEqual(len(coins), 2 + 20 + 1)
        self.assertEqual(picked[-1], {"coin": "@107", "label": "HYPE/USDC", "why": "core spot"})
        self.assertEqual(notes, ["spot pair NOPE/USDC not found"])

    def test_api_down_core_perps_still_go(self):
        def post(body):
            raise OSError("down")
        picked, notes = hlscan.pick_markets(post, core_perps=["BTC", "ETH"])
        self.assertEqual([m["coin"] for m in picked], ["BTC", "ETH"])
        self.assertEqual(len(notes), 2)

    def test_subscribe_messages(self):
        self.assertEqual(hlscan.subscribe_messages(["BTC", "xyz:XYZ100"]),
                         ['{"method":"subscribe","subscription":{"type":"trades","coin":"BTC"}}',
                          '{"method":"subscribe","subscription":{"type":"trades","coin":"xyz:XYZ100"}}'])

    def test_default_lists(self):
        self.assertIn("xyz:XYZ100", hlscan.CORE_PERPS)
        self.assertIn("ZEC", hlscan.CORE_PERPS)
        self.assertEqual(hlscan.CORE_SPOT, ["HYPE/USDC", "UBTC/USDC", "UZEC/USDC"])
        self.assertEqual(hlscan.AUTO_RANKS, (20, 40))


class TestPing(unittest.TestCase):
    def test_ping_every_50s_while_open(self):
        sent = []
        c = hlscan.HlClient(["BTC"], log=lambda *a: None)
        c.send_text = sent.append
        c.state = "open"
        c._last_ping = 100.0
        with mock.patch.object(hlscan.WsClient, "tick"):
            c.tick(149.0)
            self.assertEqual(sent, [])
            c.tick(150.0)
            c.tick(160.0)
        self.assertEqual(sent, ['{"method":"ping"}'])

    def test_no_ping_when_not_open(self):
        sent = []
        c = hlscan.HlClient(["BTC"], log=lambda *a: None)
        c.send_text = sent.append
        c.state = "connecting"
        c._last_ping = 0.0
        with mock.patch.object(hlscan.WsClient, "tick"):
            c.tick(500.0)
        self.assertEqual(sent, [])


# ---------------- report ----------------

class TestScan(unittest.TestCase):
    def test_maker_and_taker_from_side_and_users(self):
        """users = [buyer, seller]; side B = the buyer took, side A = the seller took."""
        d = hlscan.scan([feed(trade("ZEC", 0, "A", 100, 1, M, T1, 1),      # M bought as maker
                              trade("ZEC", 1000, "B", 101, 2, T2, M, 2))])  # M sold as maker
        m = d["stats"][(M, "ZEC")]
        self.assertEqual((m.maker_n, m.maker_buy, m.maker_sell, m.taker_n), (2, 100.0, 202.0, 0))
        self.assertEqual(m.net_sz, -1.0)  # +1 bought, -2 sold
        self.assertEqual((d["stats"][(T1, "ZEC")].taker_n, d["stats"][(T2, "ZEC")].taker_usd), (1, 202.0))
        self.assertEqual(d["stats"][(T1, "ZEC")].net_sz, -1.0)  # T1 sold to M
        self.assertEqual(d["stats"][(T2, "ZEC")].net_sz, 2.0)

    def test_markouts_hand_worked(self):
        """M buys @100 at 0 s and sells @101 at 1 s. At 61 s the last trade price
        before the next trade is 101: the buy is +100 bps, the sell 0 -> avg +50."""
        d = hlscan.scan([feed(trade("ZEC", 0, "A", 100, 1, M, T1, 1),
                              trade("ZEC", 1000, "B", 101, 1, T2, M, 2),
                              trade("ZEC", 61000, "B", 102, 1, T1, X, 3))])
        m = d["stats"][(M, "ZEC")]
        self.assertEqual(m.mo_n, [2, 0])  # nothing 300 s later yet
        self.assertAlmostEqual(m.mo_sum[0] / m.mo_n[0], 50.0)

    def test_300s_markout_not_blocked_by_60s_queue(self):
        d = hlscan.scan([feed(trade("ZEC", 0, "A", 100, 1, M, T1, 1),
                              trade("ZEC", 250_000, "A", 100, 1, M, T1, 2),
                              trade("ZEC", 301_000, "B", 99, 1, T1, X, 3))])
        m = d["stats"][(M, "ZEC")]
        self.assertEqual(m.mo_n, [1, 1])  # 1st fill: both due by 301 s; 2nd: neither (60 s due at 310 s)

    def test_feed_repeats_dropped(self):
        a = trade("ZEC", 5000, "A", 100, 1, M, T1, 1)
        b = trade("ZEC", 5000, "B", 100, 1, T2, M, 2)
        c = trade("ZEC", 6000, "A", 100, 1, M, T1, 3)
        old = trade("ZEC", 4000, "A", 100, 1, M, T1, 0)
        d = hlscan.scan([feed(a, b), feed(c), feed(old, a, b, c)])  # a re-subscribe repeats them
        self.assertEqual(d["stats"][(M, "ZEC")].maker_n, 3)
        self.assertEqual(d["repeats"], 4)

    def test_same_millisecond_new_trade_kept(self):
        d = hlscan.scan([feed(trade("ZEC", 5000, "A", 100, 1, M, T1, 1)),
                         feed(trade("ZEC", 5000, "A", 100, 1, M, T1, 2))])
        self.assertEqual(d["stats"][(M, "ZEC")].maker_n, 2)

    def test_other_events_ignored(self):
        evs = [{"seq": 0, "t": 0, "conn": "kraken", "type": "ws_in", "data": {"text": "{}"}},
               {"seq": 0, "t": 0, "conn": "hlscan", "type": "ws_in", "data": {"text": '{"channel":"pong"}'}},
               {"seq": 0, "t": 0, "conn": "hlscan", "type": "ws_in", "data": {"text": "not json"}}]
        self.assertEqual(len(hlscan.scan(evs)["stats"]), 0)

    def test_reads_a_one_pass_stream(self):
        evs = [feed(trade("ZEC", 0, "A", 100, 1, M, T1, 1))]
        self.assertEqual(hlscan.scan(e for e in evs)["stats"][(M, "ZEC")].maker_n, 1)


def balanced_maker(coin, n, t0=0, step=1000, who=M, tid0=0):
    """`who` makes n fills on coin, alternating buy and sell at 100."""
    out = []
    for i in range(n):
        if i % 2 == 0:
            out.append(trade(coin, t0 + i * step, "A", 100, 1, who, T1, tid0 + i))  # who buys
        else:
            out.append(trade(coin, t0 + i * step, "B", 100, 1, T2, who, tid0 + i))  # who sells
    return out


class TestReport(unittest.TestCase):
    def text(self, evs):
        with Loose():
            return "\n".join(hlscan.report(evs))

    def test_balanced_maker_is_flagged(self):
        text = self.text([feed(*balanced_maker("ZEC", 10))])
        self.assertIn(f"{M} ZEC: maker $1,000 (100.0% of the coin), 10 fills, maker 100%, balance 1.00", text)

    def test_one_sided_maker_not_flagged(self):
        trades = [trade("ZEC", i * 1000, "A", 100, 1, M, T1, i) for i in range(10)]  # only buys
        text = self.text([feed(*trades)])
        self.assertNotIn(f"{M} ZEC", text)
        self.assertIn("ZEC: 10 trades, $1,000, 1 makers, top 5 makers 100% of volume, 0 likely", text)

    def test_mostly_taker_not_flagged(self):
        trades = balanced_maker("ZEC", 4)
        trades += [trade("ZEC", 10_000 + i, "B", 100, 1, M, X, 100 + i) for i in range(5)]  # M takes 5x
        self.assertNotIn(f"{M} ZEC", self.text([feed(*trades)]))  # 4 of 9 as maker = 44%

    def test_hybrid_account_flagged_per_coin(self):
        """Like the account hlwatch followed: taker on one coin, market maker on
        another. Judged per coin, the market making still shows."""
        taking = [trade("xyz:SOXL", i * 1000, "B", 36, 100, H, X, 500 + i) for i in range(30)]
        making = balanced_maker("ZEC", 10, who=H)
        text = self.text([feed(*taking), feed(*making)])
        self.assertIn(f"{H} ZEC:", text)
        self.assertNotIn(f"{H} xyz:SOXL:", text)

    def test_active_hours_needed(self):
        trades = balanced_maker("ZEC", 10)  # all within the first hour
        trades.append(trade("ZEC", 10 * 3_600_000, "B", 100, 1, T1, X, 999))  # data runs 10 h
        with mock.patch.multiple(hlscan, MIN_MAKER_FILLS=2):  # MIN_ACTIVE stays 50%
            text = "\n".join(hlscan.report([feed(*trades)]))
        self.assertNotIn(f"{M} ZEC", text)

    def test_spot_label_used(self):
        text = self.text([session([("@107", "HYPE/USDC")]), feed(*balanced_maker("@107", 4))])
        self.assertIn(f"{M} HYPE/USDC:", text)
        self.assertIn("HYPE/USDC: 4 trades", text)

    def test_multi_coin_makers_listed(self):
        text = self.text([feed(*balanced_maker("ZEC", 4)), feed(*balanced_maker("SOL", 6, tid0=100))])
        self.assertIn(f"{M}: $1,000 as maker on SOL ($600), ZEC ($400)", text)

    def test_header_and_thresholds_line(self):
        lines = hlscan.report([session(), feed(*balanced_maker("ZEC", 3))])
        self.assertTrue(lines[0].startswith("hlscan 3 trades on 1 markets over 0.0 hours (1 file(s), 0 repeated"))
        self.assertIn("50+ maker fills, 80%+ of its $ as maker, buys/sells within 20%", lines[1])

    def test_nothing_yet(self):
        self.assertIn("hlscan   none yet", hlscan.report([]))


class TestFiles(unittest.TestCase):
    def test_report_on_a_folder_streams_files_in_order(self):
        with tempfile.TemporaryDirectory() as d:
            for name, trades in (("hlscan-20261128-000001.jsonl", balanced_maker("ZEC", 4)),
                                 ("hlscan-20261129-000001.jsonl", balanced_maker("ZEC", 4, t0=86_400_000, tid0=50))):
                w = EventWriter(os.path.join(d, name))
                w.write("hlscan", "session", {"markets": [], "notes": []})
                w.write("hlscan", "ws_in", {"text": json.dumps({"channel": "trades", "data": trades})})
                w.close()
            w = EventWriter(os.path.join(d, "other.jsonl"))  # not an hlscan file: must be ignored
            w.write("hlscan", "session", {"markets": [], "notes": []})
            w.close()
            with Loose():
                text = "\n".join(hlscan.report_paths(d))
        self.assertIn("8 trades on 1 markets over 24.0 hours (2 file(s)", text)


class TestRecord(unittest.TestCase):
    def test_records_trades_from_the_feed(self):
        lst = listener()
        port = lst.getsockname()[1]
        got = []

        def server_main():
            s = Server(lst)
            s.accept_handshake()
            deadline = time.time() + 5
            while len(got) < 2 and time.time() < deadline:
                ev = s.next_event()
                if isinstance(ev, TextMessage):
                    got.append(json.loads(ev.data))
            s.send(TextMessage(data=json.dumps({"channel": "trades", "data": [
                trade("BTC", 1, "B", 60000, 0.01, T1, M, 7)]})))
            time.sleep(2)
            s.close()
        threading.Thread(target=server_main, daemon=True).start()

        def post(body):
            raise OSError("offline test")
        with tempfile.TemporaryDirectory() as d:
            with mock.patch.multiple(hlscan, CORE_PERPS=["BTC", "xyz:XYZ100"]):
                started = time.time()
                path = hlscan.record(d, post=post, host="localhost", port=port, use_tls=False,
                                     should_stop=lambda: time.time() - started > 1.5, say=lambda *a: None)
            t = EventTailer(path)
            evs = []
            while True:
                b = t.read_available()
                if not b:
                    break
                evs += b
            t.close()
        self.assertEqual([m["subscription"]["coin"] for m in got], ["BTC", "xyz:XYZ100"])
        sess = [e for e in evs if e["type"] == "session"][0]["data"]
        self.assertEqual([m["coin"] for m in sess["markets"]], ["BTC", "xyz:XYZ100"])
        self.assertEqual(len(sess["notes"]), 2)  # the API was "down": noted, carried on
        ins = [e for e in evs if e["type"] == "ws_in" and '"trades"' in e["data"]["text"]]
        self.assertEqual(len(ins), 1)
        self.assertTrue(os.path.basename(path).startswith("hlscan-"))


if __name__ == "__main__":
    unittest.main()
