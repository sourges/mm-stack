"""Research tool: watching one Hyperliquid account's public data."""

import contextlib
import io
import os
import tempfile
import unittest

from mmstack import hlwatch
from mmstack.eventlog import EventWriter
from mmstack.main import main

ADDR = "0xC42ec9d9156d9196f9a3887380ccfaaa518e4c8d"
SEC = 1_000_000_000


class FakeWriter:
    def __init__(self):
        self.events = []
        self.t = 1_800_000_000 * SEC

    def write(self, conn, type_, data=None):
        ev = {"seq": len(self.events) + 1, "t": self.t, "conn": conn, "type": type_, "data": data or {}}
        self.events.append(ev)
        return ev

    def of(self, type_):
        return [e for e in self.events if e["type"] == type_]


def order(coin, side, px, sz):
    return {"coin": coin, "side": side, "limitPx": px, "sz": sz, "oid": 1, "timestamp": 0}


def fill(tid, coin="BTC", side="B", px="100", sz="1", crossed=False, fee="-0.001", pnl="0", time_ms=0):
    return {"coin": coin, "side": side, "px": px, "sz": sz, "tid": tid, "oid": tid, "time": time_ms,
            "crossed": crossed, "fee": fee, "closedPnl": pnl}


def state(*positions, value="1000"):
    return {"assetPositions": [{"type": "oneWay", "position": {"coin": c, "szi": szi, "positionValue": v}}
                               for c, szi, v in positions],
            "marginSummary": {"accountValue": value}}


class FakeApi:
    def __init__(self):
        self.orders = []
        self.state = state()
        self.fills = []
        self.mids = {"BTC": "100", "ETH": "10", "DOGE": "0.1"}
        self.calls = []
        self.fail = set()

    def __call__(self, body):
        self.calls.append(body["type"])
        if body["type"] in self.fail:
            raise OSError("connection reset")
        return {"frontendOpenOrders": self.orders, "clearinghouseState": self.state,
                "userFills": self.fills, "allMids": self.mids}[body["type"]]


class TestAddress(unittest.TestCase):
    def test_valid_is_lowercased(self):
        self.assertEqual(hlwatch.check_address(ADDR), ADDR.lower())

    def test_invalid(self):
        for bad in ("0x123", "c42ec9d9156d9196f9a3887380ccfaaa518e4c8d", ADDR + "0", "0xZZ" + ADDR[4:]):
            with self.assertRaises(ValueError):
                hlwatch.check_address(bad)


class TestPolling(unittest.TestCase):
    def setUp(self):
        self.api = FakeApi()
        self.w = FakeWriter()
        self.watch = hlwatch.Watcher(ADDR, self.w, post=self.api, fills_every=3)

    def test_requests_name_the_account(self):
        bodies = []
        self.watch.post = lambda body: bodies.append(body) or self.api(body)
        self.watch.poll()
        self.assertEqual([b.get("user") for b in bodies], [ADDR.lower()] * 3 + [None])

    def test_first_poll_logs_everything(self):
        self.api.orders = [order("BTC", "B", "99.9", "1")]
        self.api.fills = [fill(1)]
        status = self.watch.poll()
        self.assertEqual(self.api.calls, ["frontendOpenOrders", "clearinghouseState", "userFills", "allMids"])
        self.assertEqual([e["type"] for e in self.w.events], ["orders", "positions", "fills", "mids"])
        self.assertIn("1 open orders, 1 new fills", status)

    def test_fills_only_every_n_polls(self):
        for _ in range(6):
            self.watch.poll()
        self.assertEqual(self.api.calls.count("userFills"), 3)  # polls 1, 3, 6

    def test_only_new_fills_are_logged(self):
        self.api.fills = [fill(1), fill(2)]
        self.watch.poll()
        self.api.fills = [fill(3), fill(1), fill(2)]
        self.watch.poll()
        self.watch.poll()  # fills checked again
        logged = [f["tid"] for e in self.w.of("fills") for f in e["data"]["fills"]]
        self.assertEqual(logged, [1, 2, 3])
        self.assertEqual(self.w.of("fills")[-1]["data"]["returned"], 3)

    def test_two_fills_of_one_order_are_both_new(self):
        a, b = fill(1), fill(2)
        b["oid"] = a["oid"]  # one order filled in two pieces, seen a minute apart
        self.api.fills = [a]
        self.watch.poll()
        self.api.fills = [b, a]
        self.watch.poll()
        self.watch.poll()
        logged = [f["tid"] for e in self.w.of("fills") for f in e["data"]["fills"]]
        self.assertEqual(logged, [1, 2])

    def test_no_fills_event_when_nothing_new(self):
        self.api.fills = [fill(1)]
        self.watch.poll()
        self.watch.poll()
        self.watch.poll()
        self.assertEqual(len(self.w.of("fills")), 1)

    def test_mids_only_for_coins_seen(self):
        self.api.orders = [order("BTC", "B", "99", "1")]
        self.api.state = state(("ETH", "-2", "20"))
        self.watch.poll()
        self.assertEqual(self.w.of("mids")[-1]["data"], {"BTC": "100", "ETH": "10"})

    def test_coins_from_fills_count(self):
        self.api.fills = [fill(1, coin="DOGE")]
        self.watch.poll()
        self.assertEqual(self.w.of("mids")[-1]["data"], {"DOGE": "0.1"})

    def test_no_mids_before_any_coin(self):
        self.watch.poll()
        self.assertEqual(self.w.of("mids"), [])

    def test_errors_are_logged_and_polling_goes_on(self):
        self.api.fail = {"frontendOpenOrders"}
        status = self.watch.poll()
        [err] = self.w.of("error")
        self.assertEqual(err["data"]["request"], "orders")
        self.assertIn("connection reset", err["data"]["error"])
        self.assertIn("? open orders", status)
        self.assertEqual(len(self.w.of("positions")), 1)
        self.api.fail = set()
        self.watch.poll()
        self.assertEqual(len(self.w.of("orders")), 1)
        self.assertEqual(self.watch.errors, 1)

    def test_failed_fill_check_is_retried_next_time(self):
        self.api.fills = [fill(1)]
        self.api.fail = {"userFills"}
        self.watch.poll()
        self.api.fail = set()
        self.watch.poll()
        self.watch.poll()
        self.assertEqual(len(self.w.of("fills")), 1)


class TestRun(unittest.TestCase):
    def test_paced_polling(self):
        api, w = FakeApi(), FakeWriter()
        watch = hlwatch.Watcher(ADDR, w, post=api)
        now = [0.0]
        slept = []

        def clock():
            return now[0]

        def sleep(s):
            slept.append(s)
            now[0] += s
        polls = []
        orig = watch.poll

        def poll():
            now[0] += 1.5  # each poll takes 1.5 s
            polls.append(1)
            return orig()
        watch.poll = poll
        hlwatch.run(watch, 5, clock=clock, sleep=sleep, should_stop=lambda: len(polls) >= 3, say=lambda s: None)
        self.assertEqual(slept, [3.5, 3.5, 3.5])

    def test_slow_poll_does_not_sleep(self):
        api, w = FakeApi(), FakeWriter()
        watch = hlwatch.Watcher(ADDR, w, post=api)
        now = [0.0]
        slept = []
        watch.poll = lambda: now.__setitem__(0, now[0] + 9) or "x"
        hlwatch.run(watch, 5, clock=lambda: now[0], sleep=slept.append,
                    should_stop=lambda: now[0] > 20, say=lambda s: None)
        self.assertEqual(slept, [])


def ev(type_, t_s, data):
    return {"seq": 0, "t": int(t_s * SEC), "conn": "hlwatch", "type": type_, "data": data}


class TestReport(unittest.TestCase):
    def test_midpoint_lookup(self):
        series = [(10, 1.0), (20, 2.0), (30, 3.0)]
        self.assertEqual([hlwatch.mid_at(series, t) for t in (5, 10, 25, 99)], [None, 1.0, 2.0, 3.0])

    def test_fill_stats_hand_worked(self):
        events = [ev("fills", 0, {"fills": [
            fill(1, side="B", px="100", sz="2", crossed=False, fee="-0.002", pnl="0"),
            fill(2, side="A", px="101", sz="1", crossed=False, fee="-0.001", pnl="1.5"),
            fill(3, side="A", px="99", sz="1", crossed=True, fee="0.05", pnl="-0.5")]})]
        text = "\n".join(hlwatch.report(events))
        # traded 200 + 101 + 99 = 400; fees +0.047 = +1.18 bps; pnl 1.0 = +25 bps; maker 2 of 3
        self.assertIn("BTC: 3 fills, $400 traded, maker 67%, fees $+0.05 (+1.18 bps, - = rebate), "
                      "realized pnl $+1.00 (+25.00 bps)", text)

    def test_markouts_signed_for_the_account(self):
        mids = [ev("mids", t, {"BTC": str(px)}) for t, px in ((0, 100), (60, 101), (300, 99))]
        fills = ev("fills", 1, {"fills": [fill(1, side="B", px="100", time_ms=0),
                                          fill(2, side="A", px="100", time_ms=0)]})
        events = mids + [fills, ev("mids", 400, {"BTC": "99"})]
        text = "\n".join(hlwatch.report(events))
        # at +60 s: buy +100 bps, sell -100 bps -> 0; at +300 s: buy -100, sell +100 -> 0
        self.assertIn("markout (mid later vs fill price, + = good): 60s +0.00 bps (n=2)  300s +0.00 bps (n=2)", text)

    def test_fill_time_is_in_milliseconds(self):
        events = [ev("mids", 0, {"BTC": "100"}), ev("mids", 61, {"BTC": "101"}), ev("mids", 62, {"BTC": "101"}),
                  ev("fills", 2, {"fills": [fill(1, side="B", px="100", time_ms=1000)]})]
        self.assertIn("60s +100.00 bps (n=1)", "\n".join(hlwatch.report(events)))  # 1 s + 60 s = 61 s

    def test_markout_skips_fills_without_a_later_mid(self):
        events = [ev("mids", 0, {"BTC": "100"}), ev("mids", 60, {"BTC": "101"}), ev("mids", 61, {"BTC": "101"}),
                  ev("fills", 1, {"fills": [fill(1, side="B", px="100", time_ms=0)]})]
        text = "\n".join(hlwatch.report(events))
        self.assertIn("60s +100.00 bps (n=1)", text)
        self.assertNotIn("300s", text)

    def test_quote_distances(self):
        events = [ev("mids", 0, {"BTC": "100"}),
                  ev("orders", 1, {"orders": [order("BTC", "B", "99.9", "1"), order("BTC", "B", "99.8", "1"),
                                              order("BTC", "A", "100.2", "2")]})]
        text = "\n".join(hlwatch.report(events))
        self.assertIn("BTC: best bid 10.00 bps below mid, best ask 20.00 bps above, levels 2/1, "
                      "resting $200 bid / $200 ask", text)

    def test_quotes_without_a_mid(self):
        events = [ev("orders", 1, {"orders": [order("ETH", "B", "9.9", "1")]})]
        self.assertIn("ETH: best bid n/a below mid, best ask n/a above", "\n".join(hlwatch.report(events)))

    def test_positions(self):
        events = [ev("positions", t, state(("BTC", szi, v), value=val)) for t, szi, v, val in
                  ((0, "1", "100", "1000"), (5, "-2", "200", "1010"), (10, "0.5", "50", "990"))]
        events.append(ev("positions", 15, state(value="995")))
        text = "\n".join(hlwatch.report(events))
        self.assertIn("BTC: held in 75% of polls, median |position| $100, largest $200, long/short flips 2", text)
        self.assertIn("account value: first $1,000, last $995, change $-5", text)

    def test_header(self):
        events = [ev("orders", 0, {"orders": []}), ev("orders", 3600, {"orders": []}),
                  ev("error", 10, {"request": "orders", "error": "x"})]
        self.assertIn("hlreport 2 polls over 1.00 hours, 1 errors", hlwatch.report(events)[0])

    def test_other_events_ignored(self):
        events = [{"seq": 1, "t": 0, "conn": "kraken", "type": "ws_in", "data": {"text": "{}"}}]
        self.assertEqual(hlwatch.report(events)[0], "hlreport fills: 0 new fills logged")


class TestCommands(unittest.TestCase):
    def test_hlreport_runs_on_a_log(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "hl.jsonl")
            w = EventWriter(path)
            w.write("hlwatch", "session", {"address": ADDR.lower()})
            w.write("hlwatch", "orders", {"orders": [order("BTC", "B", "99", "1")]})
            w.write("hlwatch", "fills", {"fills": [fill(1)], "returned": 1})
            w.close()
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["hlreport", path])
        self.assertIn("hlreport fills: 1 new fills logged", buf.getvalue())

    def test_hlwatch_rejects_a_bad_address_before_making_a_log(self):
        with tempfile.TemporaryDirectory() as d:
            target = os.path.join(d, "hl")
            with self.assertRaises(ValueError):
                main(["hlwatch", "0x123", "--data-dir", target])
            self.assertFalse(os.path.exists(target) and os.listdir(target))


if __name__ == "__main__":
    unittest.main()
