"""Research tool: does the book's lean predict the next mid move?"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack import imbalance
from mmstack.books import Book
from mmstack.eventlog import EventTailer
from mmstack.main import main
from tests.helpers import T0, KrakenFeed, event
from tests.test_binanceus import depth
from tests.test_replay import write_log

MS = 1_000_000
KEY = ("binanceus", "BTCUSD")


def book(bids, asks):
    b = Book()
    for p, q in bids:
        b.bids.set(D(p), D(q))
    for p, q in asks:
        b.asks.set(D(p), D(q))
    return b


class Log:
    """Builds a Binance.US log: each step is (ms after start, bids, asks) or (ms, 'bad')."""

    def __init__(self):
        self.events = [event(0, "binanceus", "ws_open", t=T0)]
        self.uid = 0

    def add(self, ms, bids, asks):
        self.uid += 1
        self.events.append(event(len(self.events), "binanceus", "ws_in",
                                 {"text": json.dumps(depth("BTCUSD", self.uid, bids, asks))},
                                 t=T0 + ms * MS))
        return self

    def collect(self, keys=(KEY,)):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "x.jsonl")
            write_log(path, self.events)
            tailer = EventTailer(path)
            try:
                return imbalance.collect(tailer, list(keys))
            finally:
                tailer.close()


class TestReadings(unittest.TestCase):
    def test_touch(self):
        self.assertAlmostEqual(imbalance.touch_reading(book([("100", "2")], [("101", "1")])), 1 / 3)

    def test_weighted_hand_worked(self):
        # spread 1 = one tick: next level weighs 1/(1+1)^2 = 0.25; B = 2 + 0.25, A = 1 + 0.75
        b = book([("100", "2"), ("99", "1")], [("101", "1"), ("102", "3")])
        self.assertAlmostEqual(imbalance.weighted_reading(b, 10, D("1")), 0.125)

    def test_weighted_scale_is_the_spread(self):
        b = book([("100", "1"), ("99", "9")], [("102", "1")])  # spread 2: 9 / (1 + 1/2)^2 = 4
        self.assertAlmostEqual(imbalance.weighted_reading(b, 10, D("1")), (5 - 1) / 6)

    def test_weighted_scale_at_least_one_tick(self):
        b = book([("100", "1"), ("99", "1")], [("100.5", "1")])  # spread 0.5, tick 1: scale 1
        self.assertAlmostEqual(imbalance.weighted_reading(b, 10, D("1")), 0.25 / 2.25)

    def test_levels_limit(self):
        b = book([("100", "1"), ("99", "100")], [("101", "1")])
        self.assertAlmostEqual(imbalance.weighted_reading(b, 1, D("1")), 0.0)


class TestCollect(unittest.TestCase):
    A = ([("100", "3")], [("101", "1")])  # mid 100.5, leans up
    B = ([("100", "3")], [("102", "1")])  # mid 101

    def test_reading_after_a_change_settled_by_the_next(self):
        data = Log().add(0, *self.A).add(10, *self.B).add(40, [("101", "1")], [("102", "5")]).collect()[KEY]
        self.assertEqual(data["changes"], 2)
        [e] = data["events"]  # the reading taken at the first change, settled by the second
        self.assertTrue(e["up"])
        self.assertEqual(e["wait_ns"], 30 * MS)
        self.assertAlmostEqual(e["touch"], 0.5)  # book B: 3 vs 1
        self.assertEqual(e["spread_ticks"], 2)  # tick 1 read off the prices
        self.assertAlmostEqual(e["move_bps"], 0.5 / 101 * 10000)
        self.assertAlmostEqual(e["half_spread_bps"], 1 / 101 * 10000)  # book B: 100 / 102
        self.assertEqual(data["tick"], D("1"))

    def test_unchanged_mid_is_not_a_change(self):
        data = Log().add(0, *self.A).add(10, [("100", "9")], [("101", "1")]).collect()[KEY]
        self.assertEqual(data["changes"], 0)

    def test_next_change_too_late(self):
        data = (Log().add(0, *self.A).add(10, *self.B)
                .add(10 + 30_001, *self.A).add(10 + 30_002, *self.B).collect()[KEY])
        self.assertEqual(data["changes"], 3)
        self.assertEqual(len(data["events"]), 1)  # the reading at 10 ms waited too long
        self.assertTrue(data["events"][0]["up"])  # 30.011 s -> 30.012 s: 100.5 -> 101

    def test_bad_book_drops_the_waiting_reading(self):
        data = (Log().add(0, *self.A).add(10, *self.B)
                .add(20, [("101", "1")], [])  # empty ask side: not valid
                .add(30, *self.A).add(40, *self.B).collect()[KEY])
        self.assertEqual(data["changes"], 2)  # 10 ms and 40 ms; 30 ms is a fresh start
        self.assertEqual(data["events"], [])

    def test_fixed_time_moves(self):
        data = (Log().add(0, *self.A).add(10, *self.B)            # reading at 10 ms, mid 101
                .add(500, [("101", "1")], [("102", "5")])           # mid 101.5 at 500 ms
                .add(1200, [("102", "1")], [("103", "5")])          # mid 102.5 at 1.2 s
                .add(6000, *self.A).collect()[KEY])
        e = data["events"][0]
        self.assertAlmostEqual(e["move_1s_bps"], 0.5 / 101 * 10000)  # in effect at 1.01 s: 101.5
        self.assertAlmostEqual(e["move_5s_bps"], 1.5 / 101 * 10000)  # in effect at 5.01 s: 102.5

    def test_other_books_are_separate(self):
        data = Log().add(0, *self.A).add(10, *self.B).collect(keys=(KEY, ("kraken", "BTC/USD")))
        self.assertEqual(data[("kraken", "BTC/USD")]["changes"], 0)

    def test_kraken_tick_from_instrument_data(self):
        feed = KrakenFeed()
        inst = json.dumps({"channel": "instrument", "type": "snapshot", "data": {"assets": [], "pairs": [
            {"symbol": "BTC/USD", "price_increment": 0.5, "qty_increment": 0.00000001,
             "qty_min": 0.00005, "cost_min": 0.5, "status": "online"}]}})
        log = Log()
        log.events = [event(0, "kraken", "ws_open", t=T0),
                      event(1, "kraken", "ws_in", {"text": inst}, t=T0 + 1),
                      event(2, "kraken", "ws_in", {"text": feed.message(
                          "snapshot", bids=[("100.0", "1.5")], asks=[("101.0", "1.5")])}, t=T0 + 2),
                      event(3, "kraken", "ws_in", {"text": feed.message(
                          "update", asks=[("101.0", "0"), ("102.0", "1.5")])}, t=T0 + 3)]
        data = log.collect(keys=(("kraken", "BTC/USD"),))[("kraken", "BTC/USD")]
        self.assertEqual(data["changes"], 1)
        self.assertEqual(data["tick"], D("0.5"))


def ev(reading, up, move=1.0):
    return {"touch": reading, "weighted": reading, "up": up, "move_bps": move,
            "half_spread_bps": 2.0, "wait_ns": 0, "spread_ticks": 1}


class TestScore(unittest.TestCase):
    def test_hand_worked(self):
        events = [ev(0.5, True), ev(0.5, True), ev(0.5, False), ev(-0.5, False), ev(0.0, True)]
        s = imbalance.score(events, "touch")
        self.assertEqual((s["calls"], s["no_call"]), (4, 1))
        self.assertAlmostEqual(s["hit"], 3 / 4)
        # calls up 3/4, ups 2/4: base = 3/4 * 1/2 + 1/4 * 1/2
        self.assertAlmostEqual(s["base"], 0.5)
        self.assertAlmostEqual(s["signed_move_bps"], (1 + 1 + 1 - 1) / 4)

    def test_no_information_scores_the_base(self):
        events = [ev(0.5, True), ev(0.5, False), ev(-0.5, True), ev(-0.5, False)]
        s = imbalance.score(events, "touch")
        self.assertAlmostEqual(s["hit"], s["base"])

    def test_always_calling_up_in_a_rising_market_is_not_skill(self):
        events = [ev(0.5, True)] * 9 + [ev(0.5, False)]
        s = imbalance.score(events, "touch")
        self.assertAlmostEqual(s["hit"], 0.9)
        self.assertAlmostEqual(s["diff"], 0.0)

    def test_fixed_time_moves_signed(self):
        a, b = ev(0.5, True), ev(-0.5, False)
        a["move_1s_bps"], b["move_1s_bps"] = 2.0, 1.0
        s = imbalance.score([a, b], "touch")
        self.assertAlmostEqual(s["signed_1s_bps"], (2.0 - 1.0) / 2)
        self.assertIsNone(s["signed_5s_bps"])

    def test_no_calls(self):
        self.assertEqual(imbalance.score([ev(0.0, True)], "touch"), {"calls": 0, "no_call": 1})


class TestReport(unittest.TestCase):
    def test_not_enough_data(self):
        lines = imbalance.report("x", {"events": [ev(0.5, True)] * 99, "changes": 100, "tick": D("1")})
        self.assertIn("not enough data", lines[-1])

    def test_sections(self):
        events = [ev(0.5, True), ev(-0.5, False), ev(0.7, False)] * 50
        text = "\n".join(imbalance.report("x", {"events": events, "changes": 151, "tick": D("1")}))
        for part in ("x touch    hit  66.7%", "by spread", "1 tick", "by time to the next change",
                     "under 50 ms", "share of next changes up", "[+0.2, +0.6)"):
            self.assertIn(part, text)
        self.assertNotIn("150 ms+", text)  # every wait here is 0

    def test_slow_bucket_shown(self):
        events = [dict(ev(0.5, True), wait_ns=200 * MS) for _ in range(120)]
        self.assertIn("150 ms+", "\n".join(imbalance.report("x", {"events": events, "changes": 121,
                                                                   "tick": D("1")})))


class TestCommand(unittest.TestCase):
    def test_runs(self):
        log = Log()
        for i in range(300):
            log.add(i * 10, [("100", "3" if i % 2 else "1")], [("101" if i % 2 else "102", "1")])
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "x.jsonl")
            write_log(path, log.events)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["imbalance", path, "--kraken", "", "--binanceus", "BTCUSD"])
        text = buf.getvalue()
        self.assertIn("imbalance binanceus BTCUSD: 299 mid changes, 298 events", text)
        self.assertIn("imbalance binanceus BTCUSD weighted hit", text)


if __name__ == "__main__":
    unittest.main()
