"""Paper market maker: Coinbase-adjusted fair price and Kraken's rate limit."""

import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.main import main
from mmstack.state import StateMachine
from tests.helpers import T0, KrakenFeed, coinbase_l2, event, sample_session
from tests.test_paper_mm import FLAT, LAT, MS, SEC, book, mm, started
from tests.test_replay import write_log


class TestFairPrice(unittest.TestCase):
    def setUp(self):
        self.m = mm(ref_weight=D("0.5"))
        started(self.m)  # Kraken mid 100
        self.t = T0 + LAT
        self.m.on_reference(self.t, D("100.10"))  # Coinbase normally sits 0.10 higher

    def test_normal_gap_means_no_lean(self):
        self.assertEqual(self.m.ref_gap, D("0.10"))
        self.assertEqual(self.m.fair_price(self.t), D("100"))

    def test_leans_half_way_toward_a_coinbase_jump(self):
        t = self.t + 200 * MS
        self.m.on_reference(t, D("100.50"))  # Coinbase jumps 0.40; Kraken hasn't moved
        # 100 + 0.5 * (100.50 - 100 - 0.10) = 100.20
        self.assertEqual(self.m.fair_price(t), D("100.20"))
        self.assertEqual(self.m.nxt["bid"].price, D("99.19"))  # 100.20 - 1.002 = 99.198, floored
        self.assertEqual(self.m.nxt["ask"].price, D("101.21"))  # 100.20 + 1.002, rounded up

    def test_gap_is_a_slow_average(self):
        self.m.on_reference(self.t + 500 * MS, D("100.30"))  # under a second: gap not updated
        self.assertEqual(self.m.ref_gap, D("0.10"))
        self.m.on_reference(self.t + 60 * SEC, D("100.30"))
        # one update after 60 s with a 60 s average: half way from 0.10 to 0.30
        self.assertEqual(self.m.ref_gap, D("0.20"))

    def test_old_reference_is_ignored(self):
        self.m.on_reference(self.t + 200 * MS, D("100.50"))
        self.assertEqual(self.m.fair_price(self.t + 3 * SEC), D("100"))  # 2.8 s old

    def test_lost_reference_resets(self):
        self.m.on_reference(self.t + 200 * MS, D("100.50"))
        self.m.on_reference(self.t + 300 * MS, None)
        self.assertEqual(self.m.fair_price(self.t + 300 * MS), D("100"))
        self.m.on_reference(self.t + 400 * MS, D("100.50"))  # comes back: this is the new normal
        self.assertEqual(self.m.fair_price(self.t + 400 * MS), D("100"))

    def test_weight_zero_is_off(self):
        m = mm()
        started(m)
        m.on_reference(T0 + LAT, D("100.10"))
        m.on_reference(T0 + LAT + 200 * MS, D("105"))
        self.assertEqual(m.fair_price(T0 + SEC), D("100"))
        self.assertEqual(m.stats["requotes"], 0)
        self.assertIsNone(m.ref_mid)  # not even looked at

    def test_summary_line(self):
        self.m.on_reference(self.t + 200 * MS, D("100.50"))
        text = "\n".join(self.m.summary_lines(10 * SEC))
        # 2 of the 4 quote calculations happened before Coinbase's price was known
        self.assertIn("coinbase_signal weight=0.5 used=50%", text)


class TestRateLimit(unittest.TestCase):
    """Starter level: threshold 60, decays 1 point a second. A re-quote of an
    order under 5 s old costs 8 (cancel) + 1 (new order) = 9."""

    def rounds(self, m, n):
        started(m)  # 2 new orders: counter 2
        for i in range(1, n + 1):
            mid = 100 + i  # big moves: both sides want a re-quote every time
            m.on_book(T0 + LAT + i * 200 * MS, book(f"{mid - 0.01:.2f}", f"{mid + 0.01:.2f}"), True)
        return T0 + LAT + n * 200 * MS

    def test_holds_back_when_it_could_not_also_cancel(self):
        m = mm(rate_tier="starter")
        now = self.rounds(m, 3)
        # round 1: 2 -> 20, round 2: 20 -> 38. Round 3 would need 38 + 9 + 16 (room to
        # cancel both resting orders) = 63 > 60: held back.
        self.assertEqual(m.stats["requotes"], 4)
        self.assertEqual(m.stats["held_back"], 2)  # bid and ask
        self.assertIsNone(m.bid.dead_at)  # the old quotes just stay where they are
        self.assertLess(m.rate["starter"].level(now), 60)

    def test_hold_back_counted_once_per_stretch(self):
        m = mm(rate_tier="starter")
        self.rounds(m, 5)
        self.assertEqual(m.stats["held_back"], 2)

    def test_there_is_always_room_to_cancel(self):
        m = mm(rate_tier="starter")
        now = self.rounds(m, 5)
        bid, ask = m.bid, m.ask
        m.on_disconnect(now + MS)  # pull everything
        self.assertIsNotNone(bid.dead_at)
        self.assertIsNotNone(ask.dead_at)
        self.assertEqual(m.stats["cancel_blocked"], 0)
        self.assertLessEqual(m.rate["starter"].counter, 60)

    def test_no_tier_means_no_holding_back(self):
        m = mm(rate_tier=None)
        self.rounds(m, 5)
        self.assertEqual(m.stats["requotes"], 10)
        self.assertEqual(m.stats["held_back"], 0)

    def test_blocked_cancel_is_retried(self):
        m = mm(rate_tier="starter")
        started(m)
        now = T0 + SEC
        m.rate["starter"].counter, m.rate["starter"].last_t = D(59), now  # as if nearly used up
        q = m.bid
        m.on_disconnect(now)  # wants to pull, but 59 + 8 > 60
        self.assertIsNone(q.dead_at)
        self.assertEqual(m.stats["cancel_blocked"], 2)
        m.on_timer(now + 10 * SEC)  # 10 points decayed: 49 + 4 fits (order is 10 s old now)
        self.assertIsNotNone(q.dead_at)

    def test_unknown_tier_rejected(self):
        with self.assertRaises(ValueError):
            mm(rate_tier="gold")

    def test_summary_line(self):
        m = mm(rate_tier="starter")
        self.rounds(m, 3)
        text = "\n".join(m.summary_lines(10 * SEC))
        self.assertIn("rate_limit enforced=starter held_back=2 cancel_blocked=0 peak=38/60", text)


class TestWiring(unittest.TestCase):
    CFG = {"symbols": ["BTC/USD"], "spread": "0.01", "coinbase": "0.5", "rate_tier": "pro"}

    def feed(self, sm):
        k = KrakenFeed("BTC/USD")
        sm.update(event(1, "kraken", "ws_in", {"text": k.message(
            "snapshot", bids=[("99.9", "1.0")], asks=[("100.1", "1.0")])}))
        sm.update(event(2, "coinbase", "ws_in", {"text": coinbase_l2(
            1, "snapshot", "BTC-USD", [("bid", "100.10", "1"), ("offer", "100.30", "1")])}))

    def test_coinbase_mid_reaches_the_market_maker(self):
        sm = StateMachine(paper_mm=self.CFG)
        self.feed(sm)
        m = sm.paper["BTC/USD"]
        self.assertEqual(m.ref_mid, D("100.20"))
        self.assertEqual((m.ref_weight, m.rate_tier), (D("0.5"), "pro"))
        self.assertIn("coinbase=0.5 rate_limit=pro", "\n".join(sm.summary()))

    def test_coinbase_disconnect_drops_the_reference(self):
        sm = StateMachine(paper_mm=self.CFG)
        self.feed(sm)
        sm.update(event(3, "coinbase", "conn_close", {"reason": "eof"}))
        self.assertIsNone(sm.paper["BTC/USD"].ref_mid)

    def test_coinbase_sequence_gap_drops_the_reference(self):
        sm = StateMachine(paper_mm=self.CFG)
        self.feed(sm)
        sm.update(event(3, "coinbase", "ws_in", {"text": coinbase_l2(
            5, "update", "BTC-USD", [("bid", "100.20", "1")])}))  # 1 -> 5: gap
        self.assertIsNone(sm.paper["BTC/USD"].ref_mid)

    def test_old_logs_keep_old_behaviour(self):
        """A session recorded before these settings existed must replay as it ran."""
        sm = StateMachine()
        sm.update(event(0, "main", "session", {"paper_mm": {"symbols": ["BTC/USD"], "spread": "0.01"}}))
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.ref_weight, m.rate_tier), (D(0), None))

    def test_command_line_settings(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            for flags, expected in (([], "coinbase=0 rate_limit=intermediate"),
                                    (["--mm-coinbase", "0.65", "--mm-rate-tier", "pro"],
                                     "coinbase=0.65 rate_limit=pro")):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    main(["replay", path, "--paper-mm", "BTC/USD"] + flags)
                self.assertIn(expected, buf.getvalue())


if __name__ == "__main__":
    unittest.main()
