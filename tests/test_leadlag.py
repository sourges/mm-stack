"""Lead-lag research tool, checked on made-up series where the answer is known."""

import contextlib
import io
import os
import random
import tempfile
import unittest

from mmstack import leadlag
from mmstack.eventlog import EventTailer
from mmstack.main import main
from tests.helpers import T0, KrakenFeed, coinbase_l2, event
from tests.test_replay import write_log

MS = 1_000_000


def random_walk(n, seed=1):
    rng = random.Random(seed)
    x, out = 100.0, []
    for _ in range(n):
        x += rng.choice((-0.01, 0.0, 0.01))
        out.append(x)
    return out


def delayed(series, steps):
    return [series[0]] * steps + series[:len(series) - steps]


class TestMaths(unittest.TestCase):
    def test_changes_bps(self):
        self.assertEqual(leadlag.changes_bps([100.0, 101.0, None, 50.0]), [100.0, None, None])

    def test_correlation_finds_the_delay(self):
        a = random_walk(5000)
        b = delayed(a, 4)  # b copies a, 4 steps later
        corr = leadlag.lag_correlation(a, b, 10)
        self.assertEqual(max(corr, key=lambda k: corr[k]), 4)
        self.assertAlmostEqual(corr[4], 1.0, places=2)
        self.assertLess(abs(corr[-4]), 0.1)
        # swap the roles: the peak moves to the negative side
        corr = leadlag.lag_correlation(b, a, 10)
        self.assertEqual(max(corr, key=lambda k: corr[k]), -4)

    def test_event_study_hand_worked(self):
        # 50 ms steps. Leader jumps 100.00 -> 100.03 (3 bps) at step 100;
        # follower makes the same move 4 steps (200 ms) later.
        n = 400
        lead = [100.0] * 100 + [100.03] * (n - 100)
        follow = [100.0] * 104 + [100.03] * (n - 104)
        ev = leadlag.event_study(lead, follow, step_ms=50, threshold_bps=2.0)
        self.assertEqual(ev["events"], 1)
        self.assertAlmostEqual(ev["lead_jump"], 3.0, places=6)
        self.assertAlmostEqual(ev["follow_at_jump"], 0.0, places=6)
        after = dict(zip(leadlag.HORIZONS_MS, ev["follow_later"]))
        self.assertAlmostEqual(after[50], 0.0, places=6)
        self.assertAlmostEqual(after[100], 0.0, places=6)
        self.assertAlmostEqual(after[200], 3.0, places=6)
        self.assertAlmostEqual(after[1000], 3.0, places=6)

    def test_event_study_direction_of_down_jumps(self):
        lead = [100.0] * 100 + [99.97] * 300
        follow = [100.0] * 102 + [99.97] * 298
        ev = leadlag.event_study(lead, follow, step_ms=50, threshold_bps=2.0)
        self.assertGreater(ev["follow_later"][-1], 2.9)  # follower moved WITH the jump: positive

    def test_follower_move_is_measured_from_the_start_of_the_window(self):
        # Follower moves 1 bp just BEFORE the leader's jump lands, then 2 more after.
        lead = [100.0] * 100 + [100.03] * 300
        follow = [100.0] * 99 + [100.01] * 5 + [100.03] * 296
        ev = leadlag.event_study(lead, follow, step_ms=50, threshold_bps=2.0)
        self.assertAlmostEqual(ev["follow_at_jump"], 1.0, places=6)  # already done
        self.assertAlmostEqual(ev["follow_later"][-1], 3.0, places=6)  # total, not 2

    def test_small_moves_are_not_events(self):
        lead = [100.0] * 100 + [100.01] * 300  # 1 bp
        self.assertEqual(leadlag.event_study(lead, lead, 50, threshold_bps=2.0), {"events": 0})

    def test_report_names_the_leader(self):
        a = random_walk(5000)
        series = {"cb": a, "kr": delayed(a, 3)}
        text = "\n".join(leadlag.report(series, "cb", "kr", 50))
        self.assertIn("strongest at +150 ms: cb tends to move first", text)
        text = "\n".join(leadlag.report(series, "kr", "cb", 50))
        self.assertIn("strongest at -150 ms: cb tends to move first", text)

    def test_report_with_too_little_data(self):
        text = "\n".join(leadlag.report({"a": [1.0] * 5, "b": [1.0] * 5}, "a", "b", 50))
        self.assertIn("not enough data", text)


class TestSampling(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "s.jsonl")
        k = KrakenFeed("BTC/USD")
        evs = [
            event(1, "kraken", "ws_in", {"text": k.message(
                "snapshot", bids=[("99.0", "1.0")], asks=[("101.0", "1.0")])}, t=T0),
            event(2, "coinbase", "ws_in", {"text": coinbase_l2(
                1, "snapshot", "BTC-USD", [("bid", "199", "1"), ("offer", "201", "1")])}, t=T0 + 120 * MS),
            event(3, "kraken", "ws_in", {"text": k.message("update", bids=[("100.0", "1.0")])},
                  t=T0 + 250 * MS),
            event(4, "timer", "timer", {"n": 1}, t=T0 + 400 * MS),
        ]
        write_log(self.path, evs)

    def tearDown(self):
        self.dir.cleanup()

    def test_samples_are_the_mid_in_effect_at_each_grid_time(self):
        t = EventTailer(self.path)
        series, start = leadlag.sample_mids(
            t, {"k": ("kraken", "BTC/USD"), "c": ("coinbase", "BTC-USD")}, 100 * MS)
        t.close()
        self.assertEqual(start, T0)
        # grid: T0, +100, +200, +300 ms.  Kraken mid 100.0 until the update at 250 ms, then 100.5.
        self.assertEqual(series["k"], [100.0, 100.0, 100.0, 100.5])
        # Coinbase book arrives at 120 ms: unknown at T0 and +100.
        self.assertEqual(series["c"], [None, None, 200.0, 200.0])

    def test_invalid_book_is_not_sampled(self):
        k = KrakenFeed("BTC/USD")
        evs = [event(1, "kraken", "ws_in", {"text": k.message(
                   "snapshot", bids=[("99.0", "1.0")], asks=[("101.0", "1.0")])}, t=T0),
               event(2, "kraken", "ws_in", {"text": k.message(
                   "update", bids=[("100.0", "1.0")], checksum=1)}, t=T0 + 150 * MS),  # bad checksum
               event(3, "timer", "timer", {"n": 1}, t=T0 + 400 * MS)]
        path = os.path.join(self.dir.name, "bad.jsonl")
        write_log(path, evs)
        t = EventTailer(path)
        series, _ = leadlag.sample_mids(t, {"k": ("kraken", "BTC/USD")}, 100 * MS)
        t.close()
        self.assertEqual(series["k"], [100.0, 100.0, None, None])

    def test_command_runs(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            main(["leadlag", self.path, "--step-ms", "100"])
        self.assertIn("leadlag coinbase BTC-USD vs kraken BTC/USD", buf.getvalue())
        self.assertIn("not enough data", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
