"""Paper market maker: the volatility-scaled Coinbase alarm
(ring when Coinbase moves K times its normal move per window)."""

import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.main import main
from mmstack.paper_mm import VOL_WARMUP_NS
from mmstack.state import StateMachine
from tests.helpers import T0, event, sample_session
from tests.test_paper_mm import LAT, MS, SEC, mm, started
from tests.test_replay import write_log

W = 100 * MS  # alarm window


class VolCase(unittest.TestCase):
    """K = 2. Kraken mid 100, quotes live at 99 / 101, Coinbase starts at 100."""

    def setUp(self):
        self.m = mm(alarm_vol=D("2"))
        started(self.m)
        self.t = T0 + LAT
        self.m.on_reference(self.t, D("100"))

    def warm(self, var="1", tau_ns=None):
        """Pretend the average is already built: normal move = sqrt(var) bps."""
        self.m.vol_var = D(var)
        self.m._vol_seen_ns = VOL_WARMUP_NS
        if tau_ns is not None:
            self.m.alarm_vol_tau_ns = tau_ns

    def ref(self, price, after):
        self.m.on_reference(self.t + after, D(price))
        return self.t + after


class TestAverage(VolCase):
    def test_first_full_window_sets_the_average(self):
        self.ref("100.01", W)  # 1 bp in one window
        self.assertEqual(self.m.vol_var, D(1))

    def test_ticks_inside_a_window_do_not_count(self):
        self.ref("100.01", W - 1)
        self.assertIsNone(self.m.vol_var)

    def test_quiet_gap_shares_the_move(self):
        self.ref("100.01", 5 * W)  # 1 bp over five windows: 1/5 bp^2 per window
        self.assertEqual(self.m.vol_var, D("0.2"))
        self.assertEqual(self.m._vol_seen_ns, 5 * W)

    def test_grid_moves_by_whole_windows(self):
        self.ref("100.01", 5 * W + 30 * MS)
        self.assertEqual(self.m._vol_t, self.t + 5 * W)
        self.assertEqual(self.m._vol_p, D("100.01"))

    def test_later_samples_are_averaged_over_tau(self):
        self.ref("100.01", W)  # var 1
        self.m.alarm_vol_tau_ns = 3 * W  # weight of one window = 1/4
        self.ref("100.01", 2 * W)  # no move: sample 0
        self.assertEqual(self.m.vol_var, D("0.75"))

    def test_outage_starts_a_new_grid(self):
        self.ref("100.01", W)
        self.m.on_reference(self.t + W + 10 * MS, None)
        self.ref("101", 3 * W)  # first price after the outage: only a starting point
        self.assertEqual(self.m.vol_var, D(1))
        self.assertEqual(self.m._vol_t, self.t + 3 * W)


class TestThreshold(VolCase):
    def test_silent_while_warming_up(self):
        self.m.vol_var = D(1)
        self.m._vol_seen_ns = VOL_WARMUP_NS - 1
        self.assertIsNone(self.m.alarm_threshold())
        self.ref("101", 50 * MS)  # 100 bps: would ring if warm
        self.assertEqual(self.m.stats["alarms_up"], 0)

    def test_k_times_the_normal_move(self):
        self.warm("2.25")  # normal move 1.5 bps
        self.assertEqual(self.m.alarm_threshold(), D("3.0"))

    def test_floor(self):
        self.warm("0.01")  # normal move 0.1 bp -> 0.2 bp, below the floor
        self.m.alarm_bps = D("1.5")
        self.assertEqual(self.m.alarm_threshold(), D("1.5"))

    def test_warm_up_takes_sixty_seconds_of_data(self):
        price = D("100")
        for i in range(1, 601):  # 600 windows = 60 s
            price += D("0.01") if i % 2 else D("-0.01")
            self.ref(price, i * W)
        self.assertEqual(self.m._vol_seen_ns, VOL_WARMUP_NS)
        self.assertIsNotNone(self.m.alarm_threshold())


class TestRinging(VolCase):
    def test_rings_at_k_times_normal(self):
        self.warm("1")  # threshold 2 bps
        self.ref("100.02", 50 * MS)
        self.assertEqual(self.m.stats["alarms_up"], 1)
        self.assertIsNotNone(self.m.cur["ask"].dead_at)

    def test_below_k_times_normal_does_not_ring(self):
        self.warm("1")
        self.ref("100.019", 50 * MS)
        self.assertEqual(self.m.stats["alarms_up"], 0)

    def test_same_jump_is_bigger_news_on_a_quiet_day(self):
        self.warm("0.25")  # normal move 0.5 bp -> threshold 1 bp
        self.ref("99.99", 50 * MS)
        self.assertEqual(self.m.stats["alarms_down"], 1)

    def test_a_jump_does_not_raise_its_own_bar(self):
        self.warm("1", tau_ns=W)  # one window weighs half: checking after the update would need 3.8 bps
        self.ref("100.025", W)
        self.assertEqual(self.m.stats["alarms_up"], 1)
        self.assertGreater(self.m.vol_var, D(1))  # ...but it does count for next time

    def test_no_move_never_rings_in_a_flat_market(self):
        self.warm("0")  # threshold 0
        self.ref("100", 50 * MS)
        self.ref("100", 2 * W)
        self.assertEqual(self.m.stats["alarms_up"] + self.m.stats["alarms_down"], 0)

    def test_fixed_alarm_unchanged_when_vol_is_off(self):
        m = mm(alarm_bps=D("2"))
        started(m)
        m.on_reference(self.t, D("100"))
        self.assertIsNone(m.vol_var)
        m.on_reference(self.t + W, D("100.01"))
        self.assertIsNone(m.vol_var)  # the average is only kept in volatility mode
        self.assertEqual(m.alarm_threshold(), D("2"))


class TestSummary(VolCase):
    def test_summary_shows_the_levels_used(self):
        self.warm("1")  # threshold 2
        self.ref("100.005", 20 * MS)
        self.warm("2.25")  # threshold 3
        self.ref("100.005", 40 * MS)
        text = "\n".join(self.m.summary_lines(10 * SEC))
        self.assertIn("coinbase_alarm jump>=2x_normal_move (avg 2.50bps min 2.00 max 3.00) "
                      "within=100ms hold=1000ms alarms up=0 down=0", text)

    def test_summary_floor_and_not_warmed_up(self):
        self.m.alarm_bps = D("1.5")
        text = "\n".join(self.m.summary_lines(10 * SEC))
        self.assertIn("jump>=2x_normal_move (never warmed up) floor=1.5bps within=100ms", text)


class TestWiring(unittest.TestCase):
    def test_settings_reach_the_market_maker(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.01", "alarm_vol": "4",
                                    "alarm_vol_tau_s": 300, "alarm_bps": "1"})
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.alarm_vol, m.alarm_vol_tau_ns, m.alarm_bps), (D(4), 300 * SEC, D(1)))
        self.assertTrue(m.alarm_on)

    def test_old_logs_have_no_volatility_alarm(self):
        sm = StateMachine()
        sm.update(event(0, "main", "session", {"paper_mm": {"symbols": ["BTC/USD"], "spread": "0.01"}}))
        m = sm.paper["BTC/USD"]
        self.assertEqual((m.alarm_vol, m.alarm_vol_tau_ns), (D(0), 600 * SEC))
        self.assertFalse(m.alarm_on)

    def test_command_line_settings(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            for flags, expected in (
                    (["--mm-alarm-vol", "4"],
                     "rate_limit=intermediate alarm=4x_normal_move/100ms/1000ms avg_over=600s"),
                    (["--mm-alarm-vol", "5", "--mm-alarm-vol-tau-s", "120", "--mm-alarm-bps", "1.5"],
                     "alarm=5x_normal_move/100ms/1000ms avg_over=120s floor=1.5bps"),
                    (["--mm-alarm-bps", "2"], "rate_limit=intermediate alarm=2bps/100ms/1000ms")):
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    main(["replay", path, "--paper-mm", "BTC/USD"] + flags)
                self.assertIn(expected, buf.getvalue())


if __name__ == "__main__":
    unittest.main()
