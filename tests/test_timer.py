"""Step 7: the logged 1-second timer."""

import threading
import time
import unittest

from mmstack.net import Reactor
from mmstack.state import TEST_ORDER_HOLD_NS
from tests.helpers import executions
from tests.test_actions import SEC, Session
from tests.test_net import NetTestBase, wait_for


class TestReactorTimer(NetTestBase):
    def run_timer(self, interval, seconds):
        reactor = Reactor([], poll_timeout=10.0, timer_log=self.writer.write, timer_interval=interval)
        self.reactor_thread = threading.Thread(target=reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()
        time.sleep(seconds)
        self.stop.set()
        self.reactor_thread.join(timeout=5)
        return reactor

    def timer_events(self):
        from mmstack.eventlog import EventTailer
        t = EventTailer(self.log_path)
        evs = []
        while True:
            b = t.read_available()
            if not b:
                break
            evs.extend(b)
        t.close()
        return [e for e in evs if e["type"] == "timer"]

    def test_ticks_on_time_even_with_long_poll_timeout(self):
        # poll_timeout is 10 s, so ticks only happen if select wakes up for the timer.
        self.run_timer(0.05, 0.5)
        ticks = self.timer_events()
        self.assertGreaterEqual(len(ticks), 7)
        self.assertLessEqual(len(ticks), 11)
        self.assertEqual([e["data"]["n"] for e in ticks], list(range(1, len(ticks) + 1)))
        self.assertEqual(ticks[0]["conn"], "timer")
        gaps = [(b["t"] - a["t"]) / 1e9 for a, b in zip(ticks, ticks[1:])]
        self.assertLess(max(gaps), 0.2)

    def test_falls_behind_without_burst(self):
        """If the loop is blocked for several ticks, we log ONE late tick, not a burst."""
        clock = [100.0]
        reactor = Reactor([], clock=lambda: clock[0], timer_log=self.writer.write, timer_interval=1.0)
        reactor._next_timer = 101.0
        clock[0] = 104.5  # blocked for 3.5 ticks
        reactor._fire_timer(clock[0])
        reactor._fire_timer(clock[0])
        ticks = self.timer_events()
        self.assertEqual(len(ticks), 1)
        self.assertAlmostEqual(ticks[0]["data"]["late_ms"], 3500.0)
        self.assertEqual(reactor._next_timer, 105.0)

    def test_select_timeout_shrinks_to_next_tick(self):
        clock = [10.0]
        reactor = Reactor([], clock=lambda: clock[0], timer_log=self.writer.write, timer_interval=1.0)
        reactor._next_timer = 10.25
        self.assertAlmostEqual(reactor._select_timeout(0.1), 0.1)
        self.assertAlmostEqual(reactor._select_timeout(5.0), 0.25)
        clock[0] = 11.0
        self.assertEqual(reactor._select_timeout(5.0), 0.0)

    def test_no_timer_by_default(self):
        reactor = Reactor([], poll_timeout=0.01)
        self.reactor_thread = threading.Thread(target=reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()
        time.sleep(0.2)
        self.stop.set()
        self.reactor_thread.join(timeout=5)
        self.assertEqual(self.timer_events(), [])


class TestStateMachineOnTimer(unittest.TestCase):
    def test_cancel_fires_in_a_silent_market(self):
        """The book stops updating after the order opens; only timer ticks
        arrive. The cancel must still go out after 5 s."""
        s = Session(test_order_symbol="BTC/USD")
        _, actions = s.ready()
        cl = actions[0]["cl_ord_id"]
        s.priv(executions("update", [{"cl_ord_id": cl, "exec_type": "new", "order_status": "new"}]))
        fired_at = None
        for n in range(1, 10):
            lines, acts = s.feed("timer", "timer", {"n": n, "late_ms": 0.0}, dt=SEC)
            if acts:
                fired_at = n
                self.assertEqual(acts[0]["action"], "cancel")
                break
        self.assertEqual(fired_at, TEST_ORDER_HOLD_NS // SEC)  # on the 5th tick
        self.assertEqual(s.sm.stats["events.timer"], fired_at)

    def test_timer_events_print_nothing(self):
        s = Session()
        lines, acts = s.feed("timer", "timer", {"n": 1, "late_ms": 0.0})
        self.assertEqual((lines, acts), ([], []))


if __name__ == "__main__":
    unittest.main()


class TestLiveWiring(unittest.TestCase):
    def test_live_reactor_has_one_second_timer(self):
        from mmstack.main import build_reactor
        logged = []

        class W:
            def write(self, *a):
                logged.append(a)

        r = build_reactor([], W())
        self.assertEqual(r.timer_interval, 1.0)
        r._next_timer = 0.0
        r._fire_timer(0.0)
        self.assertEqual(logged[0][:2], ("timer", "timer"))
