"""Quiet screen mode, the status line, and stopping when behind the recorder."""

import io
import os
import tempfile
import unittest

from mmstack.main import Output, consume, is_important, status_line
from mmstack.state import StateMachine
from tests.helpers import T0, event

PRICE = "2026-11-16T19:49:56.000001Z kraken BTC/USD bid=91000.1 ask=91000.2 mid=91000.15"


class TestQuietScreen(unittest.TestCase):
    def test_price_lines_are_not_important(self):
        self.assertFalse(is_important(PRICE))
        self.assertFalse(is_important(PRICE.replace("kraken BTC/USD", "coinbase BTC-USD")))

    def test_important_lines(self):
        for text in ("kraken connected", "coinbase disconnected: eof from server",
                     "ACTION reconnect conn=kraken", "kraken BTC/USD CHECKSUM MISMATCH - reconnecting",
                     "coinbase SEQ GAP 5 -> 7 - reconnecting", "PAPER FILL BTC/USD buy 0.00006 @ 91000.1",
                     "PAPER BTC/USD quotes pulled (book stale or invalid)", "order mm1t1 new status=new",
                     "TEST ORDER COMPLETE", "action 1 rejected: trading disabled",
                     "kraken subscribe error: x", "summary kraken.checksum_ok=5"):
            self.assertTrue(is_important(text), text)

    def test_quiet_hides_price_lines_on_screen_but_keeps_them_in_the_file(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "out.txt")
            screen = io.StringIO()
            out = Output(path, stream=screen, quiet=True)
            out.emit(PRICE)
            out.emit("x kraken connected")
            out.close()
            self.assertEqual(screen.getvalue(), "x kraken connected\n")
            with open(path, encoding="utf-8") as f:
                self.assertEqual(f.read(), PRICE + "\nx kraken connected\n")

    def test_verbose_shows_everything(self):
        screen = io.StringIO()
        out = Output(stream=screen, quiet=False)
        out.emit(PRICE)
        self.assertEqual(screen.getvalue(), PRICE + "\n")


class TestStatusLine(unittest.TestCase):
    WALL = 1_794_862_000_000_000_000

    def test_keeping_up(self):
        line = status_line(self.WALL, self.WALL - 200_000_000, 1234567, 9_550_816_299)
        self.assertIn("1,234,567 events, log 9,551 MB, 0.2s behind", line)
        self.assertNotIn("falling behind", line)

    def test_falling_behind_is_flagged(self):
        line = status_line(self.WALL, self.WALL - 3600 * 10**9, 5, 1_000_000)
        self.assertIn("3600.0s behind  <-- falling behind", line)

    def test_no_data_yet(self):
        self.assertIn("waiting for data", status_line(self.WALL, None, 0, 0))


class EndlessTailer:
    """A log that always has more events: the recorder is ahead of us."""

    def __init__(self):
        self.n = 0

    def read_available(self):
        self.n += 1
        if self.n > 1000:
            raise AssertionError("consume never stopped")
        return [event(self.n, "timer", "timer", {"n": self.n}, t=T0 + self.n)]


class ListTailer:
    def __init__(self, batches):
        self.batches = list(batches)

    def read_available(self):
        return self.batches.pop(0) if self.batches else []


class TestStopping(unittest.TestCase):
    def test_stop_works_even_when_behind_the_recorder(self):
        """The bug: stop was only checked once caught up, so Ctrl+C did nothing
        while there was a backlog."""
        tailer = EndlessTailer()
        stopped = consume(tailer, StateMachine(), Output(stream=io.StringIO()), follow=True,
                          should_stop=lambda: tailer.n >= 3, poll=0)
        self.assertTrue(stopped)
        self.assertEqual(tailer.n, 3)

    def test_catch_up_can_be_skipped(self):
        tailer = EndlessTailer()
        stopped = consume(tailer, StateMachine(), Output(stream=io.StringIO()), follow=False,
                          should_stop=lambda: tailer.n >= 2)
        self.assertTrue(stopped)

    def test_reads_to_the_end_when_not_stopped(self):
        sm = StateMachine()
        batches = [[event(1, "timer", "timer")], [event(2, "timer", "timer")]]
        stopped = consume(ListTailer(batches), sm, Output(stream=io.StringIO()), follow=False)
        self.assertFalse(stopped)
        self.assertEqual(sm.stats["events.timer"], 2)

    def test_on_batch_gets_the_last_event_and_count(self):
        seen = []
        batches = [[event(1, "timer", "timer"), event(2, "timer", "timer")]]
        consume(ListTailer(batches), StateMachine(), Output(stream=io.StringIO()), follow=False,
                on_batch=lambda last, n: seen.append((last["seq"] if last else None, n)))
        self.assertEqual(seen, [(2, 2), (None, 0)])


if __name__ == "__main__":
    unittest.main()
