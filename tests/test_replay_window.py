"""Replay --from / --to: the books build from the whole log, but the paper
market maker only exists (and only counts) inside the window."""

import contextlib
import io
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.eventlog import EventTailer, WindowTailer
from mmstack.main import main, parse_when
from mmstack.state import StateMachine, iso
from tests.helpers import INSTRUMENT_BTC, T0, KrakenFeed, event
from tests.test_replay import write_log

SEC = 1_000_000_000
BTC = {"symbols": ["BTC/USD"], "spread": "0.01", "latency_ms": 100, "fills": "through"}


def trade(side, price, qty="0.001"):
    return ('{"channel":"trade","type":"update","data":[{"symbol":"BTC/USD","side":"%s",'
            '"price":%s,"qty":%s}]}' % (side, price, qty))


def market(minutes=10):
    """Kraken BTC/USD around mid 100 for `minutes` minutes. Each minute: a book
    update at +1 s, then at +30 s a print far outside the book, which fills a
    resting paper quote (fill mode 'through'): a sell print in even minutes (our
    bid fills), a buy print in odd ones (our ask fills), so inventory never hits
    the cap. One fill per minute, if quoting."""
    k = KrakenFeed("BTC/USD")
    evs, n = [], 0

    def add(t, type_, data=None):
        nonlocal n
        n += 1
        evs.append(event(n, "kraken", type_, data, t=t))

    add(T0, "conn_open", {"host": "ws.kraken.com"})
    add(T0 + 1, "ws_open")
    add(T0 + 2, "ws_in", {"text": INSTRUMENT_BTC})
    add(T0 + 3, "ws_in", {"text": k.message("snapshot", bids=[("99.9", "1.00000000")],
                                            asks=[("100.1", "1.00000000")])})
    for m in range(minutes):
        t = T0 + m * 60 * SEC
        qty = f"{1 + m / 100:.8f}"  # a real change, so the book update counts
        add(t + SEC, "ws_in", {"text": k.message("update", bids=[("99.9", qty)])})
        side, price = ("sell", "90.0") if m % 2 == 0 else ("buy", "110.0")
        add(t + 30 * SEC, "ws_in", {"text": trade(side, price)})
    return evs


def run(sm, events):
    out = []
    for ev in events:
        out.extend(sm.update(ev))
    return out


def replay(path, *flags):
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        main(["replay", path, "--paper-mm", "BTC/USD", "--mm-spread", "1",
              "--mm-fills", "through", "--mm-latency-ms", "100"] + list(flags))
    return buf.getvalue().splitlines()


def fills(lines):
    return sum(1 for line in lines if "PAPER FILL" in line)


class TestParseWhen(unittest.TestCase):
    def test_utc_z(self):
        self.assertEqual(parse_when("1970-01-01T00:00:01Z"), SEC)

    def test_offset_is_the_same_instant(self):
        self.assertEqual(parse_when("2026-11-20T11:13:28-05:00"), parse_when("2026-11-20T16:13:28Z"))

    def test_fractions_kept_to_the_microsecond(self):
        self.assertEqual(parse_when("1970-01-01T00:00:00.000250Z"), 250_000)

    def test_no_time_zone_refused(self):
        with self.assertRaises(ValueError) as e:
            parse_when("2026-11-20T16:13:28")
        self.assertIn("time zone", str(e.exception))

    def test_garbage_refused(self):
        with self.assertRaises(ValueError):
            parse_when("tuesday")

    def test_round_trip_with_iso(self):
        t = T0 + 123_456_000
        self.assertEqual(parse_when(iso(t)), t)


class TestWindowTailer(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "w.jsonl")
        write_log(self.path, [event(i, "kraken", "timer", t=T0 + i * SEC) for i in range(6)])

    def tearDown(self):
        self.dir.cleanup()

    def read_all(self, until):
        w = WindowTailer(EventTailer(self.path), until_ns=until)
        got = []
        while True:
            evs = w.read_available()
            if not evs:
                break
            got.extend(evs)
        self.assertEqual(w.read_available(), [])  # stays ended
        w.close()
        return [ev["t"] for ev in got], w.reached_end

    def test_stops_before_the_first_event_at_the_end(self):
        ts, ended = self.read_all(T0 + 3 * SEC)
        self.assertEqual(ts, [T0, T0 + SEC, T0 + 2 * SEC])
        self.assertTrue(ended)

    def test_no_end_reads_everything(self):
        ts, ended = self.read_all(None)
        self.assertEqual(len(ts), 6)
        self.assertFalse(ended)

    def test_end_after_the_log_reads_everything(self):
        ts, ended = self.read_all(T0 + 60 * SEC)
        self.assertEqual(len(ts), 6)
        self.assertFalse(ended)


class TestStateMachineWindow(unittest.TestCase):
    def test_no_paper_before_the_window(self):
        evs = market(10)
        start = T0 + 5 * 60 * SEC
        sm = StateMachine(paper_mm=BTC, paper_from_ns=start)
        run(sm, [ev for ev in evs if ev["t"] < start])
        self.assertEqual(sm.paper, {})
        self.assertNotIn("BTC/USD", "\n".join(sm.summary()).split("summary window")[0])

    def test_starts_on_the_first_event_at_the_window_with_what_is_known(self):
        evs = market(10)
        start = T0 + 5 * 60 * SEC  # the minute-5 marker: first event at/after it is t+1s
        sm = StateMachine(paper_mm=BTC, paper_from_ns=start)
        run(sm, evs)
        m = sm.paper["BTC/USD"]
        self.assertEqual(sm.paper_start_t, start + SEC)
        self.assertEqual(m.tick, D("0.1"))  # instrument from before the window was passed on
        self.assertEqual(len(m.fills), 5)  # minutes 5..9 only

    def test_whole_log_without_a_window(self):
        sm = StateMachine(paper_mm=BTC)
        run(sm, market(10))
        self.assertEqual(len(sm.paper["BTC/USD"].fills), 10)
        self.assertNotIn("summary window", "\n".join(sm.summary()))

    def test_summary_window_line_and_time_base(self):
        start = T0 + 5 * 60 * SEC
        sm = StateMachine(paper_mm=BTC, paper_from_ns=start, paper_to_ns=T0 + 9 * 60 * SEC)
        run(sm, [ev for ev in market(10) if ev["t"] < T0 + 9 * 60 * SEC])
        text = "\n".join(sm.summary())
        self.assertIn(f"summary window from={iso(start)} to={iso(T0 + 9 * 60 * SEC)} "
                      f"paper_start={iso(start + SEC)} last_event={iso(T0 + 8 * 60 * SEC + 30 * SEC)} "
                      f"hours=0.06", text)

    def test_quoted_time_is_a_share_of_the_window_not_the_log(self):
        sm = StateMachine(paper_mm=BTC, paper_from_ns=T0 + 5 * 60 * SEC)
        run(sm, market(10))
        line = next(x for x in sm.summary() if "quoted_time=" in x)
        share = D(line.split("quoted_time=")[1].split("%")[0])
        self.assertGreater(share, 80)  # ~89% of the window; about half that if timed from the log start

    def test_session_config_waits_for_the_window_too(self):
        start = T0 + 5 * 60 * SEC
        evs = [event(0, "main", "session", {"paper_mm": BTC}, t=T0 - 1)] + market(10)
        sm = StateMachine(paper_from_ns=start)
        run(sm, evs[:3])
        self.assertEqual(sm.paper, {})
        run(sm, evs[3:])
        self.assertEqual(len(sm.paper["BTC/USD"].fills), 5)

    def test_command_line_settings_beat_the_logged_session(self):
        start = T0 + 5 * 60 * SEC
        logged = dict(BTC, spread="0.02")
        evs = [event(0, "main", "session", {"paper_mm": logged}, t=T0 - 1)] + market(10)
        sm = StateMachine(paper_mm=BTC, paper_from_ns=start)
        run(sm, evs)
        self.assertEqual(sm.paper_config["spread"], "0.01")


class TestReplayCommand(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.dir.name, "m.jsonl")
        write_log(self.path, market(10))

    def tearDown(self):
        self.dir.cleanup()

    def test_window_counts_only_its_own_fills(self):
        self.assertEqual(fills(replay(self.path)), 10)
        lines = replay(self.path, "--from", iso(T0 + 3 * 60 * SEC), "--to", iso(T0 + 7 * 60 * SEC))
        self.assertEqual(fills(lines), 4)  # minutes 3, 4, 5, 6
        self.assertTrue(any(line.startswith("summary window from=") for line in lines))
        self.assertTrue(any("summary paper BTC/USD fills=4 " in line for line in lines))

    def test_only_from(self):
        self.assertEqual(fills(replay(self.path, "--from", iso(T0 + 8 * 60 * SEC))), 2)

    def test_only_to(self):
        lines = replay(self.path, "--to", iso(T0 + 2 * 60 * SEC))
        self.assertEqual(fills(lines), 2)
        window = next(x for x in lines if x.startswith("summary window"))
        self.assertIn(f"from=- to={iso(T0 + 2 * 60 * SEC)} paper_start={iso(T0)} ", window)
        self.assertIn("hours=0.02", window)  # T0 .. 1 min 30 s = 0.025 h

    def test_same_window_twice_identical(self):
        flags = ("--from", iso(T0 + 2 * 60 * SEC), "--to", iso(T0 + 6 * 60 * SEC))
        self.assertEqual(replay(self.path, *flags), replay(self.path, *flags))

    def test_offset_and_z_give_the_same_result(self):
        z = replay(self.path, "--from", "2025-11-13T02:16:40Z")
        offset = replay(self.path, "--from", "2025-11-12T21:16:40-05:00")
        self.assertEqual(z, offset)

    def test_from_after_to_refused(self):
        with self.assertRaises(SystemExit) as e:
            replay(self.path, "--from", iso(T0 + 5 * 60 * SEC), "--to", iso(T0 + 5 * 60 * SEC))
        self.assertIn("before", str(e.exception.code))

    def test_time_without_zone_refused(self):
        with self.assertRaises(SystemExit) as e:
            replay(self.path, "--from", "2026-11-20T16:13:28")
        self.assertIn("time zone", str(e.exception.code))


if __name__ == "__main__":
    unittest.main()
