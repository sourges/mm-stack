"""Binance (global) market data: recorded like Binance.US, usable as the reference price."""

import contextlib
import io
import json
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack import venues
from mmstack.eventlog import EventWriter
from mmstack.main import add_paper_args, build_clients, main, paper_config
from mmstack.state import StateMachine
from tests.helpers import T0, coinbase_l2, event
from tests.test_binanceus import BOOK, depth, trade
from tests.test_replay import write_log


class TestVenue(unittest.TestCase):
    def test_market_data_only_stream(self):
        spec = venues.binance(["BTCUSDT"])
        self.assertEqual((spec["name"], spec["host"], spec["port"]), ("binance", "data-stream.binance.vision", 443))
        self.assertEqual(spec["path"], "/stream?streams=btcusdt@depth20@100ms/btcusdt@trade/btcusdt@bookTicker")
        self.assertEqual(spec["subscribe"], [])

    def test_binance_us_unchanged(self):
        spec = venues.binanceus(["BTCUSD"])
        self.assertEqual((spec["name"], spec["host"], spec["port"]), ("binanceus", "stream.binance.us", 9443))

    def test_live_builds_it_when_asked(self):
        w = EventWriter.__new__(EventWriter)
        self.assertEqual([c.name for c in build_clients(w, [], [], binanceus_symbols=["BTCUSD"],
                                                         binance_symbols=["BTCUSDT"])],
                         ["binanceus", "binance"])
        self.assertEqual([c.name for c in build_clients(w, ["BTC/USD"], [], binance_symbols=[])], ["kraken"])


class Feed(unittest.TestCase):
    PAPER = None

    def setUp(self):
        self.sm = StateMachine(paper_mm=self.PAPER)
        self.n = 0
        self.t = T0

    def send(self, conn, type_, data=None, dt_ms=1):
        self.n += 1
        self.t += dt_ms * 1_000_000
        return self.sm.update(event(self.n, conn, type_, data, t=self.t))

    def msg(self, conn, m, **kw):
        return self.send(conn, "ws_in", {"text": json.dumps(m)}, **kw)

    def stat(self, name):
        return self.sm.stats.get(name, 0)


class TestRecording(Feed):
    def test_book_and_top_line(self):
        self.send("binance", "ws_open")
        out = self.msg("binance", depth("BTCUSDT", 100, *BOOK))
        self.assertTrue(self.sm.valid[("binance", "BTCUSDT")])
        self.assertIn("binance BTCUSDT bid=64000.1 ask=64001 mid=64000.55", str(out))
        self.assertEqual((self.stat("binance.books_ok"), self.stat("binanceus.books_ok")), (1, 0))

    def test_venues_kept_apart(self):
        self.send("binance", "ws_open")
        self.send("binanceus", "ws_open")
        self.msg("binanceus", depth("BTCUSD", 100, *BOOK))
        self.msg("binance", depth("BTCUSD", 50, *BOOK))  # same name, other venue: not "out of order"
        self.msg("binanceus", trade("BTCUSD", 10))
        self.msg("binance", trade("BTCUSD", 500))
        self.msg("binanceus", trade("BTCUSD", 11))
        self.assertEqual(self.stat("binance.books_out_of_order") + self.stat("binanceus.books_out_of_order"), 0)
        self.assertEqual(self.stat("binance.trade_gaps") + self.stat("binanceus.trade_gaps"), 0)

    def test_reconnect_resets_only_that_venue(self):
        self.msg("binanceus", depth("BTCUSD", 100, *BOOK))
        self.msg("binance", depth("BTCUSD", 100, *BOOK))
        self.send("binance", "ws_open")
        self.msg("binance", depth("BTCUSD", 90, *BOOK))  # fresh connection: fine
        self.msg("binanceus", depth("BTCUSD", 90, *BOOK))  # same connection: older, ignored
        self.assertEqual((self.stat("binance.books_out_of_order"), self.stat("binanceus.books_out_of_order")), (0, 1))

    def test_checks_under_its_own_name(self):
        self.msg("binance", trade("BTCUSDT", 10))
        out = self.msg("binance", trade("BTCUSDT", 13))
        self.assertEqual(self.stat("binance.trade_gaps"), 1)
        self.assertIn("binance BTCUSDT TRADE GAP 10 -> 13", str(out))
        self.msg("binance", {"stream": "btcusdt@bookTicker", "data": {"u": 7, "b": "64000.1", "a": "64001"}})
        self.msg("binance", depth("BTCUSDT", 7, *BOOK))
        self.assertEqual(self.stat("binance.top_check_ok"), 1)
        out = self.msg("binance", depth("BTCUSDT", 8, [("64002", "1")], [("64001", "1")]))
        self.assertIn("binance BTCUSDT BAD BOOK", str(out))
        out = self.msg("binance", {"code": 2, "msg": "x"})
        self.assertIn("binance error:", str(out))


class Spy:
    def __init__(self, mm, name):
        self.calls = []
        real = getattr(mm, name)

        def wrapper(*args):
            self.calls.append(args)
            return real(*args)
        setattr(mm, name, wrapper)


CB = coinbase_l2(1, "snapshot", "BTC-USD", [("bid", "64000", "1"), ("offer", "64001", "1")])


class TestBinanceReference(Feed):
    PAPER = {"symbols": ["BTC/USD"], "spread": "0.0005", "ref": "binance"}

    def setUp(self):
        super().setUp()
        self.mm = self.sm.paper["BTC/USD"]
        self.ref = Spy(self.mm, "on_reference")

    def test_binance_book_is_the_reference(self):
        self.msg("binance", depth("BTCUSDT", 1, *BOOK))
        self.assertEqual(self.ref.calls[-1][1], D("64000.55"))

    def test_coinbase_is_not(self):
        self.msg("coinbase", {"channel": "l2_data", "sequence_num": 0, "events": []})
        self.send("coinbase", "ws_in", {"text": CB})
        self.assertEqual(self.ref.calls, [])

    def test_bad_book_drops_the_reference(self):
        self.msg("binance", depth("BTCUSDT", 1, *BOOK))
        self.msg("binance", depth("BTCUSDT", 2, [("64002", "1")], [("64001", "1")]))
        self.assertIsNone(self.ref.calls[-1][1])

    def test_binance_disconnect_drops_it_coinbase_does_not(self):
        self.send("coinbase", "conn_close", {"reason": "x"})
        self.assertEqual(self.ref.calls, [])
        self.send("binance", "conn_close", {"reason": "24h"})
        self.assertEqual(len(self.ref.calls), 1)
        self.assertIsNone(self.ref.calls[0][1])

    def test_other_pairs_and_binance_us_ignored(self):
        self.msg("binance", depth("LINKUSDT", 1, [("9", "1")], [("9.01", "1")]))
        self.msg("binanceus", depth("BTCUSD", 1, *BOOK))
        self.assertEqual(self.ref.calls, [])

    def test_binance_us_usdt_pair_is_not_the_reference(self):
        self.msg("binanceus", depth("BTCUSDT", 1, *BOOK))  # Binance.US lists USDT pairs too
        self.assertEqual(self.ref.calls, [])

    def test_binance_never_feeds_quotes_or_fills(self):
        trades = Spy(self.mm, "on_trade")
        books = Spy(self.mm, "on_book")
        self.msg("binance", depth("BTCUSDT", 1, *BOOK))
        self.msg("binance", trade("BTCUSDT", 1))
        self.assertEqual((trades.calls, books.calls), ([], []))

    def test_alarm_rings_on_a_binance_jump(self):
        self.mm.alarm_bps = D("2")
        self.mm.alarm_on = True
        self.msg("binance", depth("BTCUSDT", 1, [("64000", "1")], [("64001", "1")]))
        self.msg("binance", depth("BTCUSDT", 2, [("64020", "1")], [("64021", "1")]), dt_ms=50)  # ~3 bps up
        self.assertEqual(self.mm.stats["alarms_up"], 1)

    def test_settings_line(self):
        self.assertIn("coinbase=0 rate_limit=none ref=binance", "\n".join(self.sm.summary()))


class TestQuotingOnBinanceUs(Feed):
    PAPER = {"symbols": ["BTC/USD"], "spread": "0.0005", "venue": "binanceus"}

    def test_a_global_binance_usd_pair_never_feeds_quotes(self):
        mm = self.sm.paper["BTC/USD"]
        books, trades = Spy(mm, "on_book"), Spy(mm, "on_trade")
        self.msg("binance", depth("BTCUSD", 1, *BOOK))
        self.msg("binance", trade("BTCUSD", 1))
        self.assertEqual((books.calls, trades.calls), ([], []))
        self.msg("binanceus", depth("BTCUSD", 1, *BOOK))
        self.assertEqual(len(books.calls), 1)


class TestCoinbaseStillDefault(Feed):
    PAPER = {"symbols": ["BTC/USD"], "spread": "0.0005"}

    def test_coinbase_feeds_binance_does_not(self):
        mm = self.sm.paper["BTC/USD"]
        ref = Spy(mm, "on_reference")
        self.msg("binance", depth("BTCUSDT", 1, *BOOK))
        self.assertEqual(ref.calls, [])
        self.send("coinbase", "ws_in", {"text": CB})
        self.assertEqual(ref.calls[-1][1], D("64000.5"))
        self.send("binance", "conn_close", {"reason": "x"})
        self.assertEqual(len(ref.calls), 1)
        self.assertNotIn("ref=", "\n".join(self.sm.summary()))


class TestCommandLine(unittest.TestCase):
    def test_ref_only_written_when_set(self):
        import argparse
        p = argparse.ArgumentParser()
        add_paper_args(p)
        self.assertNotIn("ref", paper_config(p.parse_args(["--paper-mm"])))
        self.assertEqual(paper_config(p.parse_args(["--paper-mm", "--mm-ref", "binance"]))["ref"], "binance")

    def log(self, d):
        path = os.path.join(d, "x.jsonl")
        evs = [event(0, "binance", "ws_open", t=T0), event(1, "kraken", "ws_open", t=T0)]
        for i in range(200):
            px = 64000 + (i % 7)
            evs.append(event(len(evs), "binance", "ws_in", {"text": json.dumps(
                depth("BTCUSDT", i + 1, [(str(px), "1")], [(str(px + 1), "1")]))}, t=T0 + i * 50_000_000))
        write_log(path, evs)
        return path

    def test_leadlag_any_two_books(self):
        with tempfile.TemporaryDirectory() as d:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["leadlag", self.log(d), "--books", "binance:BTCUSDT,kraken:BTC/USD"])
        self.assertIn("leadlag binance BTCUSDT vs kraken BTC/USD:", buf.getvalue())

    def test_leadlag_bad_books(self):
        with tempfile.TemporaryDirectory() as d:
            path = self.log(d)
            for bad in ("binance:BTCUSDT", "binance,kraken:BTC/USD", "a:x,a:x", "a:x,b:y,c:z"):
                with self.assertRaises(SystemExit):
                    main(["leadlag", path, "--books", bad])

    def test_replay_with_binance_ref(self):
        with tempfile.TemporaryDirectory() as d:
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["replay", self.log(d), "--paper-mm", "BTC/USD", "--mm-ref", "binance"])
        self.assertIn("ref=binance", buf.getvalue())
        self.assertIn("summary binance.books_ok=200", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
