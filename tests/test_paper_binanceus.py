"""Paper market maker quoting on Binance.US's book and trades (--mm-venue binanceus)."""

import contextlib
import io
import json
import os
import tempfile
import unittest
from decimal import Decimal as D

from mmstack.books import Book
from mmstack.main import main
from mmstack.paper_mm import level_visible
from mmstack.state import StateMachine, bn_decimal
from tests.helpers import KrakenFeed, coinbase_l2, event, sample_session
from tests.test_binanceus import BOOK, depth, trade
from tests.test_replay import write_log

KRAKEN_TRADE = json.dumps({"channel": "trade", "type": "update", "data": [
    {"symbol": "BTC/USD", "side": "buy", "price": 64000.5, "qty": 0.01, "ord_type": "market",
     "trade_id": 1, "timestamp": "2026-11-12T22:00:00.000000Z"}]})
KRAKEN_INSTRUMENT = json.dumps({"channel": "instrument", "type": "snapshot", "data": {"assets": [], "pairs": [
    {"symbol": "BTC/USD", "price_increment": 0.1, "qty_increment": 0.00000001, "qty_min": 0.00005,
     "cost_min": 0.5, "status": "online"}]}})


class Spy:
    """Records calls to one market-maker method, then does what it did before."""

    def __init__(self, mm, name):
        self.calls = []
        real = getattr(mm, name)

        def wrapper(*args):
            self.calls.append(args)
            return real(*args)
        setattr(mm, name, wrapper)


class Venue(unittest.TestCase):
    VENUE = "binanceus"

    def setUp(self):
        cfg = {"symbols": ["BTC/USD"], "spread": "0.0005"}
        if self.VENUE:
            cfg["venue"] = self.VENUE
        self.sm = StateMachine(paper_mm=cfg)
        self.mm = self.sm.paper["BTC/USD"]
        self.n = 0

    def send(self, conn, type_, data=None):
        self.n += 1
        return self.sm.update(event(self.n, conn, type_, data))

    def bn(self, msg):
        return self.send("binanceus", "ws_in", {"text": json.dumps(msg)})

    def kraken(self, text):
        return self.send("kraken", "ws_in", {"text": text})


class TestSetup(Venue):
    def test_binance_settings(self):
        self.assertEqual((self.mm.book_depth, self.mm.rate_tier, self.mm.kraken_limits), (20, None, False))

    def test_rate_tier_is_ignored_on_binance(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.0005",
                                    "venue": "binanceus", "rate_tier": "intermediate"})
        self.assertIsNone(sm.paper["BTC/USD"].rate_tier)

    def test_kraken_is_the_default(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.0005", "rate_tier": "pro"})
        mm = sm.paper["BTC/USD"]
        self.assertEqual((sm.paper_venue, mm.book_depth, mm.rate_tier, mm.kraken_limits),
                         ("kraken", 10, "pro", True))


class TestBinanceFeedsThePaperMM(Venue):
    def test_binance_book_sets_the_mid(self):
        self.bn(depth("BTCUSD", 100, *BOOK))
        self.assertEqual(self.mm.last_mid, D("64000.55"))
        self.assertTrue(self.mm.book_ok)

    def test_tick_read_from_cleaned_prices(self):
        self.bn(depth("BTCUSD", 100, [("62310.42000000", "1")], [("62311.06000000", "1")]))
        self.assertEqual(self.mm.tick, D("0.01"))

    def test_kraken_book_is_ignored(self):
        self.bn(depth("BTCUSD", 100, *BOOK))
        self.kraken(KrakenFeed().message("snapshot", bids=[("70000.0", "1.5")], asks=[("70001.0", "1.5")]))
        self.assertEqual(self.mm.last_mid, D("64000.55"))

    def test_kraken_instrument_is_ignored(self):
        self.kraken(KRAKEN_INSTRUMENT)
        self.assertIsNone(self.mm.tick)

    def test_kraken_trades_are_ignored(self):
        spy = Spy(self.mm, "on_trade")
        self.kraken(KRAKEN_TRADE)
        self.assertEqual(spy.calls, [])

    def test_trade_sides(self):
        spy = Spy(self.mm, "on_trade")
        self.bn(trade("BTCUSD", 10, price="64000.10000000", qty="0.00100000", buyer_maker=True))
        self.bn(trade("BTCUSD", 11, price="64001.00000000", qty="0.002", buyer_maker=False))
        self.assertEqual([c[1:] for c in spy.calls],
                         [("sell", D("64000.1"), D("0.001")), ("buy", D("64001"), D("0.002"))])

    def test_repeated_trade_is_not_fed_twice(self):
        spy = Spy(self.mm, "on_trade")
        self.bn(trade("BTCUSD", 10))
        self.bn(trade("BTCUSD", 10))
        self.assertEqual(len(spy.calls), 1)

    def test_other_pairs_are_not_fed(self):
        spy = Spy(self.mm, "on_trade")
        self.bn(trade("LINKUSD", 10))
        self.bn(depth("LINKUSD", 1, [("9.27", "1")], [("9.28", "1")]))
        self.assertEqual(spy.calls, [])
        self.assertIsNone(self.mm.last_mid)

    def test_bad_book_pulls_quotes(self):
        self.bn(depth("BTCUSD", 100, *BOOK))
        self.assertTrue(self.mm.book_ok)
        self.bn(depth("BTCUSD", 101, [("64002", "1")], [("64001", "1")]))
        self.assertFalse(self.mm.book_ok)

    def test_binance_disconnect_reaches_the_paper_mm(self):
        spy = Spy(self.mm, "on_disconnect")
        self.send("kraken", "conn_close", {"reason": "x"})
        self.send("kraken", "ws_open")
        self.assertEqual(len(spy.calls), 0)
        self.send("binanceus", "conn_close", {"reason": "24h"})
        self.assertEqual(len(spy.calls), 1)
        self.send("binanceus", "ws_open")
        self.assertEqual(len(spy.calls), 2)

    def test_queue_seen_down_to_level_20(self):
        # quotes land ~13 levels deep: out of view on Kraken (10), in view here (20)
        bids = [(str(D("64000") - D("2.5") * i), "1") for i in range(20)]
        asks = [(str(D("64001") + D("2.5") * i), "1") for i in range(20)]
        self.bn(depth("BTCUSD", 1, bids, asks))
        bid, ask = self.mm.cur["bid"].price, self.mm.cur["ask"].price
        self.assertTrue(D(bids[13][0]) < bid < D(bids[12][0]), bid)
        self.assertTrue(D(asks[12][0]) < ask < D(asks[13][0]), ask)
        t = self.n
        self.sm.update(event(t + 1, "binanceus", "ws_in",
                             {"text": json.dumps(depth("BTCUSD", 2, bids, asks))}, t=self.sm.now + 200_000_000))
        self.assertEqual((self.mm.stats["queue_seen"], self.mm.stats["queue_unseen"]), (2, 0))
        self.assertEqual(self.mm.cur["bid"].level_size, D(0))  # an empty level in view, not "unknown"
        bids2 = bids[:13] + [(str(bid), "3")] + bids[13:19]
        self.sm.update(event(t + 2, "binanceus", "ws_in",
                             {"text": json.dumps(depth("BTCUSD", 3, bids2, asks))}, t=self.sm.now + 100_000_000))
        self.assertEqual(self.mm.cur["bid"].level_size, D(3))  # still tracked at level 14

    def test_coinbase_still_feeds_the_alarm_reference(self):
        spy = Spy(self.mm, "on_reference")
        self.send("coinbase", "ws_in", {"text": coinbase_l2(1, "snapshot", "BTC-USD",
                                                           [("bid", "64000", "1"), ("offer", "64001", "1")])})
        self.assertEqual(spy.calls[-1][1], D("64000.5"))


class TestKrakenVenueIgnoresBinance(Venue):
    VENUE = None

    def test_binance_book_and_trades_are_ignored(self):
        spy = Spy(self.mm, "on_trade")
        self.bn(depth("BTCUSD", 100, *BOOK))
        self.bn(trade("BTCUSD", 10))
        self.assertIsNone(self.mm.last_mid)
        self.assertEqual(spy.calls, [])

    def test_kraken_still_feeds(self):
        spy = Spy(self.mm, "on_trade")
        self.kraken(KrakenFeed().message("snapshot", bids=[("70000.0", "1.5")], asks=[("70001.0", "1.5")]))
        self.kraken(KRAKEN_TRADE)
        self.assertEqual(self.mm.last_mid, D("70000.5"))
        self.assertEqual(len(spy.calls), 1)

    def test_binance_disconnect_does_not_reach_the_paper_mm(self):
        spy = Spy(self.mm, "on_disconnect")
        self.send("binanceus", "conn_close", {"reason": "24h"})
        self.send("kraken", "conn_close", {"reason": "x"})
        self.assertEqual(len(spy.calls), 1)


class TestSummary(Venue):
    def test_settings_and_rate_lines(self):
        self.bn(depth("BTCUSD", 100, *BOOK))
        text = "\n".join(self.sm.summary())
        self.assertIn("coinbase=0 venue=binanceus rate_limit=not_modeled", text)
        self.assertIn("summary paper BTC/USD rate_limit not modeled (order actions=", text)
        self.assertNotIn("rate_limit_blocked", text)

    def test_kraken_summary_unchanged(self):
        sm = StateMachine(paper_mm={"symbols": ["BTC/USD"], "spread": "0.0005", "rate_tier": "intermediate"})
        text = "\n".join(sm.summary())
        self.assertIn("coinbase=0 rate_limit=intermediate", text)
        self.assertIn("rate_limit_blocked starter=", text)
        self.assertNotIn("venue=", text)


class TestHelpers(unittest.TestCase):
    def test_bn_decimal(self):
        for text, want in (("62310.42000000", "62310.42"), ("62300.00000000", "62300"),
                           ("0.00100000", "0.001"), ("5", "5"), ("100", "100")):
            self.assertEqual(str(bn_decimal(text)), want)

    def test_short_book_is_all_visible(self):
        book = Book()
        for i in range(15):
            book.bids.set(D(100 - i), D(1))
        self.assertTrue(level_visible(book.bids, D("50"), 20))  # fewer than 20 levels: we see the whole book
        self.assertFalse(level_visible(book.bids, D("50")))  # with 10 shown, 50 is past the 10th

    def test_visible_depth(self):
        book = Book()
        for i in range(20):
            book.bids.set(D(100 - i), D(1))
        self.assertFalse(level_visible(book.bids, D("85.5")))  # past Kraken's 10 levels
        self.assertTrue(level_visible(book.bids, D("85.5"), 20))
        self.assertFalse(level_visible(book.bids, D("80.5"), 20))


class TestCommandLine(unittest.TestCase):
    def run_replay(self, flags):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "s.jsonl")
            write_log(path, sample_session())
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                main(["replay", path, "--paper-mm", "BTC/USD"] + flags)
            return buf.getvalue()

    def test_venue_flag(self):
        self.assertIn("venue=binanceus rate_limit=not_modeled", self.run_replay(["--mm-venue", "binanceus"]))

    def test_config_only_names_a_venue_when_set(self):
        import argparse
        from mmstack.main import add_paper_args, paper_config
        p = argparse.ArgumentParser()
        add_paper_args(p)
        self.assertNotIn("venue", paper_config(p.parse_args(["--paper-mm"])))
        self.assertEqual(paper_config(p.parse_args(["--paper-mm", "--mm-venue", "binanceus"]))["venue"],
                         "binanceus")

    def test_default_has_no_venue(self):
        out = self.run_replay([])
        self.assertIn("rate_limit=intermediate", out)
        self.assertNotIn("venue=", out)


if __name__ == "__main__":
    unittest.main()


class TestQuietBookIsNotStale(unittest.TestCase):
    """Binance.US only sends a book when it changes. A quiet LINK book while the
    connection is busy with BTC is current, not stale: any Binance.US message
    keeps a good book alive. (Before: pulled after 10 s of no LINK book.)"""

    SEC = 1_000_000_000

    def setUp(self):
        self.sm = StateMachine(paper_mm={"symbols": ["LINK/USD"], "spread": "0.0005", "venue": "binanceus"})
        self.mm = self.sm.paper["LINK/USD"]
        self.n = 0
        self.t0 = 1_800_000_000 * self.SEC

    def at(self, s, conn, type_, data=None):
        self.n += 1
        return self.sm.update(event(self.n, conn, type_, data, t=self.t0 + int(s * self.SEC)))

    def bn(self, s, msg):
        return self.at(s, "binanceus", "ws_in", {"text": json.dumps(msg)})

    def link_book(self, s):
        return self.bn(s, depth("LINKUSD", s + 1, [("9.270", "50")], [("9.280", "50")]))

    def test_busy_connection_keeps_a_quiet_book_alive(self):
        self.link_book(0)
        for s in range(1, 30):  # BTC traffic every second, LINK's book never changes
            self.bn(s, depth("BTCUSD", 100 + s, *BOOK))
        out = self.at(30, "main", "timer")
        self.assertFalse(self.mm.pulled)
        self.assertEqual(self.mm.stats["stale_pulls"], 0)
        self.assertEqual(out, [])

    def test_a_silent_connection_still_goes_stale(self):
        self.link_book(0)
        out = self.at(11, "main", "timer")
        self.assertTrue(self.mm.pulled)
        self.assertTrue(any("quotes pulled" in line for line in out))

    def test_alive_never_brings_back_pulled_quotes(self):
        self.link_book(0)
        self.at(11, "main", "timer")  # pulled
        self.bn(12, depth("BTCUSD", 200, *BOOK))
        self.assertTrue(self.mm.pulled)
        self.assertEqual(self.mm.last_book_t, self.t0)  # its age isn't refreshed either

    def test_a_bad_book_is_not_kept_alive(self):
        self.link_book(0)
        self.bn(1, depth("LINKUSD", 50, [("9.29", "1")], [("9.28", "1")]))  # crossed: bad
        self.bn(2, depth("BTCUSD", 300, *BOOK))
        self.assertFalse(self.mm.book_ok)
        self.assertEqual(self.mm.last_book_t, self.t0)

    def test_other_pairs_keep_it_alive_for_60s_at_most(self):
        """If only LINK's feed froze while BTC kept talking, BTC's messages
        must not keep an old LINK book 'current' forever (2026-11-26 review):
        after 60 s with no LINK message of its own, the normal 10 s stale
        rule takes over."""
        self.link_book(0)
        for s in range(1, 80):
            self.bn(s, depth("BTCUSD", 100 + s, *BOOK))
            if s == 65:
                self.at(65, "main", "timer")
                self.assertFalse(self.mm.pulled)  # 60 s of help, then 10 s of normal staleness
        self.at(80, "main", "timer")
        self.assertTrue(self.mm.pulled)
        self.assertEqual(self.mm.last_book_t, self.t0 + 60 * self.SEC)

    def test_its_own_message_restarts_the_60s(self):
        self.link_book(0)
        for s in range(1, 106):
            if s == 50:
                self.link_book(50)
            else:
                self.bn(s, depth("BTCUSD", 100 + s, *BOOK))
        self.at(106, "main", "timer")
        self.assertFalse(self.mm.pulled)  # own book at 50: helped until 110

    def test_only_when_quoting_on_binanceus(self):
        sm = StateMachine(paper_mm={"symbols": ["LINK/USD"], "spread": "0.0005"})  # Kraken booth
        mm = sm.paper["LINK/USD"]
        spy = Spy(mm, "on_feed_alive")
        sm.update(event(1, "binanceus", "ws_in", {"text": json.dumps(
            depth("LINKUSD", 1, [("9.270", "50")], [("9.280", "50")]))}))  # recorded, valid, not ours
        sm.update(event(2, "binanceus", "ws_in", {"text": json.dumps(depth("BTCUSD", 2, *BOOK))}))
        self.assertEqual(spy.calls, [])  # Binance.US traffic must not keep a Kraken book "alive"
