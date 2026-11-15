"""Step 10 (paper): hand-worked checks of the paper market maker."""

import unittest
from decimal import Decimal as D

from mmstack.books import Book
from mmstack.paper_mm import PaperMarketMaker, tick_from_prices
from mmstack.state import StateMachine
from tests.helpers import T0, KrakenFeed, event

MS = 1_000_000
SEC = 1_000_000_000


def book(bid, ask):
    b = Book()
    b.bids.set(D(bid), D("1"))
    b.asks.set(D(ask), D("1"))
    return b


def mm(**kw):
    """Round numbers: mid 100, spread 1% -> quotes 1.00 from mid, size 1, max inv 2."""
    args = dict(spread=D("0.01"), max_inv_multiple=2, requote=D("0.1"),
                latency_ns=100 * MS, stale_ns=10 * SEC, size=D("1"))
    args.update(kw)
    m = PaperMarketMaker("TEST", **args)
    m.set_instrument({"price_increment": D("0.01"), "qty_min": D("1")})
    return m


class TestQuotes(unittest.TestCase):
    def test_flat_inventory_symmetric(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        self.assertEqual((m.bid.price, m.ask.price), (D("99.00"), D("101.00")))

    def test_guide_skew_when_long(self):
        # inventory 1 of max 2 -> x = 0.5, d = 1.00
        # bid (adds): d + d*x/(1-x) = 1 + 1 = 2.00 below mid
        # ask (reduces): d - d*x = 0.50 above mid
        m = mm()
        m.inventory = D(1)
        self.assertEqual(m.wanted_quotes(D(100)), (D("98.00"), D("100.50")))

    def test_guide_skew_when_short(self):
        m = mm()
        m.inventory = D(-1)
        self.assertEqual(m.wanted_quotes(D(100)), (D("99.50"), D("102.00")))

    def test_cap_stops_adding_side(self):
        m = mm()
        m.inventory = D(2)  # at max
        bid, ask = m.wanted_quotes(D(100))
        self.assertIsNone(bid)
        self.assertEqual(ask, D("100.00"))  # d - d*1 = 0: right at mid

    def test_rounds_away_from_mid(self):
        m = mm()
        bid, ask = m.wanted_quotes(D("100.005"))  # d = 1.00005
        self.assertEqual((bid, ask), (D("99.00"), D("101.01")))

    def test_requote_threshold_keeps_queue(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        first = m.bid
        m.on_book(T0 + MS, book("100.04", "100.06"), True)  # mid +0.05 < 0.1 threshold
        self.assertIs(m.bid, first)
        m.on_book(T0 + 2 * MS, book("100.19", "100.21"), True)  # mid +0.20 -> requote
        self.assertIsNot(m.bid, first)
        self.assertEqual(m.bid.price, D("99.19"))  # 100.20 - 1.002 = 99.198, floored to 99.19

    def test_never_crosses_the_book(self):
        m = mm(spread=D("0.00001"))
        m.on_book(T0, book("99.00", "101.00"), True)  # wide market, tiny spread
        self.assertLess(m.bid.price, D("101.00"))
        self.assertGreater(m.ask.price, D("99.00"))


class TestFills(unittest.TestCase):
    def setUp(self):
        self.m = mm()
        self.m.on_book(T0, book("99.99", "100.01"), True)  # bid 99.00 / ask 101.00, live at +100ms

    def test_trade_at_our_price_does_not_fill(self):
        self.assertEqual(self.m.on_trade(T0 + SEC, "sell", D("99.00"), D("5")), [])

    def test_trade_through_fills(self):
        out = self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("5"))
        self.assertIn("PAPER FILL TEST buy 1 @ 99.00", out[0])
        self.assertEqual(self.m.inventory, D(1))
        self.assertEqual(self.m.cash, D("-99.00"))

    def test_not_live_before_latency(self):
        self.assertEqual(self.m.on_trade(T0 + 50 * MS, "sell", D("98.00"), D("5")), [])
        self.assertTrue(self.m.on_trade(T0 + 150 * MS, "sell", D("98.00"), D("5")))

    def test_taker_side_mapping(self):
        # a taker BUY lifts asks: our ask fills, never our bid
        self.assertEqual(self.m.on_trade(T0 + SEC, "buy", D("98.00"), D("5")), [])
        out = self.m.on_trade(T0 + SEC, "buy", D("101.50"), D("5"))
        self.assertIn("sell 1 @ 101.00", out[0])

    def test_partial_fill_size(self):
        self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("0.3"))
        self.assertEqual(self.m.inventory, D("0.3"))

    def test_fill_requotes_with_new_skew(self):
        self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("0.5"))
        # inventory 0.5 of 2 -> x = 0.25: bid 1 + 1*0.25/0.75 = 1.3333 -> 98.66 (floor), ask 1 - 0.25 = 0.75 -> 100.75
        self.assertEqual((self.m.bid.price, self.m.ask.price), (D("98.66"), D("100.75")))
        self.assertEqual(self.m.bid.live_at, T0 + SEC + 100 * MS)


class TestPnl(unittest.TestCase):
    def test_round_trip_pnl_fees_and_breakeven(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy 1 @ 99.00
        m.on_book(T0 + 2 * SEC, book("99.99", "100.01"), True)
        # long 1: ask at mid + 0.50 = 100.50
        m.on_trade(T0 + 3 * SEC, "buy", D("101.00"), D("1"))  # sell 1 @ 100.50
        self.assertEqual(m.inventory, D(0))
        self.assertEqual(m.pnl(), D("1.50"))  # 100.50 - 99.00
        self.assertEqual(m.notional, D("199.50"))
        self.assertEqual(m.pnl(D("0.004")), D("1.50") - D("199.50") * D("0.004"))  # 0.702
        lines = m.summary_lines(10 * SEC)
        self.assertIn("fee0.40%=0.7020", lines[1])
        self.assertIn("breakeven_fee=0.752%", lines[1])  # 1.50 / 199.50

    def test_open_inventory_marked_to_mid(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy 1 @ 99
        m.on_book(T0 + 2 * SEC, book("97.99", "98.01"), True)  # mid 98
        self.assertEqual(m.pnl(), D("-1.00"))


class TestMarkouts(unittest.TestCase):
    def test_markout_sign_and_horizons(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy @ 99.00
        m.on_book(T0 + SEC + SEC // 2, book("98.00", "98.02"), True)  # mid drops to 98.01
        m.on_timer(T0 + 2 * SEC + 1)  # 1 s after the fill
        self.assertEqual(len(m.markouts[1 * SEC]), 1)
        self.assertEqual(m.markouts[5 * SEC], [])
        # bought 99.00, mid 98.01: -0.99 / 99 = -100 bps (picked off)
        self.assertEqual(m.markouts[1 * SEC][0], D("-100"))
        m.on_timer(T0 + 7 * SEC)
        self.assertEqual(m.markouts[5 * SEC][0], D("-100"))

    def test_markout_uses_mid_in_effect_at_the_horizon(self):
        """A book update that arrives AFTER the horizon must not count."""
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy @ 99.00, mid 100
        m.on_book(T0 + 3 * SEC, book("98.00", "98.02"), True)  # first update 2 s after the fill
        # at the 1 s mark mid was still 100: +1.00 / 99 = +101.01 bps
        self.assertEqual(round(m.markouts[1 * SEC][0], 2), D("101.01"))


class TestStale(unittest.TestCase):
    def test_pull_when_stale_and_no_fills_while_pulled(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        self.assertEqual(m.on_timer(T0 + 5 * SEC), [])
        out = m.on_timer(T0 + 11 * SEC)
        self.assertIn("pulled", out[0])
        self.assertIsNone(m.bid)
        self.assertEqual(m.on_trade(T0 + 12 * SEC, "sell", D("50"), D("5")), [])
        m.on_book(T0 + 13 * SEC, book("99.99", "100.01"), True)
        self.assertIsNotNone(m.bid)  # back after fresh data

    def test_invalid_book_pulls(self):
        m = mm()
        m.on_book(T0, book("99.99", "100.01"), True)
        m.on_book(T0 + MS, book("99.99", "100.01"), False)
        self.assertIsNone(m.bid)
        self.assertEqual(m.stats["stale_pulls"], 1)


class TestHelpers(unittest.TestCase):
    def test_tick_from_prices(self):
        self.assertEqual(tick_from_prices([D("68337.3"), D("68337.4")]), D("0.1"))
        self.assertEqual(tick_from_prices([D("9.87614"), D("9.88")]), D("0.00001"))
        self.assertEqual(tick_from_prices([D("100")]), D("1"))

    def test_size_without_instrument_is_about_5_dollars(self):
        m = PaperMarketMaker("X")
        m.on_book(T0, book("99.99", "100.01"), True)
        self.assertEqual(m.size, D("0.05"))
        self.assertEqual(m.tick, D("0.01"))


class TestInStateMachine(unittest.TestCase):
    def run_session(self, sm):
        k = KrakenFeed("BTC/USD")
        evs = [event(1, "kraken", "ws_open"),
               event(2, "kraken", "ws_in", {"text": k.message(
                   "snapshot", bids=[("99.9", "1.0")], asks=[("100.1", "1.0")])}),
               # after the 150 ms latency, so it WOULD fill if snapshots weren't ignored
               event(300, "kraken", "ws_in", {"text": '{"channel":"trade","type":"snapshot","data":'
                                            '[{"symbol":"BTC/USD","side":"sell","price":1.0,"qty":9.0}]}'}),
               event(1000, "kraken", "ws_in", {"text": '{"channel":"trade","type":"update","data":'
                                              '[{"symbol":"BTC/USD","side":"sell","price":98.0,"qty":9.0}]}'}),
               event(1100, "timer", "timer", {"n": 1, "late_ms": 0})]
        out = []
        for e in evs:
            out.extend(sm.update(e))
        return out

    def config(self):
        return {"symbols": ["BTC/USD"], "spread": "0.01", "max_inv": 5, "requote": "0.1",
                "latency_ms": 150}

    def test_fill_through_state_machine_and_snapshot_trades_ignored(self):
        sm = StateMachine(paper_mm=self.config())
        out = self.run_session(sm)
        fills = [x for x in out if "PAPER FILL" in x]
        self.assertEqual(len(fills), 1)  # the old snapshot trade at 1.0 must not fill
        self.assertIn("buy", fills[0])
        summary = "\n".join(sm.summary())
        self.assertIn("summary paper BTC/USD fills=1", summary)
        self.assertIn("spread=1.00%", summary)

    def test_session_event_turns_it_on(self):
        sm = StateMachine()
        sm.update(event(0, "main", "session", {"paper_mm": self.config()}))
        out = self.run_session(sm)
        self.assertTrue(any("PAPER FILL" in x for x in out))

    def test_deterministic(self):
        a = self.run_session(StateMachine(paper_mm=self.config()))
        b = self.run_session(StateMachine(paper_mm=self.config()))
        self.assertEqual(a, b)


if __name__ == "__main__":
    unittest.main()
