"""Step 10 (paper): hand-worked checks of the paper market maker."""

import unittest
from decimal import Decimal as D

from mmstack.books import Book
from mmstack.paper_mm import (RATE_TIERS, PaperMarketMaker, RateCounter, cancel_cost,
                              level_visible, tick_from_prices)
from mmstack.state import StateMachine
from tests.helpers import T0, KrakenFeed, event

MS = 1_000_000
SEC = 1_000_000_000
LAT = 100 * MS


def book(bids, asks):
    """bids/asks: a price string, or a list of (price, qty)."""
    b = Book()
    for side, levels in ((b.bids, bids), (b.asks, asks)):
        if isinstance(levels, str):
            levels = [(levels, "1")]
        for p, q in levels:
            side.set(D(p), D(q))
    return b


def mm(**kw):
    """Round numbers: mid 100, spread 1% -> quotes 1.00 from mid, size 1, max inv 2,
    latency 100 ms."""
    args = dict(spread=D("0.01"), max_inv_multiple=2, requote=D("0.1"),
                latency_ns=LAT, stale_ns=10 * SEC, size=D("1"))
    args.update(kw)
    m = PaperMarketMaker("TEST", **args)
    m.set_instrument({"price_increment": D("0.01"), "qty_min": D("1")})
    return m


FLAT = ("99.99", "100.01")  # mid 100


def started(m, bids=FLAT[0], asks=FLAT[1]):
    """Quote on this book and wait until the quotes are live (and looked at)."""
    bk = book(bids, asks)
    m.on_book(T0, bk, True)
    m.on_book(T0 + LAT, bk, True)
    return bk


class TestWantedQuotes(unittest.TestCase):
    def test_flat_inventory_symmetric(self):
        m = mm()
        started(m)
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

    def test_no_skew_is_symmetric_but_keeps_the_cap(self):
        m = mm(skew=False)
        m.inventory = D(1)
        self.assertEqual(m.wanted_quotes(D(100)), (D("99.00"), D("101.00")))
        m.inventory = D(-2)  # at max short
        self.assertEqual(m.wanted_quotes(D(100)), (D("99.00"), None))

    def test_rounds_away_from_mid(self):
        m = mm()
        bid, ask = m.wanted_quotes(D("100.005"))  # d = 1.00005
        self.assertEqual((bid, ask), (D("99.00"), D("101.01")))

    def test_never_crosses_the_book(self):
        m = mm(spread=D("0.00001"))
        m.on_book(T0, book("99.00", "101.00"), True)  # wide market, tiny spread
        self.assertLess(m.bid.price, D("101.00"))
        self.assertGreater(m.ask.price, D("99.00"))

    def test_bad_fill_mode_rejected(self):
        with self.assertRaises(ValueError):
            mm(fills="maybe")


class TestRequoteAndLatency(unittest.TestCase):
    def test_small_move_keeps_the_quote(self):
        m = mm()
        started(m)
        first = m.bid
        m.on_book(T0 + SEC, book("100.04", "100.06"), True)  # mid +0.05 < 0.1 threshold
        self.assertIs(m.bid, first)
        self.assertIsNone(first.dead_at)
        self.assertEqual(m.stats["requotes"], 0)

    def test_big_move_replaces_after_latency(self):
        m = mm()
        started(m)
        first = m.bid
        t = T0 + SEC
        m.on_book(t, book("100.19", "100.21"), True)  # mid +0.20 -> requote
        self.assertIs(m.bid, first)  # the old quote is still on the book...
        self.assertEqual(first.dead_at, t + LAT)  # ...until the cancel lands
        self.assertEqual(m.nxt["bid"].price, D("99.19"))  # 100.20 - 1.002 = 99.198, floored
        m.on_timer(t + LAT)
        self.assertEqual(m.bid.price, D("99.19"))
        self.assertEqual(m.bid.live_at, t + LAT)

    def test_one_change_in_flight_per_side(self):
        m = mm()
        started(m)
        t = T0 + SEC
        m.on_book(t, book("100.19", "100.21"), True)
        m.on_book(t + 50 * MS, book("100.49", "100.51"), True)  # moves again mid-flight
        self.assertEqual(m.nxt["bid"].price, D("99.19"))  # unchanged
        self.assertEqual(m.stats["requotes"], 2)  # one per side, not two
        m.on_book(t + LAT, book("100.49", "100.51"), True)  # landed: now it can move again
        self.assertEqual(m.nxt["bid"].price, D("99.49"))

    def test_new_quote_not_live_before_latency(self):
        m = mm()
        m.on_book(T0, book(*FLAT), True)
        self.assertEqual(m.on_trade(T0 + 50 * MS, "sell", D("98.00"), D("5")), [])
        self.assertTrue(m.on_trade(T0 + 150 * MS, "sell", D("98.00"), D("5")))

    def test_stale_fill_on_a_quote_being_cancelled(self):
        """Price drops, we decide to move the bid down, and a trade hits the
        OLD bid 50 ms later. That fill must count (the review's problem 2)."""
        m = mm()
        started(m)
        t = T0 + SEC
        m.on_book(t, book("99.49", "99.51"), True)  # mid 99.50: bid should move to 98.50
        out = m.on_trade(t + 50 * MS, "sell", D("98.90"), D("5"))
        self.assertIn("buy 1 @ 99.00 (through stale)", out[0])
        self.assertTrue(m.fills[0]["stale"])
        # 50 ms after the cancel landed the old quote is gone
        m.on_timer(t + LAT)
        self.assertEqual(m.bid.price, D("98.50"))

    def test_pulled_quotes_stay_live_for_the_latency(self):
        m = mm()
        bk = started(m)
        t = T0 + SEC
        m.on_book(t, bk, False)  # book goes invalid: pull
        self.assertTrue(m.on_trade(t + 50 * MS, "buy", D("102.00"), D("5")))  # ask still there
        self.assertEqual(m.on_trade(t + 150 * MS, "sell", D("50.00"), D("5")), [])  # bid gone
        self.assertIsNone(m.bid)


class TestQueue(unittest.TestCase):
    BIDS = [("99.99", "1"), ("99.00", "5")]  # 5 already resting at our bid price

    def setUp(self):
        self.m = mm()
        self.bk = started(self.m, self.BIDS, "100.01")
        self.q = self.m.bid

    def test_queue_ahead_noted_when_live(self):
        self.assertEqual(self.q.price, D("99.00"))
        self.assertEqual(self.q.ahead, D(5))
        self.assertEqual(self.m.stats["queue_seen"], 2)  # bid and ask

    def test_trades_at_our_price_use_the_queue_first(self):
        self.assertEqual(self.m.on_trade(T0 + SEC, "sell", D("99.00"), D("3")), [])
        self.assertEqual(self.q.ahead, D(2))
        out = self.m.on_trade(T0 + 2 * SEC, "sell", D("99.00"), D("3"))  # 2 ahead, 1 reaches us
        self.assertIn("buy 1 @ 99.00 (queue)", out[0])

    def test_partial_fill_from_what_is_left(self):
        out = self.m.on_trade(T0 + SEC, "sell", D("99.00"), D("5.4"))
        self.assertIn("buy 0.4 @ 99.00 (queue)", out[0])

    def test_trade_then_book_update_counted_once(self):
        self.m.on_trade(T0 + SEC, "sell", D("99.00"), D("3"))
        self.bk.bids.set(D("99.00"), D("2"))  # the book catches up
        self.m.on_book(T0 + SEC + MS, self.bk, True)
        self.assertEqual(self.q.ahead, D(2))

    def test_book_update_then_trade_counted_once(self):
        self.bk.bids.set(D("99.00"), D("2"))  # book first: looks like a cancel of 3
        self.m.on_book(T0 + SEC, self.bk, True)
        self.assertEqual(self.q.ahead, D(2))  # 5 - 3*5/5
        self.assertEqual(self.m.on_trade(T0 + SEC + MS, "sell", D("99.00"), D("3")), [])
        self.assertEqual(self.q.ahead, D(2))  # recognised as the same 3, not 3 more

    def test_cancels_spread_evenly_and_joins_go_behind(self):
        self.bk.bids.set(D("99.00"), D("10"))  # 5 join behind us
        self.m.on_book(T0 + SEC, self.bk, True)
        self.assertEqual(self.q.ahead, D(5))
        self.bk.bids.set(D("99.00"), D("6"))  # 4 cancel: half the queue is ahead of us
        self.m.on_book(T0 + 2 * SEC, self.bk, True)
        self.assertEqual(self.q.ahead, D(3))  # 5 - 4 * 5/10

    def test_a_real_cancel_then_a_later_trade(self):
        self.bk.bids.set(D("99.00"), D("2"))
        self.m.on_book(T0 + SEC, self.bk, True)  # ahead 2
        out = self.m.on_trade(T0 + 3 * SEC, "sell", D("99.00"), D("3"))  # long after: a new trade
        self.assertIn("buy 1 @ 99.00 (queue)", out[0])

    def test_trade_through_fills_at_most_the_trade(self):
        """Our order is paper, not on the real book: had it been there, a taker
        selling 0.3 would have hit it for 0.3 and stopped. So a print through
        our price fills at most its own size (2026-11-26 review)."""
        out = self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("0.3"))
        self.assertIn("buy 0.3 @ 99.00 (through)", out[0])
        self.assertEqual(self.m.inventory, D("0.3"))

    def test_trade_through_bigger_than_us_fills_all(self):
        out = self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("5"))
        self.assertIn("buy 1 @ 99.00 (through)", out[0])

    def test_partial_through_fills_add_up(self):
        self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("0.3"))
        out = self.m.on_trade(T0 + SEC + MS, "sell", D("98.98"), D("0.9"))
        self.assertIn("buy 0.7 @ 99.00 (through", out[0])  # only what was left (now re-quoting: stale)
        self.assertEqual(self.m.inventory, D(1))

    def test_trade_through_empties_the_queue_ahead(self):
        """A print below our price means the real level at our price is empty:
        nothing is ahead of us any more, so the next print at our price reaches us."""
        self.m.on_trade(T0 + SEC, "sell", D("98.99"), D("0.3"))
        self.assertEqual(self.q.ahead, D(0))
        out = self.m.on_trade(T0 + SEC + MS, "sell", D("99.00"), D("0.2"))
        self.assertIn("buy 0.2 @ 99.00 (queue", out[0])

    def test_taker_side_mapping(self):
        # a taker BUY lifts asks: never our bid
        self.assertEqual(self.m.on_trade(T0 + SEC, "buy", D("98.00"), D("5")), [])
        out = self.m.on_trade(T0 + SEC, "buy", D("101.50"), D("5"))
        self.assertIn("sell 1 @ 101.00", out[0])


class TestQueueVisibility(unittest.TestCase):
    TEN = [(f"99.{90 + i}", "1") for i in range(10)]  # 99.90 .. 99.99: our 99.00 is out of view

    def test_inside_the_spread_means_nobody_ahead(self):
        m = mm()
        started(m, "98.50", "101.50")
        self.assertEqual(m.bid.ahead, D(0))
        out = m.on_trade(T0 + SEC, "sell", D("99.00"), D("0.4"))
        self.assertIn("buy 0.4 @ 99.00 (queue)", out[0])

    def test_level_visible(self):
        b = book(self.TEN, "100.01")
        self.assertFalse(level_visible(b.bids, D("99.00")))
        self.assertTrue(level_visible(b.bids, D("99.90")))
        self.assertTrue(level_visible(b.bids, D("99.985")))
        self.assertTrue(level_visible(book("99.99", "100.01").bids, D("50")))  # fewer than 10 levels

    def test_unknown_queue_no_at_price_fill_then_worst_case(self):
        m = mm()
        bk = started(m, self.TEN, "100.01")
        self.assertIsNone(m.bid.ahead)
        self.assertEqual(m.stats["queue_unseen"], 1)
        self.assertEqual(m.on_trade(T0 + SEC, "sell", D("99.00"), D("50")), [])  # can't know
        for p, _ in self.TEN[:9]:
            bk.bids.set(D(p), D("0"))
        bk.bids.set(D("99.00"), D("7"))  # now in view, 7 resting there
        m.on_book(T0 + 2 * SEC, bk, True)
        self.assertEqual(m.bid.ahead, D(7))  # worst case: all of it is ahead

    def test_out_of_view_and_back(self):
        m = mm()
        bk = started(m, [("99.99", "1"), ("99.00", "5")], "100.01")
        for i in range(10):  # ten better bids appear: 99.00 falls out of the top 10
            bk.bids.set(D(f"99.{80 + i}"), D("1"))
        bk.bids.truncate(10)
        m.on_book(T0 + SEC, bk, True)
        self.assertEqual(m.bid.ahead, D(5))  # unchanged, not treated as a cancel
        for i in range(10):
            bk.bids.set(D(f"99.{80 + i}"), D("0"))
        bk.bids.set(D("99.00"), D("3"))
        m.on_book(T0 + SEC + MS, bk, True)  # (same price target: no requote)
        self.assertEqual(m.bid.ahead, D(3))  # can't be more than what's there


class TestThroughMode(unittest.TestCase):
    def test_old_rule(self):
        m = mm(fills="through")
        started(m, [("99.99", "1"), ("99.00", "0.1")], "100.01")
        self.assertEqual(m.on_trade(T0 + SEC, "sell", D("99.00"), D("50")), [])  # at price: never
        out = m.on_trade(T0 + SEC, "sell", D("98.99"), D("0.3"))
        self.assertIn("buy 0.3 @ 99.00 (through)", out[0])  # at most the trade's size


class TestPnl(unittest.TestCase):
    def test_round_trip_pnl_fees_split_and_breakeven(self):
        m = mm()
        bk = started(m)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy 1 @ 99.00
        m.on_book(T0 + 2 * SEC, bk, True)  # long 1: ask moves to mid + 0.50 = 100.50
        self.assertEqual(m.ask.price, D("100.50"))
        m.on_trade(T0 + 3 * SEC, "buy", D("101.00"), D("1"))  # sell 1 @ 100.50
        self.assertEqual(m.inventory, D(0))
        self.assertEqual(m.pnl(), D("1.50"))  # 100.50 - 99.00
        self.assertEqual(m.notional, D("199.50"))
        self.assertEqual(m.pnl(D("0.004")), D("1.50") - D("199.50") * D("0.004"))  # 0.702
        lines = m.summary_lines(10 * SEC)
        self.assertIn("fills=2 (at_price=0 through=2 stale=0)", lines[0])
        self.assertIn("fee0.40%=0.7020", lines[1])
        self.assertIn("breakeven_fee=0.752%", lines[1])  # 1.50 / 199.50
        # bought 1.00 under mid, sold 0.50 over mid; mid never moved
        self.assertIn("spread_earned=1.5000 inventory_drift=0.0000", lines[2])

    def test_drift_is_what_the_spread_did_not_earn(self):
        m = mm()
        started(m)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy 1 @ 99, mid 100: earned 1.00
        m.on_book(T0 + 2 * SEC, book("97.99", "98.01"), True)  # mid falls to 98
        self.assertEqual(m.pnl(), D("-1.00"))
        self.assertEqual(m.spread_earned, D("1.00"))
        self.assertIn("inventory_drift=-2.0000", m.summary_lines(10 * SEC)[2])


class TestMarkouts(unittest.TestCase):
    def test_markout_sign_and_horizons(self):
        m = mm()
        started(m)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy @ 99.00
        m.on_book(T0 + SEC + SEC // 2, book("98.00", "98.02"), True)  # mid drops to 98.01
        m.on_timer(T0 + 2 * SEC + 1)  # 1 s after the fill
        self.assertEqual(len(m.markouts[1 * SEC]), 1)
        self.assertEqual(m.markouts[5 * SEC], [])
        # bought 99.00, mid 98.01: -0.99 / 99 = -100 bps (picked off)
        self.assertEqual(m.markouts[1 * SEC][0], (0, D("-100")))
        m.on_timer(T0 + 7 * SEC)
        self.assertEqual(m.markouts[5 * SEC][0][1], D("-100"))
        self.assertIn("through=-100.00bps", m.summary_lines(10 * SEC)[4])

    def test_markout_uses_mid_in_effect_at_the_horizon(self):
        """A book update that arrives AFTER the horizon must not count."""
        m = mm()
        started(m)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("1"))  # buy @ 99.00, mid 100
        m.on_book(T0 + 3 * SEC, book("98.00", "98.02"), True)  # first update 2 s after the fill
        # at the 1 s mark mid was still 100: +1.00 / 99 = +101.01 bps
        self.assertEqual(round(m.markouts[1 * SEC][0][1], 2), D("101.01"))


class TestStale(unittest.TestCase):
    def test_pull_when_stale_then_back_on_fresh_data(self):
        m = mm()
        bk = started(m)
        self.assertEqual(m.on_timer(T0 + 5 * SEC), [])
        out = m.on_timer(T0 + 11 * SEC)
        self.assertIn("pulled", out[0])
        m.on_timer(T0 + 12 * SEC)
        self.assertIsNone(m.bid)
        self.assertEqual(m.on_trade(T0 + 12 * SEC, "sell", D("50"), D("5")), [])
        m.on_book(T0 + 13 * SEC, bk, True)
        self.assertIsNotNone(m.bid)

    def test_invalid_book_pulls(self):
        m = mm()
        bk = started(m)
        m.on_book(T0 + SEC, bk, False)
        self.assertEqual(m.stats["stale_pulls"], 1)
        self.assertEqual(m.bid.dead_at, T0 + SEC + LAT)


class TestRateCounter(unittest.TestCase):
    def test_kraken_cancel_costs_by_order_age(self):
        costs = [cancel_cost(s * SEC) for s in (3, 7, 12, 30, 60, 100, 400)]
        self.assertEqual(costs, [8, 6, 5, 4, 2, 1, 0])

    def test_kraken_docs_example(self):
        # Kraken: 20 orders placed and cancelled within 5 s = 20 * (1 + 8) = 180 points,
        # exactly the Pro threshold; one more is blocked.
        c = RateCounter(*RATE_TIERS["pro"])
        for _ in range(20):
            c.charge(T0, D(1))
            c.charge(T0, D(8))
        self.assertEqual((c.counter, c.blocked), (D(180), 0))
        c.charge(T0, D(1))
        self.assertEqual(c.blocked, 1)
        c.charge(T0 + SEC, D(1))  # 3.75 decays in a second: room again
        self.assertEqual(c.blocked, 1)
        self.assertEqual(c.counter, D("177.25"))

    def test_decay_never_goes_below_zero(self):
        c = RateCounter(*RATE_TIERS["starter"])
        c.charge(T0, D(9))
        c.charge(T0 + 100 * SEC, D(1))
        self.assertEqual(c.counter, D(1))

    def test_requote_charges_cancel_and_add(self):
        m = mm()
        started(m)  # 2 places = 2 points
        m.on_book(T0 + SEC, book("100.19", "100.21"), True)  # both sides: cancel (8, age 1 s) + add (1)
        pro = m.rate["pro"]
        self.assertEqual(pro.actions, 6)
        self.assertEqual(pro.counter, D(18))  # the first 2 points decayed away in that second
        self.assertIn("(order actions=6)", m.summary_lines(10 * SEC)[6])


class TestHelpers(unittest.TestCase):
    def test_tick_from_prices(self):
        self.assertEqual(tick_from_prices([D("68337.3"), D("68337.4")]), D("0.1"))
        self.assertEqual(tick_from_prices([D("9.87614"), D("9.88")]), D("0.00001"))
        self.assertEqual(tick_from_prices([D("100")]), D("1"))

    def test_size_without_instrument_is_about_5_dollars(self):
        m = PaperMarketMaker("X")
        m.on_book(T0, book(*FLAT), True)
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

    def config(self, **extra):
        return dict({"symbols": ["BTC/USD"], "spread": "0.01", "max_inv": 5, "requote": "0.1",
                     "latency_ms": 150}, **extra)

    def test_fill_through_state_machine_and_snapshot_trades_ignored(self):
        sm = StateMachine(paper_mm=self.config())
        out = self.run_session(sm)
        fills = [x for x in out if "PAPER FILL" in x]
        self.assertEqual(len(fills), 1)  # the old snapshot trade at 1.0 must not fill
        self.assertIn("buy", fills[0])
        summary = "\n".join(sm.summary())
        self.assertIn("summary paper BTC/USD fills=1", summary)
        self.assertIn("spread=1.00%", summary)
        self.assertIn("fills=queue skew=on", summary)

    def test_fill_rule_and_skew_settings_reach_the_market_maker(self):
        sm = StateMachine(paper_mm=self.config(fills="through", skew=False))
        self.assertEqual(sm.paper["BTC/USD"].fill_mode, "through")
        self.assertFalse(sm.paper["BTC/USD"].skew)
        self.run_session(sm)
        self.assertIn("fills=through skew=off", "\n".join(sm.summary()))

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


class TestInventoryRoom(unittest.TestCase):
    """The max inventory is a hard limit: a side that adds inventory is only
    quoted if a full fill of it, plus anything still resting or being
    cancelled on that side, stays within the max (2026-11-26 review)."""

    def at_inventory(self, inv):
        """Long `inv` (size 1, max 2), quotes moved to match and landed."""
        m = mm()
        bk = started(m)
        t = T0 + SEC

        def settle(t):
            for _ in range(4):  # let re-quotes go out and land
                m.on_book(t, bk, True)
                m.on_timer(t + LAT)
                t += 2 * LAT
            return t
        while m.inventory < inv:  # buy in pieces of at most one order (through the bid)
            m.on_trade(t, "sell", D("90.00"), min(inv - m.inventory, D(1)))
            t = settle(t + MS)
        return m, t

    def test_reviewer_case_no_breach(self):
        # long 1.5 of max 2 with size 1: a full bid fill would make 2.5
        m, t = self.at_inventory(D("1.5"))
        self.assertEqual(m.inventory, D("1.5"))
        m.on_trade(t, "sell", D("90.00"), D("5"))  # a big sell through any bid
        self.assertLessEqual(m.inventory, m.max_inventory)

    def test_room_for_a_whole_order_still_quotes(self):
        m, t = self.at_inventory(D("1"))  # 1 + 1 = 2: exactly fits
        self.assertIsNotNone(m.bid)
        m.on_trade(t, "sell", D("90.00"), D("5"))
        self.assertEqual(m.inventory, D(2))

    def test_reducing_side_still_quoted(self):
        m, _ = self.at_inventory(D("1.5"))
        self.assertIsNotNone(m.ask)  # selling only reduces a long

    def test_dying_order_counts_as_exposure(self):
        """A bid being cancelled can still fill during the latency, so a new
        bid must leave room for both."""
        m = mm()
        bk = started(m)
        m.on_trade(T0 + SEC, "sell", D("98.00"), D("0.5"))  # long 0.5, bid 0.5 left resting
        t = T0 + 2 * SEC
        m.on_book(t, bk, True)
        # the 0.5 remainder is dying (re-quote to full size); exposure if both fill
        bids = [q for q in (m.cur["bid"], m.nxt["bid"]) if q is not None]
        self.assertLessEqual(m.inventory + sum(q.size for q in bids), m.max_inventory)

    def test_resting_order_cancelled_when_inventory_leaves_no_room(self):
        """With skew off the bid price doesn't move when inventory changes, so
        the old bid would just stay. If its fill could pass the max, it goes."""
        m = mm(skew=False)
        bk = started(m)
        m.on_trade(T0 + SEC, "sell", D("90.00"), D("1"))  # long 1, bid gone (filled)
        t = T0 + SEC + MS
        for _ in range(4):
            m.on_book(t, bk, True)
            m.on_timer(t + LAT)
            t += 2 * LAT
        self.assertIsNotNone(m.bid)  # 1 + 1 fits
        m.on_trade(t, "sell", D("90.00"), D("0.5"))  # long 1.5, the same bid has 0.5 left
        m.on_book(t + MS, bk, True)
        m.on_trade(t + 2 * MS, "sell", D("90.00"), D("5"))
        self.assertLessEqual(m.inventory, m.max_inventory)

    def test_old_order_and_its_replacement_together_stay_within_max(self):
        """Long 0.7 with an old bid of 0.7 left: a new full bid (1) fits on its
        own (1.7), but if the old one fills while its cancel travels AND then
        the new one fills, that's 2.4 of max 2. So: cancel first, re-place later."""
        m = mm()
        bk = started(m)
        t = T0 + SEC
        m.on_trade(t, "sell", D("90.00"), D("0.4"))  # long 0.4
        for _ in range(4):
            t += 2 * LAT
            m.on_book(t, bk, True)
            m.on_timer(t + LAT)
        t += 2 * LAT
        self.assertEqual(m.bid.size, D(1))  # a fresh full bid
        m.on_trade(t, "sell", D("90.00"), D("0.3"))  # long 0.7, 0.7 of the bid left
        m.on_book(t + MS, bk, True)  # inventory changed: the booth reacts
        m.on_trade(t + 50 * MS, "sell", D("90.00"), D("5"))  # hits the old bid in flight
        for i in range(1, 6):
            m.on_book(t + i * LAT, bk, True)
            m.on_trade(t + i * LAT + MS, "sell", D("90.00"), D("5"))  # and anything after
        self.assertLessEqual(m.inventory, m.max_inventory)

    def test_worst_inventory_in_summary(self):
        m, t = self.at_inventory(D("1.5"))
        m.on_trade(t, "buy", D("110.00"), D("5"))  # sell through our ask
        lines = "\n".join(m.summary_lines(10 * SEC))
        self.assertIn("summary paper TEST risk worst_inventory=1.5 of_max=75.0%", lines)


class TestAverageInventory(unittest.TestCase):
    def test_time_weighted(self):
        # flat for 1 s, then long 1 for 2 s: average = (0*1 + 1*2) / 3
        m = mm()
        started(m)
        m.on_trade(T0 + SEC, "sell", D("90.00"), D("1"))
        m.on_timer(T0 + 3 * SEC)
        line = m.summary_lines(3 * SEC)[-1]
        self.assertIn("worst_inventory=1 of_max=50.0% avg_inventory=0.66666667", line)
