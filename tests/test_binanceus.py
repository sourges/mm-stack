"""Binance.US: venue spec, full-book messages, trade-id gaps, top cross-check."""

import io
import json
import unittest
from decimal import Decimal as D

from mmstack import venues
from mmstack.eventlog import EventWriter
from mmstack.main import Output, build_clients, is_important
from mmstack.state import StateMachine
from tests.helpers import event


def depth(sym, uid, bids, asks):
    return {"stream": f"{sym.lower()}@depth20@100ms",
            "data": {"lastUpdateId": uid, "bids": [[p, q] for p, q in bids],
                     "asks": [[p, q] for p, q in asks]}}


def trade(sym, tid, price="64000.10", qty="0.001", buyer_maker=True):
    return {"stream": f"{sym.lower()}@trade",
            "data": {"e": "trade", "E": 1, "s": sym, "t": tid, "p": price, "q": qty,
                     "T": 1, "m": buyer_maker}}


def ticker(sym, uid, bid, ask):
    return {"stream": f"{sym.lower()}@bookTicker",
            "data": {"u": uid, "s": sym, "b": bid, "B": "1.0", "a": ask, "A": "1.0"}}


BOOK = ([("64000.10", "0.5"), ("63999.00", "1.2")], [("64001.00", "0.3"), ("64002.50", "2")])


class Feed(unittest.TestCase):
    def setUp(self):
        self.sm = StateMachine()
        self.n = 0
        self.send_type("ws_open")

    def send_type(self, type_, data=None):
        self.n += 1
        return self.sm.update(event(self.n, "binanceus", type_, data))

    def send(self, msg):
        return self.send_type("ws_in", {"text": json.dumps(msg)})

    def book(self, sym="BTCUSD"):
        return self.sm.books[("binanceus", sym)]

    def valid(self, sym="BTCUSD"):
        return self.sm.valid.get(("binanceus", sym))

    def stat(self, name):
        return self.sm.stats.get(f"binanceus.{name}", 0)


class TestVenue(unittest.TestCase):
    def test_streams_in_the_url(self):
        spec = venues.binanceus(["BTCUSD", "LINKUSD"])
        self.assertEqual((spec["name"], spec["host"], spec["port"]),
                         ("binanceus", "stream.binance.us", 9443))
        self.assertEqual(spec["path"], "/stream?streams="
                         "btcusd@depth20@100ms/btcusd@trade/btcusd@bookTicker/"
                         "linkusd@depth20@100ms/linkusd@trade/linkusd@bookTicker")
        self.assertEqual(spec["subscribe"], [])

    def test_live_builds_the_client_only_when_asked(self):
        w = EventWriter.__new__(EventWriter)  # never written to here
        names = [c.name for c in build_clients(w, ["BTC/USD"], ["BTC-USD"], binanceus_symbols=["BTCUSD"])]
        self.assertEqual(names, ["kraken", "coinbase", "binanceus"])
        self.assertEqual([c.name for c in build_clients(w, ["BTC/USD"], [], binanceus_symbols=[])],
                         ["kraken"])


class TestBook(Feed):
    def test_full_book_message(self):
        out = self.send(depth("BTCUSD", 100, *BOOK))
        self.assertTrue(self.valid())
        self.assertEqual((self.book().best_bid(), self.book().best_ask()), (D("64000.10"), D("64001.00")))
        self.assertEqual(self.book().bids.top(2), [(D("64000.10"), D("0.5")), (D("63999.00"), D("1.2"))])
        self.assertIn("binanceus BTCUSD bid=64000.1 ask=64001 mid=64000.55", out[0]["text"]
                      if isinstance(out[0], dict) else out[0])
        self.assertEqual(self.stat("books_ok"), 1)

    def test_each_message_replaces_the_book(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send(depth("BTCUSD", 101, [("64000.20", "1")], [("64001.00", "0.3")]))
        self.assertEqual(self.book().bids.top(5), [(D("64000.20"), D("1"))])

    def test_unchanged_top_prints_nothing(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.assertEqual(self.send(depth("BTCUSD", 101, *BOOK)), [])

    def test_older_message_is_ignored(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send(depth("BTCUSD", 99, [("1", "1")], [("2", "1")]))
        self.assertEqual(self.book().best_bid(), D("64000.10"))
        self.assertEqual(self.stat("books_out_of_order"), 1)

    def test_same_id_again_is_fine(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send(depth("BTCUSD", 100, *BOOK))
        self.assertEqual((self.stat("books_ok"), self.stat("books_out_of_order")), (2, 0))

    def test_crossed_book_is_not_used(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        out = self.send(depth("BTCUSD", 101, [("64002.00", "1")], [("64001.00", "1")]))
        self.assertFalse(self.valid())
        self.assertEqual(self.stat("books_bad"), 1)
        self.assertTrue(any("BTCUSD BAD BOOK bid=64002 ask=64001" in str(x) for x in out))
        self.assertTrue(any(is_important(str(x)) for x in out))
        self.assertEqual(self.book().best_bid(), D("64000.10"))  # the old copy is kept, but not trusted

    def test_touching_book_is_not_used(self):
        self.send(depth("BTCUSD", 100, [("64001.00", "1")], [("64001.00", "1")]))
        self.assertFalse(self.valid())

    def test_empty_bid_side_is_not_used(self):
        self.send(depth("BTCUSD", 100, [], [("64001.00", "1")]))
        self.assertFalse(self.valid())

    def test_empty_side_is_not_used(self):
        self.send(depth("BTCUSD", 100, [("64000.10", "1")], []))
        self.assertFalse(self.valid())
        self.assertEqual(self.stat("books_bad"), 1)

    def test_bad_book_line_only_once(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send(depth("BTCUSD", 101, [("1", "1")], []))
        self.assertEqual(self.send(depth("BTCUSD", 102, [("1", "1")], [])), [])
        self.assertEqual(self.stat("books_bad"), 2)

    def test_good_book_after_a_bad_one(self):
        self.send(depth("BTCUSD", 100, [("1", "1")], []))
        out = self.send(depth("BTCUSD", 101, *BOOK))
        self.assertTrue(self.valid())
        self.assertTrue(any("bid=64000.1 " in str(x) for x in out))

    def test_symbols_are_separate(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send(depth("LINKUSD", 5, [("9.27", "40")], [("9.28", "35")]))
        self.assertEqual(self.book("LINKUSD").mid(), D("9.275"))
        self.assertEqual(self.book().best_bid(), D("64000.10"))


class TestTopCheck(Feed):
    def test_same_update_id_and_same_top(self):
        self.send(ticker("BTCUSD", 100, "64000.10", "64001.00"))
        self.send(depth("BTCUSD", 100, *BOOK))
        self.assertEqual((self.stat("top_check_ok"), self.stat("top_check_differs")), (1, 0))

    def test_same_update_id_different_top(self):
        self.send(ticker("BTCUSD", 100, "64000.20", "64001.00"))
        self.send(depth("BTCUSD", 100, *BOOK))
        self.assertEqual((self.stat("top_check_ok"), self.stat("top_check_differs")), (0, 1))

    def test_ask_is_compared_too(self):
        self.send(ticker("BTCUSD", 100, "64000.10", "64001.50"))
        self.send(depth("BTCUSD", 100, *BOOK))
        self.assertEqual(self.stat("top_check_differs"), 1)

    def test_different_update_id_is_not_compared(self):
        self.send(ticker("BTCUSD", 99, "1", "2"))
        self.send(depth("BTCUSD", 100, *BOOK))
        self.assertEqual(self.stat("top_check_ok") + self.stat("top_check_differs"), 0)
        self.assertEqual(self.stat("tickers"), 1)


class TestTrades(Feed):
    def test_consecutive_ids(self):
        for tid in (10, 11, 12):
            self.assertEqual(self.send(trade("BTCUSD", tid)), [])
        self.assertEqual((self.stat("trades"), self.stat("trade_gaps")), (3, 0))

    def test_gap(self):
        self.send(trade("BTCUSD", 10))
        out = self.send(trade("BTCUSD", 14))
        self.assertEqual((self.stat("trade_gaps"), self.stat("trades_missed")), (1, 3))
        self.assertTrue(any("binanceus BTCUSD TRADE GAP 10 -> 14" in str(x) for x in out))
        self.assertTrue(is_important(str(out[0])))

    def test_gap_of_one(self):
        self.send(trade("BTCUSD", 10))
        self.send(trade("BTCUSD", 12))
        self.assertEqual((self.stat("trade_gaps"), self.stat("trades_missed")), (1, 1))

    def test_repeat_is_counted_not_a_gap(self):
        self.send(trade("BTCUSD", 10))
        self.send(trade("BTCUSD", 10))
        self.send(trade("BTCUSD", 11))
        self.assertEqual((self.stat("trades_repeated"), self.stat("trade_gaps")), (1, 0))

    def test_symbols_counted_separately(self):
        self.send(trade("BTCUSD", 10))
        self.send(trade("LINKUSD", 500))
        self.send(trade("BTCUSD", 11))
        self.assertEqual(self.stat("trade_gaps"), 0)


class TestReconnect(Feed):
    def test_new_connection_forgets_old_ids(self):
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send(trade("BTCUSD", 10))
        self.send(ticker("BTCUSD", 100, "64000.10", "64001.00"))
        self.send_type("conn_close", {"reason": "24h limit"})
        self.assertFalse(self.valid())
        self.send_type("ws_open")
        self.send(trade("BTCUSD", 500))  # trades missed while away are not a stream gap
        self.send(depth("BTCUSD", 90, *BOOK))  # ids compared per connection only
        self.assertEqual(self.stat("trade_gaps"), 0)
        self.assertEqual(self.stat("books_out_of_order"), 0)
        self.assertTrue(self.valid())
        self.assertEqual(self.stat("top_check_ok"), 0)  # the old ticker is gone

    def test_reconnect_without_a_close_event(self):
        self.send(trade("BTCUSD", 10))
        self.send(depth("BTCUSD", 100, *BOOK))
        self.send_type("ws_open")
        self.send(trade("BTCUSD", 50))
        self.send(depth("BTCUSD", 90, *BOOK))
        self.assertEqual((self.stat("trade_gaps"), self.stat("books_out_of_order")), (0, 0))

    def test_disconnect_line(self):
        out = self.send_type("conn_close", {"reason": "eof"})
        self.assertTrue(any("binanceus disconnected: eof" in str(x) for x in out))


class TestErrors(Feed):
    def test_error_message(self):
        out = self.send({"code": 2, "msg": "Invalid request"})
        self.assertTrue(any("binanceus error:" in str(x) for x in out))
        self.assertTrue(is_important(str(out[0])))

    def test_other_messages_are_ignored(self):
        self.assertEqual(self.send({"result": None, "id": 1}), [])


if __name__ == "__main__":
    unittest.main()
