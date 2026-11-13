import unittest
from decimal import Decimal as D

from mmstack.books import Book, BookSide, kraken_checksum


# Kraken's own example from the WebSocket v2 "Book checksum" guide.
DOC_ASKS = [("45285.2", "0.00100000"), ("45286.4", "1.54571953"), ("45286.6", "1.54571109"),
            ("45289.6", "1.54560911"), ("45290.2", "0.15890660"), ("45291.8", "1.54553491"),
            ("45294.7", "0.04454749"), ("45296.1", "0.35380000"), ("45297.5", "0.09945542"),
            ("45299.5", "0.18772827")]
DOC_BIDS = [("45283.5", "0.10000000"), ("45283.4", "1.54582015"), ("45282.1", "0.10000000"),
            ("45281.0", "0.10000000"), ("45280.3", "1.54592586"), ("45279.0", "0.07990000"),
            ("45277.6", "0.03310103"), ("45277.5", "0.30000000"), ("45277.3", "1.54602737"),
            ("45276.6", "0.15445238")]
DOC_CHECKSUM = 3310070434


def book_from(asks, bids):
    b = Book()
    for p, q in asks:
        b.asks.set(D(p), D(q))
    for p, q in bids:
        b.bids.set(D(p), D(q))
    return b


class TestBookSide(unittest.TestCase):
    def test_bid_best_is_highest(self):
        s = BookSide(is_bid=True)
        for p in ("10", "12", "11"):
            s.set(D(p), D("1"))
        self.assertEqual(s.best(), D("12"))
        self.assertEqual([p for p, _ in s.top(2)], [D("12"), D("11")])

    def test_ask_best_is_lowest(self):
        s = BookSide(is_bid=False)
        for p in ("10", "12", "11"):
            s.set(D(p), D("1"))
        self.assertEqual(s.best(), D("10"))
        self.assertEqual([p for p, _ in s.top(2)], [D("10"), D("11")])

    def test_zero_qty_removes_level(self):
        s = BookSide(is_bid=True)
        s.set(D("10"), D("1"))
        s.set(D("11"), D("1"))
        s.set(D("11"), D("0.00000000"))
        self.assertEqual(s.best(), D("10"))
        self.assertEqual(len(s), 1)

    def test_removing_missing_level_is_harmless(self):
        s = BookSide(is_bid=False)
        s.set(D("10"), D("0"))
        self.assertEqual(len(s), 0)
        self.assertIsNone(s.best())

    def test_update_existing_level_keeps_one_entry(self):
        s = BookSide(is_bid=False)
        s.set(D("10"), D("1"))
        s.set(D("10"), D("2"))
        self.assertEqual(len(s), 1)
        self.assertEqual(s.levels[D("10")], D("2"))

    def test_truncate_drops_worst_levels(self):
        bids = BookSide(is_bid=True)
        asks = BookSide(is_bid=False)
        for p in range(1, 6):
            bids.set(D(p), D("1"))
            asks.set(D(p), D("1"))
        bids.truncate(2)
        asks.truncate(2)
        self.assertEqual(sorted(bids.levels), [D(4), D(5)])  # highest bids kept
        self.assertEqual(sorted(asks.levels), [D(1), D(2)])  # lowest asks kept


class TestBook(unittest.TestCase):
    def test_mid_exact_decimal(self):
        b = book_from([("100.3", "1")], [("100.0", "1")])
        self.assertEqual(b.mid(), D("100.15"))

    def test_mid_none_when_one_side_empty(self):
        b = book_from([("100.3", "1")], [])
        self.assertIsNone(b.mid())


class TestKrakenChecksum(unittest.TestCase):
    def test_kraken_documented_example(self):
        self.assertEqual(kraken_checksum(book_from(DOC_ASKS, DOC_BIDS)), DOC_CHECKSUM)

    def test_order_of_inserts_does_not_matter(self):
        b = book_from(list(reversed(DOC_ASKS)), list(reversed(DOC_BIDS)))
        self.assertEqual(kraken_checksum(b), DOC_CHECKSUM)

    def test_only_top_10_counted(self):
        b = book_from(DOC_ASKS + [("45400.0", "9.0")], DOC_BIDS + [("45000.0", "9.0")])
        self.assertEqual(kraken_checksum(b), DOC_CHECKSUM)

    def test_trailing_zeros_matter(self):
        # 0.10000000 and 0.1 are equal numbers but different checksum text.
        asks = [(p, q) for p, q in DOC_ASKS]
        bids = [("45283.5", "0.1")] + DOC_BIDS[1:]
        self.assertNotEqual(kraken_checksum(book_from(asks, bids)), DOC_CHECKSUM)


if __name__ == "__main__":
    unittest.main()
