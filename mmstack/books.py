"""Order book kept as exact Decimals (never floats).

Floats would break two things: Kraken's checksum (needs the exact digits the
exchange sent) and replay determinism.
"""

import zlib
from bisect import bisect_left, insort


class BookSide:
    """One side of the book: price -> qty, plus a sorted price list."""

    def __init__(self, is_bid):
        self.is_bid = is_bid
        self.levels = {}  # Decimal price -> Decimal qty
        self._prices = []  # ascending

    def set(self, price, qty):
        """Set the quantity at a price. qty 0 removes the level."""
        if qty == 0:
            if price in self.levels:
                del self.levels[price]
                del self._prices[bisect_left(self._prices, price)]
            return
        if price not in self.levels:
            insort(self._prices, price)
        self.levels[price] = qty

    def best(self):
        if not self._prices:
            return None
        return self._prices[-1] if self.is_bid else self._prices[0]

    def top(self, n):
        """Best n levels, best first, as (price, qty)."""
        prices = self._prices[::-1][:n] if self.is_bid else self._prices[:n]
        return [(p, self.levels[p]) for p in prices]

    def truncate(self, n):
        """Keep only the best n levels."""
        while len(self._prices) > n:
            worst = self._prices.pop(0) if self.is_bid else self._prices.pop()
            del self.levels[worst]

    def clear(self):
        self.levels.clear()
        self._prices.clear()

    def __len__(self):
        return len(self._prices)


class Book:
    def __init__(self):
        self.bids = BookSide(is_bid=True)
        self.asks = BookSide(is_bid=False)

    def best_bid(self):
        return self.bids.best()

    def best_ask(self):
        return self.asks.best()

    def mid(self):
        bb, ba = self.best_bid(), self.best_ask()
        if bb is None or ba is None:
            return None
        return (bb + ba) / 2

    def clear(self):
        self.bids.clear()
        self.asks.clear()


def _checksum_digits(value):
    """Kraken format: drop the '.', then drop leading zeros. '0.00100000' -> '100000'."""
    return format(value, "f").replace(".", "").lstrip("0")


def kraken_checksum(book):
    """CRC32 of the top 10 asks (low -> high) then top 10 bids (high -> low).

    Matches Kraken's WebSocket v2 'Book checksum' guide (tested against its
    documented example, expected 3310070434).
    """
    parts = []
    for price, qty in book.asks.top(10):
        parts.append(_checksum_digits(price) + _checksum_digits(qty))
    for price, qty in book.bids.top(10):
        parts.append(_checksum_digits(price) + _checksum_digits(qty))
    return zlib.crc32("".join(parts).encode("ascii")) & 0xFFFFFFFF
