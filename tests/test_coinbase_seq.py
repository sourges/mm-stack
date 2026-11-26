"""Coinbase's sequence_num is ONE counter per connection, across every
channel and product (2026-11-26 review said it was per product; the real
recording says otherwise). This pins the working behavior with real data.

Fixture: the first 200 Coinbase lines of the 2026-11-13 recording (connection
setup lines dropped), messages unchanged except that the two book snapshots
are cut to their 10 best levels a side and trade snapshots to 5 trades.
BTC and LINK book updates, trades and heartbeats interleave in it."""

import gzip
import json
import os
import unittest

from mmstack.state import StateMachine

FIXTURE = os.path.join(os.path.dirname(__file__), "fixtures", "coinbase_mixed_20261113.jsonl.gz")


def load():
    with gzip.open(FIXTURE, "rt", encoding="utf-8") as f:
        return [json.loads(line) for line in f]


def messages(evs):
    return [json.loads(e["data"]["text"]) for e in evs if e["type"] == "ws_in"]


class TestRealCoinbaseOrder(unittest.TestCase):
    def test_fixture_really_interleaves(self):
        msgs = messages(load())
        channels = {m["channel"] for m in msgs}
        self.assertTrue({"l2_data", "market_trades", "heartbeats", "subscriptions"} <= channels)
        order = [ev["product_id"] for m in msgs if m["channel"] == "l2_data" for ev in m["events"]]
        switches = sum(1 for a, b in zip(order, order[1:]) if a != b)
        self.assertGreater(switches, 50)  # BTC and LINK alternate all the time
        seqs = [m["sequence_num"] for m in msgs]
        self.assertEqual(seqs, list(range(len(seqs))))  # one counter, +1 per message

    def test_no_seq_gap_on_real_order(self):
        sm = StateMachine()
        lines = []
        for e in load():
            lines += sm.update(e)
        self.assertNotIn("coinbase.seq_gaps", sm.stats)
        self.assertFalse([x for x in lines if "SEQ GAP" in x])
        self.assertEqual(sm.cb_last_seq, len(messages(load())) - 1)

    def test_a_per_product_counter_would_be_wrong_here(self):
        """What the review proposed: per product, this real feed would 'gap'
        almost every message."""
        last, gaps = {}, 0
        for m in messages(load()):
            for ev in m.get("events", []):
                p = ev.get("product_id")
                if m["channel"] != "l2_data" or p is None:
                    continue
                if p in last and m["sequence_num"] != last[p] + 1:
                    gaps += 1
                last[p] = m["sequence_num"]
        self.assertGreater(gaps, 100)

    def test_a_real_gap_is_still_caught(self):
        evs = load()
        ws = [i for i, e in enumerate(evs) if e["type"] == "ws_in"]
        del evs[ws[40]]  # one message lost
        sm = StateMachine()
        lines = []
        for e in evs:
            lines += sm.update(e)
        self.assertEqual(sm.stats.get("coinbase.seq_gaps"), 1)
        self.assertTrue(any("SEQ GAP 39 -> 41" in x for x in lines))


if __name__ == "__main__":
    unittest.main()
