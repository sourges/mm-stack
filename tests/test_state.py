import time
import unittest
from unittest import mock

from mmstack.state import StateMachine, iso
from tests.helpers import KrakenFeed, coinbase_heartbeat, coinbase_l2, event, sample_session


def run(events):
    sm = StateMachine()
    out = []
    for ev in events:
        out.extend(sm.update(ev))
    return sm, out


class TestIso(unittest.TestCase):
    def test_iso_uses_integer_maths(self):
        self.assertEqual(iso(1_763_000_000_123_456_789), "2025-11-13T02:13:20.123456Z")


class TestKraken(unittest.TestCase):
    def setUp(self):
        self.k = KrakenFeed("BTC/USD")
        self.n = 0

    def ev(self, text):
        self.n += 1
        return event(self.n, "kraken", "ws_in", {"text": text})

    def test_snapshot_prints_top_of_book(self):
        sm, out = run([self.ev(self.k.message("snapshot",
                                               bids=[("100.0", "1.00000000")],
                                               asks=[("100.5", "2.00000000")]))])
        self.assertEqual(len(out), 1)
        self.assertIn("kraken BTC/USD bid=100.0 ask=100.5 mid=100.25", out[0])
        self.assertEqual(sm.stats["kraken.checksum_ok"], 1)

    def test_update_that_keeps_top_prints_nothing(self):
        evs = [self.ev(self.k.message("snapshot", bids=[("100.0", "1.0")], asks=[("100.5", "2.0")])),
               self.ev(self.k.message("update", bids=[("99.0", "5.0")]))]
        _, out = run(evs)
        self.assertEqual(len(out), 1)

    def test_update_that_moves_top_prints_new_line(self):
        evs = [self.ev(self.k.message("snapshot", bids=[("100.0", "1.0")], asks=[("100.5", "2.0")])),
               self.ev(self.k.message("update", bids=[("100.2", "1.0")]))]
        _, out = run(evs)
        self.assertIn("bid=100.2 ask=100.5", out[-1])

    def test_checksum_mismatch_invalidates_until_next_snapshot(self):
        evs = [self.ev(self.k.message("snapshot", bids=[("100.0", "1.0")], asks=[("100.5", "2.0")])),
               self.ev(self.k.message("update", bids=[("100.1", "1.0")], checksum=1)),
               self.ev(self.k.message("update", bids=[("100.2", "1.0")])),
               self.ev(self.k.message("snapshot", bids=[("100.3", "1.0")], asks=[("100.5", "2.0")]))]
        sm, out = run(evs)
        self.assertIn("CHECKSUM MISMATCH", out[1])
        self.assertEqual(len(out), 3)  # nothing printed for the skipped update
        self.assertIn("bid=100.3", out[2])
        self.assertEqual(sm.stats["kraken.checksum_bad"], 1)
        self.assertEqual(sm.stats["kraken.updates_skipped_invalid_book"], 1)

    def test_levels_beyond_depth_are_trimmed(self):
        # 11 bids: the 11th (worst) must be dropped or the checksum would fail.
        bids = [(f"{100 - i}.0", "1.0") for i in range(11)]
        evs = [self.ev(self.k.message("snapshot", bids=bids, asks=[("101.0", "1.0")]))]
        sm, out = run(evs)
        self.assertNotIn("kraken.checksum_bad", sm.stats)
        self.assertEqual(len(sm.books[("kraken", "BTC/USD")].bids), 10)

    def test_trades_counted(self):
        text = '{"channel":"trade","type":"update","data":[{"price":1.0,"qty":1.0},{"price":1.0,"qty":2.0}]}'
        sm, out = run([self.ev(text)])
        self.assertEqual(out, [])
        self.assertEqual(sm.stats["kraken.trades"], 2)

    def test_subscribe_error_reported(self):
        text = '{"method":"subscribe","success":false,"error":"Currency pair not supported"}'
        _, out = run([self.ev(text)])
        self.assertIn("subscribe error", out[0])


class TestCoinbase(unittest.TestCase):
    def ev(self, n, text):
        return event(n, "coinbase", "ws_in", {"text": text})

    def test_snapshot_and_update(self):
        evs = [event(1, "coinbase", "ws_open"),
               self.ev(2, coinbase_l2(1, "snapshot", "BTC-USD",
                                      [("bid", "10.00", "1"), ("offer", "10.50", "1")])),
               self.ev(3, coinbase_l2(2, "update", "BTC-USD", [("offer", "10.50", "0"),
                                                               ("offer", "10.40", "2")]))]
        _, out = run(evs)
        self.assertIn("coinbase connected", out[0])
        self.assertIn("coinbase BTC-USD bid=10.00 ask=10.50 mid=10.25", out[1])
        self.assertIn("bid=10.00 ask=10.40", out[2])

    def test_sequence_gap_invalidates_books(self):
        evs = [self.ev(1, coinbase_l2(1, "snapshot", "BTC-USD", [("bid", "10", "1"), ("offer", "11", "1")])),
               self.ev(2, coinbase_heartbeat(2)),
               self.ev(3, coinbase_l2(4, "update", "BTC-USD", [("bid", "10.5", "1")]))]
        sm, out = run(evs)
        self.assertIn("SEQ GAP 2 -> 4", out[-1])
        self.assertFalse(sm.valid[("coinbase", "BTC-USD")])
        self.assertEqual(sm.stats["coinbase.seq_gaps"], 1)

    def test_new_connection_resets_sequence(self):
        evs = [self.ev(1, coinbase_heartbeat(10)),
               event(2, "coinbase", "ws_open"),
               self.ev(3, coinbase_heartbeat(0))]
        sm, out = run(evs)
        self.assertNotIn("coinbase.seq_gaps", sm.stats)


class TestConnections(unittest.TestCase):
    def test_disconnect_invalidates_and_reports(self):
        k = KrakenFeed()
        evs = [event(1, "kraken", "ws_in", {"text": k.message("snapshot", bids=[("1.0", "1.0")],
                                                              asks=[("2.0", "1.0")])}),
               event(2, "kraken", "conn_close", {"reason": "idle timeout"})]
        sm, out = run(evs)
        self.assertIn("kraken disconnected: idle timeout", out[-1])
        self.assertFalse(sm.valid[("kraken", "BTC/USD")])


class TestDeterminism(unittest.TestCase):
    def test_same_events_same_output(self):
        _, a = run(sample_session())
        _, b = run(sample_session())
        self.assertEqual(a, b)

    def test_never_reads_the_system_clock(self):
        with mock.patch.object(time, "time", side_effect=AssertionError("clock used")), \
             mock.patch.object(time, "time_ns", side_effect=AssertionError("clock used")):
            run(sample_session())

    def test_output_time_comes_from_event(self):
        k = KrakenFeed()
        text = k.message("snapshot", bids=[("1.0", "1.0")], asks=[("2.0", "1.0")])
        _, out = run([event(1, "kraken", "ws_in", {"text": text}, t=0)])
        self.assertTrue(out[0].startswith("1970-01-01T00:00:00.000000Z"))

    def test_sample_session_output(self):
        sm, out = run(sample_session())
        text = "\n".join(out)
        self.assertIn("CHECKSUM MISMATCH", text)
        self.assertIn("SEQ GAP 3 -> 5", text)
        self.assertIn("coinbase disconnected", text)
        self.assertEqual(sm.stats["kraken.checksum_ok"], 4)


if __name__ == "__main__":
    unittest.main()
