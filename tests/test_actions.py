"""Step 4 (actions) and step 5 (one test order) in the state machine."""

import unittest
from decimal import ROUND_CEILING, Decimal as D

from mmstack.state import StateMachine, TEST_ORDER_HOLD_NS, round_to
from tests.helpers import (INSTRUMENT_BTC, T0, KrakenFeed, ack, coinbase_heartbeat, event,
                           executions)

SEC = 1_000_000_000


class Session:
    """Feeds events to a state machine with increasing seq/time."""

    def __init__(self, **sm_kwargs):
        self.sm = StateMachine(**sm_kwargs)
        self.seq = 0
        self.t = T0
        self.out = []
        self.actions = []
        self.k = KrakenFeed("BTC/USD")

    def feed(self, conn, type_, data=None, dt=SEC // 100):
        self.seq += 1
        self.t += dt
        lines = self.sm.update(event(self.seq, conn, type_, data, t=self.t))
        self.out.extend(lines)
        new = self.sm.take_actions()
        self.actions.extend(new)
        return lines, new

    def priv(self, text, dt=SEC // 100):
        return self.feed("kraken_priv", "ws_in", {"text": text}, dt)

    def pub(self, text, dt=SEC // 100):
        return self.feed("kraken", "ws_in", {"text": text}, dt)

    def ready(self, bid="90001.3", instrument=True, private=True):
        if private:
            self.feed("kraken_priv", "ws_open")
            self.priv(executions("snapshot", []))
        if instrument:
            self.pub(INSTRUMENT_BTC)
        return self.pub(self.k.message("snapshot", bids=[(bid, "1.00000000")],
                                       asks=["90002.0 1.00000000".split()]))

    def tick(self, dt):
        """Any public event later in time (the book keeps ticking in real life)."""
        return self.pub(self.k.message("update", bids=[("1.0", "0.5")]), dt=dt)


class TestTestOrderPlace(unittest.TestCase):
    def test_places_one_post_only_buy_5pct_under_bid(self):
        s = Session(test_order_symbol="BTC/USD")
        _, actions = s.ready(bid="90001.3")
        self.assertEqual(len(actions), 1)
        a = actions[0]
        self.assertEqual(a["action"], "place")
        self.assertEqual(a["conn"], "kraken_priv")
        self.assertEqual(a["side"], "buy")
        self.assertEqual(a["price"], "85501.2")  # 90001.3 * 0.95 = 85501.235, rounded DOWN to 0.1
        self.assertEqual(a["qty"], "0.00005")  # qty_min (never "5E-5": Kraken needs plain digits)
        self.assertLessEqual(len(a["cl_ord_id"]), 18)
        self.assertTrue(a["cl_ord_id"].startswith("mm"))

    def test_only_one_order_ever(self):
        s = Session(test_order_symbol="BTC/USD")
        s.ready()
        for _ in range(5):
            s.tick(SEC)
        self.assertEqual([a["action"] for a in s.actions], ["place"])

    def test_waits_for_private_feed(self):
        s = Session(test_order_symbol="BTC/USD")
        s.ready(private=False)
        self.assertEqual(s.actions, [])

    def test_waits_for_instrument_info(self):
        s = Session(test_order_symbol="BTC/USD")
        s.ready(instrument=False)
        self.assertEqual(s.actions, [])

    def test_no_order_without_flag(self):
        s = Session()
        s.ready()
        self.assertEqual(s.actions, [])

    def test_cost_min_raises_qty(self):
        s = Session(test_order_symbol="BTC/USD")
        s.pub(INSTRUMENT_BTC.replace('"cost_min":0.5', '"cost_min":10'))
        s.feed("kraken_priv", "ws_open")
        s.priv(executions("snapshot", []))
        _, actions = s.pub(s.k.message("snapshot", bids=[("90001.3", "1.0")], asks=[("90002.0", "1.0")]))
        expected = round_to(D(10) / D("85501.2"), D("0.00000001"), ROUND_CEILING)
        self.assertEqual(actions[0]["qty"], format(expected, "f"))
        self.assertGreaterEqual(D(actions[0]["qty"]) * D("85501.2"), D(10))

    def test_session_event_turns_it_on(self):
        s = Session()
        s.feed("main", "session", {"test_order": "BTC/USD"})
        _, actions = s.ready()
        self.assertEqual(actions[0]["action"], "place")

    def test_pretend_private_for_dry_run(self):
        s = Session(test_order_symbol="BTC/USD", pretend_private_ready=True)
        _, actions = s.ready(private=False)
        self.assertEqual(actions[0]["action"], "place")


class TestTestOrderLifecycle(unittest.TestCase):
    def placed(self):
        s = Session(test_order_symbol="BTC/USD")
        _, actions = s.ready()
        self.cl = actions[0]["cl_ord_id"]
        return s

    def test_full_cycle(self):
        s = self.placed()
        s.priv(ack("add_order"))
        s.priv(executions("update", [{"cl_ord_id": self.cl, "exec_type": "pending_new",
                                      "order_status": "pending_new"}]))
        s.priv(executions("update", [{"cl_ord_id": self.cl, "exec_type": "new",
                                      "order_status": "new"}]))
        _, a = s.tick(TEST_ORDER_HOLD_NS - SEC)  # 4 s later: too early
        self.assertEqual(a, [])
        _, a = s.tick(2 * SEC)  # past 5 s
        self.assertEqual(a, [{"id": 2, "action": "cancel", "conn": "kraken_priv", "cl_ord_id": self.cl}])
        s.priv(ack("cancel_order"))
        lines, _ = s.priv(executions("update", [{"cl_ord_id": self.cl, "exec_type": "canceled",
                                                 "order_status": "canceled", "reason": "User requested"}]))
        self.assertIn("TEST ORDER COMPLETE", lines[-1])
        self.assertIn("summary test_order=done", s.sm.summary())
        for _ in range(3):
            s.tick(SEC)
        self.assertEqual([x["action"] for x in s.actions], ["place", "cancel"])

    def test_add_order_error(self):
        s = self.placed()
        lines, _ = s.priv(ack("add_order", success=False, error="EOrder:Insufficient funds"))
        self.assertIn("TEST ORDER FAILED: EOrder:Insufficient funds", lines[-1])
        self.assertEqual(s.sm.test_phase, "done")

    def test_network_refused(self):
        s = self.placed()
        lines, _ = s.feed("kraken_priv", "action_rejected",
                          {"id": 1, "reason": "trading disabled (run with --enable-trading)"})
        self.assertIn("TEST ORDER FAILED: trading disabled", lines[-1])

    def test_canceled_before_open(self):
        s = self.placed()
        lines, _ = s.priv(executions("update", [{"cl_ord_id": self.cl, "exec_type": "canceled",
                                                 "order_status": "canceled",
                                                 "reason": "Post only order"}]))
        self.assertIn("TEST ORDER FAILED", lines[-1])

    def test_fill_warns(self):
        s = self.placed()
        lines, _ = s.priv(executions("update", [{"cl_ord_id": self.cl, "exec_type": "filled",
                                                 "order_status": "filled"}]))
        self.assertIn("TEST ORDER WARNING", lines[-1])
        self.assertEqual(s.sm.stats["kraken_priv.fills"], 1)

    def test_other_orders_ignored(self):
        s = self.placed()
        lines, _ = s.priv(executions("update", [{"cl_ord_id": "spreadbot-1", "exec_type": "new",
                                                 "order_status": "new"}]))
        self.assertEqual(lines, [])
        self.assertNotIn("spreadbot-1", s.sm.orders)


class TestReconnectActions(unittest.TestCase):
    def test_one_reconnect_until_reopened(self):
        s = Session()
        s.pub(s.k.message("snapshot", bids=[("1.0", "1.0")], asks=[("2.0", "1.0")]))
        s.pub(s.k.message("update", bids=[("1.5", "1.0")], checksum=1))
        s.pub(s.k.message("snapshot", bids=[("1.0", "1.0")], asks=[("2.0", "1.0")]))
        s.pub(s.k.message("update", bids=[("1.6", "1.0")], checksum=1))
        self.assertEqual([a["action"] for a in s.actions], ["reconnect"])  # not twice
        s.feed("kraken", "ws_open")
        s.pub(s.k.message("snapshot", bids=[("1.0", "1.0")], asks=[("2.0", "1.0")]))
        s.pub(s.k.message("update", bids=[("1.7", "1.0")], checksum=1))
        self.assertEqual([a["action"] for a in s.actions], ["reconnect", "reconnect"])

    def test_coinbase_gap_reconnects(self):
        s = Session()
        s.feed("coinbase", "ws_in", {"text": coinbase_heartbeat(1)})
        _, a = s.feed("coinbase", "ws_in", {"text": coinbase_heartbeat(5)})
        self.assertEqual(a[0]["conn"], "coinbase")
        self.assertEqual(a[0]["action"], "reconnect")


class TestLoggedActionCheck(unittest.TestCase):
    def test_matching_log_is_quiet(self):
        s = Session(test_order_symbol="BTC/USD")
        _, actions = s.ready()
        lines, _ = s.feed("sm", "action", dict(actions[0]))
        self.assertEqual(lines, [])
        self.assertNotIn("actions.mismatch", s.sm.stats)
        self.assertEqual(s.sm.stats["actions.logged"], 1)

    def test_different_logged_action_is_flagged(self):
        s = Session(test_order_symbol="BTC/USD")
        _, actions = s.ready()
        tampered = dict(actions[0], price="1.0")
        lines, _ = s.feed("sm", "action", tampered)
        self.assertIn("ACTION MISMATCH", lines[0])
        self.assertEqual(s.sm.stats["actions.mismatch"], 1)

    def test_logged_action_never_produced_is_flagged(self):
        s = Session()
        lines, _ = s.feed("sm", "action", {"id": 1, "action": "place"})
        self.assertIn("produced nothing", lines[0])

    def test_produced_but_not_logged_shows_in_summary(self):
        s = Session(test_order_symbol="BTC/USD")
        s.ready()
        self.assertIn("summary actions.not_in_log=1", s.sm.summary())


class TestReplayOfTradingSession(unittest.TestCase):
    def test_replay_of_live_log_has_no_mismatches(self):
        """Build the log the way live mode would (actions logged a few events
        after they were produced), then replay it with a fresh state machine."""
        live = Session()
        log = []

        def add(conn, type_, data=None, dt=SEC // 100):
            lines, actions = live.feed(conn, type_, data, dt)
            log.append(event(live.seq, conn, type_, data, t=live.t))
            return actions

        add("main", "session", {"test_order": "BTC/USD"})
        add("kraken_priv", "ws_open")
        add("kraken_priv", "ws_in", {"text": executions("snapshot", [])})
        add("kraken", "ws_in", {"text": INSTRUMENT_BTC})
        place = add("kraken", "ws_in", {"text": live.k.message(
            "snapshot", bids=[("90001.3", "1.0")], asks=[("90002.0", "1.0")])})
        add("kraken", "ws_in", {"text": live.k.message("update", bids=[("90001.0", "0.2")])})
        add("sm", "action", place[0])  # logged slightly later, like the real thing
        cl = place[0]["cl_ord_id"]
        add("kraken_priv", "ws_in", {"text": ack("add_order")})
        add("kraken_priv", "ws_in", {"text": executions(
            "update", [{"cl_ord_id": cl, "exec_type": "new", "order_status": "new"}])})
        cancel = add("kraken", "ws_in", {"text": live.k.message("update", bids=[("90000.0", "0.2")])},
                     dt=6 * SEC)
        add("sm", "action", cancel[0])
        add("kraken_priv", "ws_in", {"text": executions(
            "update", [{"cl_ord_id": cl, "exec_type": "canceled", "order_status": "canceled"}])})

        replay = StateMachine()
        replay_out = []
        for ev in log:
            replay_out.extend(replay.update(ev))
        self.assertEqual(replay_out, live.out)
        self.assertNotIn("actions.mismatch", replay.stats)
        self.assertEqual(replay.stats["actions.logged"], 2)
        self.assertIn("summary test_order=done", replay.summary())
        self.assertNotIn("not_in_log", " ".join(replay.summary()))


if __name__ == "__main__":
    unittest.main()
