"""Kraken private client and reactor actions, against a local server."""

import json
import threading
import time
import unittest

from wsproto.events import TextMessage

from mmstack.eventlog import EventTailer
from mmstack.kraken_private import KrakenPrivateClient
from mmstack.net import Reactor, WsClient
from tests.test_net import NetTestBase, Server, listener, wait_for

PLACE = {"id": 1, "action": "place", "conn": "kraken_priv", "symbol": "BTC/USD", "side": "buy",
         "qty": "0.00005", "price": "85501.2", "cl_ord_id": "mm1763000000t1"}


class PrivateTestBase(NetTestBase):
    def start_private(self, trading_enabled=True, token_getter=lambda: "TOK123", keys=("k", "s"),
                      max_orders=1):
        self.lst = listener()
        port = self.lst.getsockname()[1]
        self.received = []
        self.server_ready = threading.Event()
        self.server_done = threading.Event()

        def server_main():
            try:
                s = Server(self.lst)
                s.accept_handshake()
                self.server_ready.set()
                while True:
                    ev = s.next_event()
                    if ev is None:
                        break
                    if isinstance(ev, TextMessage):
                        self.received.append(json.loads(ev.data))
            except OSError:
                pass
            finally:
                self.server_done.set()

        threading.Thread(target=server_main, daemon=True).start()
        self.client = KrakenPrivateClient(keys, self.writer, trading_enabled=trading_enabled,
                                          max_orders=max_orders, token_getter=token_getter,
                                          host="localhost", port=port, use_tls=False)
        self.reactor = Reactor([self.client], poll_timeout=0.01)
        self.reactor_thread = threading.Thread(target=self.reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()

    def wait_received(self, n, timeout=5):
        deadline = time.time() + timeout
        while time.time() < deadline and len(self.received) < n:
            time.sleep(0.01)
        return self.received

    def log_events(self):
        t = EventTailer(self.log_path)
        evs = []
        while True:
            b = t.read_available()
            if not b:
                break
            evs.extend(b)
        t.close()
        return evs

    def tearDown(self):
        super().tearDown()
        if hasattr(self, "lst"):
            self.lst.close()


class TestPrivateClient(PrivateTestBase):
    def test_subscribes_with_token_but_logs_it_redacted(self):
        self.start_private()
        msgs = self.wait_received(1)
        self.assertEqual(msgs[0]["params"]["channel"], "executions")
        self.assertEqual(msgs[0]["params"]["token"], "TOK123")
        with open(self.log_path, encoding="utf-8") as f:
            text = f.read()
        self.assertNotIn("TOK123", text)
        self.assertIn("<redacted>", text)

    def test_place_and_cancel_sent(self):
        self.start_private()
        self.wait_received(1)
        self.reactor.submit(dict(PLACE))
        self.reactor.submit({"id": 2, "action": "cancel", "conn": "kraken_priv",
                             "cl_ord_id": PLACE["cl_ord_id"]})
        msgs = self.wait_received(3)
        add, cancel = msgs[1], msgs[2]
        self.assertEqual(add["method"], "add_order")
        p = add["params"]
        self.assertEqual((p["order_type"], p["side"], p["symbol"], p["post_only"], p["cl_ord_id"]),
                         ("limit", "buy", "BTC/USD", True, PLACE["cl_ord_id"]))
        self.assertEqual(p["token"], "TOK123")
        self.assertEqual(cancel["method"], "cancel_order")
        self.assertEqual(cancel["params"]["cl_ord_id"], [PLACE["cl_ord_id"]])
        # exact digits on the wire
        raw = [e for e in self.log_events() if e["type"] == "ws_out"][1]["data"]["text"]
        self.assertIn('"order_qty":0.00005,', raw)
        self.assertIn('"limit_price":85501.2,', raw)

    def test_trading_disabled_rejects(self):
        self.start_private(trading_enabled=False)
        self.wait_received(1)
        self.reactor.submit(dict(PLACE))
        evs = wait_for(self.log_path, lambda e: e["type"] == "action_rejected")
        rej = [e for e in evs if e["type"] == "action_rejected"][0]
        self.assertEqual(rej["data"]["id"], 1)
        self.assertIn("trading disabled", rej["data"]["reason"])
        time.sleep(0.2)
        self.assertEqual(len(self.received), 1)  # only the subscribe

    def test_order_cap(self):
        self.start_private(max_orders=1)
        self.wait_received(1)
        self.reactor.submit(dict(PLACE))
        self.reactor.submit(dict(PLACE, id=2, cl_ord_id="mm1763000000t2"))
        evs = wait_for(self.log_path, lambda e: e["type"] == "action_rejected")
        rej = [e for e in evs if e["type"] == "action_rejected"][0]
        self.assertEqual(rej["data"]["id"], 2)
        self.assertIn("order cap", rej["data"]["reason"])
        self.assertEqual([m["method"] for m in self.wait_received(2)], ["subscribe", "add_order"])

    def test_shutdown_cancels_our_orders_only(self):
        self.start_private()
        self.wait_received(1)
        self.reactor.submit(dict(PLACE))
        self.wait_received(2)
        self.stop.set()
        self.reactor_thread.join(timeout=5)
        self.server_done.wait(5)
        last = self.received[-1]
        self.assertEqual(last["method"], "cancel_order")
        self.assertEqual(last["params"]["cl_ord_id"], [PLACE["cl_ord_id"]])
        self.assertNotIn("cancel_all", json.dumps(self.received))
        self.assertNotIn("cancel_all_orders_after", json.dumps(self.received))

    def test_no_keys(self):
        self.start_private(keys=None)
        evs = wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        close = [e for e in evs if e["type"] == "conn_close"][0]
        self.assertIn("no Kraken API keys", close["data"]["reason"])
        time.sleep(0.2)
        self.assertEqual([e["type"] for e in self.log_events()].count("conn_open"), 1)  # no retry

    def test_token_error_retries_later_not_immediately(self):
        def bad_token():
            raise RuntimeError("EAPI:Invalid key")
        self.start_private(token_getter=bad_token)
        evs = wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        self.assertIn("EAPI:Invalid key", evs[-1]["data"]["reason"])
        time.sleep(0.3)
        self.assertEqual([e["type"] for e in self.log_events()].count("conn_open"), 1)
        self.assertGreater(self.client._reconnect_at - time.monotonic(), 20)

    def test_not_open_rejects(self):
        self.start_private(token_getter=lambda: (_ for _ in ()).throw(RuntimeError("no")))
        wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        self.reactor.submit(dict(PLACE))
        evs = wait_for(self.log_path, lambda e: e["type"] == "action_rejected")
        self.assertIn("not open", [e for e in evs if e["type"] == "action_rejected"][0]["data"]["reason"])


class TestReactorActions(NetTestBase):
    def test_reconnect_action(self):
        lst = listener()
        port = lst.getsockname()[1]

        def server_main():
            s1 = Server(lst)
            s1.accept_handshake()
            s1.next_event()  # wait for our close frame
            s2 = Server(lst)
            s2.accept_handshake()
            time.sleep(1)

        threading.Thread(target=server_main, daemon=True).start()
        client = WsClient("kraken", "localhost", port, "/", [], self.writer, use_tls=False,
                          reconnect_delay=60)
        reactor = Reactor([client], poll_timeout=0.01)
        self.reactor_thread = threading.Thread(target=reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()
        wait_for(self.log_path, lambda e: e["type"] == "ws_open")
        reactor.submit({"id": 1, "action": "reconnect", "conn": "kraken", "reason": "checksum"})
        evs = wait_for(self.log_path, lambda e: e["type"] == "ws_open" and e["seq"] > 5)
        types = [e["type"] for e in evs]
        self.assertEqual(types.count("ws_open"), 2)
        closes = [e for e in evs if e["type"] == "conn_close"]
        self.assertIn("reconnect requested", closes[0]["data"]["reason"])
        lst.close()

    def test_unknown_conn_rejected(self):
        lst = listener()
        port = lst.getsockname()[1]
        client = WsClient("kraken", "localhost", port, "/", [], self.writer, use_tls=False)
        reactor = Reactor([client], poll_timeout=0.01)
        self.reactor_thread = threading.Thread(target=reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()
        reactor.submit({"id": 7, "action": "place", "conn": "binance"})
        evs = wait_for(self.log_path, lambda e: e["type"] == "action_rejected")
        rej = [e for e in evs if e["type"] == "action_rejected"][0]
        self.assertEqual(rej["data"]["id"], 7)
        lst.close()

    def test_dns_failure_retries(self):
        client = WsClient("kraken", "no-such-host.invalid", 443, "/", [], self.writer,
                          use_tls=False, reconnect_delay=0.05)
        reactor = Reactor([client], poll_timeout=0.01)
        self.reactor_thread = threading.Thread(target=reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()
        # Must keep retrying: a 2nd failure must also be logged and retried.
        evs = wait_for(self.log_path, lambda e: e["type"] == "conn_open" and e["seq"] >= 5)
        closes = [e for e in evs if e["type"] == "conn_close"]
        self.assertGreaterEqual(len(closes), 2)
        self.assertIn("connect setup failed", closes[1]["data"]["reason"])


if __name__ == "__main__":
    unittest.main()
