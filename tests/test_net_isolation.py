"""One connection's failure never takes down the others (2026-11-26 review,
second round):
  3.1 a bad WebSocket handshake reply (or any unexpected error inside one
      connection) closes and reconnects THAT connection only; before, the
      error escaped and stopped the whole network thread (every venue).
  3.2 an older setup attempt's late answer can't overwrite a newer one."""

import selectors
import threading
import time

from wsproto.events import TextMessage

from mmstack.net import WsClient
from tests.test_net import NetTestBase, Server, listener, wait_for


def talking_server():
    """Accepts one client and keeps sending it a message every 50 ms."""
    lst = listener()
    port = lst.getsockname()[1]

    def server_main():
        s = Server(lst)
        s.accept_handshake()
        try:
            for i in range(60):
                s.send(TextMessage(data=f"tick {i}"))
                time.sleep(0.05)
        except OSError:
            pass
        s.close()
    threading.Thread(target=server_main, daemon=True).start()
    return port


def bad_handshake_server():
    """Answers the WebSocket upgrade with a wrong accept token, every time."""
    lst = listener()
    port = lst.getsockname()[1]

    def server_main():
        while True:
            try:
                conn, _ = lst.accept()
            except OSError:
                return
            conn.settimeout(5)
            try:
                conn.recv(65536)
                conn.sendall(b"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
                             b"Connection: Upgrade\r\nSec-WebSocket-Accept: bad\r\n\r\n")
                time.sleep(0.5)
            except OSError:
                pass
            conn.close()
    threading.Thread(target=server_main, daemon=True).start()
    return port


class TestOneBadConnection(NetTestBase):
    def test_bad_handshake_closes_only_that_connection(self):
        bad = WsClient("bad", "localhost", bad_handshake_server(), "/", [], self.writer,
                       use_tls=False, reconnect_delay=0.2)
        good = WsClient("good", "localhost", talking_server(), "/", [], self.writer, use_tls=False)
        self.start_reactor([bad, good])
        wait_for(self.log_path, lambda e: e["conn"] == "bad" and e["type"] == "conn_close")
        time.sleep(0.6)  # the good one must keep receiving after the bad one failed
        evs = wait_for(self.log_path, lambda e: e["conn"] == "bad" and e["type"] == "conn_open"
                       and e["seq"] > 0, timeout=3)
        closes = [e for e in evs if e["conn"] == "bad" and e["type"] == "conn_close"]
        self.assertIn("ws protocol error", closes[0]["data"]["reason"])
        self.assertTrue(self.reactor_thread.is_alive())
        first_close = closes[0]["t"]
        later = [e for e in evs if e["conn"] == "good" and e["type"] == "ws_in" and e["t"] > first_close]
        self.assertGreater(len(later), 3)  # still recording
        self.assertGreaterEqual(sum(e["conn"] == "bad" and e["type"] == "conn_open" for e in evs), 2)

    def test_any_unexpected_error_in_one_connection_is_contained(self):
        class Broken(WsClient):
            def on_open(self, now):
                raise RuntimeError("boom")
        lst = listener()
        port = lst.getsockname()[1]

        def server_main():
            s = Server(lst)
            s.accept_handshake()
            time.sleep(3)
        threading.Thread(target=server_main, daemon=True).start()
        broken = Broken("broken", "localhost", port, "/", [], self.writer, use_tls=False,
                        reconnect_delay=30)
        good = WsClient("good", "localhost", talking_server(), "/", [], self.writer, use_tls=False)
        self.start_reactor([broken, good])
        evs = wait_for(self.log_path, lambda e: e["conn"] == "broken" and e["type"] == "conn_close")
        reason = [e for e in evs if e["conn"] == "broken" and e["type"] == "conn_close"][0]["data"]["reason"]
        self.assertIn("unexpected error", reason)
        self.assertIn("boom", reason)
        time.sleep(0.5)
        self.assertTrue(self.reactor_thread.is_alive())
        evs = wait_for(self.log_path, lambda e: e["conn"] == "good" and e["type"] == "ws_in"
                       and e["data"]["text"] == "tick 15", timeout=3)
        self.assertTrue(evs)


class TestErrorInTick(NetTestBase):
    def test_error_while_finishing_setup_is_contained(self):
        """tick() finishes a connection's setup on the loop: an error there
        closes that connection only."""
        class Broken(WsClient):
            def after_prepare(self, now, extra):
                raise RuntimeError("tick boom")
        broken = Broken("broken", "localhost", 9, "/", [], self.writer, use_tls=False,
                        reconnect_delay=30)
        good = WsClient("good", "localhost", talking_server(), "/", [], self.writer, use_tls=False)
        self.start_reactor([broken, good])
        evs = wait_for(self.log_path, lambda e: e["conn"] == "broken" and e["type"] == "conn_close")
        reason = [e for e in evs if e["conn"] == "broken" and e["type"] == "conn_close"][0]["data"]["reason"]
        self.assertIn("unexpected error", reason)
        self.assertIn("tick boom", reason)
        evs = wait_for(self.log_path, lambda e: e["conn"] == "good" and e["type"] == "ws_in"
                       and e["data"]["text"] == "tick 10", timeout=3)
        self.assertTrue(self.reactor_thread.is_alive())


class TestOldAttemptCantOverwrite(NetTestBase):
    def test_newer_answer_survives_an_older_late_one(self):
        """Attempt 2 answers first, then attempt 1's late answer arrives before
        the loop looked: attempt 2 must still go ahead with its own answer."""
        import socket
        lst = listener()
        port = lst.getsockname()[1]
        gate = threading.Event()
        calls = []

        def resolver(host, p, type=0):
            calls.append(1)
            if len(calls) == 1:
                gate.wait(5)  # attempt 1: slow
            return socket.getaddrinfo("localhost", port, type=type)
        c = WsClient("c", "localhost", port, "/", [], self.writer, use_tls=False, resolver=resolver)
        sel = selectors.DefaultSelector()
        try:
            c.start(sel, 0.0)  # attempt 1 (stuck in its lookup)
            c.close(0.1, "connect timeout")
            c.start(sel, 0.2)  # attempt 2 answers at once
            deadline = time.time() + 3
            while c._prepared is None and time.time() < deadline:
                time.sleep(0.01)
            gate.set()  # now attempt 1's late answer comes in
            time.sleep(0.3)
            c._take_prepared(0.3)
            self.assertEqual(c.state, "connecting")  # attempt 2 went ahead
        finally:
            gate.set()
            if c.sock is not None:
                c.close(1.0, "done", reconnect=False)
            sel.close()
            lst.close()


if __name__ == "__main__":
    import unittest
    unittest.main()
