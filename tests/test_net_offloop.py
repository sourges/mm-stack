"""Slow connect setup (DNS lookup, Kraken's REST token) runs on a helper
thread, so it never stalls the other connections (2026-11-26 review: both
used to run on the network loop's own thread, up to 10 s for the token)."""

import threading
import time

from wsproto.events import TextMessage

from mmstack.kraken_private import KrakenPrivateClient
from mmstack.net import WsClient
from tests.test_net import NetTestBase, Server, listener, wait_for


def slow_resolver(seconds, release=None):
    import socket

    def resolve(host, port, type=0):
        if release is not None:
            release.wait(seconds)
        else:
            time.sleep(seconds)
        return socket.getaddrinfo(host, port, type=type)
    return resolve


class TestSetupOffTheLoop(NetTestBase):
    def busy_server(self):
        """A server that sends a message as soon as its client opens."""
        lst = listener()
        port = lst.getsockname()[1]

        def server_main():
            s = Server(lst)
            s.accept_handshake()
            s.send(TextMessage(data="tick"))
            time.sleep(3)
            s.close()
        threading.Thread(target=server_main, daemon=True).start()
        return port

    def test_slow_dns_doesnt_stall_other_connections(self):
        release = threading.Event()
        try:
            slow = WsClient("slow", "localhost", 9, "/", [], self.writer, use_tls=False,
                            resolver=slow_resolver(5, release))
            fast = WsClient("fast", "localhost", self.busy_server(), "/", [], self.writer, use_tls=False)
            t0 = time.monotonic()
            self.start_reactor([slow, fast])
            evs = wait_for(self.log_path, lambda e: e["conn"] == "fast" and e["type"] == "ws_in", timeout=3)
            self.assertLess(time.monotonic() - t0, 2)  # didn't wait for slow's 5 s lookup
            self.assertEqual(slow.state, "resolving")
            self.assertTrue(any(e["conn"] == "slow" and e["type"] == "conn_open" for e in evs))
        finally:
            release.set()

    def test_slow_token_doesnt_stall_other_connections(self):
        release = threading.Event()
        try:
            def token():
                release.wait(5)
                return "TOK"
            priv = KrakenPrivateClient(("k", "s"), self.writer, token_getter=token,
                                       host="localhost", port=9, use_tls=False)
            fast = WsClient("fast", "localhost", self.busy_server(), "/", [], self.writer, use_tls=False)
            t0 = time.monotonic()
            self.start_reactor([priv, fast])
            wait_for(self.log_path, lambda e: e["conn"] == "fast" and e["type"] == "ws_in", timeout=3)
            self.assertLess(time.monotonic() - t0, 2)
        finally:
            release.set()

    def test_late_result_after_timeout_is_ignored(self):
        """Setup took longer than the connect timeout: the attempt was closed,
        so its late answer must not start a connection behind our back."""
        release = threading.Event()
        c = WsClient("slow", "localhost", 9, "/", [], self.writer, use_tls=False,
                     resolver=slow_resolver(5, release), connect_timeout=0.1, reconnect_delay=30)
        self.start_reactor([c])
        wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        release.set()
        time.sleep(0.3)
        self.assertEqual(c.state, "closed")
        self.assertIsNone(c.sock)

    def test_old_attempts_answer_not_used_by_the_next_attempt(self):
        """Attempt 1 timed out and attempt 2 is setting up when attempt 1's
        late answer arrives: attempt 2 must keep waiting for its own."""
        import socket
        first, forever = threading.Event(), threading.Event()
        calls = []

        def resolver(host, port, type=0):
            calls.append(1)
            (first if len(calls) == 1 else forever).wait(10)
            return socket.getaddrinfo("localhost", 9, type=type)
        c = WsClient("slow", "localhost", 9, "/", [], self.writer, use_tls=False,
                     resolver=resolver, connect_timeout=0.1, reconnect_delay=0.05)
        try:
            self.start_reactor([c])
            deadline = time.monotonic() + 5
            while len(calls) < 2 and time.monotonic() < deadline:
                time.sleep(0.01)
            c.connect_timeout = 30  # attempt 2 may wait as long as it likes
            first.set()  # attempt 1's answer arrives late
            time.sleep(0.3)
            self.assertEqual(c.state, "resolving")  # still waiting for attempt 2's own answer
            self.assertEqual(len(calls), 2)  # attempt 2 wasn't hijacked (and failed into a 3rd)
        finally:
            forever.set()

    def test_dns_failure_closes_with_a_reason(self):
        def broken(host, port, type=0):
            raise OSError("no such host")
        c = WsClient("bad", "nowhere.invalid", 443, "/", [], self.writer, use_tls=False,
                     resolver=broken, reconnect_delay=30)
        self.start_reactor([c])
        evs = wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        self.assertIn("connect setup failed: no such host", evs[-1]["data"]["reason"])


if __name__ == "__main__":
    import unittest
    unittest.main()
