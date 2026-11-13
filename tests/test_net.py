"""Network layer against a local WebSocket server (no internet needed).

The server side is a tiny blocking wsproto server in a thread. The TLS test
needs the `openssl` command to make a throwaway certificate; it is skipped
if openssl isn't installed.
"""

import collections
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest

from wsproto import ConnectionType, WSConnection
from wsproto.events import (AcceptConnection, CloseConnection, Ping, Pong, Request,
                            TextMessage)

from mmstack.eventlog import EventTailer, EventWriter
from mmstack.net import Reactor, WsClient


def listener():
    family, type_, proto, _, addr = socket.getaddrinfo("localhost", 0, type=socket.SOCK_STREAM)[0]
    s = socket.socket(family, type_, proto)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(addr[:2])
    s.listen()
    s.settimeout(10)
    return s


class Server:
    """One accepted connection, driven step by step from a script."""

    def __init__(self, sock, tls_ctx=None):
        conn, _ = sock.accept()
        if tls_ctx:
            conn = tls_ctx.wrap_socket(conn, server_side=True)
        conn.settimeout(10)
        self.conn = conn
        self.ws = WSConnection(ConnectionType.SERVER)
        self.pending = collections.deque()

    def next_event(self):
        while not self.pending:
            data = self.conn.recv(65536)
            if not data:
                return None
            self.ws.receive_data(data)
            self.pending.extend(self.ws.events())
        return self.pending.popleft()

    def send(self, event):
        self.conn.sendall(self.ws.send(event))

    def accept_handshake(self):
        ev = self.next_event()
        assert isinstance(ev, Request), ev
        self.request_target = ev.target
        self.send(AcceptConnection())

    def close(self):
        try:
            self.conn.close()
        except OSError:
            pass


def wait_for(log_path, predicate, timeout=10):
    """Wait until some logged event matches predicate; return all events so far."""
    deadline = time.time() + timeout
    tailer = EventTailer(log_path)
    events = []
    try:
        while time.time() < deadline:
            events.extend(tailer.read_available())
            if any(predicate(e) for e in events):
                return events
            time.sleep(0.01)
    finally:
        tailer.close()
    raise AssertionError(f"timed out; events: {[e['type'] for e in events]}")


class NetTestBase(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.log_path = os.path.join(self.dir.name, "net.jsonl")
        self.writer = EventWriter(self.log_path)
        self.stop = threading.Event()
        self.reactor_thread = None

    def tearDown(self):
        self.stop.set()
        if self.reactor_thread:
            self.reactor_thread.join(timeout=5)
        self.writer.close()
        self.dir.cleanup()

    def start_reactor(self, clients):
        reactor = Reactor(clients, poll_timeout=0.01)
        self.reactor_thread = threading.Thread(target=reactor.run, args=(self.stop,), daemon=True)
        self.reactor_thread.start()

    def logged_texts(self, type_):
        tailer = EventTailer(self.log_path)
        evs = tailer.read_available()
        tailer.close()
        return [e["data"]["text"] for e in evs if e["type"] == type_]


class TestPlainWebSocket(NetTestBase):
    def run_full_session(self, tls_server_ctx=None, client_ctx=None):
        lst = listener()
        port = lst.getsockname()[1]
        subscribe = ['{"sub":1}', '{"sub":2}']
        server_seen = {}

        def server_main():
            s = Server(lst, tls_server_ctx)
            s.accept_handshake()
            got = []
            for _ in subscribe:
                ev = s.next_event()
                got.append(ev.data)
                # The client must have logged ws_out BEFORE sending it.
                server_seen.setdefault("logged_first", []).append(ev.data in self.logged_texts("ws_out"))
            server_seen["subscribe"] = got
            server_seen["target"] = s.request_target
            s.send(TextMessage(data="hello"))
            s.send(TextMessage(data="frag-", message_finished=False))
            s.send(TextMessage(data="ment", message_finished=True))
            s.send(Ping(payload=b"pp"))
            ev = s.next_event()
            server_seen["pong"] = isinstance(ev, Pong) and bytes(ev.payload) == b"pp"
            s.send(CloseConnection(code=1000, reason="bye"))
            ev = s.next_event()
            server_seen["close_reply"] = isinstance(ev, CloseConnection)
            s.close()
            lst.close()

        srv = threading.Thread(target=server_main, daemon=True)
        srv.start()
        client = WsClient("test", "localhost", port, "/v2", subscribe, self.writer,
                          use_tls=tls_server_ctx is not None, ssl_context=client_ctx,
                          reconnect_delay=60)
        self.start_reactor([client])
        events = wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        srv.join(timeout=5)
        return events, server_seen

    def check_session(self, events, server_seen, tls):
        types = [e["type"] for e in events]
        expected = ["conn_open", "tcp_connected"] + (["tls_ok"] if tls else []) + [
            "http_out", "http_in", "ws_open", "ws_out", "ws_out",
            "ws_in", "ws_in", "ws_ping_in", "ws_pong_out", "ws_close_in", "conn_close"]
        self.assertEqual(types, expected)
        texts = [e["data"]["text"] for e in events if e["type"] == "ws_in"]
        self.assertEqual(texts, ["hello", "frag-ment"])  # fragments joined into one message
        self.assertEqual(server_seen["subscribe"], ['{"sub":1}', '{"sub":2}'])
        self.assertEqual(server_seen["logged_first"], [True, True])
        self.assertEqual(server_seen["target"], "/v2")
        self.assertTrue(server_seen["pong"])
        self.assertTrue(server_seen["close_reply"])
        self.assertEqual([e["seq"] for e in events], list(range(1, len(events) + 1)))

    def test_full_session_plain(self):
        events, seen = self.run_full_session()
        self.check_session(events, seen, tls=False)

    @unittest.skipUnless(shutil.which("openssl"), "openssl not installed")
    def test_full_session_tls(self):
        cert = os.path.join(self.dir.name, "cert.pem")
        key = os.path.join(self.dir.name, "key.pem")
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
                        "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost",
                        "-addext", "subjectAltName=DNS:localhost"],
                       check=True, capture_output=True)
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        server_ctx.load_cert_chain(cert, key)
        client_ctx = ssl.create_default_context(cafile=cert)
        events, seen = self.run_full_session(server_ctx, client_ctx)
        self.check_session(events, seen, tls=True)
        tls_ev = [e for e in events if e["type"] == "tls_ok"][0]
        self.assertTrue(tls_ev["data"]["version"].startswith("TLS"))


class TestReconnectAndTimeouts(NetTestBase):
    def test_reconnects_after_server_drops(self):
        lst = listener()
        port = lst.getsockname()[1]

        def server_main():
            s1 = Server(lst)
            s1.accept_handshake()
            s1.close()  # drop without a close frame
            s2 = Server(lst)
            s2.accept_handshake()
            time.sleep(1)
            s2.close()

        threading.Thread(target=server_main, daemon=True).start()
        client = WsClient("test", "localhost", port, "/", [], self.writer, use_tls=False,
                          reconnect_delay=0.1)
        self.start_reactor([client])
        events = wait_for(self.log_path,
                          lambda e: e["type"] == "ws_open" and e["seq"] > 5)
        types = [e["type"] for e in events]
        self.assertEqual(types.count("conn_open"), 2)
        self.assertEqual(types.count("ws_open"), 2)
        self.assertIn("conn_close", types)
        lst.close()

    def test_connect_failure_is_logged_and_retried(self):
        lst = listener()
        port = lst.getsockname()[1]
        lst.close()  # nothing listening now
        client = WsClient("test", "localhost", port, "/", [], self.writer, use_tls=False,
                          reconnect_delay=0.05)
        self.start_reactor([client])
        events = wait_for(self.log_path,
                          lambda e: e["type"] == "conn_open" and e["seq"] >= 3)
        closes = [e for e in events if e["type"] == "conn_close"]
        self.assertTrue(closes)
        self.assertIn("connect failed", closes[0]["data"]["reason"])

    def test_idle_timeout_closes(self):
        lst = listener()
        port = lst.getsockname()[1]
        holder = {}

        def server_main():
            s = Server(lst)
            s.accept_handshake()
            holder["s"] = s  # stay silent

        threading.Thread(target=server_main, daemon=True).start()
        client = WsClient("test", "localhost", port, "/", [], self.writer, use_tls=False,
                          reconnect_delay=60, idle_timeout=0.3)
        self.start_reactor([client])
        events = wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        close = [e for e in events if e["type"] == "conn_close"][0]
        self.assertEqual(close["data"]["reason"], "idle timeout")
        self.assertIn("ws_close_out", [e["type"] for e in events])
        if "s" in holder:
            holder["s"].close()
        lst.close()

    def test_shutdown_closes_cleanly(self):
        lst = listener()
        port = lst.getsockname()[1]
        holder = {}

        def server_main():
            s = Server(lst)
            s.accept_handshake()
            holder["s"] = s
            holder["close"] = s.next_event()

        srv = threading.Thread(target=server_main, daemon=True)
        srv.start()
        client = WsClient("test", "localhost", port, "/", [], self.writer, use_tls=False)
        self.start_reactor([client])
        wait_for(self.log_path, lambda e: e["type"] == "ws_open")
        self.stop.set()
        self.reactor_thread.join(timeout=5)
        events = wait_for(self.log_path, lambda e: e["type"] == "conn_close")
        close = [e for e in events if e["type"] == "conn_close"][-1]
        self.assertEqual(close["data"]["reason"], "shutdown")
        srv.join(timeout=5)
        self.assertIsInstance(holder.get("close"), CloseConnection)
        holder["s"].close()
        lst.close()


if __name__ == "__main__":
    unittest.main()
