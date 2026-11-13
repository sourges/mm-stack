"""Networking: all connections run on ONE thread, with no async.

We only use "sans-io" libraries, which never touch the network themselves:
  - ssl.MemoryBIO / SSLObject (Python standard library) for TLS
  - wsproto for WebSocket framing and the HTTP upgrade handshake
We own the sockets and do every read and write ourselves, driven by one
selectors loop (select on Windows, epoll on Linux).

Logging rules (from the guide):
  - incoming events are logged as soon as we see them
  - outgoing messages are logged BEFORE we send them
"""

import base64
import errno
import selectors
import socket
import ssl
import time

from wsproto import ConnectionType, WSConnection
from wsproto.events import (
    AcceptConnection,
    BytesMessage,
    CloseConnection,
    Ping,
    Pong,
    RejectConnection,
    RejectData,
    Request,
    TextMessage,
)

# connect() on a non-blocking socket returns one of these while still connecting.
# 10035 is WSAEWOULDBLOCK on Windows.
_IN_PROGRESS = {errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EAGAIN, 10035}

CONNECT_TIMEOUT = 15.0  # seconds to get from TCP connect to WebSocket open
IDLE_TIMEOUT = 60.0  # seconds without any data on an open connection
RECONNECT_DELAY = 5.0


class WsClient:
    """One WebSocket connection.

    States: idle -> connecting -> tls -> ws_handshake -> open -> closed
    After closing it reconnects by itself after reconnect_delay seconds
    (unless the reactor is shutting down).
    """

    def __init__(self, name, host, port, path, subscribe, log,
                 use_tls=True, ssl_context=None, reconnect_delay=RECONNECT_DELAY,
                 connect_timeout=CONNECT_TIMEOUT, idle_timeout=IDLE_TIMEOUT):
        self.name = name
        self.host = host
        self.port = port
        self.path = path
        self.subscribe = list(subscribe)  # text messages sent once the WebSocket opens
        self._log = log  # EventWriter
        self.use_tls = use_tls
        self._ssl_context = ssl_context
        self.reconnect_delay = reconnect_delay
        self.connect_timeout = connect_timeout
        self.idle_timeout = idle_timeout

        self.state = "idle"
        self.sock = None
        self.selector = None
        self._outbuf = b""
        self._tls = None
        self._tls_in = None
        self._tls_out = None
        self._tls_done = False
        self._ws = None
        self._text_parts = []
        self._bytes_parts = []
        self._started_at = 0.0
        self._last_data_at = 0.0
        self._reconnect_at = None

    # ---------- logging helper ----------
    def log(self, type_, data=None):
        return self._log.write(self.name, type_, data)

    # ---------- lifecycle ----------
    def start(self, selector, now):
        """Begin connecting (DNS lookup is blocking; fine at connect time)."""
        self.selector = selector
        self._reset()
        self._started_at = now
        self._last_data_at = now
        self._reconnect_at = None
        self.log("conn_open", {"host": self.host, "port": self.port,
                               "path": self.path, "tls": self.use_tls})
        try:
            family, type_, proto, _, addr = socket.getaddrinfo(
                self.host, self.port, type=socket.SOCK_STREAM)[0]
            sock = socket.socket(family, type_, proto)
            sock.setblocking(False)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        except OSError as e:
            self.close(now, f"connect setup failed: {e}")
            return
        self.sock = sock
        self._addr = addr
        err = sock.connect_ex(addr)
        if err != 0 and err not in _IN_PROGRESS:
            self.close(now, f"connect failed: {errno.errorcode.get(err, err)}")
            return
        self.state = "connecting"
        selector.register(sock, selectors.EVENT_READ | selectors.EVENT_WRITE, self)

    def _reset(self):
        self.sock = None
        self._outbuf = b""
        self._tls = self._tls_in = self._tls_out = None
        self._tls_done = False
        self._ws = None
        self._text_parts = []
        self._bytes_parts = []

    def close(self, now, reason, reconnect=True):
        """Close the connection (sending a WebSocket close frame if open)."""
        if self.state == "closed":
            return
        if self.state == "open" and self._ws is not None and self.sock is not None:
            try:
                data = self._ws.send(CloseConnection(code=1000, reason=reason[:100]))
                self.log("ws_close_out", {"code": 1000, "reason": reason})
                self._send_raw(data)
                self.sock.send(self._outbuf)  # one try; don't wait for it
            except Exception:
                pass  # best effort; we are closing anyway
        if self.sock is not None:
            try:
                self.selector.unregister(self.sock)
            except (KeyError, ValueError):
                pass
            try:
                self.sock.close()
            except OSError:
                pass
        self.state = "closed"
        self.sock = None
        self.log("conn_close", {"reason": reason})
        self._reconnect_at = now + self.reconnect_delay if reconnect else None

    def tick(self, now):
        """Timers: reconnect, connect timeout, idle timeout."""
        if self.state == "closed" and self._reconnect_at is not None and now >= self._reconnect_at:
            self.start(self.selector, now)
        elif self.state in ("connecting", "tls", "ws_handshake"):
            if now - self._started_at > self.connect_timeout:
                self.close(now, "connect timeout")
        elif self.state == "open":
            if now - self._last_data_at > self.idle_timeout:
                self.close(now, "idle timeout")

    # ---------- selector callbacks ----------
    def on_writable(self, now):
        if self.state == "connecting":
            err = self.sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                self.close(now, f"connect failed: {errno.errorcode.get(err, err)}")
                return
            self.log("tcp_connected", {"addr": str(self._addr[0]), "port": self._addr[1]})
            if self.use_tls:
                self.state = "tls"
                ctx = self._ssl_context or ssl.create_default_context()
                self._tls_in = ssl.MemoryBIO()
                self._tls_out = ssl.MemoryBIO()
                self._tls = ctx.wrap_bio(self._tls_in, self._tls_out,
                                         server_hostname=self.host)
                self._tls_handshake(now)
            else:
                self._start_ws()
        self._flush(now)

    def on_readable(self, now):
        if self.state == "connecting":
            # Windows/Linux can report a failed connect as readable too.
            self.on_writable(now)
            return
        try:
            data = self.sock.recv(65536)
        except (BlockingIOError, InterruptedError):
            return
        except OSError as e:
            self.close(now, f"recv error: {e}")
            return
        if not data:
            self.close(now, "eof from server")
            return
        self._last_data_at = now
        if self.use_tls:
            self._on_tls_bytes(now, data)
        else:
            self._on_plain_bytes(now, data)

    # ---------- TLS (sans-io) ----------
    def _tls_handshake(self, now):
        try:
            self._tls.do_handshake()
        except ssl.SSLWantReadError:
            pass
        except ssl.SSLError as e:
            self._move_tls_out()
            self.close(now, f"tls error: {e}")
            return
        else:
            self._tls_done = True
            self.log("tls_ok", {"version": self._tls.version(),
                                "cipher": self._tls.cipher()[0]})
        self._move_tls_out()
        if self._tls_done:
            self._start_ws()

    def _move_tls_out(self):
        pending = self._tls_out.read()
        if pending:
            self._outbuf += pending

    def _on_tls_bytes(self, now, data):
        self._tls_in.write(data)
        if not self._tls_done:
            self._tls_handshake(now)
            if not self._tls_done or self.state == "closed":
                self._flush(now)
                return
        plain = []
        while True:
            try:
                chunk = self._tls.read(65536)
            except ssl.SSLWantReadError:
                break
            except ssl.SSLZeroReturnError:
                self.close(now, "tls closed by server")
                return
            except ssl.SSLError as e:
                self.close(now, f"tls error: {e}")
                return
            if not chunk:
                break
            plain.append(chunk)
        self._move_tls_out()
        if plain:
            self._on_plain_bytes(now, b"".join(plain))
        self._flush(now)

    # ---------- WebSocket (sans-io) ----------
    def _start_ws(self):
        self.state = "ws_handshake"
        self._ws = WSConnection(ConnectionType.CLIENT)
        request = self._ws.send(Request(host=self.host, target=self.path))
        self.log("http_out", {"request": request.decode("latin-1")})
        self._send_raw(request)

    def _on_plain_bytes(self, now, data):
        self._ws.receive_data(data)
        for ev in self._ws.events():
            if self.state == "closed":
                return
            if isinstance(ev, AcceptConnection):
                self.log("http_in", {"status": 101, "subprotocol": ev.subprotocol,
                                     "extensions": [str(x) for x in ev.extensions]})
                self.state = "open"
                self.log("ws_open")
                for text in self.subscribe:
                    self.send_text(text)
            elif isinstance(ev, RejectConnection):
                self.log("http_in", {"status": ev.status_code,
                                     "headers": [[k.decode("latin-1"), v.decode("latin-1")]
                                                 for k, v in ev.headers]})
                if not ev.has_body:
                    self.close(now, f"handshake rejected {ev.status_code}")
            elif isinstance(ev, RejectData):
                self.log("http_in_body", {"body": ev.data.decode("utf-8", "replace")})
                if ev.body_finished:
                    self.close(now, "handshake rejected")
            elif isinstance(ev, TextMessage):
                self._text_parts.append(ev.data)
                if ev.message_finished:
                    text = "".join(self._text_parts)
                    self._text_parts = []
                    self.log("ws_in", {"text": text})
            elif isinstance(ev, BytesMessage):
                self._bytes_parts.append(bytes(ev.data))
                if ev.message_finished:
                    raw = b"".join(self._bytes_parts)
                    self._bytes_parts = []
                    self.log("ws_in_bytes", {"b64": base64.b64encode(raw).decode("ascii")})
            elif isinstance(ev, Ping):
                self.log("ws_ping_in", {"payload": bytes(ev.payload).hex()})
                pong = ev.response()
                self.log("ws_pong_out", {"payload": bytes(pong.payload).hex()})
                self._send_raw(self._ws.send(pong))
            elif isinstance(ev, Pong):
                self.log("ws_pong_in", {"payload": bytes(ev.payload).hex()})
            elif isinstance(ev, CloseConnection):
                self.log("ws_close_in", {"code": ev.code, "reason": ev.reason})
                try:
                    self._send_raw(self._ws.send(ev.response()))
                    self._flush(now)
                except Exception:
                    pass
                self.state = "closing"  # don't send our own close frame too
                self.close(now, f"closed by server ({ev.code})")
                return
        self._flush(now)

    def send_text(self, text):
        """Log, THEN send a text message."""
        self.log("ws_out", {"text": text})
        self._send_raw(self._ws.send(TextMessage(data=text)))

    # ---------- raw socket output ----------
    def _send_raw(self, data):
        if self.use_tls and self._tls is not None:
            self._tls.write(data)
            self._move_tls_out()
        else:
            self._outbuf += data

    def _flush(self, now):
        if self.sock is None:
            return
        while self._outbuf:
            try:
                n = self.sock.send(self._outbuf)
            except (BlockingIOError, InterruptedError):
                break
            except OSError as e:
                self.close(now, f"send error: {e}")
                return
            self._outbuf = self._outbuf[n:]
        want = selectors.EVENT_READ
        if self._outbuf or self.state == "connecting":
            want |= selectors.EVENT_WRITE
        try:
            if self.selector.get_key(self.sock).events != want:
                self.selector.modify(self.sock, want, self)
        except (KeyError, ValueError):
            pass


class Reactor:
    """Runs every WsClient on one thread with one selector."""

    def __init__(self, clients, clock=time.monotonic, poll_timeout=0.1):
        self.clients = clients
        self._clock = clock
        self.poll_timeout = poll_timeout
        self.selector = selectors.DefaultSelector()

    def run(self, stop_event):
        now = self._clock()
        for c in self.clients:
            c.start(self.selector, now)
        try:
            while not stop_event.is_set():
                if self.selector.get_map():
                    ready = self.selector.select(self.poll_timeout)
                else:
                    # Windows select() errors with no sockets registered.
                    time.sleep(self.poll_timeout)
                    ready = []
                now = self._clock()
                for key, mask in ready:
                    client = key.data
                    if mask & selectors.EVENT_READ and client.sock is not None:
                        client.on_readable(now)
                    if mask & selectors.EVENT_WRITE and client.sock is not None:
                        client.on_writable(now)
                for c in self.clients:
                    c.tick(now)
        finally:
            now = self._clock()
            for c in self.clients:
                c.close(now, "shutdown", reconnect=False)
            self.selector.close()
