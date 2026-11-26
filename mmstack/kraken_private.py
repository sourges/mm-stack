"""Kraken private (authenticated) WebSocket: our orders.

Safety rules, enforced HERE in the network layer (not just in the strategy):
  - no orders unless trading_enabled (the --enable-trading flag)
  - at most max_orders orders per session (default 1)
  - only post-only limit orders
  - we only ever cancel OUR orders, by cl_ord_id. Never cancel_all and never
    the dead man's switch: both act on the whole Kraken account, which would
    also cancel the spread bot's orders.
  - on shutdown, cancel every order this session placed
"""

import json
from decimal import Decimal

from .kraken_auth import REDACTED, get_ws_token
from .net import WsClient

HOST = "ws-auth.kraken.com"
PATH = "/v2"
TOKEN_RETRY_SECONDS = 30.0


class KrakenPrivateClient(WsClient):
    def __init__(self, keys, log, trading_enabled=False, max_orders=1,
                 token_getter=None, allowed_symbols=(), **kwargs):
        kwargs.setdefault("host", HOST)
        kwargs.setdefault("port", 443)
        kwargs.setdefault("path", PATH)
        super().__init__(name="kraken_priv", subscribe=[], log=log, **kwargs)
        self._keys = keys
        self.trading_enabled = trading_enabled
        self.max_orders = max_orders
        self._token_getter = token_getter or (lambda: get_ws_token(self._keys, self.log))
        self._token = None
        self._req_id = 0
        self.orders_sent = 0
        self.placed_cl_ord_ids = []  # every cl_ord_id sent this session, for shutdown cancels
        self.allowed_symbols = frozenset(allowed_symbols)  # the only pairs we may place on

    # ---------- connect ----------
    def before_connect(self, now):
        if self._keys is None:
            self.close(now, "no Kraken API keys configured", reconnect=False)
            return False
        return True

    def prepare_blocking(self):
        """The REST token request (up to 10 s): on the helper thread, so a
        slow Kraken API never stalls the market-data connections."""
        return self._token_getter()

    def after_prepare(self, now, token):
        self._token = token
        return True

    def on_prepare_error(self, now, error):
        self.close(now, f"token error: {error}")
        self._reconnect_at = now + max(self.reconnect_delay, TOKEN_RETRY_SECONDS)

    def on_open(self, now):
        self._send({"method": "subscribe",
                    "params": {"channel": "executions", "snap_orders": True, "snap_trades": False}})

    # ---------- sending with the token kept out of the log ----------
    def _send(self, msg):
        self._req_id += 1
        msg = dict(msg, req_id=self._req_id)
        params = dict(msg["params"], token=self._token)
        real = dict(msg, params=params)
        shown = dict(msg, params=dict(params, token=REDACTED))
        self.send_text(_dumps(real), log_text=_dumps(shown))

    def _reject(self, action, reason):
        self.log("action_rejected", {"id": action.get("id"), "reason": reason})

    # ---------- actions from the state machine ----------
    def _order_problem(self, action):
        """Checks every order HERE, whatever the strategy sent (None = fine)."""
        if action.get("side") not in ("buy", "sell"):
            return f"side must be buy or sell, not {action.get('side')!r}"
        for field in ("qty", "price"):
            try:
                v = Decimal(str(action.get(field)))
            except (ArithmeticError, ValueError):
                return f"{field} must be a number, not {action.get(field)!r}"
            if not v.is_finite():
                return f"{field} must be a number, not {action.get(field)!r}"
            if v <= 0:
                return f"{field} must be above 0, not {action.get(field)}"
        if action.get("symbol") not in self.allowed_symbols:
            return f"symbol {action.get('symbol')} not allowed this session"
        if not action.get("cl_ord_id"):
            return "no cl_ord_id"
        return None

    def handle_action(self, action, now):
        kind = action.get("action")
        if kind == "place":
            if not self.trading_enabled:
                return self._reject(action, "trading disabled (run with --enable-trading)")
            problem = self._order_problem(action)
            if problem:
                return self._reject(action, problem)
            if self.orders_sent >= self.max_orders:
                return self._reject(action, f"order cap reached ({self.max_orders} per session)")
            if self.state != "open":
                return self._reject(action, "private connection not open")
            self.orders_sent += 1
            self.placed_cl_ord_ids.append(action["cl_ord_id"])
            self._send({"method": "add_order", "params": {
                "order_type": "limit",
                "side": action["side"],
                "order_qty": Decimal(action["qty"]),
                "symbol": action["symbol"],
                "limit_price": Decimal(action["price"]),
                "post_only": True,
                "cl_ord_id": action["cl_ord_id"],
            }})
        elif kind == "cancel":
            if action.get("cl_ord_id") not in self.placed_cl_ord_ids:
                # the spread bot shares this account: only ever cancel our own
                return self._reject(action, f"{action.get('cl_ord_id')} is not an order this "
                                            f"session placed")
            if self.state != "open":
                return self._reject(action, "private connection not open")
            self._send({"method": "cancel_order", "params": {"cl_ord_id": [action["cl_ord_id"]]}})
        else:
            super().handle_action(action, now)

    def prepare_shutdown(self, now):
        if self.state == "open" and self.placed_cl_ord_ids:
            self._send({"method": "cancel_order",
                        "params": {"cl_ord_id": list(self.placed_cl_ord_ids)}})


def _dumps(obj):
    """json.dumps, but Decimals become plain JSON numbers with their exact digits
    (e.g. 0.00005, never 5e-05 or 0.000049999...)."""
    numbers = []

    def placeholder(value):
        if isinstance(value, Decimal):
            numbers.append(format(value, "f"))
            return f"__num{len(numbers) - 1}__"
        raise TypeError(f"can't encode {type(value)}")

    text = json.dumps(obj, separators=(",", ":"), default=placeholder)
    for i, digits in enumerate(numbers):
        text = text.replace(f'"__num{i}__"', digits, 1)
    return text
