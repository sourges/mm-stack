"""Kraken API keys, request signing, and the WebSocket token.

Keys come from (first found wins):
  1. environment variables KRAKEN_API_KEY and KRAKEN_API_SECRET
  2. the file secrets/kraken.json  ->  {"key": "...", "secret": "..."}
     (the secrets/ folder is in .gitignore)

Secrets never go in the event log: API-Key, API-Sign and the WebSocket token
are replaced with "<redacted>" before anything is written.
"""

import base64
import hashlib
import hmac
import http.client
import json
import os
import time
import urllib.parse

REDACTED = "<redacted>"
API_HOST = "api.kraken.com"
TOKEN_PATH = "/0/private/GetWebSocketsToken"
DEFAULT_KEY_FILE = os.path.join("secrets", "kraken.json")


def load_keys(env=None, key_file=DEFAULT_KEY_FILE):
    """Return (key, secret) or None if no keys are configured."""
    env = os.environ if env is None else env
    key, secret = env.get("KRAKEN_API_KEY"), env.get("KRAKEN_API_SECRET")
    if key and secret:
        return key, secret
    if key_file and os.path.exists(key_file):
        with open(key_file, encoding="utf-8") as f:
            data = json.load(f)
        if data.get("key") and data.get("secret"):
            return data["key"], data["secret"]
    return None


def sign(url_path, post_data, nonce, secret):
    """API-Sign = base64(HMAC-SHA512(url_path + SHA256(nonce + post_data), base64decode(secret)))."""
    sha = hashlib.sha256((str(nonce) + post_data).encode()).digest()
    mac = hmac.new(base64.b64decode(secret), url_path.encode() + sha, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


def get_ws_token(keys, log, clock=time.time_ns, connection_factory=None, timeout=10):
    """Fetch a WebSocket token over REST (blocking, once per private connect).

    log(type_, data) writes events; secrets are redacted. Returns the token,
    or raises RuntimeError with Kraken's error message.
    """
    key, secret = keys
    nonce = clock() // 1_000_000  # milliseconds, always increasing
    body = urllib.parse.urlencode({"nonce": nonce})
    headers = {
        "API-Key": key,
        "API-Sign": sign(TOKEN_PATH, body, nonce, secret),
        "Content-Type": "application/x-www-form-urlencoded; charset=utf-8",
    }
    log("http_out", {"method": "POST", "host": API_HOST, "path": TOKEN_PATH, "body": body,
                     "headers": {k: (REDACTED if k in ("API-Key", "API-Sign") else v)
                                 for k, v in headers.items()}})
    factory = connection_factory or (lambda: http.client.HTTPSConnection(API_HOST, timeout=timeout))
    conn = factory()
    try:
        conn.request("POST", TOKEN_PATH, body=body, headers=headers)
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", "replace")
        status = resp.status
    finally:
        conn.close()
    try:
        data = json.loads(raw)
    except ValueError:
        log("http_in", {"status": status, "body": raw[:500]})
        raise RuntimeError(f"token request: HTTP {status}, not JSON")
    token = (data.get("result") or {}).get("token")
    shown = json.loads(raw)
    if token:
        shown["result"]["token"] = REDACTED
    log("http_in", {"status": status, "body": shown})
    if data.get("error") or not token:
        raise RuntimeError(f"token request failed: {data.get('error') or 'no token'}")
    return token
