import json
import os
import tempfile
import unittest

from mmstack.kraken_auth import REDACTED, get_ws_token, load_keys, sign


class TestSign(unittest.TestCase):
    def test_kraken_documented_example(self):
        # Worked example from Kraken's "Spot REST Authentication" guide.
        secret = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg=="
        body = "nonce=1616492376594&ordertype=limit&pair=XBTUSD&price=37500&type=buy&volume=1.25"
        self.assertEqual(sign("/0/private/AddOrder", body, 1616492376594, secret),
                         "4/dpxb3iT4tp/ZCVEwSnEsLxx0bqyhLpdfOpc6fn7OR8+UClSV5n9E6aSS8MPtnRfp32bAb0nmbRn6H8ndwLUQ==")


class TestLoadKeys(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.file = os.path.join(self.dir.name, "kraken.json")

    def tearDown(self):
        self.dir.cleanup()

    def test_env_first(self):
        with open(self.file, "w") as f:
            json.dump({"key": "fk", "secret": "fs"}, f)
        self.assertEqual(load_keys({"KRAKEN_API_KEY": "ek", "KRAKEN_API_SECRET": "es"}, self.file),
                         ("ek", "es"))

    def test_file(self):
        with open(self.file, "w") as f:
            json.dump({"key": "fk", "secret": "fs"}, f)
        self.assertEqual(load_keys({}, self.file), ("fk", "fs"))

    def test_none(self):
        self.assertIsNone(load_keys({}, self.file))
        self.assertIsNone(load_keys({"KRAKEN_API_KEY": "only-key"}, self.file))


class FakeResponse:
    def __init__(self, status, body):
        self.status, self._body = status, body

    def read(self):
        return self._body.encode()


class FakeConnection:
    def __init__(self, status, body):
        self.status, self.body, self.sent = status, body, None

    def request(self, method, path, body=None, headers=None):
        self.sent = (method, path, body, headers)

    def getresponse(self):
        return FakeResponse(self.status, self.body)

    def close(self):
        pass


SECRET = "kQH5HW/8p1uGOVjbgWA7FunAmGO8lsSUXNsu3eow76sz84Q18fWxnyRzBHCd3pd5nE9qa99HAZtuZuj6F1huXg=="


class TestToken(unittest.TestCase):
    def run_token(self, status, body):
        logged = []
        conn = FakeConnection(status, body)
        result = None
        error = None
        try:
            result = get_ws_token(("MYKEY", SECRET), lambda t, d: logged.append((t, d)),
                                  clock=lambda: 1_700_000_000_123_000_000,
                                  connection_factory=lambda: conn)
        except RuntimeError as e:
            error = e
        return result, error, logged, conn

    def test_token_returned_and_secrets_not_logged(self):
        token, err, logged, conn = self.run_token(200, '{"error":[],"result":{"token":"SECRETTOKEN","expires":900}}')
        self.assertEqual(token, "SECRETTOKEN")
        self.assertIsNone(err)
        method, path, body, headers = conn.sent
        self.assertEqual((method, path, body), ("POST", "/0/private/GetWebSocketsToken", "nonce=1700000000123"))
        self.assertEqual(headers["API-Sign"], sign(path, body, 1700000000123, SECRET))
        text = json.dumps(logged)
        for secret in ("SECRETTOKEN", "MYKEY", headers["API-Sign"]):
            self.assertNotIn(secret, text)
        self.assertEqual([t for t, _ in logged], ["http_out", "http_in"])
        self.assertEqual(logged[1][1]["body"]["result"]["token"], REDACTED)

    def test_kraken_error_raises(self):
        token, err, logged, _ = self.run_token(200, '{"error":["EAPI:Invalid key"]}')
        self.assertIsNone(token)
        self.assertIn("EAPI:Invalid key", str(err))
        self.assertEqual(logged[-1][0], "http_in")

    def test_not_json_raises(self):
        _, err, _, _ = self.run_token(502, "<html>bad gateway</html>")
        self.assertIn("HTTP 502", str(err))


if __name__ == "__main__":
    unittest.main()
