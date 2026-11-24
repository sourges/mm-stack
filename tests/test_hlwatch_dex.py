"""hlwatch and HIP-3 dexes (e.g. "xyz" stock perps): orders, positions and
mids are per dex, so hlwatch asks each dex the account trades on and merges."""

import unittest

from mmstack import hlwatch
from tests.test_hlwatch import ADDR, FakeWriter, fill, order, state


class DexApi:
    """Answers per (request type, dex). The xyz dex answers WITHOUT the 'xyz:'
    prefix on purpose: hlwatch must add it either way."""

    def __init__(self):
        self.main = {"frontendOpenOrders": [order("BTC", "B", "99", "1")],
                     "clearinghouseState": state(("BTC", "1", "100"), value="1000"),
                     "userFills": [fill(1, coin="BTC"), fill(2, coin="xyz:XYZ100", px="25000")],
                     "allMids": {"BTC": "100", "ETH": "10"}}
        self.xyz = {"frontendOpenOrders": [order("XYZ100", "A", "25010", "0.1"),
                                           order("xyz:TSLA", "B", "300", "1")],  # already prefixed
                    "clearinghouseState": state(("XYZ100", "-0.1", "2500"), value="5000"),
                    "allMids": {"XYZ100": "25005", "TSLA": "301"}}
        self.calls = []
        self.fail = set()

    def __call__(self, body):
        dex = body.get("dex", "")
        self.calls.append((body["type"], dex))
        if (body["type"], dex) in self.fail:
            raise OSError("connection reset")
        if dex == "":
            return self.main[body["type"]]
        assert body["type"] != "userFills", "userFills has no dex field"
        return self.xyz[body["type"]]


class TestDex(unittest.TestCase):
    def setUp(self):
        self.w = FakeWriter()
        self.api = DexApi()

    def watcher(self, **kw):
        return hlwatch.Watcher(ADDR, self.w, post=self.api, **kw)

    def test_on_dex(self):
        self.assertEqual(hlwatch.on_dex("XYZ100", "xyz"), "xyz:XYZ100")
        self.assertEqual(hlwatch.on_dex("xyz:XYZ100", "xyz"), "xyz:XYZ100")
        self.assertIsNone(hlwatch.on_dex(None, "xyz"))

    def test_a_fill_on_xyz_adds_the_dex_from_the_next_poll(self):
        w = self.watcher()
        w.poll()
        self.assertEqual(w.dexes, ["xyz"])
        self.assertEqual([c for c in self.api.calls if c[1]], [])  # no xyz request at all in poll 1
        self.assertEqual(self.w.of("dex")[0]["data"], {"dex": "xyz", "why": "seen in xyz:XYZ100"})
        status = w.poll()
        self.assertIn(("frontendOpenOrders", "xyz"), self.api.calls)
        self.assertIn("dexes: xyz", status)
        self.assertEqual(len(self.w.of("dex")), 1)  # added once

    def test_merged_events_use_prefixed_names(self):
        w = self.watcher(dexes=["xyz"])
        w.poll()
        coins = [o["coin"] for o in self.w.of("orders")[0]["data"]["orders"]]
        self.assertEqual(coins, ["BTC", "xyz:XYZ100", "xyz:TSLA"])
        pos = self.w.of("positions")[0]["data"]
        self.assertEqual([p["position"]["coin"] for p in pos["assetPositions"]], ["BTC", "xyz:XYZ100"])
        self.assertEqual(pos["marginSummary"], {"accountValue": "1000"})  # main account unchanged
        self.assertEqual(pos["dex_margin"], {"xyz": {"accountValue": "5000"}})
        mids = self.w.of("mids")[0]["data"]
        self.assertEqual(mids, {"BTC": "100", "xyz:TSLA": "301", "xyz:XYZ100": "25005"})
        self.assertEqual(self.w.of("dex")[0]["data"]["why"], "asked (--dex)")

    def test_a_failing_dex_is_noted_and_the_rest_still_logged(self):
        self.api.fail = {("frontendOpenOrders", "xyz"), ("clearinghouseState", "xyz")}
        w = self.watcher(dexes=["xyz"])
        w.poll()
        orders = self.w.of("orders")[0]["data"]
        self.assertEqual(orders["missing_dexes"], ["xyz"])
        self.assertEqual([o["coin"] for o in orders["orders"]], ["BTC"])
        self.assertNotIn("dex_margin", self.w.of("positions")[0]["data"])
        self.assertEqual(w.errors, 2)

    def test_without_any_dex_nothing_changes(self):
        self.api.main["userFills"] = [fill(1, coin="BTC")]
        w = self.watcher()
        w.poll()
        w.poll()
        self.assertTrue(all(dex == "" for _, dex in self.api.calls))
        self.assertNotIn("dex_margin", self.w.of("positions")[0]["data"])
        self.assertNotIn("missing_dexes", self.w.of("orders")[0]["data"])

    def test_report_shows_each_dex_account_and_its_coins(self):
        w = self.watcher(dexes=["xyz"])
        w.poll()
        self.api.xyz["clearinghouseState"] = state(("XYZ100", "-0.1", "2500"), value="5100")
        self.w.t += 5 * 10**9
        w.poll()
        text = "\n".join(hlwatch.report(self.w.events))
        self.assertIn("hlreport account value: first $1,000, last $1,000, change $+0", text)
        self.assertIn("hlreport account value on xyz: first $5,000, last $5,100, change $+100", text)
        self.assertIn("hlreport   xyz:XYZ100: held in 100% of polls", text)
        self.assertIn("hlreport dex xyz added: asked (--dex)", text)


if __name__ == "__main__":
    unittest.main()
