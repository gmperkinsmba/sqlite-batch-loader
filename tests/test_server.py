import json
import tempfile
import threading
import unittest
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib.request import Request, urlopen

from bd3.server import make_handler


class ServerIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.token = "synthetic-test-token"
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(Path(self.temp.name), self.token))
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{self.server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.temp.cleanup()

    def request(self, path, body=None):
        payload = None if body is None else json.dumps(body).encode()
        headers = {}
        if body is not None:
            headers = {"Content-Type": "application/json", "X-BD3-Token": self.token}
        req = Request(self.base + path, data=payload, headers=headers)
        with urlopen(req, timeout=10) as response:
            return response.status, json.loads(response.read())

    def test_demo_run_replays_identically_and_reaches_duckdb(self):
        status, first = self.request("/api/demo/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(len(first["batches"]), 3)
        self.assertEqual([b["receipt"]["accepted"] for b in first["batches"]], [2, 1, 1])
        self.assertTrue(all(b["delivery"]["status"] == "acknowledged" for b in first["batches"]))

        status, replay = self.request("/api/demo/run", {})
        self.assertEqual(status, 200)
        self.assertEqual(len(replay["batches"]), 3)
        self.assertTrue(all(b["delivery"]["status"] == "acknowledged" for b in replay["batches"]))

    def test_recovery_endpoint_reaches_receipt_after_commit(self):
        status, result = self.request("/api/demo/recovery", {})
        self.assertEqual(status, 200)
        self.assertTrue(result["recovered"])
        self.assertEqual(result["receipt"]["accepted"], 2)

    def test_settings_save_and_readback(self):
        status, initial = self.request("/api/settings")
        self.assertEqual(status, 200)
        self.assertFalse(initial["configured"])
        status, saved = self.request("/api/settings", {"destination": "destination.duckdb"})
        self.assertEqual(status, 200)
        self.assertTrue(saved["configured"])
        status, readback = self.request("/api/settings")
        self.assertEqual(status, 200)
        self.assertEqual(readback["destination"], "destination.duckdb")
        self.assertTrue(readback["configured"])


if __name__ == "__main__":
    unittest.main()
