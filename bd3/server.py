"""Loopback-only operator UI and bounded JSON API."""
from __future__ import annotations

import json
import os
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import socketserver
from urllib.parse import urlparse

from .engine import BatchError, Pipeline

MAX_BODY = 2_000_000
HOSTS = {"127.0.0.1", "localhost", "[::1]"}


def demo_records() -> list[dict]:
    return [
        {"record_id": "000042", "source_id": "synthetic-orders", "revision": "1", "occurred_at": "2026-10-30T09:12:00-05:00", "amount": "125.40", "payload": "Order accepted", "deleted": False},
        {"record_id": "000043", "source_id": "synthetic-orders", "revision": "1", "occurred_at": None, "amount": None, "payload": "Nullable fields stay null", "deleted": False},
        {"record_id": "000042", "source_id": "synthetic-orders", "revision": "1", "occurred_at": "2026-10-30T09:12:00-05:00", "amount": "125.40", "payload": "Order accepted", "deleted": False},
        {"record_id": "000044", "source_id": "synthetic-orders", "revision": "1", "occurred_at": "2026-10-30T11:00:00Z", "amount": "0.000000000000000001", "payload": "Exact decimal", "deleted": False},
    ]


def make_handler(runtime: Path, token: str):
    pipeline = Pipeline(runtime)

    class Handler(BaseHTTPRequestHandler):
        server_version = "BD3/1.0"

        def setup(self):
            super().setup()
            self.connection.settimeout(5)

        def log_message(self, fmt, *args):
            # Request paths may carry user content; keep logs minimal.
            print(f"bd3 {self.client_address[0]} {self.command}")

        def _json(self, status: int, data: object):
            body = json.dumps(data, ensure_ascii=False).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _valid_origin(self) -> bool:
            expected_host = f"127.0.0.1:{self.server.server_port}"
            if self.headers.get("Host", "") != expected_host:
                return False
            origin = self.headers.get("Origin")
            if origin:
                try:
                    parsed = urlparse(origin)
                except ValueError:
                    return False
                return parsed.scheme == "http" and parsed.hostname == "127.0.0.1" and parsed.netloc == expected_host and parsed.username is None and parsed.password is None
            return True

        def do_GET(self):
            if not self._valid_origin():
                return self._json(403, {"error": "Host or origin rejected"})
            path = urlparse(self.path).path
            if path == "/api/settings":
                settings_path = runtime / "settings.json"
                try:
                    settings = json.loads(settings_path.read_text(encoding="utf-8"))
                except FileNotFoundError:
                    settings = {"destination": "destination.duckdb"}
                return self._json(200, {**settings, "configured": settings_path.exists()})
            if path == "/api/session":
                return self._json(200, {"token": token})
            if path == "/api/status":
                return self._json(200, pipeline.status())
            if path in ("/", "/index.html", "/app.js", "/style.css", "/desktop.css"):
                name = "index.html" if path in ("", "/", "/index.html") else path.lstrip("/")
                data = (Path(__file__).parent.parent / "web" / name).read_bytes()
                mime = "text/html" if name.endswith("html") else "text/javascript" if name.endswith("js") else "text/css"
                self.send_response(200)
                self.send_header("Content-Type", f"{mime}; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("X-Content-Type-Options", "nosniff")
                self.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self' data:")
                self.end_headers()
                self.wfile.write(data)
                return
            self._json(404, {"error": "Not found"})

        def do_POST(self):
            if not self._valid_origin() or self.headers.get("X-BD3-Token") != token:
                return self._json(403, {"error": "Request boundary rejected"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size < 0 or size > MAX_BODY:
                    return self._json(413, {"error": "Request exceeds size limit"})
                data = json.loads(self.rfile.read(size) or b"{}")
                if not isinstance(data, dict):
                    return self._json(400, {"error": "Request body must be a JSON object"})
                path = urlparse(self.path).path
                if path == "/api/demo/run":
                    batch_id = f"demo-{time.strftime('%Y%m%d-%H%M%S')}"
                    records = demo_records()
                    batches = []
                    m1 = pipeline.write_shard("worker-a", batch_id + "-a", "synthetic-orders", "1", records[:2])
                    m2 = pipeline.write_shard("worker-b", batch_id + "-b", "synthetic-orders", "1", records[2:])
                    tombstone = dict(records[1], revision="2", payload="Source record removed in revision 2", deleted=True)
                    m3 = pipeline.write_shard("worker-a", batch_id + "-c", "synthetic-orders", "2", [tombstone])
                    for m in (m1, m2, m3):
                        rec = pipeline.consolidate(m)
                        delivery = pipeline.deliver(m.batch_id)
                        batches.append({"manifest": m.__dict__, "receipt": rec.__dict__, "delivery": delivery.__dict__})
                    return self._json(200, {"batches": batches})
                if path == "/api/demo/recovery":
                    batch_id = f"recovery-{time.strftime('%Y%m%d-%H%M%S')}"
                    rs = demo_records()[:2]
                    m = pipeline.publish(batch_id, "synthetic-orders", "1", rs)
                    try:
                        pipeline.consolidate(m, failpoint="after_commit")
                    except RuntimeError:
                        pass
                    rec = pipeline.consolidate(m)
                    return self._json(200, {"manifest": m.__dict__, "receipt": rec.__dict__, "recovered": True})
                if path == "/api/settings":
                    # Runtime settings are local; destination path remains under runtime.
                    dest = data.get("destination")
                    if dest not in (None, "destination.duckdb"):
                        return self._json(400, {"error": "Choose the local DuckDB destination."})
                    settings = runtime / "settings.json"
                    temp = runtime / ".settings.tmp"
                    temp.write_text(json.dumps({"destination": "destination.duckdb"}), encoding="utf-8")
                    os.chmod(temp, 0o600)
                    os.replace(temp, settings)
                    return self._json(200, {"destination": "destination.duckdb", "configured": True})
                return self._json(404, {"error": "Not found"})
            except RuntimeError as exc:
                return self._json(503, {"error": str(exc)[:300]})
            except (ValueError, BatchError, KeyError, OSError) as exc:
                return self._json(400, {"error": str(exc)[:300]})

    return Handler


class LoopbackHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def server_bind(self):
        socketserver.TCPServer.server_bind(self)
        self.server_name = "127.0.0.1"
        self.server_port = self.socket.getsockname()[1]


def make_server(runtime: str | Path | None = None, port: int = 0):
    root = Path(runtime or os.environ.get("BD3_RUNTIME", Path.home() / ".local/share/bd3"))
    root = root.expanduser().resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(root, 0o700)
    return LoopbackHTTPServer(("127.0.0.1", port), make_handler(root, secrets.token_urlsafe(32)))


def serve(host: str = "127.0.0.1", port: int = 8331, runtime: str | Path | None = None):
    if host != "127.0.0.1":
        raise ValueError("Server must bind to 127.0.0.1.")
    server = make_server(runtime, port)
    print(f"SQLite Batch Loader listening at http://127.0.0.1:{server.server_port}")
    try:
        server.serve_forever()
    finally:
        server.server_close()
