#!/usr/bin/env python3
"""Native macOS shell for BigDaddyDataDelegator."""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

import webview
from webview.menu import Menu, MenuAction

import bd3.server
from desktop_policy import install_policy


def wait_until_ready(url: str) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except (OSError, URLError):
            time.sleep(0.05)
    raise RuntimeError("The local service did not start.")


def run() -> None:
    runtime = Path(os.environ.get("BD3_RUNTIME", Path.home() / ".local/share/bd3")).expanduser()
    server = bd3.server.make_server(runtime=runtime, port=0)
    worker = threading.Thread(target=server.serve_forever, name="bd3-http", daemon=True)
    window = None
    url = f"http://127.0.0.1:{server.server_port}/?desktop=1"
    origin = f"http://127.0.0.1:{server.server_port}"

    def reload_app(_window=None):
        if window is not None:
            window.load_url(url)

    def open_settings(_window=None):
        if window is not None:
            window.load_url(url + "#settings")

    def quit_app(_window=None):
        if window is not None:
            window.destroy()

    menu = [
        Menu("BigDaddyDataDelegator", [MenuAction("Settings…", open_settings), MenuAction("Quit", quit_app)]),
        Menu("View", [MenuAction("Reload", reload_app)]),
    ]
    try:
        worker.start()
        wait_until_ready(url)
        window = webview.create_window(
            "BigDaddyDataDelegator", url, width=1080, height=760, min_size=(800, 580), menu=menu
        )
        install_policy(origin)
        webview.start()
    finally:
        if worker.is_alive():
            server.shutdown()
        server.server_close()
        worker.join(timeout=3)


if __name__ == "__main__":
    try:
        run()
    except Exception:
        import AppKit

        alert = AppKit.NSAlert.alloc().init()
        alert.setMessageText_("BigDaddyDataDelegator could not start")
        alert.setInformativeText_("The local app failed to open. Restart BigDaddyDataDelegator and try again.")
        alert.addButtonWithTitle_("OK")
        alert.runModal()
        raise SystemExit(1)
