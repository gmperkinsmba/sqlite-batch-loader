#!/usr/bin/env python3
"""Start the local operator UI."""
import argparse
from bd3.server import serve

parser = argparse.ArgumentParser(description="Run SQLite Batch Loader locally")
parser.add_argument("--host", default="127.0.0.1")
parser.add_argument("--port", type=int, default=8331)
parser.add_argument("--runtime", help="Private runtime folder outside the checkout")
args = parser.parse_args()
serve(args.host, args.port, args.runtime)
