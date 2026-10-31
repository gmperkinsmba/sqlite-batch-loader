# SQLite Batch Loader

**First principle:** Send bounded source batches to one verified dataset. Use receipts to resume safely after a stop.

Two worker-owned SQLite shards publish each completed batch as a consistent snapshot with an immutable manifest. One canonical SQLite writer combines the batches, then sends data to a local DuckDB file. Transport receipts track deliveries. Domain-record checks prevent duplicate records. If local acknowledgement fails after DuckDB commits, retries use a DuckDB receipt to find that commit.

This release supports only a local SQLite-to-DuckDB path. It does not support PostgreSQL, Supabase, Snowflake, Databricks, cloud storage, remote scheduling, or hosted ingestion. It makes no universal exactly-once claim. All demo records are fabricated.

**Install as a Mac app**

Build this native app on macOS with Python 3.12. The build uses the pinned package list in `requirements-desktop.lock.txt`.

```sh
python3.12 -m venv .desktop-venv
.desktop-venv/bin/python -m pip install -r requirements-desktop.lock.txt
.desktop-venv/bin/python build_desktop.py
.desktop-venv/bin/python install_desktop.py
```

The install step puts `SQLite Batch Loader.app` in `~/Applications`. Open it in Finder. Close its window or choose **Quit** to stop its local server. The app bundle includes Python. Its runtime data stays in `~/.local/share/bd3` unless you set `BD3_RUNTIME` before launch. Remove the app by moving it from `~/Applications` to Trash; this leaves runtime data in place.

The bundle uses ad-hoc signing. It has no Developer ID signature and is not notarized. macOS may ask you to approve opening it. The native app builds on macOS only.

To work on the native shell, activate `.desktop-venv` and run `.desktop-venv/bin/python desktop.py`.

## Run in a browser

Python 3.11 or newer is required. DuckDB 2.0.0 is the only runtime dependency. The version pin is in `requirements.txt`.

```sh
python3 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements.txt
python run.py
```

Open <http://127.0.0.1:8331>. The default runtime folder is `~/.local/share/bd3`, outside this checkout. Set `BD3_RUNTIME` or pass `--runtime` to choose another private local folder. The server listens on loopback only. Settings shows the active adapter. Destinations must stay in the runtime folder.

Choose **Run sample intake** to send batches through both worker shards. The sample has a duplicate domain record and an ID with leading zeros. It also has an exact decimal, null timestamps and amounts, and a later tombstone. Choose **Try recovery** to stop after the canonical commit. Then retry from its receipt. The batch table shows stage and destination receipts. All sample source data is synthetic.

## Engine API

The importable `bd3` package has no web-server dependency. It stores state in a private local folder.

| Method | Purpose |
| --- | --- |
| `Pipeline(root, sink_path=None)` | Creates private local state. |
| `write_shard(worker, batch_id, source_id, source_revision, records, schema_version="1")` | Writes a worker-owned shard and publishes a validated snapshot with a manifest. |
| `publish(...)` | Assigns a batch to one of the two shards. |
| `consolidate(manifest, failpoint=None)` | Validates data, then atomically stores domain rows, an ingest receipt, and an outbox entry in canonical SQLite. |
| `deliver(batch_id, failpoint=None)` | Serializes delivery for the runtime, upserts current canonical state, and writes a sink receipt. |
| `status()` and `records(include_deleted=True)` | Show the current stage state and records. |

A nonblocking process lease covers each shard write and publication.

Each record has these fields:

| Field | Type |
| --- | --- |
| `record_id` | String |
| `source_id` | String |
| `revision` | Non-negative integer stored as a string |
| `occurred_at` | ISO timestamp or null |
| `amount` | Decimal string or null |
| `payload` | String |
| `deleted` | Boolean |

IDs never pass through a numeric type. The manifest records the source and schema versions, producer, row counts, and a SHA-256 digest. It also names the snapshot file and gives its publication time. Revisions sort by numeric value, so `10` follows `9`. The canonical quarantine table holds records that reuse a revision but differ in content.

The engine has three failpoints: `before_commit`, `after_commit`, and `before_ack`. They stop at transaction boundaries for recovery checks. The canonical quarantine table also records invalid batches. SQLite snapshots use its backup API to include the write-ahead log. The app does not copy only the live database file. DuckDB decimal values stay within the exact `DECIMAL(38,18)` range.

## Test

Install the pinned DuckDB dependency with `python -m pip install -r requirements.txt`. Then run:

```sh
PYTHONPATH=. python -m unittest discover -s tests -v
```

Tests cover both worker files and snapshots. They reject bad schemas and duplicate deliveries or domain records. They check interruptions before and after commit, retries after a sink commit, tombstones, old replays, and long string IDs. They also check decimal precision, nulls, and timestamps.

## Data and security

The runtime folder holds state, batches, snapshots, the canonical database, and DuckDB output. Keep it private and local. Do not use real client or employer exports in a public demo. The local server checks loopback Host and Origin. Each write needs a custom token for that server process. JSON requests are limited to 2 MB. The app accepts no cloud credentials or user-provided SQL.

## License

Original code is available under the MIT License. DuckDB is an optional external dependency. See its package metadata and license for its terms.
