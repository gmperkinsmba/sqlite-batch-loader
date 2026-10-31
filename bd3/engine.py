"""SQLite shard -> canonical stage -> DuckDB with replay-safe receipts."""
from __future__ import annotations

import hashlib
import itertools
import json
import os
import fcntl
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import duckdb
except ImportError:  # CLI can explain the optional adapter dependency.
    duckdb = None


@contextmanager
def _sqlite(*args, **kwargs):
    connection = sqlite3.connect(*args, **kwargs)
    try:
        with connection:
            yield connection
    finally:
        connection.close()


FIELDS = ("record_id", "source_id", "revision", "occurred_at", "amount", "payload", "deleted")
MAX_RECORDS = 100_000
MAX_TEXT = 1_000_000


class BatchError(ValueError):
    """Invalid or conflicting batch input."""


def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _canonical_record(raw: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(raw, dict) or set(raw) != set(FIELDS):
        raise BatchError("Record fields must match the declared schema.")
    rid, sid = raw["record_id"], raw["source_id"]
    if not isinstance(rid, str) or not rid or len(rid) > 256 or not isinstance(sid, str) or not sid or len(sid) > 256:
        raise BatchError("Record and source IDs must be non-empty strings up to 256 characters.")
    rev = raw["revision"]
    if not isinstance(rev, str) or not rev.isascii() or not rev.isdigit() or len(rev) > 18:
        raise BatchError("Revision must be a non-negative integer encoded as a string.")
    stamp = raw["occurred_at"]
    if stamp is not None:
        if not isinstance(stamp, str) or len(stamp) > 64:
            raise BatchError("Timestamp must be an ISO string or null.")
        try:
            datetime.fromisoformat(stamp.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BatchError("Timestamp must be a valid ISO timestamp.") from exc
    amt = raw["amount"]
    if amt is not None:
        if not isinstance(amt, str):
            raise BatchError("Amount must be a decimal string or null.")
        try:
            d = Decimal(str(amt))
            integer_digits = max(0, d.copy_abs().adjusted() + 1) if d else 0
            if not d.is_finite() or integer_digits > 20 or abs(d.as_tuple().exponent) > 18:
                raise InvalidOperation
            amt = format(d, "f")
        except (InvalidOperation, ValueError):
            raise BatchError("Amount must be a finite decimal with at most 38 digits and 18 decimal places.")
    payload = raw["payload"]
    if not isinstance(payload, str) or len(payload.encode("utf-8")) > MAX_TEXT:
        raise BatchError("Payload must be text within the size limit.")
    deleted = raw["deleted"]
    if not isinstance(deleted, (bool, int)) or deleted not in (False, True, 0, 1):
        raise BatchError("Deleted must be a boolean.")
    return {"record_id": rid, "source_id": sid, "revision": rev, "occurred_at": stamp,
            "amount": amt, "payload": payload, "deleted": int(deleted)}


def _json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


@dataclass(frozen=True)
class Manifest:
    batch_id: str
    source_id: str
    source_revision: str
    schema_version: str
    producer: str
    record_count: int
    unique_record_count: int
    digest: str
    snapshot: str
    created_at: str


@dataclass(frozen=True)
class Receipt:
    batch_id: str
    status: str
    accepted: int
    duplicate_records: int
    tombstones: int
    committed_at: str


@dataclass(frozen=True)
class DeliveryReceipt:
    batch_id: str
    sink: str
    status: str
    row_count: int
    digest: str
    committed_at: str


class Pipeline:
    """Durable two-shard intake and canonical staging pipeline.

    Record IDs are strings. Domain versions are resolved by revision then batch
    creation time; tombstones remain in stage and therefore suppress older replays.
    """

    def __init__(self, root: str | Path, sink_path: str | Path | None = None):
        self.root = Path(root).expanduser().resolve()
        self.shards = self.root / "shards"
        self.batches = self.root / "batches"
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.shards.mkdir(exist_ok=True, mode=0o700)
        self.batches.mkdir(exist_ok=True, mode=0o700)
        os.chmod(self.root, 0o700)
        os.chmod(self.shards, 0o700)
        os.chmod(self.batches, 0o700)
        self.stage_path = self.root / "canonical.sqlite"
        self.sink_path = Path(sink_path).expanduser().resolve() if sink_path else self.root / "destination.duckdb"
        self._init_stage()

    @contextmanager
    def _connect(self):
        with _sqlite(self.stage_path, timeout=10) as db:
            db.row_factory = sqlite3.Row
            db.execute("PRAGMA foreign_keys=ON")
            yield db

    def _init_stage(self) -> None:
        with self._connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
            CREATE TABLE IF NOT EXISTS ingest_receipts(
              batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, source_id TEXT NOT NULL,
              source_revision TEXT NOT NULL, record_count INTEGER NOT NULL,
              accepted INTEGER NOT NULL, duplicate_records INTEGER NOT NULL,
              tombstones INTEGER NOT NULL, committed_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS records(
              record_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, revision TEXT NOT NULL,
              occurred_at TEXT, amount TEXT, payload TEXT NOT NULL, deleted INTEGER NOT NULL,
              batch_id TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS quarantines(
              batch_id TEXT PRIMARY KEY, reason TEXT NOT NULL, quarantined_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS outbox(
              batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, created_at TEXT NOT NULL,
              acked_at TEXT);
            CREATE TABLE IF NOT EXISTS delivery_receipts(
              batch_id TEXT PRIMARY KEY, sink TEXT NOT NULL, digest TEXT NOT NULL,
              row_count INTEGER NOT NULL, committed_at TEXT NOT NULL);
            """)

    @contextmanager
    def _worker_lock(self, worker: str):
        """Hold one nonblocking process lease through shard snapshot publication."""
        if worker not in ("worker-a", "worker-b"):
            raise BatchError("Worker must be worker-a or worker-b.")
        fd = os.open(self.shards / f".{worker}.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError(f"{worker} is owned by another active process.") from exc
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    @contextmanager
    def _delivery_lock(self):
        """Serialize canonical reads, DuckDB transactions, and local acknowledgements."""
        fd = os.open(self.root / ".delivery.lock", os.O_CREAT | os.O_RDWR, 0o600)
        try:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError as exc:
                raise RuntimeError("DuckDB delivery is owned by another active process.") from exc
            yield
        finally:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            finally:
                os.close(fd)

    def write_shard(self, worker: str, batch_id: str, source_id: str, source_revision: str,
                    records: Iterable[dict[str, Any]], schema_version: str = "1") -> Manifest:
        """Write one worker-owned shard, publish consistent snapshot, manifest last."""
        with self._worker_lock(worker):
            try:
                return self._write_shard_locked(worker, batch_id, source_id, source_revision, records, schema_version)
            except BatchError as exc:
                if isinstance(batch_id, str) and batch_id:
                    self.record_quarantine(batch_id, str(exc))
                raise

    def _write_shard_locked(self, worker: str, batch_id: str, source_id: str, source_revision: str,
                            records: Iterable[dict[str, Any]], schema_version: str) -> Manifest:
        if worker not in ("worker-a", "worker-b"):
            raise BatchError("Worker must be worker-a or worker-b.")
        for val, label in ((batch_id, "batch ID"), (source_id, "source ID"), (source_revision, "source revision"), (schema_version, "schema version")):
            if not isinstance(val, str) or not val or len(val) > 256:
                raise BatchError(f"{label} must be a non-empty string up to 256 characters.")
        if not source_revision.isascii() or not source_revision.isdigit() or len(source_revision) > 18:
            raise BatchError("Source revision must be a non-negative integer encoded as a string.")
        if schema_version != "1":
            raise BatchError("Unsupported schema version.")
        try:
            rows = [_canonical_record(x) for x in itertools.islice(records, MAX_RECORDS + 1)]
            if len(rows) > MAX_RECORDS:
                raise BatchError(f"Batch exceeds {MAX_RECORDS} records.")
        except (BatchError, TypeError) as exc:
            self.record_quarantine(batch_id, str(exc))
            raise
        if any(r["source_id"] != source_id or r["revision"] != source_revision for r in rows):
            raise BatchError("Every row must match the manifest source and revision.")
        # Deterministic canonical bytes are the trusted content digest.
        content = _json(rows).encode("utf-8")
        digest = hashlib.sha256(content).hexdigest()
        published = self._manifest_path(batch_id)
        if published.exists():
            try:
                existing = Manifest(**json.loads(published.read_text(encoding="utf-8")))
            except (ValueError, TypeError, KeyError) as exc:
                raise BatchError("A published manifest is malformed and cannot be replaced.") from exc
            if existing.digest != digest:
                self.record_quarantine(batch_id, "Batch ID already published with different content.")
                raise BatchError("Batch ID already published with different content.")
            return existing
        shard = self.shards / f"{worker}.sqlite"
        with _sqlite(shard, timeout=10) as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.execute("CREATE TABLE IF NOT EXISTS batches(batch_id TEXT PRIMARY KEY, digest TEXT NOT NULL, body TEXT NOT NULL)")
            old = db.execute("SELECT digest FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if old and old[0] != digest:
                raise BatchError("Batch ID already exists with different content.")
            db.execute("INSERT OR IGNORE INTO batches VALUES(?,?,?)", (batch_id, digest, content.decode("utf-8")))
        # SQLite backup captures WAL state; no live-file copy race.
        staged = self.batches / f".{batch_id}.snapshot.tmp"
        safe = hashlib.sha256(batch_id.encode()).hexdigest()[:24]
        staged = self.batches / f".{safe}.snapshot.tmp"
        target = self.batches / f"{safe}.sqlite"
        with _sqlite(shard) as src, _sqlite(staged) as dst:
            src.backup(dst)
        os.replace(staged, target)
        manifest = Manifest(batch_id, source_id, source_revision, schema_version, worker,
                            len(rows), len({r["record_id"] for r in rows}), digest, target.name, _utcnow())
        path = self.batches / f"{safe}.manifest.json"
        temp = path.with_suffix(".tmp")
        temp.write_text(_json(asdict(manifest)), encoding="utf-8")
        os.replace(temp, path)
        return manifest

    def publish(self, batch_id: str, source_id: str, source_revision: str, records: Iterable[dict[str, Any]],
                schema_version: str = "1") -> Manifest:
        """Assign batches between two isolated worker shard files."""
        worker = "worker-a" if int(hashlib.sha256(batch_id.encode()).hexdigest()[:2], 16) % 2 == 0 else "worker-b"
        return self.write_shard(worker, batch_id, source_id, source_revision, records, schema_version)

    def _manifest_path(self, batch_id: str) -> Path:
        safe = hashlib.sha256(batch_id.encode()).hexdigest()[:24]
        return self.batches / f"{safe}.manifest.json"

    def _load_manifest(self, manifest: Manifest | str | Path) -> tuple[Manifest, list[dict[str, Any]]]:
        batch_id = "unknown"
        try:
            if isinstance(manifest, Manifest):
                m = manifest
            else:
                p = Path(manifest).resolve()
                if p.parent != self.batches.resolve():
                    raise BatchError("Manifest must be inside the batch directory.")
                obj = json.loads(p.read_text(encoding="utf-8"))
                m = Manifest(**obj)
            batch_id = m.batch_id
            if m.schema_version != "1":
                raise BatchError("Unsupported schema version.")
            snap = (self.batches / Path(m.snapshot).name).resolve()
            if snap.parent != self.batches.resolve() or not snap.is_file():
                raise BatchError("Snapshot is missing or outside the batch directory.")
            with _sqlite(f"file:{snap}?mode=ro", uri=True) as db:
                found = db.execute("SELECT digest,body FROM batches WHERE batch_id=?", (m.batch_id,)).fetchone()
            if not found or found[0] != m.digest or hashlib.sha256(found[1].encode("utf-8")).hexdigest() != m.digest:
                raise BatchError("Snapshot digest does not match the manifest.")
            raw = json.loads(found[1])
            rows = [_canonical_record(r) for r in raw]
            if len(rows) != m.record_count or len({r["record_id"] for r in rows}) != m.unique_record_count:
                raise BatchError("Snapshot counts do not match the manifest.")
            if any(r["source_id"] != m.source_id or r["revision"] != m.source_revision for r in rows):
                raise BatchError("Snapshot schema provenance does not match the manifest.")
            return m, rows
        except (BatchError, ValueError, TypeError, KeyError, OSError, sqlite3.Error) as exc:
            self.record_quarantine(batch_id, str(exc))
            if isinstance(exc, BatchError):
                raise
            raise BatchError("Manifest or snapshot could not be validated.") from exc

    def consolidate(self, manifest: Manifest | str | Path, failpoint: str | None = None) -> Receipt:
        """Validate and atomically commit canonical records plus ingest receipt."""
        m, rows = self._load_manifest(manifest)
        if failpoint == "before_commit":
            raise RuntimeError("Injected stop before canonical commit")
        now = _utcnow()
        try:
            with self._connect() as db:
                previous = db.execute("SELECT digest,accepted,duplicate_records,tombstones,committed_at FROM ingest_receipts WHERE batch_id=?", (m.batch_id,)).fetchone()
                if previous:
                    if previous[0] != m.digest:
                        raise BatchError("Batch ID conflicts with a prior ingest receipt.")
                    return Receipt(m.batch_id, "committed", previous[1], previous[2], previous[3], previous[4])
                accepted = duplicates = tombstones = 0
                # Domain dedup is separate from batch/delivery identity. Higher numeric revision wins.
                for r in rows:
                    current = db.execute("SELECT source_id,revision,occurred_at,amount,payload,deleted FROM records WHERE record_id=?", (r["record_id"],)).fetchone()
                    if current:
                        existing = tuple(current)
                        incoming = (r["source_id"], r["revision"], r["occurred_at"], r["amount"], r["payload"], r["deleted"])
                        old_rev, new_rev = int(current[1]), int(r["revision"])
                        if new_rev < old_rev:
                            duplicates += 1
                            continue
                        if new_rev == old_rev:
                            if existing != incoming:
                                raise BatchError("Record ID has conflicting content at the same revision.")
                            duplicates += 1
                            continue
                        db.execute("UPDATE records SET source_id=?,revision=?,occurred_at=?,amount=?,payload=?,deleted=?,batch_id=?,updated_at=? WHERE record_id=?",
                                   (r["source_id"], r["revision"], r["occurred_at"], r["amount"], r["payload"], r["deleted"], m.batch_id, now, r["record_id"]))
                    else:
                        db.execute("INSERT INTO records VALUES(?,?,?,?,?,?,?,?,?)",
                                   (r["record_id"], r["source_id"], r["revision"], r["occurred_at"], r["amount"], r["payload"], r["deleted"], m.batch_id, now))
                    accepted += 1
                    tombstones += r["deleted"]
                db.execute("INSERT INTO ingest_receipts VALUES(?,?,?,?,?,?,?,?,?)",
                           (m.batch_id, m.digest, m.source_id, m.source_revision, m.record_count, accepted, duplicates, tombstones, now))
                db.execute("INSERT INTO outbox VALUES(?,?,?,NULL)", (m.batch_id, m.digest, now))
        except BatchError as exc:
            self.record_quarantine(m.batch_id, str(exc))
            raise
        if failpoint == "after_commit":
            raise RuntimeError("Injected stop after canonical commit")
        return Receipt(m.batch_id, "committed", accepted, duplicates, tombstones, now)

    def deliver(self, batch_id: str, failpoint: str | None = None) -> DeliveryReceipt:
        """Upsert current canonical state to DuckDB with sink-side receipt lookup."""
        with self._delivery_lock():
            return self._deliver_locked(batch_id, failpoint)

    def _deliver_locked(self, batch_id: str, failpoint: str | None = None) -> DeliveryReceipt:
        if duckdb is None:
            raise RuntimeError("DuckDB adapter requires `pip install duckdb`.")
        with self._connect() as db:
            out = db.execute("SELECT digest FROM outbox WHERE batch_id=?", (batch_id,)).fetchone()
            if not out:
                raise BatchError("Batch has not been consolidated.")
            recorded = db.execute("SELECT sink,digest,row_count,committed_at FROM delivery_receipts WHERE batch_id=?", (batch_id,)).fetchone()
            if recorded and recorded[0] == str(self.sink_path) and recorded[1] == out[0]:
                return DeliveryReceipt(batch_id, recorded[0], "acknowledged", recorded[2], recorded[1], recorded[3])
            rows = [dict(r) for r in db.execute("SELECT record_id,source_id,revision,occurred_at,amount,payload,deleted,batch_id FROM records ORDER BY record_id")]
        # DuckDB serializes a JSON receipt and typed source records in a single transaction.
        conn = duckdb.connect(str(self.sink_path))
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS records(record_id VARCHAR PRIMARY KEY, source_id VARCHAR, revision VARCHAR, occurred_at VARCHAR, amount DECIMAL(38,18), payload VARCHAR, deleted BOOLEAN, batch_id VARCHAR)")
            conn.execute("CREATE TABLE IF NOT EXISTS delivery_receipts(batch_id VARCHAR PRIMARY KEY, digest VARCHAR, row_count BIGINT, committed_at VARCHAR)")
            sink_receipt = conn.execute("SELECT digest,row_count,committed_at FROM delivery_receipts WHERE batch_id=?", [batch_id]).fetchone()
            if sink_receipt and sink_receipt[0] == out[0]:
                rows_count, committed = sink_receipt[1], sink_receipt[2]
            elif sink_receipt:
                raise BatchError("DuckDB already has a conflicting receipt for this batch.")
            else:
                conn.execute("BEGIN TRANSACTION")
                try:
                    for row in rows:
                        conn.execute("INSERT INTO records VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(record_id) DO UPDATE SET source_id=excluded.source_id, revision=excluded.revision, occurred_at=excluded.occurred_at, amount=excluded.amount, payload=excluded.payload, deleted=excluded.deleted, batch_id=excluded.batch_id",
                                     [row["record_id"], row["source_id"], row["revision"], row["occurred_at"], row["amount"], row["payload"], bool(row["deleted"]), row["batch_id"]])
                    committed = _utcnow()
                    conn.execute("INSERT OR REPLACE INTO delivery_receipts VALUES(?,?,?,?)", [batch_id, out[0], len(rows), committed])
                    conn.execute("COMMIT")
                    rows_count = len(rows)
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
        finally:
            conn.close()
        if failpoint == "before_ack":
            raise RuntimeError("Injected stop after sink commit and before local acknowledgement")
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO delivery_receipts VALUES(?,?,?,?,?)", (batch_id, str(self.sink_path), out[0], rows_count, committed))
            db.execute("UPDATE outbox SET acked_at=? WHERE batch_id=?", (_utcnow(), batch_id))
        return DeliveryReceipt(batch_id, str(self.sink_path), "acknowledged", rows_count, out[0], committed)

    def status(self) -> dict[str, Any]:
        with self._connect() as db:
            return {
                "batches": [dict(r) for r in db.execute("SELECT batch_id,source_id,source_revision,record_count,accepted,duplicate_records,tombstones,committed_at FROM ingest_receipts ORDER BY committed_at DESC")],
                "queue": [dict(r) for r in db.execute("SELECT o.batch_id,o.created_at,o.acked_at,CASE WHEN o.acked_at IS NULL THEN 'pending' ELSE 'delivered' END AS status FROM outbox o ORDER BY o.created_at")],
                "quarantines": [dict(r) for r in db.execute("SELECT batch_id,reason,quarantined_at FROM quarantines ORDER BY quarantined_at DESC")],
                "record_count": db.execute("SELECT count(*) FROM records").fetchone()[0],
                "tombstone_count": db.execute("SELECT count(*) FROM records WHERE deleted=1").fetchone()[0],
                "worker_shards": [p.name for p in sorted(self.shards.glob("worker-*.sqlite"))],
                "duckdb_available": duckdb is not None,
                "runtime_path": str(self.root),
            }

    def record_quarantine(self, batch_id: str, reason: str) -> None:
        with self._connect() as db:
            db.execute("INSERT OR REPLACE INTO quarantines VALUES(?,?,?)", (batch_id, reason[:1000], _utcnow()))

    def records(self, include_deleted: bool = True) -> list[dict[str, Any]]:
        with self._connect() as db:
            sql = "SELECT record_id,source_id,revision,occurred_at,amount,payload,deleted,batch_id FROM records"
            if not include_deleted:
                sql += " WHERE deleted=0"
            return [dict(r) for r in db.execute(sql + " ORDER BY record_id")]

