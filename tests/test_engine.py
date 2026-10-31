import json
import itertools
import tempfile
import unittest
from pathlib import Path

from bd3 import BatchError, Pipeline
from bd3.engine import duckdb


def row(rid="000042", revision="1", **overrides):
    data = {"record_id": rid, "source_id": "fixture", "revision": revision,
            "occurred_at": "2026-10-30T09:12:00-05:00", "amount": "12345678901234567890.123456789012345678",
            "payload": "synthetic", "deleted": False}
    data.update(overrides)
    return data


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.pipe = Pipeline(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_two_worker_files_and_consistent_snapshot(self):
        a = self.pipe.write_shard("worker-a", "a", "fixture", "1", [row()])
        b = self.pipe.write_shard("worker-b", "b", "fixture", "1", [row("000043")])
        self.assertTrue((Path(self.tmp.name) / "shards/worker-a.sqlite").exists())
        self.assertTrue((Path(self.tmp.name) / "shards/worker-b.sqlite").exists())
        self.assertEqual(self.pipe.consolidate(a).accepted, 1)
        self.assertEqual(self.pipe.consolidate(b).accepted, 1)

    def test_worker_lease_and_batch_publication_are_immutable(self):
        m = self.pipe.write_shard("worker-a", "stable", "fixture", "1", [row()])
        replay = self.pipe.write_shard("worker-b", "stable", "fixture", "1", [row()])
        self.assertEqual(replay, m)
        with self.pipe._worker_lock("worker-a"):
            with self.assertRaisesRegex(RuntimeError, "owned by another active process"):
                self.pipe.write_shard("worker-a", "blocked", "fixture", "1", [row("000043")])
            self.pipe.write_shard("worker-b", "parallel", "fixture", "1", [row("000044")])

    def test_record_count_bound_stops_consuming_generator(self):
        consumed = 0
        def rows():
            nonlocal consumed
            for n in itertools.count():
                consumed += 1
                yield row(rid=f"id-{n}")
        with self.assertRaisesRegex(BatchError, "exceeds 100000"):
            self.pipe.publish("too-large", "fixture", "1", rows())
        self.assertEqual(consumed, 100001)

    def test_duplicate_delivery_and_domain_dedupe_are_separate(self):
        m = self.pipe.publish("batch-1", "fixture", "1", [row(), row()])
        receipt = self.pipe.consolidate(m)
        self.assertEqual((receipt.accepted, receipt.duplicate_records), (1, 1))
        self.assertEqual(self.pipe.consolidate(m), receipt)
        self.assertEqual(self.pipe.status()["record_count"], 1)

    def test_reject_malformed_schema_and_manifest_tamper(self):
        bad = row(); bad["amount"] = "NaN"
        with self.assertRaises(BatchError):
            self.pipe.publish("bad", "fixture", "1", [bad])
        bad_float = row(); bad_float["amount"] = 12.5
        with self.assertRaises(BatchError):
            self.pipe.publish("bad-float", "fixture", "1", [bad_float])
        too_wide = row(); too_wide["amount"] = "123456789012345678901.00"
        with self.assertRaises(BatchError):
            self.pipe.publish("too-wide", "fixture", "1", [too_wide])
        too_precise = row(); too_precise["amount"] = "0.0000000000000000001"
        with self.assertRaises(BatchError):
            self.pipe.publish("too-precise", "fixture", "1", [too_precise])
        self.assertEqual(len(self.pipe.status()["quarantines"]), 4)
        m = self.pipe.publish("good", "fixture", "1", [row()])
        with self.assertRaisesRegex(BatchError, "different content"):
            self.pipe.publish("good", "fixture", "1", [row(payload="changed")])
        p = self.pipe._manifest_path(m.batch_id)
        data = json.loads(p.read_text()); data["record_count"] = 99; p.write_text(json.dumps(data))
        with self.assertRaises(BatchError):
            self.pipe.consolidate(p)
        self.assertEqual(len(self.pipe.status()["quarantines"]), 5)

    def test_numeric_revisions_and_equal_revision_conflict(self):
        m9 = self.pipe.publish("rev-nine", "fixture", "9", [row(revision="9")])
        self.pipe.consolidate(m9)
        m10 = self.pipe.publish("rev-ten", "fixture", "10", [row(revision="10", payload="newer")])
        self.pipe.consolidate(m10)
        self.assertEqual(self.pipe.records()[0]["payload"], "newer")
        conflict = self.pipe.publish("rev-ten-conflict", "fixture", "10", [row(revision="10", payload="different")])
        with self.assertRaisesRegex(BatchError, "conflicting content"):
            self.pipe.consolidate(conflict)
        self.assertIn("rev-ten-conflict", [q["batch_id"] for q in self.pipe.status()["quarantines"]])

    def test_crash_before_and_after_stage_commit(self):
        m = self.pipe.publish("batch", "fixture", "1", [row()])
        with self.assertRaisesRegex(RuntimeError, "before canonical"):
            self.pipe.consolidate(m, failpoint="before_commit")
        self.assertEqual(self.pipe.status()["record_count"], 0)
        with self.assertRaisesRegex(RuntimeError, "after canonical"):
            self.pipe.consolidate(m, failpoint="after_commit")
        recovered = self.pipe.consolidate(m)
        self.assertEqual(recovered.accepted, 1)
        self.assertEqual(self.pipe.status()["record_count"], 1)

    def test_tombstone_correction_and_old_replay_guard(self):
        old = self.pipe.publish("old", "fixture", "1", [row(revision="1")])
        self.pipe.consolidate(old)
        corrected = self.pipe.publish("corrected", "fixture", "2", [row(revision="2", payload="corrected", amount="10.01", deleted=True)])
        self.pipe.consolidate(corrected)
        self.assertTrue(self.pipe.records()[0]["deleted"])
        replay = self.pipe.publish("old-replay", "fixture", "1", [row(revision="1", payload="old replay")])
        result = self.pipe.consolidate(replay)
        self.assertEqual(result.duplicate_records, 1)
        record = self.pipe.records()[0]
        self.assertTrue(record["deleted"])
        self.assertEqual(record["payload"], "corrected")

    def test_null_exact_decimal_timestamp_and_string_id_mapping(self):
        m = self.pipe.publish("types", "fixture", "1", [row(rid="0000000000000000000042", occurred_at=None, amount="0.000000000000000001")])
        self.pipe.consolidate(m)
        record = self.pipe.records()[0]
        self.assertEqual(record["record_id"], "0000000000000000000042")
        self.assertIsNone(record["occurred_at"])
        self.assertEqual(record["amount"], "0.000000000000000001")

    @unittest.skipIf(duckdb is None, "duckdb package not installed")
    def test_sink_commit_before_ack_reconciles_from_sink_receipt(self):
        m = self.pipe.publish("delivery", "fixture", "1", [row()])
        self.pipe.consolidate(m)
        with self.assertRaisesRegex(RuntimeError, "before local acknowledgement"):
            self.pipe.deliver(m.batch_id, failpoint="before_ack")
        self.assertIsNone(self.pipe.status()["queue"][0]["acked_at"])
        receipt = self.pipe.deliver(m.batch_id)
        self.assertEqual(receipt.status, "acknowledged")
        self.assertEqual(receipt.row_count, 1)
        conn = duckdb.connect(str(self.pipe.sink_path), read_only=True)
        try:
            self.assertEqual(conn.execute("SELECT record_id FROM records").fetchone()[0], "000042")
            self.assertEqual(conn.execute("SELECT count(*) FROM delivery_receipts").fetchone()[0], 1)
        finally:
            conn.close()

    @unittest.skipIf(duckdb is None, "duckdb package not installed")
    def test_delivery_lease_and_conflicting_sink_receipt(self):
        m = self.pipe.publish("conflict", "fixture", "1", [row()])
        self.pipe.consolidate(m)
        with self.pipe._delivery_lock():
            with self.assertRaisesRegex(RuntimeError, "owned by another active process"):
                self.pipe.deliver(m.batch_id)
        with self.assertRaisesRegex(RuntimeError, "before local acknowledgement"):
            self.pipe.deliver(m.batch_id, failpoint="before_ack")
        conn = duckdb.connect(str(self.pipe.sink_path))
        try:
            conn.execute("UPDATE delivery_receipts SET digest='wrong-digest' WHERE batch_id=?", [m.batch_id])
        finally:
            conn.close()
        with self.assertRaisesRegex(BatchError, "conflicting receipt"):
            self.pipe.deliver(m.batch_id)


if __name__ == "__main__":
    unittest.main()
