# Domain and invariants

## Batch
A batch has a stable string ID, source and revision, fixed schema version, canonical content digest, row counts, and immutable published SQLite snapshot. Replaying the same ID and digest returns its original receipt. A conflicting digest is rejected.

## Worker shard
Each worker writes to its own SQLite file. Two fixed workers bound the first release. SQLite's backup API creates a consistent snapshot that includes committed WAL state. The manifest is published only after the snapshot.

## Canonical stage
One SQLite database owns normalized domain records, ingest receipts, quarantines, and the destination outbox. Record IDs remain strings. Decimal values remain exact decimal text in the stage. Null values remain null. Tombstones stay in the stage to block older replays.

## Destination receipt
DuckDB owns destination rows and a sink-side delivery receipt in one transaction. The local acknowledgement follows that transaction. If the process stops between them, retry looks up the sink receipt before acknowledging locally.

## Limits
This build supports only the local DuckDB adapter. Revision labels are compared as strings and must sort in the same order as source revisions. Staging retention, cloud adapters, distributed workers, and production-scale throughput are not implemented.
