# Security

SQLite Batch Loader moves bounded batches from worker SQLite shards through a canonical SQLite store into DuckDB. It validates manifests, snapshot hashes, and record counts, and uses worker and delivery locks. SHA-256 detects a changed snapshot when its expected digest is intact; it does not protect against replacing both. The local server checks Host and expected loopback Origin and requires a token for writes. Runtime data is plaintext protected by account permissions. Use made-up records in reports.

Use your Mac’s disk encryption to protect saved files. The native app has an ad-hoc code signature. This lets the installer check whether its files changed. It does not prove who made the app. The build is not notarized.

Report security issues through [GitHub private vulnerability reporting](https://github.com/gmperkinsmba/sqlite-batch-loader/security/advisories/new).
