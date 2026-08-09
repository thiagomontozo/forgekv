# Use a Checksummed Write-Ahead Log

## Status

Accepted

## Context

ForgeKV must persist and recover its own data without delegating storage to SQLite or another database. The persistent representation needs mutation ordering, explicit corruption detection, and a clear crash-tail policy.

## Decision

Append a versioned binary record for every `SET`, `DEL`, `SETEX`, and `PERSIST` before updating memory. Protect complete records with CRC32, offer `always` and `none` fsync modes, replay sequentially at startup, truncate only an incomplete final record, and fail on complete-record corruption.

## Consequences

Recovery behavior is deterministic and the durability trade-off is configurable. Mutations are serialized by one ordering guard, the log grows without bound, and CRC32 detects accidental damage rather than malicious modification. Compaction, snapshots, group commit, and `everysec` are deferred.
