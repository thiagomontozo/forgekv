# Add Generation-Aware Leader/Follower Replication

## Status

Accepted

## Context

Incremental replication must preserve the leader's WAL order without treating a byte offset from an old, compacted, or different WAL as current. Followers must also recover their progress without acknowledging data that was not made durable locally.

## Decision

Use a dedicated bounded binary TCP protocol. Persist a stable node identity and monotonically increasing WAL generation on the leader. A follower requests complete WAL records using identity, generation, and offset. Any mismatch triggers a checksummed full snapshot captured at the current WAL boundary without changing generation. Followers remain read-only, force replicated WAL batches to disk before atomically persisting checksummed progress, and complete an initial synchronization before accepting clients.

## Consequences

Normal replication transfers only new WAL records and reuses existing record validation. Compaction and leader changes are detected rather than inferred from file length. Full sync pauses leader mutations while the snapshot is captured and materializes a bounded snapshot in memory. Replication remains asynchronous and provides no election, quorum, fencing, automatic promotion, authentication, or encryption.
