# Add Checksummed Snapshots and WAL Compaction

## Status

Accepted

## Context

An append-only WAL grows indefinitely and increases recovery time. Compaction must not let concurrent mutations create a snapshot that disagrees with the remaining log, and a crash during file replacement must retain at least one recoverable state.

## Decision

Serialize compaction with the existing WAL mutation guard. Capture live entries, write a versioned checksummed snapshot to a synchronized temporary file, install it with a recoverable backup, and only then reset the WAL. Load the snapshot before WAL replay at startup.

## Consequences

WAL growth and restart replay are bounded by the configured threshold. Mutations pause while the entry list is captured and the snapshot worker completes. The full data set must fit in memory twice during compaction, and snapshots remain single-node artifacts without incremental transfer.
