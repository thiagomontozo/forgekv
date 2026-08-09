# Changelog

## 0.4.0

- Add experimental static cluster membership and deterministic consistent hashing.
- Route key commands to one partition owner before local store or WAL access.
- Add a typed binary redirect response and bounded redirect-aware client behavior.
- Add cluster counters, integration tests, a three-node Compose example, documentation, and an ADR.
- Keep cluster partitioning explicitly separate from leader/follower replication.

## 0.3.0

- Add asynchronous leader/follower replication on a dedicated binary TCP protocol.
- Add incremental WAL transfer with identity and generation validation.
- Add checksummed full-snapshot fallback and persistent follower offsets.
- Make followers read-only and synchronize them before accepting client traffic.
- Add replication metrics, integration tests, container examples, documentation, and an ADR.

## 0.2.0

- Add checksummed snapshots and size-triggered WAL compaction.
- Add `fsync=everysec` with periodic and shutdown synchronization.
- Add ordered client pipelining.
- Add configurable concurrent connection limits.
- Add a bounded Prometheus text metrics endpoint.
- Extend metrics, tests, benchmarks, container configuration, and documentation.

## 0.1.0

- Initial TCP database, binary protocol, sharded store, TTL, WAL recovery, CLI, tests, benchmarks, containers, and CI.
