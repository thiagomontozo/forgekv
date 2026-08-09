# Use a Sharded In-Memory Store

## Status

Accepted

## Context

A single `Mutex<HashMap>` would serialize all in-memory access and obscure the concurrency design. A large concurrent-map dependency would reduce the opportunity to expose lock scope and trade-offs.

## Decision

Split keys across a fixed configurable vector of `RwLock<HashMap>` shards selected by deterministic FNV-1a hashing. Keep each critical section synchronous and never hold a shard lock across an await.

## Consequences

Unrelated shards can be accessed concurrently, the implementation remains small, and behavior is easy to audit. Hot shards still contend, resizing the shard count requires rebuilding memory, and background expiration scans every shard.
