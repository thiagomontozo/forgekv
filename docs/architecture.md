# ForgeKV Architecture

## Scope

ForgeKV v0.1 is a single-node persistent key-value database. It owns its TCP protocol, in-memory data structures, expiration, persistence, and recovery. It deliberately excludes HTTP, external databases, Redis compatibility, clustering, and advanced authentication.

## Networking and async runtime

`forgekv-server` uses Tokio's multi-threaded runtime. A `TcpListener` accepts clients and creates one lightweight Tokio task per connection. Each connection repeatedly reads exactly one bounded frame, parses it into a typed command, executes it, and writes one typed response.

The four-byte frame length is read first. The server checks it against `FORGEKV_MAX_FRAME_SIZE` before allocating the body. A clean EOF before a new length is a normal disconnect. EOF after any part of a frame is a protocol error. Invalid versions, opcodes, lengths, and trailing payload data return a structured `INVALID_REQUEST` response; input does not trigger a panic.

## Connection lifecycle

An atomic guard increments `connections_total` and `connections_active` when the handler starts and decrements the active count on every exit path. Connection logs contain peer addresses and error categories but never values. A client disconnect, malformed request, I/O error, or shutdown notification terminates only that connection task.

There is no connection cap in v0.1. Adding a semaphore-based limit is a v0.2 objective.

## Command execution

The router distinguishes read-only and mutating commands:

- Reads (`GET`, `EXISTS`, `TTL`, `INFO`, `STATS`) access the store or atomic metrics directly.
- Mutations (`SET`, `DEL`, `SETEX`, `PERSIST`) acquire the WAL ordering guard, append and apply the mutation, then release it.
- `PING` has no state access.

The WAL guard is intentionally held across the asynchronous append and the following short in-memory mutation. This serializes mutations, but ensures another task cannot persist and apply a later command before the earlier command reaches memory. No store shard lock is held across an `.await`.

## Store and sharding

The store owns a fixed vector of shards. Each shard is a standard `RwLock<HashMap<Vec<u8>, Entry>>`. There is no global map lock. ForgeKV computes a deterministic FNV-1a 64-bit hash and selects `hash % shard_count`.

Per-shard locking reduces contention for unrelated keys and keeps dependencies small. It does not eliminate contention for hot keys or keys mapping to the same shard. FNV-1a is chosen for stable internal distribution, not cryptographic protection. Because v0.1 does not expose hash values or persist shard placement, a future version may change the hash function as long as replay rebuilds the store.

Store operations are synchronous and short. They use `std::sync::RwLock` so no runtime-aware lock is held around an await. Lock poisoning becomes an explicit `StoreError`; it is not ignored.

## Entry model

An entry contains only:

- the binary value as `Bytes`;
- an optional absolute `SystemTime` expiration.

The key is owned once by the shard's `HashMap`. Persistent entries use `None`; expiring entries use `Some(timestamp)`.

## TTL

TTL has two complementary paths:

1. Lazy expiration: `GET`, `EXISTS`, and `TTL` detect an expired entry, release any read guard, acquire the shard write guard, re-check it, and remove it.
2. Background expiration: one server-level task ticks at `FORGEKV_EXPIRATION_INTERVAL_MS`, scans each shard briefly, and retains only live entries.

There is never one task per key. The background task receives the same shutdown signal as connection handlers and is awaited by the server.

## WAL and recovery

The write-ahead log is the source used to rebuild memory. Every mutation has a typed record. A CRC32 protects each complete record. Startup validates the file header, record magic, version, reserved bytes, length limits, checked size arithmetic, type-specific fields, and checksum before applying a record.

A short final record is treated as a crash tail and truncated to the last valid offset. Invalid magic, invalid fields, excessive lengths, or checksum failure in a complete record stops recovery. ForgeKV does not silently scan past corruption.

See [Persistence](persistence.md) for the exact format.

## Metrics and logging

Hot-path counters use relaxed atomics because they are observational and do not establish correctness ordering. `STATS` returns a snapshot of all counters. `INFO` derives version, uptime, live key count, shard count, actual listening address, and fsync mode from active state.

`tracing` records startup, WAL initialization and replay, connections, protocol and persistence errors, expiration, shutdown, and WAL flush. Stored values are never logged.

## Graceful shutdown

Ctrl+C sends a watch notification:

1. the accept loop stops accepting;
2. idle connection reads are cancelled;
3. commands already executing finish their current lifecycle operation;
4. the expiration task stops;
5. the server awaits all connection tasks;
6. the main process flushes the WAL and synchronizes it when `fsync=always`;
7. resources are dropped and the process exits.

The server owns and awaits its background task and connection `JoinSet`, preventing orphaned tasks during a normal shutdown.
