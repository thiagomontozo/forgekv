# ForgeKV Architecture

## Scope

ForgeKV v0.4 is a persistent key-value database with standalone operation, asynchronous leader/read-only-follower replication, and experimental statically configured partitioning. It owns its TCP protocols, in-memory data structures, expiration, persistence, recovery, replication state, and key ownership routing. It deliberately excludes HTTP CRUD, external databases, Redis compatibility, consensus, automatic failover, dynamic membership, and advanced authentication.

## Networking and async runtime

`forgekv-server` uses Tokio's multi-threaded runtime. A `TcpListener` accepts clients and creates one lightweight Tokio task per connection. Each connection repeatedly reads exactly one bounded frame, parses it into a typed command, executes it, and writes one typed response.

The four-byte frame length is read first. The server checks it against `FORGEKV_MAX_FRAME_SIZE` before allocating the body. A clean EOF before a new length is a normal disconnect. EOF after any part of a frame is a protocol error. Invalid versions, opcodes, lengths, and trailing payload data return a structured `INVALID_REQUEST` response; input does not trigger a panic.

## Connection lifecycle

An atomic guard increments `connections_total` and `connections_active` when the handler starts and decrements the active count on every exit path. Connection logs contain peer addresses and error categories but never values. A client disconnect, malformed request, I/O error, or shutdown notification terminates only that connection task.

The accept loop uses a Tokio semaphore sized by `FORGEKV_MAX_CONNECTIONS`. Excess connections are closed immediately and counted without creating a handler task.

## Command execution

The router distinguishes read-only and mutating commands:

- Reads (`GET`, `EXISTS`, `TTL`, `INFO`, `STATS`) access the store or atomic metrics directly.
- Mutations (`SET`, `DEL`, `SETEX`, `PERSIST`) acquire the WAL ordering guard, append and apply the mutation, then release it.
- `PING` has no state access.

The WAL guard is intentionally held across the asynchronous append and the following short in-memory mutation. This serializes mutations, but ensures another task cannot persist and apply a later command before the earlier command reaches memory. No store shard lock is held across an `.await`.

Clients may pipeline up to 1,024 commands. The client writes all frames before reading responses; the server executes them sequentially per connection, preserving response order without request identifiers.

When static cluster mode is enabled, routing happens after payload validation and before persistence or store access. Key-bearing commands use the cluster ring; a non-owner returns a typed redirect containing the configured client address. `PING`, `INFO`, and `STATS` always execute on the contacted node. The bundled client follows a bounded redirect chain and detects repeated addresses. Redirected pipeline elements are resolved individually after the seed node returns the ordered initial responses.

Followers reject `SET`, `DEL`, `SETEX`, and `PERSIST` before they reach the WAL. Reads remain available while continuous replication reconnects, so they may be stale. A follower performs one initial synchronization before its client listener is bound.

## Store and sharding

The store owns a fixed vector of shards. Each shard is a standard `RwLock<HashMap<Vec<u8>, Entry>>`. There is no global map lock. ForgeKV computes a deterministic FNV-1a 64-bit hash and selects `hash % shard_count`.

Per-shard locking reduces contention for unrelated keys and keeps dependencies small. It does not eliminate contention for hot keys or keys mapping to the same shard. FNV-1a is chosen for stable internal distribution, not cryptographic protection. Because ForgeKV does not expose hash values or persist shard placement, a future version may change the hash function as long as replay rebuilds the store.

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

A short final record is treated as a crash tail and truncated to the last valid offset. Invalid magic, invalid fields, excessive lengths, or checksum failure in a complete record stops recovery. Automatic compaction takes the same WAL ordering guard, captures live entries, writes a checksummed snapshot on a blocking worker, atomically installs it, and resets the WAL. Recovery loads the snapshot first and then replays the WAL.

See [Persistence](persistence.md) for the exact format.

## Replication

Leaders expose a dedicated bounded TCP endpoint. A follower identifies the last durable leader node, WAL generation, and byte offset. When these match, the leader reads only complete validated records up to the configured batch limit. The follower validates, appends, applies, synchronizes, and then persists its checksummed progress.

Compaction increments persistent WAL generation. A new follower, different node identity, generation mismatch, or incompatible offset forces a full checksummed snapshot. Snapshot capture and WAL reads take the same mutation ordering guard, so every response represents a coherent boundary. The full wire contract is in [Replication](replication.md).

## Experimental partitioning

Every cluster node receives the same static `node-id@host:port` membership and virtual-node count. Membership is sorted by node ID before a deterministic FNV-1a ring is constructed, so input order does not alter placement. Each member contributes a fixed number of ring points. A key belongs to the first point whose hash is at or after the key hash, wrapping to the first point at the end of the ring.

Partition ownership is independent from the in-process shard calculation: the cluster ring chooses a server, then that server's shard hash chooses a local lock and map. Nodes do not proxy requests or communicate for ownership. The client follows the advertised owner address. There is no membership protocol, failure detector, replica placement, data movement, or availability guarantee. Cluster and leader/follower modes are rejected as an invalid combination in v0.4. See [Cluster Partitioning](cluster.md).

## Metrics and logging

Hot-path counters use relaxed atomics because they are observational and do not establish correctness ordering. `STATS` returns a snapshot of all counters, including cluster-local routing and redirects. `INFO` derives version, uptime, live key count, shard count, actual listening address, fsync mode, replication role, and cluster identity from active state.

`tracing` records startup, WAL initialization and replay, connections, protocol, persistence and replication errors, expiration, synchronization boundaries, shutdown, and WAL flush. Stored values are never logged.

A separate bounded HTTP listener exports Prometheus text metrics. It accepts only `GET /metrics`, caps request headers at 8 KiB, limits concurrent metrics clients to 64, uses short I/O timeouts, and is not part of the database command protocol.

## Graceful shutdown

Ctrl+C sends a watch notification:

1. the accept loop stops accepting;
2. idle connection reads are cancelled;
3. commands already executing finish their current lifecycle operation;
4. expiration, periodic fsync, compaction, metrics, and replication tasks stop;
5. the server awaits all connection tasks;
6. the main process flushes the WAL and synchronizes it when required by the configured policy;
7. resources are dropped and the process exits.

The server owns and awaits its background task and connection `JoinSet`, preventing orphaned tasks during a normal shutdown.
