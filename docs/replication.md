# ForgeKV Replication Protocol v1

## Scope

ForgeKV v0.3 provides asynchronous replication from one writable leader to read-only followers. Replication uses a dedicated TCP listener, defaults to port `6381`, and is separate from the client protocol on port `6380`. It does not implement consensus, leader election, quorum acknowledgement, automatic promotion, or multi-leader conflict resolution.

## State model

Every data directory has a persistent hexadecimal node identity in `forgekv.node`. A leader also exposes a positive `u64` WAL generation stored in `forgekv.generation`. Every successful compaction advances the generation before the WAL is reset.

A follower persists the last acknowledged leader identity, generation, and WAL byte offset in checksummed `forgekv.replica`. The follower synchronizes its local WAL to disk before atomically advancing this file. Reapplying a batch after a crash is safe because mutation records rebuild the same final key state.

## Transport rules

- Transport: TCP
- Default leader port: `6381`
- Byte order: unsigned big-endian integers
- Protocol magic: ASCII `FKRP`
- Protocol version: `0x01`
- Maximum node identity: 64 ASCII hexadecimal bytes
- One request and one response per TCP connection
- Incremental payload limit: `FORGEKV_REPLICATION_MAX_BATCH_SIZE`
- Snapshot payload limit: `FORGEKV_REPLICATION_MAX_SNAPSHOT_SIZE`
- Connection setup and protocol I/O use finite timeouts so shutdown cannot wait indefinitely

All lengths and offset arithmetic must be checked before allocation or slicing. EOF in any declared field is an error.

## HELLO request

The follower sends:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `FKRP` |
| 4 | 1 | version | `0x01` |
| 5 | 1 | kind | `0x01` (`HELLO`) |
| 6 | 2 | leader_id_length | Expected leader identity length |
| 8 | 8 | generation | Last durable leader WAL generation |
| 16 | 8 | offset | Next leader WAL byte offset required |
| 24 | N | leader_id | Expected lowercase or uppercase hexadecimal identity |

An initial follower sends an empty identity, generation `0`, and offset `0`. An empty identity is legal only in `HELLO`.

## Response header

The leader returns a fixed 48-byte header, then the leader identity and payload:

| Offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `FKRP` |
| 4 | 1 | version | `0x01` |
| 5 | 1 | kind | `0x02` batch or `0x03` snapshot |
| 6 | 2 | leader_id_length | Identity bytes following the fixed header |
| 8 | 8 | generation | Current leader WAL generation |
| 16 | 8 | start_offset | First WAL byte represented by a batch; zero for snapshot |
| 24 | 8 | end_offset | Next follower offset after applying this response |
| 32 | 8 | leader_end | Leader WAL length at response capture time |
| 40 | 8 | payload_length | Bytes following the leader identity |
| 48 | N | leader_id | Current leader identity |
| 48+N | P | payload | Complete WAL records or one snapshot file |

The follower is caught up to the response capture point when `end_offset >= leader_end`.

## Incremental batch (`0x02`)

The leader sends a batch only when all of these match current state:

- requested leader identity;
- WAL generation;
- offset within the active WAL, at a record boundary.

The payload is a concatenation of complete WAL records in the exact format documented in [Persistence](persistence.md). It never splits a record. `end_offset - start_offset` must equal `payload_length`, and `end_offset` must not exceed `leader_end`.

The follower validates every record, appends it to its own WAL, applies it in order, forces the local WAL to stable storage, and only then persists the new replication offset.

## Full snapshot (`0x03`)

The leader sends a full snapshot when the follower is new, identifies another leader, requests an old generation, or presents an incompatible offset. The leader serializes snapshot capture with mutations and records the current WAL end, but it does not reset the WAL or advance generation merely because a follower requested a full sync. This lets multiple followers converge without invalidating one another.

For a snapshot response, `start_offset` is zero and both `end_offset` and `leader_end` identify the captured leader WAL end. The follower validates the entire snapshot before replacing its local snapshot, WAL, and in-memory shards. It then persists the new leader identity, generation, and offset; later responses continue incrementally from that leader position.

## Startup and continuous synchronization

A follower must complete one synchronization to the leader's captured end before opening its client listener. After startup it polls at `FORGEKV_REPLICATION_INTERVAL_MS`. Network failures leave the last durable state unchanged and are retried; client reads remain available but may be stale.

## Failure and security behavior

- Invalid identity, version, message kind, lengths, offsets, WAL records, snapshot checksum, or state checksum is an explicit error.
- Payload sizes are validated before allocation.
- The leader limits concurrent replication connections to 16.
- User keys and values are never logged.
- Replication is unauthenticated and unencrypted in v0.3. Deploy it only on a trusted network or behind an authenticated encrypted tunnel.
