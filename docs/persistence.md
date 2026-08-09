# ForgeKV Persistence

## Purpose and path

ForgeKV v0.1 persists mutations in one append-only binary write-ahead log at `${FORGEKV_DATA_DIR}/forgekv.wal`. The default path is `data/forgekv.wal`. The WAL is the durable history used to reconstruct the in-memory store; v0.1 has no snapshots or compaction.

## File header

The file begins with exactly eight bytes:

| Offset | Size | Value | Meaning |
|---:|---:|---|---|
| 0 | 4 | ASCII `FKVW` | WAL magic |
| 4 | 1 | `0x01` | WAL format version |
| 5 | 3 | zero | reserved; must be zero |

An empty new file receives this header before any record. A non-empty file with a different header fails startup.

## Record format

Every multi-byte integer is unsigned big-endian.

| Relative offset | Size | Field | Meaning |
|---:|---:|---|---|
| 0 | 4 | magic | ASCII `FKVR` |
| 4 | 1 | version | `0x01` |
| 5 | 1 | record type | mutation type |
| 6 | 2 | reserved | zero |
| 8 | 8 | timestamp_ms | append creation time since Unix epoch |
| 16 | 8 | expires_at_ms | absolute expiration; `u64::MAX` means none |
| 24 | 4 | key_length | key bytes |
| 28 | 4 | value_length | value bytes |
| 32 | K | key | binary key |
| 32+K | V | value | binary value |
| 32+K+V | 4 | checksum | CRC32 |

The minimum record size is 36 bytes. CRC32 covers bytes beginning at `version` (relative offset 4) through the final value byte; it excludes the four-byte record magic and the checksum field itself.

## Record types

| Type | Code | Value | Expiration |
|---|---:|---|---|
| `SET` | `0x01` | allowed, including empty | `u64::MAX` |
| `DEL` | `0x02` | empty | `u64::MAX` |
| `SETEX` | `0x03` | allowed, including empty | absolute epoch milliseconds |
| `PERSIST` | `0x04` | empty | `u64::MAX` |

Every record requires a non-empty key. Key and value lengths are checked against the same configured limits used by the network protocol. Type/field combinations that do not match this table are corruption.

## Append ordering

All mutations pass through one asynchronous WAL mutex. For each mutation ForgeKV:

1. encodes the complete record in memory;
2. writes all record bytes;
3. flushes the Tokio file buffer;
4. calls `sync_data` when `FORGEKV_FSYNC=always`;
5. applies the change to the selected memory shard;
6. releases the ordering mutex.

The guard deliberately covers both the awaited append and the short memory operation. This makes the applied order match the WAL order when multiple clients mutate concurrently. Store locks are acquired only after the file await and are never held across an await.

## Fsync policies

### `always`

Each acknowledged mutation has completed `sync_data` before memory changes and before its response. This is the stronger v0.1 policy, but it trades throughput and latency for durability. Filesystem, drive cache, hardware, and operating system behavior still affect end-to-end guarantees.

### `none`

Each record is written and flushed through the process buffer, but ForgeKV does not request a per-record disk synchronization. The operating system may delay physical persistence. A process or machine crash can lose recent acknowledged writes.

`everysec` is reserved for v0.2 and is not accepted by the v0.1 configuration parser.

## Recovery

Before accepting clients ForgeKV:

1. validates the file header;
2. reads the next four-byte record magic;
3. reads fixed fields without allocating key/value memory;
4. validates key/value lengths and checked total size;
5. reads exactly the bounded variable section and checksum;
6. validates version, reserved bytes, record type, field semantics, and CRC32;
7. applies the mutation in file order;
8. repeats until the physical end of the file;
9. purges expired keys and reports replay counts.

`SETEX` stores an absolute timestamp. During replay, a `SETEX` already expired at recovery time removes any previous value for that key instead of recreating it. `PERSIST` removes a live expiration if the key exists.

## Truncation and corruption behavior

A crash may leave the last append incomplete. If EOF occurs while reading the final record magic, fixed fields, key, value, or checksum, ForgeKV truncates the file back to the start of that incomplete record, synchronizes the repaired length, and continues startup.

ForgeKV does **not** treat these as a repairable tail:

- invalid complete record magic;
- unsupported record version or type;
- non-zero reserved fields;
- invalid or excessive lengths;
- type/field semantic mismatch;
- checksum mismatch.

Any of these stops recovery with an explicit error. ForgeKV does not scan for the next magic sequence or silently skip data, because doing so could construct an untrustworthy state.

## Crash behavior and limitations

- A crash before a complete record reaches disk leaves memory irrelevant; recovery uses the last valid WAL boundary.
- A crash after a valid WAL append but before memory update replays the mutation on restart.
- `always` asks the OS to synchronize each record; `none` intentionally does not.
- WAL growth is unbounded in v0.1.
- There is no snapshot, compaction, group commit, encryption, or authenticated checksum.
- CRC32 detects accidental corruption; it is not a cryptographic integrity mechanism.

