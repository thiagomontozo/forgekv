# ForgeKV Persistence

## Purpose and path

ForgeKV v0.3 persists mutations in `${FORGEKV_DATA_DIR}/forgekv.wal` and compacted state in `${FORGEKV_DATA_DIR}/forgekv.snapshot`. Recovery loads the snapshot first and replays the remaining WAL afterward. Replication metadata uses `forgekv.node`, `forgekv.generation`, and, on followers, `forgekv.replica`.

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

Each acknowledged mutation has completed `sync_data` before memory changes and before its response. This is the strongest policy, but it trades throughput and latency for durability. Filesystem, drive cache, hardware, and operating system behavior still affect end-to-end guarantees.

### `none`

Each record is written and flushed through the process buffer, but ForgeKV does not request a per-record disk synchronization. The operating system may delay physical persistence. A process or machine crash can lose recent acknowledged writes.

### `everysec`

Acknowledged mutations are flushed without a per-command `sync_data`. A cancellation-aware maintenance task synchronizes dirty WAL data once per second and graceful shutdown performs a final synchronization. A machine crash can therefore lose roughly the most recent second of acknowledged mutations.

## Snapshot format

The 16-byte snapshot header contains ASCII `FKVS`, version `0x01`, three zero reserved bytes, and a big-endian `u64` entry count. Each entry contains `u64 expires_at_ms` (`u64::MAX` for persistent), `u32 key_length`, `u32 value_length`, key bytes, value bytes, and CRC32. The checksum covers the fixed entry fields plus key and value. Snapshot truncation, invalid lengths, trailing bytes, or checksum failure stops recovery.

## Compaction

When the WAL reaches `FORGEKV_WAL_COMPACTION_THRESHOLD_BYTES`, ForgeKV holds the mutation ordering guard, captures live entries, writes and synchronizes `forgekv.snapshot.tmp`, installs it with a recoverable backup rename, and resets the WAL to its header. A crash before WAL reset replays the older WAL over an equivalent snapshot; a crash after reset starts from the installed snapshot. A threshold of `0` disables automatic compaction.

Before resetting the WAL, v0.3 atomically advances a persistent positive generation. Followers present this generation with their next required byte offset. Generation mismatch causes a full snapshot instead of reading a potentially unrelated byte position.

## Recovery

Before accepting clients ForgeKV:

1. validates and loads the snapshot when present;
2. validates the WAL file header;
3. reads each record without unbounded allocation;
4. validates lengths, fields, and CRC32;
5. applies mutations in file order;
6. purges expired keys and reports snapshot and replay counts.

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
- `always` asks the OS to synchronize each record; `everysec` synchronizes dirty data periodically; `none` intentionally does not request disk synchronization.
- Compaction is size-triggered and snapshots contain the complete live data set.
- There is no incremental snapshot, group commit, encryption, or authenticated checksum.
- Replicated WAL batches are forced to disk before follower progress is advanced, independent of the follower's normal fsync policy.
- CRC32 detects accidental corruption; it is not a cryptographic integrity mechanism.
