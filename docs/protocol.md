# ForgeKV Wire Protocol v1

## Conventions

- Transport: TCP stream
- Default port: `6380`
- Byte order: big-endian for every integer
- Key and value encoding: arbitrary bytes
- Human-readable metadata and error messages: UTF-8
- Request/response pairing: one response for each complete request, in request order
- Maximum frame body: configured by `FORGEKV_MAX_FRAME_SIZE` (default `1,048,576` bytes)

All lengths are unsigned byte counts. Implementations must use checked arithmetic and validate every length before allocation or slicing.

## Frame

Every request and response uses this envelope:

| Offset | Size | Field | Description |
|---:|---:|---|---|
| 0 | 4 | `frame_length` | Bytes after this field: version + code + payload |
| 4 | 1 | `version` | Protocol version; v1 is `0x01` |
| 5 | 1 | `code` | Request opcode or response status |
| 6 | N | `payload` | Code-specific bytes |

`frame_length` must be at least `2`. It must be accepted only when it is no greater than the configured maximum. The total wire size is `4 + frame_length`.

A receiver should treat EOF before the first length byte as a clean connection close. EOF after any part of a frame is truncation. Version mismatch, invalid code, malformed lengths, or trailing bytes are protocol errors.

## Primitive encodings

### Key

```text
u32 key_length
u8  key[key_length]
```

`key_length` must be in `1..=FORGEKV_MAX_KEY_SIZE`.

### Value

```text
u32 value_length
u8  value[value_length]
```

Empty values are valid. `value_length` must not exceed `FORGEKV_MAX_VALUE_SIZE`.

## Request opcodes

| Opcode | Name | Payload |
|---:|---|---|
| `0x01` | `PING` | empty |
| `0x02` | `SET` | key, value |
| `0x03` | `GET` | key |
| `0x04` | `DEL` | key |
| `0x05` | `EXISTS` | key |
| `0x06` | `SETEX` | key, `u64 ttl_ms`, value |
| `0x07` | `TTL` | key |
| `0x08` | `PERSIST` | key |
| `0x09` | `INFO` | empty |
| `0x0a` | `STATS` | empty |

`SETEX ttl_ms` must be greater than zero. No request accepts trailing bytes.

## Response status codes

| Status | Name | Payload |
|---:|---|---|
| `0x00` | `OK` | empty |
| `0x01` | `NOT_FOUND` | empty |
| `0x02` | `INVALID_REQUEST` | UTF-8 string |
| `0x03` | `SERVER_ERROR` | UTF-8 string |
| `0x04` | `PONG` | empty |
| `0x05` | `VALUE` | binary value |
| `0x06` | `INTEGER` | `i64` |
| `0x07` | `INFO` | string field map |
| `0x08` | `STATS` | unsigned metric map |

Error and value strings use a `u32` length followed by that many bytes. `VALUE` remains binary; only error messages require UTF-8.

### INFO field map

```text
u16 field_count
repeat field_count times:
    u16 name_length
    u8  name[name_length]       # UTF-8
    u32 value_length
    u8  value[value_length]     # UTF-8
```

v0.1 fields are `version`, `uptime_seconds`, `keys`, `shards`, `listening_address`, and `fsync`. Clients must ignore unknown fields for forward compatibility.

### STATS metric map

```text
u16 field_count
repeat field_count times:
    u16 name_length
    u8  name[name_length]       # UTF-8
    u64 value
```

v0.1 fields are `connections_total`, `connections_active`, `commands_total`, `gets_total`, `sets_total`, `deletes_total`, `hits_total`, `misses_total`, `expired_keys_total`, `protocol_errors_total`, `wal_records_written`, and `wal_bytes_written`.

## Command semantics

- `PING` returns `PONG`.
- `SET` writes or replaces a persistent value and returns `OK`.
- `GET` returns `VALUE` or `NOT_FOUND`.
- `DEL` returns integer `1` when a live key was removed, otherwise `0`.
- `EXISTS` returns integer `1` for a live key, otherwise `0`.
- `SETEX` writes or replaces a value with the supplied TTL and returns `OK`.
- `TTL` returns remaining milliseconds, `-1` for a persistent key, or `-2` for a missing/expired key.
- `PERSIST` returns `1` when an expiration was removed, otherwise `0`.
- `INFO` returns the server field map.
- `STATS` returns the metric map.

## Example: PING

Request bytes:

```text
00 00 00 02  01 01
^^^^^^^^^^^  ^^ ^^
length=2     v1 PING
```

Response bytes:

```text
00 00 00 02  01 04
^^^^^^^^^^^  ^^ ^^
length=2     v1 PONG
```

## Robust client behavior

A client must cap response lengths before allocation, verify the expected payload shape for the returned status, reject unsupported versions, and treat a truncated response as a connection failure. Protocol v1 has no request identifiers; clients sharing a socket must serialize reads and writes or preserve strict request order.

