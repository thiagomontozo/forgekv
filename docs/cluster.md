# ForgeKV Experimental Cluster Partitioning

## Scope

ForgeKV v0.4 can partition keys across a static set of independent server processes. This mode is a systems experiment, not a highly available distributed database. It implements deterministic ownership and client redirects, but no consensus, discovery, health protocol, replica placement, automatic failover, or data migration.

Cluster mode requires `FORGEKV_ROLE=standalone`. It cannot be combined with the v0.3 leader/follower replication mode.

## Configuration

Every node must receive the same membership and virtual-node count:

```text
FORGEKV_CLUSTER_ENABLED=true
FORGEKV_CLUSTER_NODE_ID=node-a
FORGEKV_CLUSTER_NODES=node-a@127.0.0.1:6380,node-b@127.0.0.1:6382
FORGEKV_CLUSTER_VIRTUAL_NODES=128
```

`FORGEKV_CLUSTER_NODE_ID` identifies the local process and must appear exactly once in the membership. IDs contain 1-64 ASCII letters, digits, `.`, `-`, or `_`. Membership entries use `node-id@host:port`; IDs and advertised addresses must both be unique. Addresses are limited to 255 bytes, ports must be in `1..=65535`, and one topology supports at most 256 nodes.

Addresses are advertised to clients, not used as bind addresses. Each address therefore must be reachable from every client that may receive it. Every node must use an identical logical membership. ForgeKV does not compare topology with peers.

## Ring construction

Nodes are sorted by ID so textual membership order has no effect. For each node and virtual-node index in `0..FORGEKV_CLUSTER_VIRTUAL_NODES`, ForgeKV hashes the UTF-8 bytes of:

```text
node-id#index
```

The hash is 64-bit FNV-1a with offset basis `0xcbf29ce484222325` and prime `0x00000100000001b3`. Ring points are sorted by `(hash, sorted-node-index)`. A binary key is hashed with the same function. Its owner is the node on the first ring point whose hash is greater than or equal to the key hash; lookup wraps to the first point when necessary.

This algorithm is deterministic but not cryptographic. Virtual nodes improve distribution for small physical memberships at the cost of startup memory and lookup-table size.

## Request routing

`SET`, `GET`, `DEL`, `EXISTS`, `SETEX`, `TTL`, and `PERSIST` are key-bearing commands. After the server validates a complete frame and parses the key, it calculates ownership before accessing the WAL or local store.

- Local owner: execute normally against the local WAL and sharded store.
- Remote owner: return protocol status `REDIRECT` (`0x09`) containing the owner's configured `host:port` as a length-prefixed UTF-8 string.
- Node-local commands: `PING`, `INFO`, and `STATS` always execute on the contacted node.

The server does not proxy a request. The bundled client reconnects and resends the identical command, follows at most five redirects by default, and rejects address loops. Pipeline responses remain ordered; redirected elements are resolved individually after the seed responses arrive.

## Persistence and topology changes

Each node has an independent data directory, WAL, snapshot, and in-memory store. Only keys owned under the active topology are routed to that node. Membership is not persisted in the data files and changing it does not copy, delete, or rebalance records.

Consequently, changing membership or the virtual-node count can move ownership while the bytes remain on the old process. Reads may then return `NOT_FOUND`, and writes may create a new copy at the new owner. Safe topology changes require an external, controlled migration procedure that v0.4 does not provide. Back up each data directory before any experiment.

## Failure behavior

- An unreachable owner produces a client connection error.
- Different topologies can create redirect loops; the bundled client detects repeated addresses.
- A stopped node makes its owned partition unavailable.
- Restarting a node with its unchanged data directory recovers that partition through its normal snapshot and WAL process.
- There is no automatic promotion, fencing, quorum, read repair, hinted handoff, or replica recovery.

## Observability

`INFO` includes `cluster_enabled`; cluster nodes also include `cluster_node_id`, `cluster_nodes`, and `cluster_virtual_nodes`. `STATS` and the Prometheus endpoint expose:

- `cluster_redirects_total`: valid key commands redirected by this node;
- `cluster_local_commands_total`: valid key commands owned and executed by this node.

Connection and command counters include both local requests and redirect attempts. Stored keys and values are not logged.

## Container example

`compose.cluster.yml` defines three independent partitions. Its advertised loopback addresses are designed for a CLI running on the same host as Docker. A client running in another container or on another machine needs membership addresses reachable from that network; update all three node configurations together.
