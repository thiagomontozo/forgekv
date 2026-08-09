# Add Static Membership and Consistent Hashing

## Status

Accepted

## Context

The v0.4 milestone explores horizontal key partitioning while retaining the custom binary protocol and avoiding the complexity of consensus or a large distributed-systems dependency. The ownership algorithm must be deterministic across processes and must not allow a non-owner to mutate its local WAL.

## Decision

Use explicitly configured `node-id@host:port` membership and a 64-bit FNV-1a consistent-hash ring with a configurable, equal number of virtual nodes per member. Sort membership by node ID before building ring points. Route every key-bearing command before persistence or store access. Return a typed binary redirect for remote ownership and let clients follow redirects with bounded depth and loop detection.

Keep partitioning mutually exclusive with leader/follower replication in v0.4. Do not add gossip, proxying, migration, replication between partitions, or failure detection.

## Consequences

Any node can serve as an initial contact and the wire protocol communicates ownership without an HTTP control plane. Adding or removing a node remaps only portions of the hash space conceptually, but ForgeKV does not move the existing bytes, so membership changes are operationally unsafe without an external migration. Static configuration can diverge, each partition is a single point of failure, and advertised addresses must be client-reachable. The implementation demonstrates partitioning mechanics without claiming availability or consensus guarantees.
