# Use Rust for the ForgeKV Core

## Status

Accepted

## Context

ForgeKV needs predictable memory ownership, strong type-driven protocol validation, efficient concurrency, and native performance while remaining deployable as small standalone binaries.

## Decision

Implement ForgeKV in stable Rust, use Tokio for asynchronous networking and file operations, and forbid unsafe code in v0.1. Keep the project as one crate with two binaries and a reusable library.

## Consequences

Ownership and error handling are explicit, many input and concurrency mistakes are rejected by the compiler, and the runtime footprint stays focused. Contributors must understand Rust's ownership model and async boundaries. Some low-level optimizations that require unsafe code are deferred.

