# Use a Custom Versioned Binary Protocol

## Status

Accepted

## Context

The project is intended to demonstrate framing, bounds checking, binary-safe data handling, and client/server protocol design. HTTP/JSON would hide those concerns, while Redis compatibility would import a much larger behavioral contract.

## Decision

Use a length-prefixed, versioned TCP protocol with typed opcodes, status codes, big-endian integers, and explicit lengths for every variable field.

## Consequences

Frames are compact and binary values require no text encoding. External clients can be implemented from the protocol specification. ForgeKV must maintain its own client tooling, compatibility rules, parser hardening, and protocol documentation.
