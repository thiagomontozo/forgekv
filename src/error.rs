use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{name} must be a valid {expected}, got {value:?}")]
    InvalidValue {
        name: &'static str,
        value: String,
        expected: &'static str,
    },
    #[error("{0}")]
    InvalidCombination(String),
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("frame length {actual} is below the minimum {minimum}")]
    FrameTooSmall { actual: usize, minimum: usize },
    #[error("frame length {actual} exceeds configured maximum {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(u8),
    #[error("invalid opcode {0:#04x}")]
    InvalidOpcode(u8),
    #[error("truncated frame")]
    Truncated,
    #[error("invalid payload: {0}")]
    InvalidPayload(&'static str),
    #[error("key length {actual} exceeds configured maximum {maximum}")]
    KeyTooLarge { actual: usize, maximum: usize },
    #[error("value length {actual} exceeds configured maximum {maximum}")]
    ValueTooLarge { actual: usize, maximum: usize },
    #[error("integer overflow while processing untrusted input")]
    IntegerOverflow,
}

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("shard count must be greater than zero")]
    InvalidShardCount,
    #[error("store shard lock is poisoned")]
    LockPoisoned,
    #[error("store size exceeded the supported range")]
    CapacityOverflow,
    #[error("TTL must be greater than zero")]
    InvalidTtl,
    #[error("expiration timestamp is outside the supported range")]
    InvalidExpiration,
}

#[derive(Debug, Error)]
pub enum PersistenceError {
    #[error("WAL I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("invalid WAL header")]
    InvalidHeader,
    #[error("unsupported WAL version {0}")]
    UnsupportedVersion(u8),
    #[error("invalid WAL record type {0:#04x}")]
    InvalidRecordType(u8),
    #[error("WAL record length is invalid or exceeds configured limits")]
    InvalidRecordLength,
    #[error("WAL checksum mismatch at byte offset {offset}")]
    ChecksumMismatch { offset: u64 },
    #[error("WAL corruption at byte offset {offset}: {reason}")]
    Corruption { offset: u64, reason: &'static str },
    #[error("system clock is before the Unix epoch")]
    ClockBeforeEpoch,
    #[error("store recovery failed: {0}")]
    Store(#[from] StoreError),
    #[error("invalid snapshot header")]
    InvalidSnapshotHeader,
    #[error("snapshot corruption at entry {entry}: {reason}")]
    SnapshotCorruption { entry: u64, reason: &'static str },
    #[error("snapshot worker failed: {0}")]
    SnapshotTask(String),
    #[error("persistent metadata is invalid: {0}")]
    InvalidMetadata(&'static str),
}

#[derive(Debug, Error)]
pub enum ReplicationError {
    #[error("replication I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("replication persistence error: {0}")]
    Persistence(#[from] PersistenceError),
    #[error("replication store error: {0}")]
    Store(#[from] StoreError),
    #[error("invalid replication protocol: {0}")]
    InvalidProtocol(&'static str),
    #[error("replication payload size {actual} exceeds configured maximum {maximum}")]
    PayloadTooLarge { actual: u64, maximum: usize },
    #[error("replication task failed: {0}")]
    Task(String),
}

#[derive(Debug, Error)]
pub enum ForgeError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Persistence(#[from] PersistenceError),
    #[error(transparent)]
    Replication(#[from] ReplicationError),
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
}
