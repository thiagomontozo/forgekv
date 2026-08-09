use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Default)]
pub struct Metrics {
    connections_total: AtomicU64,
    connections_active: AtomicU64,
    commands_total: AtomicU64,
    gets_total: AtomicU64,
    sets_total: AtomicU64,
    deletes_total: AtomicU64,
    hits_total: AtomicU64,
    misses_total: AtomicU64,
    expired_keys_total: AtomicU64,
    protocol_errors_total: AtomicU64,
    wal_records_written: AtomicU64,
    wal_bytes_written: AtomicU64,
    connections_rejected_total: AtomicU64,
    snapshots_created_total: AtomicU64,
    wal_compactions_total: AtomicU64,
    snapshot_entries_written: AtomicU64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MetricsSnapshot {
    pub connections_total: u64,
    pub connections_active: u64,
    pub commands_total: u64,
    pub gets_total: u64,
    pub sets_total: u64,
    pub deletes_total: u64,
    pub hits_total: u64,
    pub misses_total: u64,
    pub expired_keys_total: u64,
    pub protocol_errors_total: u64,
    pub wal_records_written: u64,
    pub wal_bytes_written: u64,
    pub connections_rejected_total: u64,
    pub snapshots_created_total: u64,
    pub wal_compactions_total: u64,
    pub snapshot_entries_written: u64,
}

impl Metrics {
    pub fn connection_opened(&self) {
        self.connections_total.fetch_add(1, Ordering::Relaxed);
        self.connections_active.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_closed(&self) {
        self.connections_active.fetch_sub(1, Ordering::Relaxed);
    }

    pub fn command(&self) {
        self.commands_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get(&self) {
        self.gets_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set(&self) {
        self.sets_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn delete(&self) {
        self.deletes_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn hit(&self) {
        self.hits_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn miss(&self) {
        self.misses_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn expired(&self, count: u64) {
        self.expired_keys_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn protocol_error(&self) {
        self.protocol_errors_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn connection_rejected(&self) {
        self.connections_rejected_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn compaction_completed(&self, entries: u64) {
        self.snapshots_created_total.fetch_add(1, Ordering::Relaxed);
        self.wal_compactions_total.fetch_add(1, Ordering::Relaxed);
        self.snapshot_entries_written
            .fetch_add(entries, Ordering::Relaxed);
    }

    pub fn wal_write(&self, bytes: u64) {
        self.wal_records_written.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            connections_total: self.connections_total.load(Ordering::Relaxed),
            connections_active: self.connections_active.load(Ordering::Relaxed),
            commands_total: self.commands_total.load(Ordering::Relaxed),
            gets_total: self.gets_total.load(Ordering::Relaxed),
            sets_total: self.sets_total.load(Ordering::Relaxed),
            deletes_total: self.deletes_total.load(Ordering::Relaxed),
            hits_total: self.hits_total.load(Ordering::Relaxed),
            misses_total: self.misses_total.load(Ordering::Relaxed),
            expired_keys_total: self.expired_keys_total.load(Ordering::Relaxed),
            protocol_errors_total: self.protocol_errors_total.load(Ordering::Relaxed),
            wal_records_written: self.wal_records_written.load(Ordering::Relaxed),
            wal_bytes_written: self.wal_bytes_written.load(Ordering::Relaxed),
            connections_rejected_total: self.connections_rejected_total.load(Ordering::Relaxed),
            snapshots_created_total: self.snapshots_created_total.load(Ordering::Relaxed),
            wal_compactions_total: self.wal_compactions_total.load(Ordering::Relaxed),
            snapshot_entries_written: self.snapshot_entries_written.load(Ordering::Relaxed),
        }
    }
}
