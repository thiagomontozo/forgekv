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
    replication_connections_total: AtomicU64,
    replication_syncs_total: AtomicU64,
    replication_full_syncs_total: AtomicU64,
    replication_bytes_sent_total: AtomicU64,
    replication_bytes_received_total: AtomicU64,
    replication_errors_total: AtomicU64,
    replication_lag_bytes: AtomicU64,
    cluster_redirects_total: AtomicU64,
    cluster_local_commands_total: AtomicU64,
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
    pub replication_connections_total: u64,
    pub replication_syncs_total: u64,
    pub replication_full_syncs_total: u64,
    pub replication_bytes_sent_total: u64,
    pub replication_bytes_received_total: u64,
    pub replication_errors_total: u64,
    pub replication_lag_bytes: u64,
    pub cluster_redirects_total: u64,
    pub cluster_local_commands_total: u64,
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

    pub fn replication_snapshot_created(&self, entries: u64) {
        self.snapshots_created_total.fetch_add(1, Ordering::Relaxed);
        self.snapshot_entries_written
            .fetch_add(entries, Ordering::Relaxed);
    }

    pub fn wal_write(&self, bytes: u64) {
        self.wal_records_written.fetch_add(1, Ordering::Relaxed);
        self.wal_bytes_written.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn replication_connection(&self) {
        self.replication_connections_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn replication_sent(&self, bytes: u64, full_sync: bool) {
        self.replication_syncs_total.fetch_add(1, Ordering::Relaxed);
        if full_sync {
            self.replication_full_syncs_total
                .fetch_add(1, Ordering::Relaxed);
        }
        self.replication_bytes_sent_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn replication_received(&self, bytes: u64) {
        self.replication_bytes_received_total
            .fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn replication_error(&self) {
        self.replication_errors_total
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_replication_lag(&self, bytes: u64) {
        self.replication_lag_bytes.store(bytes, Ordering::Relaxed);
    }

    pub fn cluster_redirect(&self) {
        self.cluster_redirects_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn cluster_local_command(&self) {
        self.cluster_local_commands_total
            .fetch_add(1, Ordering::Relaxed);
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
            replication_connections_total: self
                .replication_connections_total
                .load(Ordering::Relaxed),
            replication_syncs_total: self.replication_syncs_total.load(Ordering::Relaxed),
            replication_full_syncs_total: self.replication_full_syncs_total.load(Ordering::Relaxed),
            replication_bytes_sent_total: self.replication_bytes_sent_total.load(Ordering::Relaxed),
            replication_bytes_received_total: self
                .replication_bytes_received_total
                .load(Ordering::Relaxed),
            replication_errors_total: self.replication_errors_total.load(Ordering::Relaxed),
            replication_lag_bytes: self.replication_lag_bytes.load(Ordering::Relaxed),
            cluster_redirects_total: self.cluster_redirects_total.load(Ordering::Relaxed),
            cluster_local_commands_total: self
                .cluster_local_commands_total
                .load(Ordering::Relaxed),
        }
    }
}
