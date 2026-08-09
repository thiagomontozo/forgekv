use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use forgekv::{
    config::FsyncMode,
    error::PersistenceError,
    metrics::Metrics,
    persistence::{
        load_snapshot, prepare_wal, recover_wal, write_snapshot_atomic, Database, RecordType, Wal,
        WalRecord,
    },
    protocol::ProtocolLimits,
    store::{ShardedStore, SnapshotEntry, TtlState},
};
use tempfile::tempdir;

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_frame_size: 1024 * 1024,
        max_key_size: 4096,
        max_value_size: 1024 * 1024,
    }
}

fn store() -> Arc<ShardedStore> {
    Arc::new(ShardedStore::new(8, Arc::new(Metrics::default())).expect("valid shards"))
}

#[test]
fn record_encoding_validates_checksum() {
    let record = WalRecord::set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
        .expect("record should construct");
    let encoded = record.encode().expect("record should encode");
    assert_eq!(
        WalRecord::decode(&encoded, limits(), 8).expect("record should decode"),
        record
    );
    let mut corrupt = encoded.to_vec();
    corrupt[32] ^= 0x01;
    assert!(matches!(
        WalRecord::decode(&corrupt, limits(), 8),
        Err(PersistenceError::ChecksumMismatch { .. })
    ));
}

#[tokio::test]
async fn replay_applies_set_delete_setex_and_persist() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.wal");
    prepare_wal(&path, FsyncMode::None).expect("WAL should initialize");
    let metrics = Arc::new(Metrics::default());
    let mut wal = Wal::open(&path, FsyncMode::None, metrics)
        .await
        .expect("WAL should open");
    wal.append(
        &WalRecord::set(Bytes::from_static(b"deleted"), Bytes::from_static(b"old"))
            .expect("record should construct"),
    )
    .await
    .expect("append should work");
    wal.append(&WalRecord::delete(Bytes::from_static(b"deleted")).expect("record"))
        .await
        .expect("append should work");
    let expiration = SystemTime::now()
        .checked_add(Duration::from_secs(300))
        .expect("expiration should fit");
    wal.append(
        &WalRecord::set_ex(
            Bytes::from_static(b"session"),
            Bytes::from_static(b"value"),
            expiration,
        )
        .expect("record"),
    )
    .await
    .expect("append should work");
    wal.append(&WalRecord::persist(Bytes::from_static(b"session")).expect("record"))
        .await
        .expect("append should work");
    wal.flush().await.expect("flush should work");
    drop(wal);

    let recovered = store();
    let report = recover_wal(&path, &recovered, limits()).expect("replay should work");
    assert_eq!(report.records_replayed, 4);
    assert_eq!(recovered.get(b"deleted").expect("get should work"), None);
    assert_eq!(
        recovered.get(b"session").expect("get should work"),
        Some(Bytes::from_static(b"value"))
    );
    assert_eq!(
        recovered.ttl(b"session").expect("ttl should work"),
        TtlState::Persistent
    );
}

#[tokio::test]
async fn restart_rebuilds_the_memory_store() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.wal");
    prepare_wal(&path, FsyncMode::None).expect("WAL should initialize");
    let mut wal = Wal::open(&path, FsyncMode::None, Arc::new(Metrics::default()))
        .await
        .expect("WAL should open");
    wal.append(
        &WalRecord::set(Bytes::from_static(b"key"), Bytes::from_static(b"value")).expect("record"),
    )
    .await
    .expect("append should work");
    wal.flush().await.expect("flush should work");
    drop(wal);

    let first = store();
    recover_wal(&path, &first, limits()).expect("first recovery should work");
    drop(first);
    let second = store();
    recover_wal(&path, &second, limits()).expect("restart recovery should work");
    assert_eq!(
        second.get(b"key").expect("get should work"),
        Some(Bytes::from_static(b"value"))
    );
}

#[test]
fn truncated_final_record_is_removed_safely() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.wal");
    prepare_wal(&path, FsyncMode::None).expect("WAL should initialize");
    let record = WalRecord::set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
        .expect("record")
        .encode()
        .expect("encode");
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("WAL should open");
    file.write_all(&record[..record.len() - 3])
        .expect("partial record should write");
    drop(file);

    let recovered = store();
    let report = recover_wal(&path, &recovered, limits()).expect("tail should be repaired");
    assert!(report.truncated_tail_removed);
    assert_eq!(report.records_replayed, 0);
}

#[test]
fn corruption_in_complete_record_is_not_ignored() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.wal");
    prepare_wal(&path, FsyncMode::None).expect("WAL should initialize");
    let record = WalRecord {
        record_type: RecordType::Set,
        timestamp_ms: 1,
        expires_at_ms: None,
        key: Bytes::from_static(b"key"),
        value: Bytes::from_static(b"value"),
    }
    .encode()
    .expect("record should encode");
    let mut file = OpenOptions::new()
        .append(true)
        .open(&path)
        .expect("WAL should open");
    file.write_all(&record).expect("record should write");
    drop(file);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .expect("WAL should reopen");
    file.seek(SeekFrom::Start(8 + 32))
        .expect("seek should work");
    file.write_all(&[0xff]).expect("corruption should write");
    drop(file);

    assert!(matches!(
        recover_wal(&path, &store(), limits()),
        Err(PersistenceError::ChecksumMismatch { .. })
    ));
}

#[test]
fn snapshot_round_trip_and_checksum_validation() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.snapshot");
    let entries = vec![SnapshotEntry {
        key: Bytes::from_static(b"snapshot:key"),
        value: Bytes::from_static(b"binary\0value"),
        expires_at: None,
    }];
    write_snapshot_atomic(&path, &entries).expect("snapshot should write");
    let recovered = store();
    let report = load_snapshot(&path, &recovered, limits()).expect("snapshot should load");
    assert_eq!(report.entries_loaded, 1);
    assert_eq!(
        recovered.get(b"snapshot:key").expect("get should work"),
        Some(Bytes::from_static(b"binary\0value"))
    );

    let mut bytes = std::fs::read(&path).expect("snapshot should read");
    let last = bytes.last_mut().expect("snapshot should not be empty");
    *last ^= 0x01;
    std::fs::write(&path, bytes).expect("corruption should write");
    assert!(load_snapshot(&path, &store(), limits()).is_err());
}

#[test]
fn truncated_snapshot_is_rejected() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.snapshot");
    let entries = vec![SnapshotEntry {
        key: Bytes::from_static(b"key"),
        value: Bytes::from_static(b"value"),
        expires_at: None,
    }];
    write_snapshot_atomic(&path, &entries).expect("snapshot should write");
    let mut bytes = std::fs::read(&path).expect("snapshot should read");
    bytes.truncate(bytes.len() - 1);
    std::fs::write(&path, bytes).expect("truncated snapshot should write");
    assert!(load_snapshot(&path, &store(), limits()).is_err());
}

#[tokio::test]
async fn everysec_flushes_dirty_wal_on_tick() {
    let directory = tempdir().expect("temp directory should be created");
    let path = directory.path().join("forgekv.wal");
    prepare_wal(&path, FsyncMode::EverySecond).expect("WAL should initialize");
    let mut wal = Wal::open(&path, FsyncMode::EverySecond, Arc::new(Metrics::default()))
        .await
        .expect("WAL should open");
    wal.append(
        &WalRecord::set(Bytes::from_static(b"key"), Bytes::from_static(b"value"))
            .expect("record should construct"),
    )
    .await
    .expect("append should work");
    assert!(wal.sync_if_needed().await.expect("sync should work"));
    assert!(!wal
        .sync_if_needed()
        .await
        .expect("clean WAL should not sync"));
}

#[tokio::test]
async fn compaction_restarts_from_snapshot_and_new_wal() {
    let directory = tempdir().expect("temp directory should be created");
    let config = forgekv::config::Config {
        data_dir: directory.path().to_path_buf(),
        shards: 8,
        fsync: FsyncMode::None,
        wal_compaction_threshold_bytes: 1,
        metrics_enabled: false,
        ..forgekv::config::Config::default()
    };
    let metrics = Arc::new(Metrics::default());
    let active_store =
        Arc::new(ShardedStore::new(config.shards, Arc::clone(&metrics)).expect("valid shards"));
    let (database, _) = Database::open(&config, active_store, metrics)
        .await
        .expect("database should open");
    database
        .set(
            Bytes::from_static(b"compacted"),
            Bytes::from_static(b"value"),
        )
        .await
        .expect("set should work");
    assert_eq!(database.compact().await.expect("compaction should work"), 1);
    drop(database);

    let restart_metrics = Arc::new(Metrics::default());
    let restart_store = Arc::new(
        ShardedStore::new(config.shards, Arc::clone(&restart_metrics)).expect("valid shards"),
    );
    let (_database, report) = Database::open(&config, Arc::clone(&restart_store), restart_metrics)
        .await
        .expect("restart should recover");
    assert_eq!(report.snapshot_entries_loaded, 1);
    assert_eq!(
        restart_store.get(b"compacted").expect("get should work"),
        Some(Bytes::from_static(b"value"))
    );
}
