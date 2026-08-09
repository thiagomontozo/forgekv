use std::{
    fs::{self, File, OpenOptions as StdOpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use bytes::Bytes;
use tokio::{
    fs::{File as TokioFile, OpenOptions},
    io::AsyncWriteExt,
    sync::Mutex,
};

use crate::{
    config::{Config, FsyncMode},
    error::{PersistenceError, StoreError},
    metrics::Metrics,
    protocol::ProtocolLimits,
    store::{ShardedStore, SnapshotEntry},
};

use super::{
    decode_snapshot, load_snapshot,
    record::{
        validate_lengths, RecordType, WalRecord, RECORD_FIXED_AFTER_MAGIC, RECORD_FIXED_TOTAL,
        RECORD_MAGIC, WAL_HEADER,
    },
    write_snapshot_atomic,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub records_replayed: u64,
    pub recovered_keys: usize,
    pub truncated_tail_removed: bool,
    pub valid_bytes: u64,
    pub snapshot_entries_loaded: u64,
    pub snapshot_expired_entries_skipped: u64,
}

#[derive(Debug)]
pub struct Wal {
    file: TokioFile,
    fsync: FsyncMode,
    metrics: Arc<Metrics>,
    bytes_written: u64,
    dirty: bool,
}

impl Wal {
    pub async fn open(
        path: &Path,
        fsync: FsyncMode,
        metrics: Arc<Metrics>,
    ) -> Result<Self, PersistenceError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)
            .await?;
        let bytes_written = file.metadata().await?.len();
        Ok(Self {
            file,
            fsync,
            metrics,
            bytes_written,
            dirty: false,
        })
    }

    pub async fn append(&mut self, record: &WalRecord) -> Result<(), PersistenceError> {
        let encoded = record.encode()?;
        self.file.write_all(&encoded).await?;
        self.file.flush().await?;
        if self.fsync == FsyncMode::Always {
            self.file.sync_data().await?;
            self.dirty = false;
        } else {
            self.dirty = true;
        }
        self.bytes_written = self.bytes_written.saturating_add(encoded.len() as u64);
        self.metrics.wal_write(encoded.len() as u64);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), PersistenceError> {
        self.file.flush().await?;
        if self.fsync != FsyncMode::None {
            self.file.sync_data().await?;
        }
        self.dirty = false;
        Ok(())
    }

    pub async fn sync_if_needed(&mut self) -> Result<bool, PersistenceError> {
        if self.fsync == FsyncMode::EverySecond && self.dirty {
            self.file.sync_data().await?;
            self.dirty = false;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn force_sync(&mut self) -> Result<(), PersistenceError> {
        self.file.flush().await?;
        self.file.sync_data().await?;
        self.dirty = false;
        Ok(())
    }

    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }

    pub async fn reset(&mut self) -> Result<(), PersistenceError> {
        self.file.set_len(0).await?;
        self.file.write_all(&WAL_HEADER).await?;
        self.file.flush().await?;
        if self.fsync != FsyncMode::None {
            self.file.sync_data().await?;
            self.dirty = false;
        } else {
            self.dirty = true;
        }
        self.bytes_written = WAL_HEADER.len() as u64;
        Ok(())
    }
}

#[derive(Debug)]
pub struct Database {
    store: Arc<ShardedStore>,
    wal: Mutex<Wal>,
    metrics: Arc<Metrics>,
    snapshot_path: PathBuf,
    compaction_threshold_bytes: u64,
    wal_path: PathBuf,
    generation_path: PathBuf,
    wal_generation: AtomicU64,
    node_id: String,
    limits: ProtocolLimits,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationBatch {
    pub generation: u64,
    pub start_offset: u64,
    pub end_offset: u64,
    pub leader_end: u64,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplicationSnapshot {
    pub generation: u64,
    pub offset: u64,
    pub bytes: Vec<u8>,
}

impl Database {
    pub async fn open(
        config: &Config,
        store: Arc<ShardedStore>,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, RecoveryReport), PersistenceError> {
        let path = config.wal_path();
        let generation_path = config.wal_generation_path();
        let node_path = config.node_id_path();
        restore_metadata_backup_if_needed(&generation_path)?;
        restore_metadata_backup_if_needed(&node_path)?;
        let generation_exists = generation_path.exists();
        let node_exists = node_path.exists();
        if generation_exists != node_exists {
            let wal_has_records = path
                .metadata()
                .map(|metadata| metadata.len() > WAL_HEADER.len() as u64)
                .unwrap_or(false);
            if generation_exists || wal_has_records || config.snapshot_path().exists() {
                return Err(PersistenceError::InvalidMetadata(
                    "node identity and WAL generation must exist together",
                ));
            }
        }
        let node_id = load_or_create_node_id(&node_path)?;
        let generation = load_or_create_generation(&generation_path)?;
        let snapshot = load_snapshot(
            &config.snapshot_path(),
            store.as_ref(),
            ProtocolLimits::from(config),
        )?;
        prepare_wal(&path, config.fsync)?;
        let mut report = recover_wal(&path, store.as_ref(), ProtocolLimits::from(config))?;
        report.snapshot_entries_loaded = snapshot.entries_loaded;
        report.snapshot_expired_entries_skipped = snapshot.expired_entries_skipped;
        let wal = Wal::open(&path, config.fsync, Arc::clone(&metrics)).await?;
        Ok((
            Self {
                store,
                wal: Mutex::new(wal),
                metrics,
                snapshot_path: config.snapshot_path(),
                compaction_threshold_bytes: config.wal_compaction_threshold_bytes,
                wal_path: path,
                generation_path,
                wal_generation: AtomicU64::new(generation),
                node_id,
                limits: ProtocolLimits::from(config),
            },
            report,
        ))
    }

    pub fn store(&self) -> &Arc<ShardedStore> {
        &self.store
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn wal_generation(&self) -> u64 {
        self.wal_generation.load(Ordering::Acquire)
    }

    pub async fn set(&self, key: Bytes, value: Bytes) -> Result<(), PersistenceError> {
        let record = WalRecord::set(key.clone(), value.clone())?;
        let mut wal = self.wal.lock().await;
        wal.append(&record).await?;
        self.store.set(key, value)?;
        self.metrics.set();
        Ok(())
    }

    pub async fn set_ex(
        &self,
        key: Bytes,
        value: Bytes,
        ttl: Duration,
    ) -> Result<(), PersistenceError> {
        if ttl.is_zero() {
            return Err(PersistenceError::Store(StoreError::InvalidTtl));
        }
        let expires_at = SystemTime::now()
            .checked_add(ttl)
            .ok_or(StoreError::InvalidExpiration)?;
        let record = WalRecord::set_ex(key.clone(), value.clone(), expires_at)?;
        let mut wal = self.wal.lock().await;
        wal.append(&record).await?;
        self.store.set_with_expiry(key, value, Some(expires_at))?;
        self.metrics.set();
        Ok(())
    }

    pub async fn delete(&self, key: Bytes) -> Result<bool, PersistenceError> {
        let record = WalRecord::delete(key.clone())?;
        let mut wal = self.wal.lock().await;
        wal.append(&record).await?;
        let deleted = self.store.delete(&key)?;
        self.metrics.delete();
        Ok(deleted)
    }

    pub async fn persist(&self, key: Bytes) -> Result<bool, PersistenceError> {
        let record = WalRecord::persist(key.clone())?;
        let mut wal = self.wal.lock().await;
        wal.append(&record).await?;
        let persisted = self.store.persist(&key)?;
        Ok(persisted)
    }

    pub async fn flush(&self) -> Result<(), PersistenceError> {
        self.wal.lock().await.flush().await
    }

    pub async fn sync_if_needed(&self) -> Result<bool, PersistenceError> {
        self.wal.lock().await.sync_if_needed().await
    }

    pub async fn compact_if_needed(&self) -> Result<bool, PersistenceError> {
        let mut wal = self.wal.lock().await;
        if self.compaction_threshold_bytes == 0
            || wal.bytes_written() < self.compaction_threshold_bytes
        {
            return Ok(false);
        }
        let entries = self.store.snapshot_entries()?;
        let entry_count = write_snapshot_async(self.snapshot_path.clone(), entries).await?;
        self.advance_generation()?;
        wal.reset().await?;
        self.metrics.compaction_completed(entry_count as u64);
        Ok(true)
    }

    pub async fn compact(&self) -> Result<usize, PersistenceError> {
        let mut wal = self.wal.lock().await;
        let entries = self.store.snapshot_entries()?;
        let entry_count = write_snapshot_async(self.snapshot_path.clone(), entries).await?;
        self.advance_generation()?;
        wal.reset().await?;
        self.metrics.compaction_completed(entry_count as u64);
        Ok(entry_count)
    }

    pub async fn read_replication_batch(
        &self,
        expected_generation: u64,
        offset: u64,
        maximum_bytes: usize,
    ) -> Result<Option<ReplicationBatch>, PersistenceError> {
        let wal = self.wal.lock().await;
        let generation = self.wal_generation();
        let leader_end = wal.bytes_written();
        if generation != expected_generation
            || offset < WAL_HEADER.len() as u64
            || offset > leader_end
        {
            return Ok(None);
        }
        let path = self.wal_path.clone();
        let limits = self.limits;
        let bytes = tokio::task::spawn_blocking(move || {
            read_record_batch(&path, offset, maximum_bytes, limits)
        })
        .await
        .map_err(|error| PersistenceError::SnapshotTask(error.to_string()))??;
        let byte_count =
            u64::try_from(bytes.len()).map_err(|_| PersistenceError::InvalidRecordLength)?;
        let end_offset = offset
            .checked_add(byte_count)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        Ok(Some(ReplicationBatch {
            generation,
            start_offset: offset,
            end_offset,
            leader_end,
            bytes,
        }))
    }

    pub async fn replication_snapshot(
        &self,
        maximum_bytes: usize,
    ) -> Result<ReplicationSnapshot, PersistenceError> {
        let wal = self.wal.lock().await;
        let entries = self.store.snapshot_entries()?;
        let entry_count = write_snapshot_async(self.snapshot_path.clone(), entries).await?;
        let path = self.snapshot_path.clone();
        let bytes = tokio::task::spawn_blocking(move || read_file_limited(&path, maximum_bytes))
            .await
            .map_err(|error| PersistenceError::SnapshotTask(error.to_string()))??;
        let generation = self.wal_generation();
        let offset = wal.bytes_written();
        self.metrics.replication_snapshot_created(entry_count as u64);
        Ok(ReplicationSnapshot {
            generation,
            offset,
            bytes,
        })
    }

    pub async fn apply_replication_batch(
        &self,
        encoded: &[u8],
        start_offset: u64,
    ) -> Result<usize, PersistenceError> {
        let records = decode_record_batch(encoded, start_offset, self.limits)?;
        let record_count = records.len();
        let mut wal = self.wal.lock().await;
        for record in records {
            wal.append(&record).await?;
            apply_record(self.store.as_ref(), record)?;
        }
        wal.force_sync().await?;
        Ok(record_count)
    }

    pub async fn install_replication_snapshot(
        &self,
        encoded: &[u8],
    ) -> Result<usize, PersistenceError> {
        let (entries, report) = decode_snapshot(encoded, self.limits)?;
        let persisted_entries = entries.clone();
        let mut wal = self.wal.lock().await;
        write_snapshot_async(self.snapshot_path.clone(), persisted_entries).await?;
        self.advance_generation()?;
        wal.reset().await?;
        self.store.replace_all(entries)?;
        wal.force_sync().await?;
        usize::try_from(report.entries_loaded)
            .map_err(|_| PersistenceError::InvalidRecordLength)
    }

    fn advance_generation(&self) -> Result<u64, PersistenceError> {
        let current = self.wal_generation();
        let next = current
            .checked_add(1)
            .ok_or(PersistenceError::InvalidMetadata("WAL generation overflow"))?;
        write_metadata_atomic(&self.generation_path, next.to_string().as_bytes())?;
        self.wal_generation.store(next, Ordering::Release);
        Ok(next)
    }
}

async fn write_snapshot_async(
    path: PathBuf,
    entries: Vec<SnapshotEntry>,
) -> Result<usize, PersistenceError> {
    let entry_count = entries.len();
    tokio::task::spawn_blocking(move || write_snapshot_atomic(&path, &entries))
        .await
        .map_err(|error| PersistenceError::SnapshotTask(error.to_string()))??;
    Ok(entry_count)
}

fn read_record_batch(
    path: &Path,
    offset: u64,
    maximum_bytes: usize,
    limits: ProtocolLimits,
) -> Result<Vec<u8>, PersistenceError> {
    let mut file = File::open(path)?;
    let file_length = file.metadata()?.len();
    if offset > file_length {
        return Err(PersistenceError::InvalidMetadata(
            "replication offset exceeds WAL length",
        ));
    }
    file.seek(SeekFrom::Start(offset))?;
    let mut output = Vec::new();
    let mut record_offset = offset;
    while record_offset < file_length {
        let mut prefix = [0u8; 32];
        read_exact_strict_record(&mut file, &mut prefix, record_offset)?;
        if prefix[..4] != RECORD_MAGIC {
            return Err(PersistenceError::Corruption {
                offset: record_offset,
                reason: "invalid record magic",
            });
        }
        let key_length = read_fixed_u32(&prefix, 24)? as usize;
        let value_length = read_fixed_u32(&prefix, 28)? as usize;
        validate_lengths(key_length, value_length, limits)?;
        let record_length = RECORD_FIXED_TOTAL
            .checked_add(key_length)
            .and_then(|size| size.checked_add(value_length))
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let proposed_length = output
            .len()
            .checked_add(record_length)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        if !output.is_empty() && proposed_length > maximum_bytes {
            break;
        }
        if record_length > maximum_bytes {
            return Err(PersistenceError::InvalidMetadata(
                "replication batch cannot contain one WAL record",
            ));
        }
        let tail_length = record_length
            .checked_sub(prefix.len())
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let mut tail = vec![0u8; tail_length];
        read_exact_strict_record(&mut file, &mut tail, record_offset)?;
        let output_start = output.len();
        output.extend_from_slice(&prefix);
        output.extend_from_slice(&tail);
        WalRecord::decode(&output[output_start..], limits, record_offset)?;
        record_offset = record_offset
            .checked_add(
                u64::try_from(record_length)
                    .map_err(|_| PersistenceError::InvalidRecordLength)?,
            )
            .ok_or(PersistenceError::InvalidRecordLength)?;
    }
    Ok(output)
}

fn decode_record_batch(
    encoded: &[u8],
    start_offset: u64,
    limits: ProtocolLimits,
) -> Result<Vec<WalRecord>, PersistenceError> {
    let mut records = Vec::new();
    let mut position = 0usize;
    while position < encoded.len() {
        let prefix_end = position
            .checked_add(32)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let prefix = encoded
            .get(position..prefix_end)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let key_length = read_fixed_u32(prefix, 24)? as usize;
        let value_length = read_fixed_u32(prefix, 28)? as usize;
        validate_lengths(key_length, value_length, limits)?;
        let record_length = RECORD_FIXED_TOTAL
            .checked_add(key_length)
            .and_then(|size| size.checked_add(value_length))
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let end = position
            .checked_add(record_length)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let offset = start_offset
            .checked_add(
                u64::try_from(position).map_err(|_| PersistenceError::InvalidRecordLength)?,
            )
            .ok_or(PersistenceError::InvalidRecordLength)?;
        records.push(WalRecord::decode(
            encoded
                .get(position..end)
                .ok_or(PersistenceError::InvalidRecordLength)?,
            limits,
            offset,
        )?);
        position = end;
    }
    Ok(records)
}

fn read_file_limited(path: &Path, maximum_bytes: usize) -> Result<Vec<u8>, PersistenceError> {
    let mut file = File::open(path)?;
    let length = file.metadata()?.len();
    if length
        > u64::try_from(maximum_bytes).map_err(|_| PersistenceError::InvalidRecordLength)?
    {
        return Err(PersistenceError::InvalidMetadata(
            "snapshot exceeds configured replication limit",
        ));
    }
    let capacity = usize::try_from(length).map_err(|_| PersistenceError::InvalidRecordLength)?;
    let mut bytes = Vec::with_capacity(capacity);
    file.read_to_end(&mut bytes)?;
    Ok(bytes)
}

fn read_exact_strict_record(
    file: &mut File,
    buffer: &mut [u8],
    offset: u64,
) -> Result<(), PersistenceError> {
    file.read_exact(buffer)
        .map_err(|error| match error.kind() {
            io::ErrorKind::UnexpectedEof => PersistenceError::Corruption {
                offset,
                reason: "truncated record in active WAL",
            },
            _ => PersistenceError::Io(error),
        })
}

fn load_or_create_generation(path: &Path) -> Result<u64, PersistenceError> {
    restore_metadata_backup_if_needed(path)?;
    if path.exists() {
        let raw = fs::read_to_string(path)?;
        return raw
            .trim()
            .parse::<u64>()
            .ok()
            .filter(|value| *value > 0)
            .ok_or(PersistenceError::InvalidMetadata("invalid WAL generation"));
    }
    write_metadata_atomic(path, b"1")?;
    Ok(1)
}

fn load_or_create_node_id(path: &Path) -> Result<String, PersistenceError> {
    restore_metadata_backup_if_needed(path)?;
    if path.exists() {
        let node_id = fs::read_to_string(path)?;
        let node_id = node_id.trim();
        if (16..=64).contains(&node_id.len())
            && node_id.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            return Ok(node_id.to_owned());
        }
        return Err(PersistenceError::InvalidMetadata("invalid node identity"));
    }
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PersistenceError::ClockBeforeEpoch)?
        .as_nanos();
    let process = u128::from(std::process::id());
    let node_id = format!("{:032x}", timestamp ^ process.rotate_left(17));
    write_metadata_atomic(path, node_id.as_bytes())?;
    Ok(node_id)
}

fn write_metadata_atomic(path: &Path, bytes: &[u8]) -> Result<(), PersistenceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = metadata_sidecar_path(path, "tmp");
    let backup = metadata_sidecar_path(path, "bak");
    let mut file = StdOpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.flush()?;
    file.sync_data()?;
    drop(file);
    if backup.exists() {
        fs::remove_file(&backup)?;
    }
    if path.exists() {
        fs::rename(path, &backup)?;
    }
    if let Err(error) = fs::rename(&temporary, path) {
        if backup.exists() && !path.exists() {
            let _ = fs::rename(&backup, path);
        }
        return Err(PersistenceError::Io(error));
    }
    if backup.exists() {
        fs::remove_file(backup)?;
    }
    Ok(())
}

fn restore_metadata_backup_if_needed(path: &Path) -> Result<(), PersistenceError> {
    let backup = metadata_sidecar_path(path, "bak");
    if !path.exists() && backup.exists() {
        fs::rename(backup, path)?;
    }
    Ok(())
}

fn metadata_sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{suffix}"));
    PathBuf::from(name)
}

pub fn prepare_wal(path: &Path, fsync: FsyncMode) -> Result<(), PersistenceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = StdOpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)?;
    let length = file.metadata()?.len();
    if length == 0 {
        file.write_all(&WAL_HEADER)?;
        file.flush()?;
        if fsync != FsyncMode::None {
            file.sync_data()?;
        }
    } else {
        let mut header = [0u8; WAL_HEADER.len()];
        read_exact_strict(&mut file, &mut header)?;
        if header != WAL_HEADER {
            return Err(PersistenceError::InvalidHeader);
        }
    }
    Ok(())
}

pub fn recover_wal(
    path: &Path,
    store: &ShardedStore,
    limits: ProtocolLimits,
) -> Result<RecoveryReport, PersistenceError> {
    let mut file = StdOpenOptions::new().read(true).write(true).open(path)?;
    let mut header = [0u8; WAL_HEADER.len()];
    read_exact_strict(&mut file, &mut header)?;
    if header != WAL_HEADER {
        return Err(PersistenceError::InvalidHeader);
    }

    let mut report = RecoveryReport {
        valid_bytes: WAL_HEADER.len() as u64,
        ..RecoveryReport::default()
    };
    loop {
        let offset = file.stream_position()?;
        let mut magic = [0u8; 4];
        let magic_bytes = read_partial(&mut file, &mut magic)?;
        if magic_bytes == 0 {
            break;
        }
        if magic_bytes < magic.len() {
            truncate_tail(&mut file, offset)?;
            report.truncated_tail_removed = true;
            break;
        }
        if magic != RECORD_MAGIC {
            return Err(PersistenceError::Corruption {
                offset,
                reason: "invalid record magic",
            });
        }

        let mut fixed = [0u8; RECORD_FIXED_AFTER_MAGIC];
        if read_partial(&mut file, &mut fixed)? < fixed.len() {
            truncate_tail(&mut file, offset)?;
            report.truncated_tail_removed = true;
            break;
        }
        let key_length = read_fixed_u32(&fixed, 20)? as usize;
        let value_length = read_fixed_u32(&fixed, 24)? as usize;
        validate_lengths(key_length, value_length, limits)?;
        let variable_length = key_length
            .checked_add(value_length)
            .and_then(|size| size.checked_add(4))
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let mut variable = vec![0u8; variable_length];
        if read_partial(&mut file, &mut variable)? < variable.len() {
            truncate_tail(&mut file, offset)?;
            report.truncated_tail_removed = true;
            break;
        }

        let encoded_length = RECORD_FIXED_TOTAL
            .checked_add(key_length)
            .and_then(|size| size.checked_add(value_length))
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let mut encoded = Vec::with_capacity(encoded_length);
        encoded.extend_from_slice(&magic);
        encoded.extend_from_slice(&fixed);
        encoded.extend_from_slice(&variable);
        let record = WalRecord::decode(&encoded, limits, offset)?;
        apply_record(store, record)?;
        report.records_replayed = report.records_replayed.saturating_add(1);
        report.valid_bytes = file.stream_position()?;
    }
    report.recovered_keys = store.len()?;
    Ok(report)
}

fn apply_record(store: &ShardedStore, record: WalRecord) -> Result<(), PersistenceError> {
    match record.record_type {
        RecordType::Set => store.set(record.key, record.value)?,
        RecordType::Del => {
            let _ = store.delete(&record.key)?;
        }
        RecordType::SetEx => {
            let expires_at = record.expires_at()?.ok_or(PersistenceError::Corruption {
                offset: 0,
                reason: "SETEX record is missing expiration",
            })?;
            if expires_at <= SystemTime::now() {
                let _ = store.delete(&record.key)?;
            } else {
                store.set_with_expiry(record.key, record.value, Some(expires_at))?;
            }
        }
        RecordType::Persist => {
            let _ = store.persist(&record.key)?;
        }
    }
    Ok(())
}

fn read_partial(file: &mut File, buffer: &mut [u8]) -> Result<usize, PersistenceError> {
    let mut total = 0usize;
    while total < buffer.len() {
        match file.read(&mut buffer[total..]) {
            Ok(0) => break,
            Ok(read) => {
                total = total
                    .checked_add(read)
                    .ok_or(PersistenceError::InvalidRecordLength)?;
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(PersistenceError::Io(error)),
        }
    }
    Ok(total)
}

fn read_exact_strict(file: &mut File, buffer: &mut [u8]) -> Result<(), PersistenceError> {
    if read_partial(file, buffer)? == buffer.len() {
        Ok(())
    } else {
        Err(PersistenceError::InvalidHeader)
    }
}

fn truncate_tail(file: &mut File, offset: u64) -> Result<(), PersistenceError> {
    file.set_len(offset)?;
    file.seek(SeekFrom::Start(offset))?;
    file.sync_data()?;
    Ok(())
}

fn read_fixed_u32(bytes: &[u8], start: usize) -> Result<u32, PersistenceError> {
    let end = start
        .checked_add(4)
        .ok_or(PersistenceError::InvalidRecordLength)?;
    let array: [u8; 4] = bytes
        .get(start..end)
        .ok_or(PersistenceError::InvalidRecordLength)?
        .try_into()
        .map_err(|_| PersistenceError::InvalidRecordLength)?;
    Ok(u32::from_be_bytes(array))
}
