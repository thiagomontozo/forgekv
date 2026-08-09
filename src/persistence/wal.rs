use std::{
    fs::{self, File, OpenOptions as StdOpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime},
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
    store::ShardedStore,
};

use super::record::{
    validate_lengths, RecordType, WalRecord, RECORD_FIXED_AFTER_MAGIC, RECORD_FIXED_TOTAL,
    RECORD_MAGIC, WAL_HEADER,
};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RecoveryReport {
    pub records_replayed: u64,
    pub recovered_keys: usize,
    pub truncated_tail_removed: bool,
    pub valid_bytes: u64,
}

#[derive(Debug)]
pub struct Wal {
    file: TokioFile,
    fsync: FsyncMode,
    metrics: Arc<Metrics>,
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
        Ok(Self {
            file,
            fsync,
            metrics,
        })
    }

    pub async fn append(&mut self, record: &WalRecord) -> Result<(), PersistenceError> {
        let encoded = record.encode()?;
        self.file.write_all(&encoded).await?;
        self.file.flush().await?;
        if self.fsync == FsyncMode::Always {
            self.file.sync_data().await?;
        }
        self.metrics.wal_write(encoded.len() as u64);
        Ok(())
    }

    pub async fn flush(&mut self) -> Result<(), PersistenceError> {
        self.file.flush().await?;
        if self.fsync == FsyncMode::Always {
            self.file.sync_data().await?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct Database {
    store: Arc<ShardedStore>,
    wal: Mutex<Wal>,
    metrics: Arc<Metrics>,
}

impl Database {
    pub async fn open(
        config: &Config,
        store: Arc<ShardedStore>,
        metrics: Arc<Metrics>,
    ) -> Result<(Self, RecoveryReport), PersistenceError> {
        let path = config.wal_path();
        prepare_wal(&path, config.fsync)?;
        let report = recover_wal(&path, store.as_ref(), ProtocolLimits::from(config))?;
        let wal = Wal::open(&path, config.fsync, Arc::clone(&metrics)).await?;
        Ok((
            Self {
                store,
                wal: Mutex::new(wal),
                metrics,
            },
            report,
        ))
    }

    pub fn store(&self) -> &Arc<ShardedStore> {
        &self.store
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
        if fsync == FsyncMode::Always {
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
