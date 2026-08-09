use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crc32fast::Hasher;

use crate::{
    error::PersistenceError,
    protocol::ProtocolLimits,
    store::{ShardedStore, SnapshotEntry},
};

const SNAPSHOT_MAGIC: [u8; 4] = *b"FKVS";
const SNAPSHOT_VERSION: u8 = 1;
const SNAPSHOT_HEADER_SIZE: usize = 16;
const ENTRY_FIXED_SIZE: usize = 16;
const NO_EXPIRATION: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SnapshotReport {
    pub entries_loaded: u64,
    pub expired_entries_skipped: u64,
}

pub fn write_snapshot_atomic(
    path: &Path,
    entries: &[SnapshotEntry],
) -> Result<(), PersistenceError> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let temporary = sidecar_path(path, "tmp");
    let backup = sidecar_path(path, "bak");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(&SNAPSHOT_MAGIC)?;
    file.write_all(&[SNAPSHOT_VERSION, 0, 0, 0])?;
    file.write_all(
        &u64::try_from(entries.len())
            .map_err(|_| PersistenceError::InvalidRecordLength)?
            .to_be_bytes(),
    )?;
    for entry in entries {
        write_entry(&mut file, entry)?;
    }
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

pub fn load_snapshot(
    path: &Path,
    store: &ShardedStore,
    limits: ProtocolLimits,
) -> Result<SnapshotReport, PersistenceError> {
    restore_backup_if_needed(path)?;
    if !path.exists() {
        return Ok(SnapshotReport::default());
    }
    let mut file = File::open(path)?;
    let mut header = [0u8; SNAPSHOT_HEADER_SIZE];
    read_exact_snapshot(&mut file, &mut header, 0, "truncated snapshot header")?;
    if header[..4] != SNAPSHOT_MAGIC
        || header[4] != SNAPSHOT_VERSION
        || header[5..8] != [0, 0, 0]
    {
        return Err(PersistenceError::InvalidSnapshotHeader);
    }
    let entry_count = u64::from_be_bytes(
        header[8..16]
            .try_into()
            .map_err(|_| PersistenceError::InvalidSnapshotHeader)?,
    );
    let mut report = SnapshotReport::default();
    for index in 0..entry_count {
        let entry = read_entry(&mut file, index, limits)?;
        if entry.expires_at.is_some_and(|expires_at| expires_at <= SystemTime::now()) {
            report.expired_entries_skipped = report.expired_entries_skipped.saturating_add(1);
        } else {
            store.set_with_expiry(entry.key, entry.value, entry.expires_at)?;
            report.entries_loaded = report.entries_loaded.saturating_add(1);
        }
    }
    let mut trailing = [0u8; 1];
    if file.read(&mut trailing)? != 0 {
        return Err(PersistenceError::SnapshotCorruption {
            entry: entry_count,
            reason: "trailing bytes after declared entries",
        });
    }
    Ok(report)
}

fn write_entry(file: &mut File, entry: &SnapshotEntry) -> Result<(), PersistenceError> {
    if entry.key.is_empty() {
        return Err(PersistenceError::InvalidRecordLength);
    }
    let key_length =
        u32::try_from(entry.key.len()).map_err(|_| PersistenceError::InvalidRecordLength)?;
    let value_length =
        u32::try_from(entry.value.len()).map_err(|_| PersistenceError::InvalidRecordLength)?;
    let expires_at = entry
        .expires_at
        .map(system_time_to_ms)
        .transpose()?
        .unwrap_or(NO_EXPIRATION);
    let mut checksum = Hasher::new();
    checksum.update(&expires_at.to_be_bytes());
    checksum.update(&key_length.to_be_bytes());
    checksum.update(&value_length.to_be_bytes());
    checksum.update(&entry.key);
    checksum.update(&entry.value);
    file.write_all(&expires_at.to_be_bytes())?;
    file.write_all(&key_length.to_be_bytes())?;
    file.write_all(&value_length.to_be_bytes())?;
    file.write_all(&entry.key)?;
    file.write_all(&entry.value)?;
    file.write_all(&checksum.finalize().to_be_bytes())?;
    Ok(())
}

fn read_entry(
    file: &mut File,
    index: u64,
    limits: ProtocolLimits,
) -> Result<SnapshotEntry, PersistenceError> {
    let mut fixed = [0u8; ENTRY_FIXED_SIZE];
    read_exact_snapshot(file, &mut fixed, index, "truncated entry header")?;
    let expires_at_ms = u64::from_be_bytes(
        fixed[..8]
            .try_into()
            .map_err(|_| snapshot_error(index, "invalid expiration"))?,
    );
    let key_length = u32::from_be_bytes(
        fixed[8..12]
            .try_into()
            .map_err(|_| snapshot_error(index, "invalid key length"))?,
    ) as usize;
    let value_length = u32::from_be_bytes(
        fixed[12..16]
            .try_into()
            .map_err(|_| snapshot_error(index, "invalid value length"))?,
    ) as usize;
    if key_length == 0
        || key_length > limits.max_key_size
        || value_length > limits.max_value_size
    {
        return Err(snapshot_error(index, "entry length exceeds configured limits"));
    }
    let variable_length = key_length
        .checked_add(value_length)
        .and_then(|length| length.checked_add(4))
        .ok_or_else(|| snapshot_error(index, "entry length overflow"))?;
    let mut variable = vec![0u8; variable_length];
    read_exact_snapshot(file, &mut variable, index, "truncated entry body")?;
    let checksum_offset = key_length
        .checked_add(value_length)
        .ok_or_else(|| snapshot_error(index, "checksum offset overflow"))?;
    let expected_checksum = u32::from_be_bytes(
        variable[checksum_offset..]
            .try_into()
            .map_err(|_| snapshot_error(index, "invalid checksum"))?,
    );
    let mut checksum = Hasher::new();
    checksum.update(&fixed);
    checksum.update(&variable[..checksum_offset]);
    if checksum.finalize() != expected_checksum {
        return Err(snapshot_error(index, "checksum mismatch"));
    }
    let expires_at = if expires_at_ms == NO_EXPIRATION {
        None
    } else {
        Some(
            UNIX_EPOCH
                .checked_add(Duration::from_millis(expires_at_ms))
                .ok_or_else(|| snapshot_error(index, "expiration timestamp overflow"))?,
        )
    };
    Ok(SnapshotEntry {
        key: bytes::Bytes::copy_from_slice(&variable[..key_length]),
        value: bytes::Bytes::copy_from_slice(&variable[key_length..checksum_offset]),
        expires_at,
    })
}

fn read_exact_snapshot(
    file: &mut File,
    buffer: &mut [u8],
    entry: u64,
    reason: &'static str,
) -> Result<(), PersistenceError> {
    file.read_exact(buffer).map_err(|error| {
        if error.kind() == io::ErrorKind::UnexpectedEof {
            snapshot_error(entry, reason)
        } else {
            PersistenceError::Io(error)
        }
    })
}

fn restore_backup_if_needed(path: &Path) -> Result<(), PersistenceError> {
    let backup = sidecar_path(path, "bak");
    if !path.exists() && backup.exists() {
        fs::rename(backup, path)?;
    }
    Ok(())
}

fn sidecar_path(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(format!(".{suffix}"));
    PathBuf::from(name)
}

fn snapshot_error(entry: u64, reason: &'static str) -> PersistenceError {
    PersistenceError::SnapshotCorruption { entry, reason }
}

fn system_time_to_ms(value: SystemTime) -> Result<u64, PersistenceError> {
    let millis = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PersistenceError::ClockBeforeEpoch)?
        .as_millis();
    u64::try_from(millis).map_err(|_| PersistenceError::InvalidRecordLength)
}
