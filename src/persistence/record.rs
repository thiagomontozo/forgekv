use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};
use crc32fast::Hasher;

use crate::{error::PersistenceError, protocol::ProtocolLimits};

pub const WAL_VERSION: u8 = 1;
pub const WAL_HEADER: [u8; 8] = [b'F', b'K', b'V', b'W', WAL_VERSION, 0, 0, 0];
pub const RECORD_MAGIC: [u8; 4] = *b"FKVR";
pub const RECORD_FIXED_AFTER_MAGIC: usize = 28;
pub const RECORD_FIXED_TOTAL: usize = 36;
const NO_EXPIRATION: u64 = u64::MAX;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum RecordType {
    Set = 0x01,
    Del = 0x02,
    SetEx = 0x03,
    Persist = 0x04,
}

impl TryFrom<u8> for RecordType {
    type Error = PersistenceError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Set),
            0x02 => Ok(Self::Del),
            0x03 => Ok(Self::SetEx),
            0x04 => Ok(Self::Persist),
            _ => Err(PersistenceError::InvalidRecordType(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    pub record_type: RecordType,
    pub timestamp_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub key: Bytes,
    pub value: Bytes,
}

impl WalRecord {
    pub fn set(key: Bytes, value: Bytes) -> Result<Self, PersistenceError> {
        Ok(Self {
            record_type: RecordType::Set,
            timestamp_ms: now_ms()?,
            expires_at_ms: None,
            key,
            value,
        })
    }

    pub fn delete(key: Bytes) -> Result<Self, PersistenceError> {
        Ok(Self {
            record_type: RecordType::Del,
            timestamp_ms: now_ms()?,
            expires_at_ms: None,
            key,
            value: Bytes::new(),
        })
    }

    pub fn set_ex(
        key: Bytes,
        value: Bytes,
        expires_at: SystemTime,
    ) -> Result<Self, PersistenceError> {
        Ok(Self {
            record_type: RecordType::SetEx,
            timestamp_ms: now_ms()?,
            expires_at_ms: Some(system_time_to_ms(expires_at)?),
            key,
            value,
        })
    }

    pub fn persist(key: Bytes) -> Result<Self, PersistenceError> {
        Ok(Self {
            record_type: RecordType::Persist,
            timestamp_ms: now_ms()?,
            expires_at_ms: None,
            key,
            value: Bytes::new(),
        })
    }

    pub fn encode(&self) -> Result<Bytes, PersistenceError> {
        let key_length =
            u32::try_from(self.key.len()).map_err(|_| PersistenceError::InvalidRecordLength)?;
        let value_length =
            u32::try_from(self.value.len()).map_err(|_| PersistenceError::InvalidRecordLength)?;
        let capacity = RECORD_FIXED_TOTAL
            .checked_add(self.key.len())
            .and_then(|size| size.checked_add(self.value.len()))
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let mut output = BytesMut::with_capacity(capacity);
        output.extend_from_slice(&RECORD_MAGIC);
        output.put_u8(WAL_VERSION);
        output.put_u8(self.record_type as u8);
        output.put_u16(0);
        output.put_u64(self.timestamp_ms);
        output.put_u64(self.expires_at_ms.unwrap_or(NO_EXPIRATION));
        output.put_u32(key_length);
        output.put_u32(value_length);
        output.extend_from_slice(&self.key);
        output.extend_from_slice(&self.value);
        let checksum = crc32(&output[4..]);
        output.put_u32(checksum);
        Ok(output.freeze())
    }

    pub fn decode(
        encoded: &[u8],
        limits: ProtocolLimits,
        offset: u64,
    ) -> Result<Self, PersistenceError> {
        if encoded.len() < RECORD_FIXED_TOTAL {
            return Err(PersistenceError::Corruption {
                offset,
                reason: "record is shorter than its fixed fields",
            });
        }
        if encoded[..4] != RECORD_MAGIC {
            return Err(PersistenceError::Corruption {
                offset,
                reason: "invalid record magic",
            });
        }
        if encoded[4] != WAL_VERSION {
            return Err(PersistenceError::UnsupportedVersion(encoded[4]));
        }
        let record_type = RecordType::try_from(encoded[5])?;
        if encoded[6..8] != [0, 0] {
            return Err(PersistenceError::Corruption {
                offset,
                reason: "record reserved bytes are not zero",
            });
        }
        let timestamp_ms = read_u64(encoded, 8)?;
        let raw_expiration = read_u64(encoded, 16)?;
        let key_length = read_u32(encoded, 24)? as usize;
        let value_length = read_u32(encoded, 28)? as usize;
        validate_lengths(key_length, value_length, limits)?;
        let expected = RECORD_FIXED_TOTAL
            .checked_add(key_length)
            .and_then(|size| size.checked_add(value_length))
            .ok_or(PersistenceError::InvalidRecordLength)?;
        if encoded.len() != expected {
            return Err(PersistenceError::InvalidRecordLength);
        }
        let checksum_offset = expected
            .checked_sub(4)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let expected_checksum = read_u32(encoded, checksum_offset)?;
        let actual_checksum = crc32(&encoded[4..checksum_offset]);
        if actual_checksum != expected_checksum {
            return Err(PersistenceError::ChecksumMismatch { offset });
        }
        let key_start = 32usize;
        let key_end = key_start
            .checked_add(key_length)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let value_end = key_end
            .checked_add(value_length)
            .ok_or(PersistenceError::InvalidRecordLength)?;
        let key = Bytes::copy_from_slice(
            encoded
                .get(key_start..key_end)
                .ok_or(PersistenceError::InvalidRecordLength)?,
        );
        let value = Bytes::copy_from_slice(
            encoded
                .get(key_end..value_end)
                .ok_or(PersistenceError::InvalidRecordLength)?,
        );
        validate_semantics(record_type, &key, &value, raw_expiration, offset)?;
        Ok(Self {
            record_type,
            timestamp_ms,
            expires_at_ms: (raw_expiration != NO_EXPIRATION).then_some(raw_expiration),
            key,
            value,
        })
    }

    pub fn expires_at(&self) -> Result<Option<SystemTime>, PersistenceError> {
        self.expires_at_ms.map(ms_to_system_time).transpose()
    }
}

pub fn validate_lengths(
    key_length: usize,
    value_length: usize,
    limits: ProtocolLimits,
) -> Result<(), PersistenceError> {
    if key_length == 0 || key_length > limits.max_key_size || value_length > limits.max_value_size {
        return Err(PersistenceError::InvalidRecordLength);
    }
    key_length
        .checked_add(value_length)
        .and_then(|size| size.checked_add(RECORD_FIXED_TOTAL))
        .ok_or(PersistenceError::InvalidRecordLength)?;
    Ok(())
}

fn validate_semantics(
    record_type: RecordType,
    key: &Bytes,
    value: &Bytes,
    expiration: u64,
    offset: u64,
) -> Result<(), PersistenceError> {
    if key.is_empty() {
        return Err(PersistenceError::Corruption {
            offset,
            reason: "record key is empty",
        });
    }
    let valid = match record_type {
        RecordType::Set => expiration == NO_EXPIRATION,
        RecordType::Del | RecordType::Persist => value.is_empty() && expiration == NO_EXPIRATION,
        RecordType::SetEx => expiration != NO_EXPIRATION,
    };
    if valid {
        Ok(())
    } else {
        Err(PersistenceError::Corruption {
            offset,
            reason: "record fields do not match the record type",
        })
    }
}

fn read_u32(bytes: &[u8], start: usize) -> Result<u32, PersistenceError> {
    let end = start
        .checked_add(4)
        .ok_or(PersistenceError::InvalidRecordLength)?;
    let value: [u8; 4] = bytes
        .get(start..end)
        .ok_or(PersistenceError::InvalidRecordLength)?
        .try_into()
        .map_err(|_| PersistenceError::InvalidRecordLength)?;
    Ok(u32::from_be_bytes(value))
}

fn read_u64(bytes: &[u8], start: usize) -> Result<u64, PersistenceError> {
    let end = start
        .checked_add(8)
        .ok_or(PersistenceError::InvalidRecordLength)?;
    let value: [u8; 8] = bytes
        .get(start..end)
        .ok_or(PersistenceError::InvalidRecordLength)?
        .try_into()
        .map_err(|_| PersistenceError::InvalidRecordLength)?;
    Ok(u64::from_be_bytes(value))
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(bytes);
    hasher.finalize()
}

fn now_ms() -> Result<u64, PersistenceError> {
    system_time_to_ms(SystemTime::now())
}

fn system_time_to_ms(value: SystemTime) -> Result<u64, PersistenceError> {
    let millis = value
        .duration_since(UNIX_EPOCH)
        .map_err(|_| PersistenceError::ClockBeforeEpoch)?
        .as_millis();
    u64::try_from(millis).map_err(|_| PersistenceError::InvalidRecordLength)
}

fn ms_to_system_time(value: u64) -> Result<SystemTime, PersistenceError> {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(value))
        .ok_or(PersistenceError::InvalidRecordLength)
}
