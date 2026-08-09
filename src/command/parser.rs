use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};

use crate::{
    error::ProtocolError,
    protocol::{Frame, Opcode, ProtocolLimits, PROTOCOL_VERSION},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Command {
    Ping,
    Set {
        key: Bytes,
        value: Bytes,
    },
    Get {
        key: Bytes,
    },
    Del {
        key: Bytes,
    },
    Exists {
        key: Bytes,
    },
    SetEx {
        key: Bytes,
        ttl: Duration,
        value: Bytes,
    },
    Ttl {
        key: Bytes,
    },
    Persist {
        key: Bytes,
    },
    Info,
    Stats,
}

impl Command {
    pub fn into_frame(self) -> Result<Frame, ProtocolError> {
        let (opcode, payload) = match self {
            Self::Ping => (Opcode::Ping, Bytes::new()),
            Self::Set { key, value } => (Opcode::Set, encode_key_value(&key, &value)?),
            Self::Get { key } => (Opcode::Get, encode_key(&key)?),
            Self::Del { key } => (Opcode::Del, encode_key(&key)?),
            Self::Exists { key } => (Opcode::Exists, encode_key(&key)?),
            Self::SetEx { key, ttl, value } => {
                let millis =
                    u64::try_from(ttl.as_millis()).map_err(|_| ProtocolError::IntegerOverflow)?;
                let key_length =
                    u32::try_from(key.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
                let value_length =
                    u32::try_from(value.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
                let capacity = 16usize
                    .checked_add(key.len())
                    .and_then(|size| size.checked_add(value.len()))
                    .ok_or(ProtocolError::IntegerOverflow)?;
                let mut payload = BytesMut::with_capacity(capacity);
                payload.put_u32(key_length);
                payload.extend_from_slice(&key);
                payload.put_u64(millis);
                payload.put_u32(value_length);
                payload.extend_from_slice(&value);
                (Opcode::SetEx, payload.freeze())
            }
            Self::Ttl { key } => (Opcode::Ttl, encode_key(&key)?),
            Self::Persist { key } => (Opcode::Persist, encode_key(&key)?),
            Self::Info => (Opcode::Info, Bytes::new()),
            Self::Stats => (Opcode::Stats, Bytes::new()),
        };
        Ok(Frame::request(opcode, payload))
    }
}

pub fn parse_command(frame: &Frame, limits: ProtocolLimits) -> Result<Command, ProtocolError> {
    if frame.version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(frame.version));
    }
    let opcode = Opcode::try_from(frame.code)?;
    let mut cursor = Cursor::new(&frame.payload, limits);
    let command = match opcode {
        Opcode::Ping => {
            cursor.require_empty()?;
            Command::Ping
        }
        Opcode::Set => {
            let key = cursor.key()?;
            let value = cursor.value()?;
            cursor.finish()?;
            Command::Set { key, value }
        }
        Opcode::Get => Command::Get {
            key: one_key(&mut cursor)?,
        },
        Opcode::Del => Command::Del {
            key: one_key(&mut cursor)?,
        },
        Opcode::Exists => Command::Exists {
            key: one_key(&mut cursor)?,
        },
        Opcode::SetEx => {
            let key = cursor.key()?;
            let ttl_ms = cursor.u64()?;
            if ttl_ms == 0 {
                return Err(ProtocolError::InvalidPayload(
                    "TTL must be greater than zero",
                ));
            }
            let value = cursor.value()?;
            cursor.finish()?;
            Command::SetEx {
                key,
                ttl: Duration::from_millis(ttl_ms),
                value,
            }
        }
        Opcode::Ttl => Command::Ttl {
            key: one_key(&mut cursor)?,
        },
        Opcode::Persist => Command::Persist {
            key: one_key(&mut cursor)?,
        },
        Opcode::Info => {
            cursor.require_empty()?;
            Command::Info
        }
        Opcode::Stats => {
            cursor.require_empty()?;
            Command::Stats
        }
    };
    Ok(command)
}

fn one_key(cursor: &mut Cursor<'_>) -> Result<Bytes, ProtocolError> {
    let key = cursor.key()?;
    cursor.finish()?;
    Ok(key)
}

fn encode_key(key: &Bytes) -> Result<Bytes, ProtocolError> {
    let length = u32::try_from(key.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    let capacity = 4usize
        .checked_add(key.len())
        .ok_or(ProtocolError::IntegerOverflow)?;
    let mut payload = BytesMut::with_capacity(capacity);
    payload.put_u32(length);
    payload.extend_from_slice(key);
    Ok(payload.freeze())
}

fn encode_key_value(key: &Bytes, value: &Bytes) -> Result<Bytes, ProtocolError> {
    let key_length = u32::try_from(key.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    let value_length = u32::try_from(value.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    let capacity = 8usize
        .checked_add(key.len())
        .and_then(|size| size.checked_add(value.len()))
        .ok_or(ProtocolError::IntegerOverflow)?;
    let mut payload = BytesMut::with_capacity(capacity);
    payload.put_u32(key_length);
    payload.extend_from_slice(key);
    payload.put_u32(value_length);
    payload.extend_from_slice(value);
    Ok(payload.freeze())
}

struct Cursor<'a> {
    payload: &'a [u8],
    position: usize,
    limits: ProtocolLimits,
}

impl<'a> Cursor<'a> {
    fn new(payload: &'a [u8], limits: ProtocolLimits) -> Self {
        Self {
            payload,
            position: 0,
            limits,
        }
    }

    fn take(&mut self, length: usize) -> Result<Bytes, ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ProtocolError::IntegerOverflow)?;
        let value = self
            .payload
            .get(self.position..end)
            .ok_or(ProtocolError::Truncated)?;
        self.position = end;
        Ok(Bytes::copy_from_slice(value))
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        let bytes = self.take(4)?;
        let array: [u8; 4] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| ProtocolError::Truncated)?;
        Ok(u32::from_be_bytes(array))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        let bytes = self.take(8)?;
        let array: [u8; 8] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| ProtocolError::Truncated)?;
        Ok(u64::from_be_bytes(array))
    }

    fn key(&mut self) -> Result<Bytes, ProtocolError> {
        let length = self.u32()? as usize;
        if length == 0 {
            return Err(ProtocolError::InvalidPayload("key cannot be empty"));
        }
        if length > self.limits.max_key_size {
            return Err(ProtocolError::KeyTooLarge {
                actual: length,
                maximum: self.limits.max_key_size,
            });
        }
        self.take(length)
    }

    fn value(&mut self) -> Result<Bytes, ProtocolError> {
        let length = self.u32()? as usize;
        if length > self.limits.max_value_size {
            return Err(ProtocolError::ValueTooLarge {
                actual: length,
                maximum: self.limits.max_value_size,
            });
        }
        self.take(length)
    }

    fn finish(&self) -> Result<(), ProtocolError> {
        if self.position == self.payload.len() {
            Ok(())
        } else {
            Err(ProtocolError::InvalidPayload("trailing payload bytes"))
        }
    }

    fn require_empty(&self) -> Result<(), ProtocolError> {
        if self.payload.is_empty() {
            Ok(())
        } else {
            Err(ProtocolError::InvalidPayload(
                "command does not accept a payload",
            ))
        }
    }
}
