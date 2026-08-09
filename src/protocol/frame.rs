use bytes::{BufMut, Bytes, BytesMut};

use crate::{config::Config, error::ProtocolError};

pub const PROTOCOL_VERSION: u8 = 1;
pub const FRAME_HEADER_SIZE: usize = 2;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Opcode {
    Ping = 0x01,
    Set = 0x02,
    Get = 0x03,
    Del = 0x04,
    Exists = 0x05,
    SetEx = 0x06,
    Ttl = 0x07,
    Persist = 0x08,
    Info = 0x09,
    Stats = 0x0a,
}

impl TryFrom<u8> for Opcode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(Self::Ping),
            0x02 => Ok(Self::Set),
            0x03 => Ok(Self::Get),
            0x04 => Ok(Self::Del),
            0x05 => Ok(Self::Exists),
            0x06 => Ok(Self::SetEx),
            0x07 => Ok(Self::Ttl),
            0x08 => Ok(Self::Persist),
            0x09 => Ok(Self::Info),
            0x0a => Ok(Self::Stats),
            _ => Err(ProtocolError::InvalidOpcode(value)),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum StatusCode {
    Ok = 0x00,
    NotFound = 0x01,
    InvalidRequest = 0x02,
    ServerError = 0x03,
    Pong = 0x04,
    Value = 0x05,
    Integer = 0x06,
    Info = 0x07,
    Stats = 0x08,
}

impl TryFrom<u8> for StatusCode {
    type Error = ProtocolError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x00 => Ok(Self::Ok),
            0x01 => Ok(Self::NotFound),
            0x02 => Ok(Self::InvalidRequest),
            0x03 => Ok(Self::ServerError),
            0x04 => Ok(Self::Pong),
            0x05 => Ok(Self::Value),
            0x06 => Ok(Self::Integer),
            0x07 => Ok(Self::Info),
            0x08 => Ok(Self::Stats),
            _ => Err(ProtocolError::InvalidOpcode(value)),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Frame {
    pub version: u8,
    pub code: u8,
    pub payload: Bytes,
}

impl Frame {
    pub fn request(opcode: Opcode, payload: Bytes) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            code: opcode as u8,
            payload,
        }
    }

    pub fn response(status: StatusCode, payload: Bytes) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            code: status as u8,
            payload,
        }
    }

    pub fn encoded_len(&self) -> Result<usize, ProtocolError> {
        4usize
            .checked_add(FRAME_HEADER_SIZE)
            .and_then(|size| size.checked_add(self.payload.len()))
            .ok_or(ProtocolError::IntegerOverflow)
    }

    pub fn encode(&self, limits: ProtocolLimits) -> Result<Bytes, ProtocolError> {
        let body_len = FRAME_HEADER_SIZE
            .checked_add(self.payload.len())
            .ok_or(ProtocolError::IntegerOverflow)?;
        if body_len > limits.max_frame_size {
            return Err(ProtocolError::FrameTooLarge {
                actual: body_len,
                maximum: limits.max_frame_size,
            });
        }
        let body_len_u32 = u32::try_from(body_len).map_err(|_| ProtocolError::IntegerOverflow)?;
        let capacity = 4usize
            .checked_add(body_len)
            .ok_or(ProtocolError::IntegerOverflow)?;
        let mut output = BytesMut::with_capacity(capacity);
        output.put_u32(body_len_u32);
        output.put_u8(self.version);
        output.put_u8(self.code);
        output.extend_from_slice(&self.payload);
        Ok(output.freeze())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProtocolLimits {
    pub max_frame_size: usize,
    pub max_key_size: usize,
    pub max_value_size: usize,
}

impl From<&Config> for ProtocolLimits {
    fn from(config: &Config) -> Self {
        Self {
            max_frame_size: config.max_frame_size,
            max_key_size: config.max_key_size,
            max_value_size: config.max_value_size,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Response {
    Ok,
    NotFound,
    InvalidRequest(String),
    ServerError(String),
    Pong,
    Value(Bytes),
    Integer(i64),
    Info(Vec<(String, String)>),
    Stats(Vec<(String, u64)>),
}

impl Response {
    pub fn into_frame(self) -> Result<Frame, ProtocolError> {
        match self {
            Self::Ok => Ok(Frame::response(StatusCode::Ok, Bytes::new())),
            Self::NotFound => Ok(Frame::response(StatusCode::NotFound, Bytes::new())),
            Self::InvalidRequest(message) => Ok(Frame::response(
                StatusCode::InvalidRequest,
                encode_string(&message)?,
            )),
            Self::ServerError(message) => Ok(Frame::response(
                StatusCode::ServerError,
                encode_string(&message)?,
            )),
            Self::Pong => Ok(Frame::response(StatusCode::Pong, Bytes::new())),
            Self::Value(value) => {
                let length = u32::try_from(value.len())
                    .map_err(|_| ProtocolError::IntegerOverflow)?;
                let capacity = 4usize
                    .checked_add(value.len())
                    .ok_or(ProtocolError::IntegerOverflow)?;
                let mut payload = BytesMut::with_capacity(capacity);
                payload.put_u32(length);
                payload.extend_from_slice(&value);
                Ok(Frame::response(StatusCode::Value, payload.freeze()))
            }
            Self::Integer(value) => {
                let mut payload = BytesMut::with_capacity(8);
                payload.put_i64(value);
                Ok(Frame::response(StatusCode::Integer, payload.freeze()))
            }
            Self::Info(fields) => Ok(Frame::response(
                StatusCode::Info,
                encode_string_fields(fields)?,
            )),
            Self::Stats(fields) => Ok(Frame::response(
                StatusCode::Stats,
                encode_metric_fields(fields)?,
            )),
        }
    }

    pub fn from_frame(frame: Frame) -> Result<Self, ProtocolError> {
        if frame.version != PROTOCOL_VERSION {
            return Err(ProtocolError::UnsupportedVersion(frame.version));
        }
        let status = StatusCode::try_from(frame.code)?;
        match status {
            StatusCode::Ok => require_empty(&frame.payload, Self::Ok),
            StatusCode::NotFound => require_empty(&frame.payload, Self::NotFound),
            StatusCode::InvalidRequest => Ok(Self::InvalidRequest(decode_string(&frame.payload)?)),
            StatusCode::ServerError => Ok(Self::ServerError(decode_string(&frame.payload)?)),
            StatusCode::Pong => require_empty(&frame.payload, Self::Pong),
            StatusCode::Value => Ok(Self::Value(decode_value(&frame.payload)?)),
            StatusCode::Integer => {
                if frame.payload.len() != 8 {
                    return Err(ProtocolError::InvalidPayload(
                        "integer response must contain exactly eight bytes",
                    ));
                }
                let bytes: [u8; 8] = frame
                    .payload
                    .as_ref()
                    .try_into()
                    .map_err(|_| ProtocolError::Truncated)?;
                Ok(Self::Integer(i64::from_be_bytes(bytes)))
            }
            StatusCode::Info => Ok(Self::Info(decode_string_fields(&frame.payload)?)),
            StatusCode::Stats => Ok(Self::Stats(decode_metric_fields(&frame.payload)?)),
        }
    }
}

fn require_empty(payload: &Bytes, response: Response) -> Result<Response, ProtocolError> {
    if payload.is_empty() {
        Ok(response)
    } else {
        Err(ProtocolError::InvalidPayload(
            "response status does not accept a payload",
        ))
    }
}

fn encode_string(value: &str) -> Result<Bytes, ProtocolError> {
    let length = u32::try_from(value.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    let capacity = 4usize
        .checked_add(value.len())
        .ok_or(ProtocolError::IntegerOverflow)?;
    let mut payload = BytesMut::with_capacity(capacity);
    payload.put_u32(length);
    payload.extend_from_slice(value.as_bytes());
    Ok(payload.freeze())
}

fn decode_string(payload: &Bytes) -> Result<String, ProtocolError> {
    let value = decode_value(payload)?;
    String::from_utf8(value.to_vec())
        .map_err(|_| ProtocolError::InvalidPayload("message is not valid UTF-8"))
}

fn decode_value(payload: &Bytes) -> Result<Bytes, ProtocolError> {
    if payload.len() < 4 {
        return Err(ProtocolError::Truncated);
    }
    let length = u32::from_be_bytes(
        payload[..4]
            .try_into()
            .map_err(|_| ProtocolError::Truncated)?,
    ) as usize;
    let expected = 4usize
        .checked_add(length)
        .ok_or(ProtocolError::IntegerOverflow)?;
    if payload.len() != expected {
        return Err(ProtocolError::InvalidPayload("invalid byte string length"));
    }
    Ok(payload.slice(4..expected))
}

fn encode_string_fields(fields: Vec<(String, String)>) -> Result<Bytes, ProtocolError> {
    let count = u16::try_from(fields.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    let mut payload = BytesMut::new();
    payload.put_u16(count);
    for (name, value) in fields {
        put_name(&mut payload, &name)?;
        let length = u32::try_from(value.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
        payload.put_u32(length);
        payload.extend_from_slice(value.as_bytes());
    }
    Ok(payload.freeze())
}

fn encode_metric_fields(fields: Vec<(String, u64)>) -> Result<Bytes, ProtocolError> {
    let count = u16::try_from(fields.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    let mut payload = BytesMut::new();
    payload.put_u16(count);
    for (name, value) in fields {
        put_name(&mut payload, &name)?;
        payload.put_u64(value);
    }
    Ok(payload.freeze())
}

fn put_name(payload: &mut BytesMut, name: &str) -> Result<(), ProtocolError> {
    let length = u16::try_from(name.len()).map_err(|_| ProtocolError::IntegerOverflow)?;
    payload.put_u16(length);
    payload.extend_from_slice(name.as_bytes());
    Ok(())
}

fn decode_string_fields(payload: &Bytes) -> Result<Vec<(String, String)>, ProtocolError> {
    let mut cursor = PayloadCursor::new(payload);
    let count = cursor.u16()? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name = cursor.string_u16()?;
        let value = cursor.string_u32()?;
        fields.push((name, value));
    }
    cursor.finish()?;
    Ok(fields)
}

fn decode_metric_fields(payload: &Bytes) -> Result<Vec<(String, u64)>, ProtocolError> {
    let mut cursor = PayloadCursor::new(payload);
    let count = cursor.u16()? as usize;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        fields.push((cursor.string_u16()?, cursor.u64()?));
    }
    cursor.finish()?;
    Ok(fields)
}

struct PayloadCursor<'a> {
    payload: &'a [u8],
    position: usize,
}

impl<'a> PayloadCursor<'a> {
    fn new(payload: &'a [u8]) -> Self {
        Self {
            payload,
            position: 0,
        }
    }

    fn take(&mut self, length: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self
            .position
            .checked_add(length)
            .ok_or(ProtocolError::IntegerOverflow)?;
        let value = self
            .payload
            .get(self.position..end)
            .ok_or(ProtocolError::Truncated)?;
        self.position = end;
        Ok(value)
    }

    fn u16(&mut self) -> Result<u16, ProtocolError> {
        let bytes: [u8; 2] = self
            .take(2)?
            .try_into()
            .map_err(|_| ProtocolError::Truncated)?;
        Ok(u16::from_be_bytes(bytes))
    }

    fn u32(&mut self) -> Result<u32, ProtocolError> {
        let bytes: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| ProtocolError::Truncated)?;
        Ok(u32::from_be_bytes(bytes))
    }

    fn u64(&mut self) -> Result<u64, ProtocolError> {
        let bytes: [u8; 8] = self
            .take(8)?
            .try_into()
            .map_err(|_| ProtocolError::Truncated)?;
        Ok(u64::from_be_bytes(bytes))
    }

    fn string_u16(&mut self) -> Result<String, ProtocolError> {
        let length = self.u16()? as usize;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| ProtocolError::InvalidPayload("field name is not valid UTF-8"))
    }

    fn string_u32(&mut self) -> Result<String, ProtocolError> {
        let length = self.u32()? as usize;
        String::from_utf8(self.take(length)?.to_vec())
            .map_err(|_| ProtocolError::InvalidPayload("field value is not valid UTF-8"))
    }

    fn finish(self) -> Result<(), ProtocolError> {
        if self.position == self.payload.len() {
            Ok(())
        } else {
            Err(ProtocolError::InvalidPayload("trailing payload bytes"))
        }
    }
}
