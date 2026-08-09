use std::io::ErrorKind;

use bytes::Bytes;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::error::ProtocolError;

use super::{Frame, ProtocolLimits, PROTOCOL_VERSION};

pub async fn read_frame<R>(
    reader: &mut R,
    limits: ProtocolLimits,
) -> Result<Option<Frame>, ProtocolError>
where
    R: AsyncRead + Unpin,
{
    let mut length_bytes = [0u8; 4];
    let first = reader.read(&mut length_bytes[..1]).await?;
    if first == 0 {
        return Ok(None);
    }
    read_exact_or_truncated(reader, &mut length_bytes[1..]).await?;
    let frame_length = u32::from_be_bytes(length_bytes) as usize;
    if frame_length < 2 {
        return Err(ProtocolError::FrameTooSmall {
            actual: frame_length,
            minimum: 2,
        });
    }
    if frame_length > limits.max_frame_size {
        return Err(ProtocolError::FrameTooLarge {
            actual: frame_length,
            maximum: limits.max_frame_size,
        });
    }

    let mut body = vec![0u8; frame_length];
    read_exact_or_truncated(reader, &mut body).await?;
    let version = body[0];
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }

    Ok(Some(Frame {
        version,
        code: body[1],
        payload: Bytes::copy_from_slice(&body[2..]),
    }))
}

pub async fn write_frame<W>(
    writer: &mut W,
    frame: &Frame,
    limits: ProtocolLimits,
) -> Result<(), ProtocolError>
where
    W: AsyncWrite + Unpin,
{
    let encoded = frame.encode(limits)?;
    writer.write_all(&encoded).await?;
    writer.flush().await?;
    Ok(())
}

async fn read_exact_or_truncated<R>(reader: &mut R, buffer: &mut [u8]) -> Result<(), ProtocolError>
where
    R: AsyncRead + Unpin,
{
    match reader.read_exact(buffer).await {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == ErrorKind::UnexpectedEof => Err(ProtocolError::Truncated),
        Err(error) => Err(ProtocolError::Io(error)),
    }
}
