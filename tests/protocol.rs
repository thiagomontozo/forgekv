use bytes::Bytes;
use forgekv::{
    command::{parse_command, Command},
    error::ProtocolError,
    protocol::{read_frame, Frame, Opcode, ProtocolLimits, Response, PROTOCOL_VERSION},
};
use tokio::io::{AsyncWriteExt, DuplexStream};

fn limits() -> ProtocolLimits {
    ProtocolLimits {
        max_frame_size: 1024,
        max_key_size: 64,
        max_value_size: 512,
    }
}

#[test]
fn commands_encode_and_decode() {
    let commands = [
        Command::Ping,
        Command::Set {
            key: Bytes::from_static(b"key"),
            value: Bytes::from_static(b"binary\0value"),
        },
        Command::Get {
            key: Bytes::from_static(b"key"),
        },
        Command::SetEx {
            key: Bytes::from_static(b"session"),
            ttl: std::time::Duration::from_secs(30),
            value: Bytes::from_static(b"value"),
        },
        Command::Info,
        Command::Stats,
    ];
    for expected in commands {
        let frame = expected
            .clone()
            .into_frame()
            .expect("command should encode");
        assert_eq!(
            parse_command(&frame, limits()).expect("frame should parse"),
            expected
        );
    }
}

#[test]
fn rejects_invalid_opcode() {
    let frame = Frame {
        version: PROTOCOL_VERSION,
        code: 0xff,
        payload: Bytes::new(),
    };
    assert!(matches!(
        parse_command(&frame, limits()),
        Err(ProtocolError::InvalidOpcode(0xff))
    ));
}

#[test]
fn rejects_invalid_and_oversized_lengths() {
    let oversized = Command::Set {
        key: Bytes::from(vec![b'k'; 65]),
        value: Bytes::new(),
    }
    .into_frame()
    .expect("encoding only represents the supplied bytes");
    assert!(matches!(
        parse_command(&oversized, limits()),
        Err(ProtocolError::KeyTooLarge { .. })
    ));

    let truncated = Frame::request(Opcode::Get, Bytes::from_static(&[0, 0, 0, 8, b'a']));
    assert!(matches!(
        parse_command(&truncated, limits()),
        Err(ProtocolError::Truncated)
    ));
}

#[tokio::test]
async fn codec_rejects_truncated_frame() {
    let (mut writer, mut reader) = tokio::io::duplex(32);
    writer
        .write_all(&[0, 0, 0, 5, PROTOCOL_VERSION, Opcode::Ping as u8])
        .await
        .expect("duplex write should work");
    drop(writer);
    assert!(matches!(
        read_frame(&mut reader, limits()).await,
        Err(ProtocolError::Truncated)
    ));
}

#[tokio::test]
async fn codec_rejects_oversized_frame_before_body_allocation() {
    let (mut writer, mut reader): (DuplexStream, DuplexStream) = tokio::io::duplex(32);
    writer
        .write_all(&(2048u32.to_be_bytes()))
        .await
        .expect("duplex write should work");
    assert!(matches!(
        read_frame(&mut reader, limits()).await,
        Err(ProtocolError::FrameTooLarge { .. })
    ));
}

#[test]
fn frame_round_trip_preserves_binary_payload() {
    let frame = Frame::request(Opcode::Set, Bytes::from_static(b"\0\xff\x10"));
    let encoded = frame.encode(limits()).expect("frame should encode");
    assert_eq!(&encoded[6..], frame.payload.as_ref());
}

#[test]
fn redirect_response_round_trip_preserves_address() {
    let expected = Response::Redirect("127.0.0.1:6382".to_owned());
    let frame = expected
        .clone()
        .into_frame()
        .expect("redirect should encode");
    assert_eq!(
        Response::from_frame(frame).expect("redirect should decode"),
        expected
    );
}
