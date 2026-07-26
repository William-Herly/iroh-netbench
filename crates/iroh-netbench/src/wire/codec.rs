//! Length-prefixed postcard control framing.

use crate::{Error, Result};
use iroh::endpoint::{RecvStream, SendStream};

use super::ControlMessage;

/// Maximum encoded postcard payload, excluding the four-byte prefix.
pub const MAX_CONTROL_MESSAGE_SIZE: usize = 64 * 1024;

/// Encodes a control message as `u32 big-endian length || postcard payload`.
///
/// # Errors
///
/// Returns [`crate::Error::ControlMessageTooLarge`] when the encoded payload exceeds 64 KiB,
/// or [`crate::Error::Codec`] when postcard cannot serialize the value.
pub fn encode_control(message: &ControlMessage) -> Result<Vec<u8>> {
    let payload = postcard::to_allocvec(message)?;
    if payload.len() > MAX_CONTROL_MESSAGE_SIZE {
        return Err(Error::ControlMessageTooLarge {
            actual: payload.len(),
            maximum: MAX_CONTROL_MESSAGE_SIZE,
        });
    }

    let length = u32::try_from(payload.len()).map_err(|_| Error::ControlMessageTooLarge {
        actual: payload.len(),
        maximum: MAX_CONTROL_MESSAGE_SIZE,
    })?;
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&length.to_be_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

/// Decodes exactly one complete length-prefixed control frame.
///
/// # Errors
///
/// Returns an I/O error for an incomplete or inconsistent frame,
/// [`crate::Error::ControlMessageTooLarge`] for an oversized declaration,
/// or [`crate::Error::Codec`] when postcard cannot decode the payload.
pub fn decode_control(frame: &[u8]) -> Result<ControlMessage> {
    if frame.len() < 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing control frame length",
        )
        .into());
    }

    let length_bytes: [u8; 4] = frame[..4].try_into().map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::UnexpectedEof,
            "missing control frame length",
        )
    })?;
    let declared = u32::from_be_bytes(length_bytes) as usize;
    if declared > MAX_CONTROL_MESSAGE_SIZE {
        return Err(Error::ControlMessageTooLarge {
            actual: declared,
            maximum: MAX_CONTROL_MESSAGE_SIZE,
        });
    }
    if frame.len() != declared + 4 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "control frame length does not match payload",
        )
        .into());
    }
    Ok(postcard::from_bytes(&frame[4..])?)
}

/// Writes one length-prefixed message to an iroh control stream.
///
/// # Errors
///
/// Returns a codec, size-limit, or QUIC stream error.
pub async fn write_control(send: &mut SendStream, message: &ControlMessage) -> Result<()> {
    let frame = encode_control(message)?;
    send.write_all(&frame).await.map_err(Error::network)
}

/// Reads one length-prefixed message from an iroh control stream.
///
/// # Errors
///
/// Returns a size-limit, codec, unexpected EOF, or QUIC stream error.
pub async fn read_control(recv: &mut RecvStream) -> Result<ControlMessage> {
    let mut length = [0_u8; 4];
    recv.read_exact(&mut length).await.map_err(Error::network)?;
    let declared = u32::from_be_bytes(length) as usize;
    if declared > MAX_CONTROL_MESSAGE_SIZE {
        return Err(Error::ControlMessageTooLarge {
            actual: declared,
            maximum: MAX_CONTROL_MESSAGE_SIZE,
        });
    }
    let mut payload = vec![0_u8; declared];
    recv.read_exact(&mut payload)
        .await
        .map_err(Error::network)?;
    Ok(postcard::from_bytes(&payload)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::Capabilities;

    #[test]
    fn control_frame_round_trip() {
        let message = ControlMessage::ClientHello {
            protocol_versions: vec![1],
            capabilities: Capabilities {
                datagram_probes: true,
                loaded_latency: true,
                path_stats: true,
            },
        };
        let frame = encode_control(&message).unwrap();
        assert_eq!(decode_control(&frame).unwrap(), message);
    }

    #[test]
    fn upload_readiness_round_trip() {
        let message = ControlMessage::TestReady { test_id: 42 };
        let frame = encode_control(&message).unwrap();
        assert_eq!(decode_control(&frame).unwrap(), message);
    }

    #[test]
    fn rejects_declared_oversize_before_decode() {
        let oversized = u32::try_from(MAX_CONTROL_MESSAGE_SIZE + 1).unwrap();
        let mut frame = Vec::from(oversized.to_be_bytes());
        frame.resize(4, 0);
        assert!(matches!(
            decode_control(&frame),
            Err(Error::ControlMessageTooLarge { .. })
        ));
    }
}
