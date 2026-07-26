//! Datagram probe format.

use serde::{Deserialize, Serialize};

/// Magic prefix used to reject unrelated application datagrams.
pub const PROBE_MAGIC: [u8; 4] = *b"INB1";

/// Datagram role.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProbeKind {
    /// Sender expects the peer to echo this datagram.
    Request,
    /// Echo of a request.
    Response,
}

/// Small application-level datagram used for RTT and timeout observations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Probe {
    /// Protocol discriminator.
    pub magic: [u8; 4],
    /// Active test identifier.
    pub test_id: u64,
    /// Monotonic probe sequence.
    pub sequence: u64,
    /// Request or response.
    pub kind: ProbeKind,
}
