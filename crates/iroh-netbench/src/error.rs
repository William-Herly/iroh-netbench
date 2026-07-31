//! Error types.

use std::time::Duration;

/// Crate result type.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors produced by a benchmark run.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A control frame exceeded the configured maximum.
    #[error("control message is too large: {actual} bytes (maximum {maximum})")]
    ControlMessageTooLarge {
        /// Encoded message size.
        actual: usize,
        /// Configured maximum.
        maximum: usize,
    },

    /// The peer selected no mutually supported protocol version.
    #[error("no mutually supported protocol version")]
    UnsupportedProtocolVersion,

    /// A requested test exceeds a server limit.
    #[error("test duration {requested:?} exceeds server limit {maximum:?}")]
    DurationLimitExceeded {
        /// Requested duration.
        requested: Duration,
        /// Server maximum.
        maximum: Duration,
    },

    /// The operation timed out.
    #[error("operation timed out during {stage}")]
    Timeout {
        /// Human-readable stage name.
        stage: &'static str,
    },

    /// The peer allows low-bandwidth probes but denies throughput measurements.
    #[error("peer policy denies throughput measurements")]
    ThroughputDeniedByPeer,

    /// The peer rejected or failed a protocol operation.
    #[error("peer error {code}: {message}")]
    Peer {
        /// Stable wire error code.
        code: u16,
        /// Diagnostic message.
        message: String,
    },

    /// An iroh connection or stream operation failed.
    #[error("network error: {0}")]
    Network(String),

    /// A protocol invariant was violated.
    #[error("protocol error: {0}")]
    Protocol(String),

    /// The operation belongs to a later implementation phase.
    #[error("{feature} is not implemented yet")]
    NotImplemented {
        /// Missing feature name.
        feature: &'static str,
    },

    /// Control message serialization failed.
    #[error("control codec error: {0}")]
    Codec(#[from] postcard::Error),

    /// I/O failed.
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
}

impl Error {
    /// Converts an external network error without exposing it in the public error enum.
    pub(crate) fn network(error: impl std::fmt::Display) -> Self {
        Self::Network(error.to_string())
    }
}
