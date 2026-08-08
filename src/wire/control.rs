//! Reliable control-stream messages.

use serde::{Deserialize, Serialize};

/// Feature flags negotiated during the hello exchange.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Capabilities {
    /// Datagram echo probes are supported.
    pub datagram_probes: bool,
    /// RTT probes may continue during throughput phases.
    pub loaded_latency: bool,
    /// Path statistics can be included in the final report.
    pub path_stats: bool,
}

/// Limits advertised by the server.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerLimits {
    /// Maximum duration of an individual test phase.
    pub test_duration_ms: u32,
    /// Maximum unidirectional stream count per throughput phase.
    pub parallel_streams: u16,
    /// Maximum throughput chunk size.
    pub chunk_size: u32,
}

/// Stable peer-visible error category.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u16)]
pub enum ErrorCode {
    /// No protocol version in common.
    UnsupportedVersion = 1,
    /// A requested limit is invalid or too high.
    InvalidRequest = 2,
    /// The server has reached its concurrency limit.
    Busy = 3,
    /// The referenced test is not active.
    UnknownTest = 4,
    /// Internal server failure.
    Internal = 5,
}

/// Messages exchanged on the single reliable bidirectional control stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ControlMessage {
    /// Client protocol negotiation.
    ClientHello {
        /// Supported versions, in preference order.
        protocol_versions: Vec<u16>,
        /// Supported optional features.
        capabilities: Capabilities,
    },
    /// Server protocol selection and policy.
    ServerHello {
        /// Selected protocol version.
        selected_version: u16,
        /// Enforced server limits.
        limits: ServerLimits,
        /// Supported optional features.
        capabilities: Capabilities,
    },
    /// Begin idle RTT measurement.
    StartLatency {
        /// Unique identifier within the connection.
        test_id: u64,
        /// Total phase duration.
        duration_ms: u32,
        /// Delay between probes.
        interval_ms: u32,
    },
    /// Begin application Datagram timeout measurement.
    StartLoss {
        /// Unique identifier within the connection.
        test_id: u64,
        /// Total phase duration.
        duration_ms: u32,
        /// Number of probes requested per second.
        rate_per_second: u32,
        /// Timeout applied after the final probe.
        timeout_ms: u32,
    },
    /// Begin server-to-client traffic.
    StartDownload {
        /// Unique identifier within the connection.
        test_id: u64,
        /// Accounted phase duration.
        duration_ms: u32,
        /// Parallel stream count.
        streams: u16,
        /// Payload chunk size.
        chunk_size: u32,
    },
    /// Begin client-to-server traffic.
    StartUpload {
        /// Unique identifier within the connection.
        test_id: u64,
        /// Accounted phase duration.
        duration_ms: u32,
        /// Parallel stream count.
        streams: u16,
        /// Payload chunk size.
        chunk_size: u32,
    },
    /// The receiver has accepted all upload streams and is ready to account payload bytes.
    TestReady {
        /// Test whose data streams are ready.
        test_id: u64,
    },
    /// Receiver-side cumulative progress for a running throughput phase.
    ThroughputProgress {
        /// Test whose receiver is reporting progress.
        test_id: u64,
        /// Payload bytes received so far.
        received_bytes: u64,
        /// Receiver measurement duration so far.
        duration_ns: u64,
    },
    /// Stop an active phase.
    StopTest {
        /// Test to stop.
        test_id: u64,
    },
    /// Receiver-confirmed accounting for a completed phase.
    TestFinished {
        /// Completed test.
        test_id: u64,
        /// Payload bytes received by the measuring side.
        received_bytes: u64,
        /// Receiver measurement duration.
        duration_ns: u64,
    },
    /// The initiator has completed every requested phase and consumed all terminal responses.
    FlowFinished,
    /// The responder confirms clean flow completion; neither side needs peer-session EOF.
    FlowFinishedAck,
    /// Structured protocol failure.
    Error {
        /// Stable machine-readable category.
        code: ErrorCode,
        /// Human-readable context.
        message: String,
    },
}
