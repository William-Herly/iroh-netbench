//! Progress events emitted during a benchmark.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{NetBenchReport, PathKind};

/// Fine-grained connection and negotiation progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionStage {
    /// The supplied `EndpointAddr` is being prepared for dialing.
    PreparingAddress,
    /// iroh has started QUIC/TLS establishment over available paths.
    QuicHandshakeStarted,
    /// Authenticated QUIC establishment completed.
    QuicHandshakeCompleted {
        /// Elapsed time since dialing started.
        elapsed: Duration,
    },
    /// The reliable bidirectional control stream was opened.
    ControlStreamOpened,
    /// Client versions and capabilities were sent.
    ClientHelloSent,
    /// The server selected a compatible version.
    ServerHelloReceived {
        /// Negotiated wire version.
        version: u16,
    },
    /// A selected transport path was observed.
    PathObserved {
        /// Current selected path.
        path: PathKind,
    },
    /// A direct path was selected after connection establishment.
    DirectPathSelected {
        /// Time since dialing started.
        elapsed: Duration,
    },
    /// Direct-path waiting expired and Relay remains selected.
    RelayFallback {
        /// Time spent waiting for Direct.
        waited: Duration,
    },
}

/// Connection details available immediately after the handshake.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionInfo {
    /// Time spent establishing the connection.
    pub connect_time: Duration,
    /// Path observed at connection time.
    pub path: PathKind,
}

/// One RTT observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencySample {
    /// Monotonic sequence number within the test.
    pub sequence: u64,
    /// Round-trip duration.
    pub rtt: Duration,
}

/// Cumulative loss-probe progress.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LossSample {
    /// Probes sent so far.
    pub sent: u64,
    /// Echoes received so far.
    pub received: u64,
    /// Probes currently classified as timed out.
    pub timed_out: u64,
}

/// Throughput traffic direction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ThroughputDirection {
    /// Server to client.
    Download,
    /// Client to server.
    Upload,
}

/// One interval throughput observation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThroughputSample {
    /// Traffic direction.
    pub direction: ThroughputDirection,
    /// Time elapsed in the accounted measurement window.
    pub elapsed: Duration,
    /// Payload bytes received by the measuring side so far.
    pub received_bytes: u64,
    /// Rate for the most recent sampling interval.
    pub interval_bps: f64,
}

/// Progress emitted by [`crate::NetBenchTest`].
#[allow(
    clippy::large_enum_variant,
    reason = "the public API intentionally exposes Finished(NetBenchReport)"
)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NetBenchEvent {
    /// Outgoing connection has started.
    Connecting,
    /// One detailed connection/negotiation milestone.
    ConnectionStage(ConnectionStage),
    /// Connection and version negotiation completed.
    Connected(ConnectionInfo),
    /// Idle latency measurement started.
    LatencyStarted,
    /// One idle RTT was observed.
    LatencySample(LatencySample),
    /// Datagram timeout measurement started.
    LossStarted,
    /// Datagram timeout progress changed.
    LossSample(LossSample),
    /// Download warm-up traffic started.
    DownloadWarmupStarted,
    /// Download measurement started.
    DownloadStarted,
    /// Download rate was sampled.
    DownloadSample(ThroughputSample),
    /// Upload warm-up traffic started.
    UploadWarmupStarted,
    /// Upload measurement started.
    UploadStarted,
    /// Upload rate was sampled.
    UploadSample(ThroughputSample),
    /// The complete report is available.
    Finished(NetBenchReport),
}
