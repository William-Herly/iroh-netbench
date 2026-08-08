//! Progress events emitted during a benchmark.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{NetBenchReport, PathKind};

/// Fine-grained benchmark-flow negotiation and path progress.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FlowStage {
    /// The host-routed reliable bidirectional control stream became active.
    ControlStreamReady,
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
    /// A direct path was selected after the business flow started.
    DirectPathSelected {
        /// Time since the business flow started.
        elapsed: Duration,
    },
    /// Direct-path waiting expired and Relay remains selected.
    RelayRetained {
        /// Time spent waiting for Direct.
        waited: Duration,
    },
}

/// Business-flow details available after protocol negotiation and path stabilization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlowInfo {
    /// Time spent negotiating the benchmark protocol.
    pub negotiation_time: Duration,
    /// Path observed at measurement start.
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
    /// Sent probes which have not yet produced a unique echo.
    pub outstanding: u64,
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
    /// One detailed business-flow negotiation/path milestone.
    FlowStage(FlowStage),
    /// Business-flow negotiation completed.
    Ready(FlowInfo),
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
