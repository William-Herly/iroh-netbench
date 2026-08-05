//! Serializable final report types.

use std::time::Duration;

use iroh::EndpointId;
use serde::{Deserialize, Serialize};

/// High-level path classification over the lifetime of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PathKind {
    /// Direct UDP path, address family not known.
    Direct,
    /// Direct IPv4 path.
    DirectIpv4,
    /// Direct IPv6 path.
    DirectIpv6,
    /// Traffic used an iroh relay.
    Relay,
    /// Path type changed during accounted measurements.
    Mixed,
    /// Path information was unavailable.
    Unknown,
}

/// Path observations made during the benchmark business flow.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeasurementPathReport {
    /// Time from starting the business flow until the first control response.
    pub negotiation_time: Duration,
    /// Selected path at the start of the measurement window.
    pub initial_path: PathKind,
    /// Selected path at the end of the measurement window.
    pub final_path: PathKind,
    /// Whether the selected path changed during the flow.
    pub path_changed: bool,
    /// Whether a relay-to-direct transition was seen.
    pub became_direct: bool,
    /// Time from business-flow start until the first direct path.
    pub time_to_direct: Option<Duration>,
}

/// Idle or loaded RTT distribution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LatencyReport {
    /// Successful RTT sample count.
    pub samples: u32,
    /// Minimum RTT.
    pub min: Duration,
    /// Arithmetic mean RTT.
    pub mean: Duration,
    /// Median RTT.
    pub p50: Duration,
    /// 95th percentile RTT.
    pub p95: Duration,
    /// 99th percentile RTT.
    pub p99: Duration,
    /// Maximum RTT.
    pub max: Duration,
    /// Mean absolute difference between adjacent RTT samples.
    pub jitter_mean: Duration,
    /// 95th percentile adjacent-sample jitter.
    pub jitter_p95: Duration,
}

/// Application datagram echo results.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LossReport {
    /// Unique probes sent.
    pub sent: u64,
    /// Unique probe echoes received before timeout.
    pub received: u64,
    /// Probes not echoed before the deadline.
    pub timed_out: u64,
    /// Duplicate echoes observed.
    pub duplicated: u64,
    /// Out-of-order echoes observed.
    pub reordered: u64,
    /// `timed_out / sent`, or zero when no probes were sent.
    pub timeout_ratio: f64,
}

/// One throughput direction.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ThroughputReport {
    /// Payload bytes received by the measuring side.
    pub received_bytes: u64,
    /// Accounted measurement duration.
    pub measurement_duration: Duration,
    /// Payload bits per second.
    pub bits_per_second: f64,
    /// Parallel stream count.
    pub streams: u16,
}

/// RTT while throughput traffic is active.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoadedLatencyReport {
    /// Idle median RTT used as the baseline.
    pub idle_p50: Duration,
    /// Median RTT during download.
    pub download_p50: Duration,
    /// Saturated download minus idle baseline.
    pub download_increase: Duration,
    /// Median RTT during upload.
    pub upload_p50: Duration,
    /// Saturated upload minus idle baseline.
    pub upload_increase: Duration,
}

/// Delta between transport snapshots taken before and after measurements.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportReport {
    /// QUIC packets declared lost.
    pub lost_packets: u64,
    /// QUIC bytes declared lost.
    pub lost_bytes: u64,
    /// Congestion events.
    pub congestion_events: u64,
    /// UDP datagrams received.
    pub udp_rx_datagrams: u64,
    /// UDP datagrams transmitted.
    pub udp_tx_datagrams: u64,
    /// Final path maximum transmission unit.
    pub current_mtu: u16,
    /// Black-hole detections.
    pub black_holes_detected: u64,
    /// Final transport RTT estimate.
    pub final_rtt: Duration,
}

/// Complete result of a benchmark run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetBenchReport {
    /// JSON schema version.
    pub schema_version: u16,
    /// Negotiated wire protocol version.
    pub protocol_version: u16,
    /// Remote endpoint identity.
    pub peer_id: EndpointId,
    /// Wall-clock duration of the run.
    pub total_duration: Duration,
    /// Path details observed during the benchmark flow.
    pub path: MeasurementPathReport,
    /// Idle latency.
    pub idle_latency: LatencyReport,
    /// Application probe timeout results.
    pub loss: LossReport,
    /// Download rate.
    pub download: ThroughputReport,
    /// Upload rate.
    pub upload: ThroughputReport,
    /// Latency under load.
    pub loaded_latency: LoadedLatencyReport,
    /// QUIC path-statistics delta.
    pub transport: TransportReport,
}

/// Complete result of a low-bandwidth latency and loss probe run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetBenchProbeReport {
    /// JSON schema version.
    pub schema_version: u16,
    /// Negotiated wire protocol version.
    pub protocol_version: u16,
    /// Remote endpoint identity.
    pub peer_id: EndpointId,
    /// Wall-clock duration of the run.
    pub total_duration: Duration,
    /// Path details observed during the probe flow.
    pub path: MeasurementPathReport,
    /// Idle latency and jitter.
    pub idle_latency: LatencyReport,
    /// Application Datagram timeout results.
    pub loss: LossReport,
    /// QUIC path-statistics delta during the probe run.
    pub transport: TransportReport,
}
