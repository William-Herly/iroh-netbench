//! Serializable final report types.

use std::time::Duration;

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
    /// Path sampled after negotiation, before preparation/stabilization.
    pub preparation_path: PathKind,
    /// Selected path at the start of the measurement window.
    pub initial_path: PathKind,
    /// Selected path at the end of the measurement window.
    pub final_path: PathKind,
    /// Whether sampled path kinds differed during measurements, excluding preparation.
    pub path_changed: bool,
    /// Whether any measurement-window sample had no known path.
    pub unknown_path_observed: bool,
    /// Nominal sampling interval; shorter transitions can go unobserved.
    pub observation_interval: Duration,
    /// Whether a relay-to-direct transition was seen.
    pub became_direct: bool,
    /// Time from business-flow start until the first observed relay-to-direct transition.
    pub time_to_direct: Option<Duration>,
}

impl MeasurementPathReport {
    /// Summarizes observed measurement paths without treating missing samples as a known path.
    #[must_use]
    pub const fn observed_path(&self) -> PathKind {
        if self.unknown_path_observed {
            PathKind::Unknown
        } else if self.path_changed {
            PathKind::Mixed
        } else {
            self.initial_path
        }
    }
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

/// A counter delta, or the reason its snapshots cannot be compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CounterDelta {
    /// At least one snapshot lacked a value or a stable scope identity.
    Unavailable,
    /// The counter decreased within the same advertised scope.
    Reset,
    /// The counter belongs to different connection/path generations at the boundaries.
    ScopeChanged,
    /// Measured increase, including a genuine zero.
    Measured(u64),
}

/// Delta over the full observation window, including probes, warmups and both directions.
///
/// Connection counters can include other traffic on the caller-owned Session. They are not
/// per-flow payload counters, and their window differs from each throughput direction's window.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransportReport {
    /// Connection-wide QUIC packets declared lost.
    pub lost_packets: CounterDelta,
    /// Connection-wide QUIC bytes declared lost.
    pub lost_bytes: CounterDelta,
    /// Congestion events within the supplied path counter's scope.
    pub congestion_events: CounterDelta,
    /// UDP datagrams received.
    pub udp_rx_datagrams: CounterDelta,
    /// UDP datagrams transmitted.
    pub udp_tx_datagrams: CounterDelta,
    /// Final path maximum transmission unit.
    pub current_mtu: Option<u16>,
    /// Black-hole detections within the supplied path counter's scope.
    pub black_holes_detected: CounterDelta,
    /// Final transport RTT estimate.
    pub final_rtt: Option<Duration>,
}

/// Complete result of a benchmark run.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NetBenchReport {
    /// JSON schema version.
    pub schema_version: u16,
    /// Negotiated wire protocol version.
    pub protocol_version: u16,
    /// Remote peer identity.
    pub peer_id: String,
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
    /// Remote peer identity.
    pub peer_id: String,
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
