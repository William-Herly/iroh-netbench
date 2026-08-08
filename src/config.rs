//! Benchmark configuration and presets.

use std::time::Duration;

pub(crate) const LOADED_LATENCY_INTERVAL: Duration = Duration::from_millis(200);
pub(crate) const THROUGHPUT_SAMPLE_INTERVAL: Duration = Duration::from_millis(250);
pub(crate) const MAX_PROBE_RATE_PER_SECOND: u32 = 1_000;

/// Parameters for one complete benchmark run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetBenchConfig {
    /// Hard wall-clock limit for the complete benchmark flow, including negotiation and cleanup.
    pub overall_timeout: Duration,
    /// Maximum time to wait for a direct path before measurements begin.
    pub path_stabilization_timeout: Duration,
    /// Idle RTT measurement duration, at least one millisecond.
    pub latency_duration: Duration,
    /// Delay between idle RTT probes, at least one millisecond.
    pub latency_interval: Duration,
    /// Datagram timeout measurement duration, at least one millisecond.
    pub loss_duration: Duration,
    /// Number of Datagram probes sent per second, within `1..=1000`.
    pub loss_rate_per_second: u32,
    /// Time after the last probe before outstanding probes time out.
    pub probe_timeout: Duration,
    /// Warm-up before download accounting starts.
    pub download_warmup: Duration,
    /// Accounted download duration, at least one millisecond.
    pub download_duration: Duration,
    /// Warm-up before upload accounting starts.
    pub upload_warmup: Duration,
    /// Accounted upload duration, at least one millisecond.
    pub upload_duration: Duration,
    /// Non-zero parallel unidirectional stream count per throughput direction.
    pub parallel_streams: u16,
    /// Non-zero payload size written at a time.
    pub chunk_size: u32,
}

/// Parameters for a low-bandwidth latency and loss probe run.
///
/// Probe runs never open throughput streams and remain available when the peer denies
/// bandwidth-saturating measurements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetBenchProbeConfig {
    /// Hard wall-clock limit for the complete low-bandwidth probe run.
    pub overall_timeout: Duration,
    /// Maximum time to wait for a direct path before measurements begin.
    pub path_stabilization_timeout: Duration,
    /// Idle RTT measurement duration, at least one millisecond.
    pub latency_duration: Duration,
    /// Delay between idle RTT probes, at least one millisecond.
    pub latency_interval: Duration,
    /// Datagram timeout measurement duration, at least one millisecond.
    pub loss_duration: Duration,
    /// Number of Datagram probes sent per second, within `1..=1000`.
    pub loss_rate_per_second: u32,
    /// Time after the last probe before outstanding probes time out.
    pub probe_timeout: Duration,
}

impl NetBenchConfig {
    /// A roughly 15-second interactive preset.
    #[must_use]
    pub fn quick() -> Self {
        Self {
            overall_timeout: Duration::from_secs(45),
            path_stabilization_timeout: Duration::from_secs(3),
            latency_duration: Duration::from_secs(2),
            latency_interval: Duration::from_millis(100),
            loss_duration: Duration::from_secs(2),
            loss_rate_per_second: 100,
            probe_timeout: Duration::from_secs(2),
            download_warmup: Duration::from_secs(1),
            download_duration: Duration::from_secs(5),
            upload_warmup: Duration::from_secs(2),
            upload_duration: Duration::from_secs(5),
            parallel_streams: 4,
            chunk_size: 64 * 1024,
        }
    }

    /// A roughly 32-second default preset.
    #[must_use]
    pub fn standard() -> Self {
        Self {
            overall_timeout: Duration::from_secs(75),
            latency_duration: Duration::from_secs(3),
            loss_duration: Duration::from_secs(5),
            download_warmup: Duration::from_secs(2),
            download_duration: Duration::from_secs(10),
            upload_warmup: Duration::from_secs(2),
            upload_duration: Duration::from_secs(10),
            ..Self::quick()
        }
    }
}

impl Default for NetBenchConfig {
    fn default() -> Self {
        Self::standard()
    }
}

impl NetBenchProbeConfig {
    /// A short low-bandwidth diagnostic preset.
    #[must_use]
    pub fn quick() -> Self {
        let full = NetBenchConfig::quick();
        Self {
            overall_timeout: Duration::from_secs(15),
            ..Self::from(&full)
        }
    }

    /// A longer low-bandwidth diagnostic preset.
    #[must_use]
    pub fn standard() -> Self {
        let full = NetBenchConfig::standard();
        Self {
            overall_timeout: Duration::from_secs(25),
            ..Self::from(&full)
        }
    }
}

impl From<&NetBenchConfig> for NetBenchProbeConfig {
    fn from(config: &NetBenchConfig) -> Self {
        Self {
            overall_timeout: config.overall_timeout,
            path_stabilization_timeout: config.path_stabilization_timeout,
            latency_duration: config.latency_duration,
            latency_interval: config.latency_interval,
            loss_duration: config.loss_duration,
            loss_rate_per_second: config.loss_rate_per_second,
            probe_timeout: config.probe_timeout,
        }
    }
}

impl Default for NetBenchProbeConfig {
    fn default() -> Self {
        Self::standard()
    }
}
