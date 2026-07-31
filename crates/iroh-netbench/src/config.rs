//! Client benchmark configuration and presets.

use std::time::Duration;

/// Parameters for one complete benchmark run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetBenchConfig {
    /// Maximum time to wait for a direct path before measurements begin.
    pub path_stabilization_timeout: Duration,
    /// Idle RTT measurement duration.
    pub latency_duration: Duration,
    /// Delay between idle RTT probes.
    pub latency_interval: Duration,
    /// Datagram timeout measurement duration.
    pub loss_duration: Duration,
    /// Number of datagram probes sent per second.
    pub loss_rate_per_second: u32,
    /// Time after the last probe before outstanding probes time out.
    pub probe_timeout: Duration,
    /// Warm-up before download accounting starts.
    pub download_warmup: Duration,
    /// Accounted download duration.
    pub download_duration: Duration,
    /// Warm-up before upload accounting starts.
    pub upload_warmup: Duration,
    /// Accounted upload duration.
    pub upload_duration: Duration,
    /// Number of parallel unidirectional streams per throughput direction.
    pub parallel_streams: u16,
    /// Payload size written at a time.
    pub chunk_size: u32,
    /// Interval between progress samples.
    pub sample_interval: Duration,
}

/// Parameters for a low-bandwidth latency and loss probe run.
///
/// Probe runs never open throughput streams and remain available when the peer denies
/// bandwidth-saturating measurements.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetBenchProbeConfig {
    /// Maximum time to wait for a direct path before measurements begin.
    pub path_stabilization_timeout: Duration,
    /// Idle RTT measurement duration.
    pub latency_duration: Duration,
    /// Delay between idle RTT probes.
    pub latency_interval: Duration,
    /// Datagram timeout measurement duration.
    pub loss_duration: Duration,
    /// Number of datagram probes sent per second.
    pub loss_rate_per_second: u32,
    /// Time after the last probe before outstanding probes time out.
    pub probe_timeout: Duration,
}

impl NetBenchConfig {
    /// A roughly 15-second interactive preset.
    #[must_use]
    pub fn quick() -> Self {
        Self {
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
            sample_interval: Duration::from_millis(250),
        }
    }

    /// A roughly 32-second default preset.
    #[must_use]
    pub fn standard() -> Self {
        Self {
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
        Self::from(&full)
    }

    /// A longer low-bandwidth diagnostic preset.
    #[must_use]
    pub fn standard() -> Self {
        let full = NetBenchConfig::standard();
        Self::from(&full)
    }
}

impl From<&NetBenchConfig> for NetBenchProbeConfig {
    fn from(config: &NetBenchConfig) -> Self {
        Self {
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
