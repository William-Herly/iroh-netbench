//! Latency configuration contract tests.

use std::time::Duration;

use iroh_netbench::{NetBenchConfig, NetBenchProbeConfig};

#[test]
fn quick_latency_profile_is_shorter_than_standard() {
    assert!(NetBenchConfig::quick().latency_duration < NetBenchConfig::standard().latency_duration);
    assert_eq!(
        NetBenchConfig::standard().latency_interval,
        Duration::from_millis(100)
    );
}

#[test]
fn probe_presets_match_full_latency_and_loss_settings() {
    let full = NetBenchConfig::quick();
    let probes = NetBenchProbeConfig::quick();
    assert_eq!(probes.latency_duration, full.latency_duration);
    assert_eq!(probes.latency_interval, full.latency_interval);
    assert_eq!(probes.loss_duration, full.loss_duration);
    assert_eq!(probes.loss_rate_per_second, full.loss_rate_per_second);
    assert_eq!(probes.probe_timeout, full.probe_timeout);
}
