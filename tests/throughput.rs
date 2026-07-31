//! Throughput configuration contract tests.

use iroh_netbench::NetBenchConfig;
use std::time::Duration;

#[test]
fn standard_throughput_defaults_match_protocol_plan() {
    let config = NetBenchConfig::standard();
    assert_eq!(config.overall_timeout, Duration::from_secs(75));
    assert_eq!(config.parallel_streams, 4);
    assert_eq!(config.chunk_size, 64 * 1024);
    assert_eq!(config.download_warmup, Duration::from_secs(2));
    assert_eq!(config.upload_warmup, Duration::from_secs(2));
}

#[test]
fn full_presets_have_explicit_wall_clock_limits() {
    assert_eq!(
        NetBenchConfig::quick().overall_timeout,
        Duration::from_secs(45)
    );
    assert!(NetBenchConfig::quick().overall_timeout < NetBenchConfig::standard().overall_timeout);
}
