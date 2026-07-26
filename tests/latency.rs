//! Latency configuration contract tests.

use std::time::Duration;

use iroh_netbench::NetBenchConfig;

#[test]
fn quick_latency_profile_is_shorter_than_standard() {
    assert!(NetBenchConfig::quick().latency_duration < NetBenchConfig::standard().latency_duration);
    assert_eq!(
        NetBenchConfig::standard().latency_interval,
        Duration::from_millis(100)
    );
}
