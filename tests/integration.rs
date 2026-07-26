//! Cross-module public protocol contract tests.

use std::time::Duration;

use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};
use iroh_netbench::{
    NETBENCH_ALPN, NetBenchClient, NetBenchConfig, NetBenchProtocol, PROTOCOL_VERSION,
    SessionMonitor, SessionMonitorConfig,
};

#[test]
fn public_protocol_identity_is_stable() {
    assert_eq!(NETBENCH_ALPN, b"/iroh/netbench/1");
    assert_eq!(PROTOCOL_VERSION, 1);
    assert_eq!(NetBenchProtocol::default().max_parallel_streams(), 8);
}

#[tokio::test]
async fn local_endpoints_complete_a_real_benchmark() {
    let monitor_config = SessionMonitorConfig {
        sample_interval: Duration::from_millis(50),
        rate_window: Duration::from_millis(200),
        loss_window: Duration::from_millis(500),
    };
    let server_monitor = SessionMonitor::spawn(monitor_config);
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let router = Router::builder(server.clone())
        .accept(
            NETBENCH_ALPN,
            NetBenchProtocol::builder()
                .with_monitor(server_monitor.clone())
                .build(),
        )
        .spawn();
    let client_endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();

    let mut config = NetBenchConfig::quick();
    config.path_stabilization_timeout = Duration::from_millis(200);
    config.latency_duration = Duration::from_millis(200);
    config.latency_interval = Duration::from_millis(25);
    config.loss_duration = Duration::from_millis(200);
    config.loss_rate_per_second = 20;
    config.probe_timeout = Duration::from_millis(300);
    config.download_warmup = Duration::ZERO;
    config.download_duration = Duration::from_millis(300);
    config.upload_warmup = Duration::ZERO;
    config.upload_duration = Duration::from_millis(300);
    config.parallel_streams = 2;

    let client_monitor = SessionMonitor::spawn(monitor_config);
    let report = NetBenchClient::new(client_endpoint.clone())
        .with_monitor(client_monitor.clone())
        .run(server.addr(), config)
        .await
        .unwrap();
    assert!(report.idle_latency.samples > 0);
    assert!(report.download.received_bytes > 0);
    assert!(report.upload.received_bytes > 0);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let client_metrics = client_monitor.latest();
    let server_metrics = server_monitor.latest();
    assert_eq!(client_metrics.connections.len(), 1);
    assert_eq!(server_metrics.connections.len(), 1);
    assert_eq!(client_metrics.connections[0].label, "netbench-client");
    assert_eq!(server_metrics.connections[0].label, "netbench-server");
    assert!(client_metrics.total_rx_bytes > 0);
    assert!(client_metrics.total_tx_bytes > 0);

    router.shutdown().await.unwrap();
    client_endpoint.close().await;
}
