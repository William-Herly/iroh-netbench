//! Cross-module public API and protocol contract tests.

use std::time::Duration;

use iroh::{
    Endpoint, EndpointAddr, RelayMode, TransportAddr,
    endpoint::{DialPathFilter, DialPolicy, presets},
    protocol::Router,
};
use iroh_netbench::{
    Error, NETBENCH_ALPN, NetBenchConfig, NetBenchEvent, NetBenchInitiator, NetBenchProbeConfig,
    NetBenchProtocol, PROTOCOL_VERSION, SCHEMA_VERSION, SessionMonitor, SessionMonitorConfig,
    ThroughputPolicy,
};

async fn connect(endpoint: &Endpoint, peer: EndpointAddr) -> iroh::endpoint::Connection {
    endpoint.connect(peer, NETBENCH_ALPN).await.unwrap()
}

#[test]
fn public_protocol_identity_is_stable() {
    assert_eq!(NETBENCH_ALPN, b"/iroh/netbench/1");
    assert_eq!(PROTOCOL_VERSION, 1);
    assert_eq!(SCHEMA_VERSION, 2);
    assert_eq!(NetBenchProtocol::default().max_parallel_streams(), 8);
    assert_eq!(
        NetBenchProtocol::default().throughput_policy(),
        ThroughputPolicy::Allow
    );
}

#[test]
fn throughput_policy_is_shared_by_protocol_clones() {
    let protocol = NetBenchProtocol::builder()
        .throughput_policy(ThroughputPolicy::Deny)
        .build();
    let control = protocol.clone();
    assert_eq!(protocol.throughput_policy(), ThroughputPolicy::Deny);
    control.set_throughput_policy(ThroughputPolicy::Allow);
    assert_eq!(protocol.throughput_policy(), ThroughputPolicy::Allow);
}

#[tokio::test]
async fn caller_dials_and_server_peer_authorization_is_enforced() {
    let authorized = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let unauthorized = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let server = Endpoint::builder(presets::Minimal)
        .clear_ip_transports()
        .bind_addr("127.0.0.1:0")
        .unwrap()
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let router = Router::builder(server.clone())
        .accept(
            NETBENCH_ALPN,
            NetBenchProtocol::builder()
                .allowed_peer(authorized.id())
                .build(),
        )
        .spawn();
    let server_v4 = server
        .addr()
        .ip_addrs()
        .copied()
        .find(std::net::SocketAddr::is_ipv4)
        .unwrap();
    let target = EndpointAddr::from_parts(server.id(), [TransportAddr::Ip(server_v4)]);
    let policy = DialPolicy::supplied_only(DialPathFilter::DIRECT_IPV4);

    let options = iroh::endpoint::ConnectOptions::new().with_dial_policy(policy);
    let allowed_connection = authorized
        .connect_with_opts(target.clone(), NETBENCH_ALPN, options.clone())
        .await
        .unwrap()
        .await
        .unwrap();
    assert!(
        allowed_connection
            .paths()
            .iter()
            .any(|path| path.is_selected() && path.is_ip())
    );
    let mut probes = NetBenchProbeConfig::quick();
    probes.path_stabilization_timeout = Duration::ZERO;
    probes.latency_duration = Duration::from_millis(50);
    probes.loss_duration = Duration::from_millis(50);
    NetBenchInitiator::new()
        .run_probes(allowed_connection.clone(), probes.clone())
        .await
        .unwrap();

    let denied_connection = unauthorized
        .connect_with_opts(target, NETBENCH_ALPN, options)
        .await
        .unwrap()
        .await
        .unwrap();
    let denied = NetBenchInitiator::new()
        .run_probes(denied_connection.clone(), probes)
        .await;
    assert!(denied.is_err());
    allowed_connection.close(0_u8.into(), b"test complete");
    denied_connection.close(0_u8.into(), b"test complete");

    router.shutdown().await.unwrap();
    authorized.close().await;
    unauthorized.close().await;
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
    let expected_download_duration = config.download_duration;
    let expected_upload_duration = config.upload_duration;
    let overall_timeout = config.overall_timeout;

    let client_monitor = SessionMonitor::spawn(monitor_config);
    let wall_started = tokio::time::Instant::now();
    let connection = connect(&client_endpoint, server.addr()).await;
    let report = NetBenchInitiator::new()
        .with_monitor(client_monitor.clone())
        .run(connection.clone(), config)
        .await
        .unwrap();
    assert!(report.idle_latency.samples > 0);
    assert!(report.download.received_bytes > 0);
    assert!(report.upload.received_bytes > 0);
    assert_eq!(
        report.download.measurement_duration,
        expected_download_duration
    );
    assert_eq!(report.upload.measurement_duration, expected_upload_duration);
    assert!(wall_started.elapsed() < overall_timeout);
    tokio::time::sleep(Duration::from_millis(100)).await;
    let client_metrics = client_monitor.latest();
    let server_metrics = server_monitor.latest();
    assert_eq!(client_metrics.connections.len(), 1);
    assert_eq!(server_metrics.connections.len(), 1);
    assert_eq!(client_metrics.connections[0].label, "netbench-client");
    assert_eq!(server_metrics.connections[0].label, "netbench-server");
    assert!(client_metrics.total_rx_bytes > 0);
    assert!(client_metrics.total_tx_bytes > 0);
    connection.close(0_u8.into(), b"test complete");

    router.shutdown().await.unwrap();
    client_endpoint.close().await;
}

#[tokio::test]
async fn full_benchmark_can_be_cancelled_and_joined() {
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let router = Router::builder(server.clone())
        .accept(NETBENCH_ALPN, NetBenchProtocol::default())
        .spawn();
    let client_endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let connection = connect(&client_endpoint, server.addr()).await;
    let client = NetBenchInitiator::new();

    let mut config = NetBenchConfig::quick();
    config.overall_timeout = Duration::from_secs(20);
    config.path_stabilization_timeout = Duration::ZERO;
    config.latency_duration = Duration::from_millis(100);
    config.latency_interval = Duration::from_millis(20);
    config.loss_duration = Duration::from_millis(100);
    config.loss_rate_per_second = 20;
    config.probe_timeout = Duration::from_millis(100);
    config.download_warmup = Duration::ZERO;
    config.download_duration = Duration::from_secs(10);
    let mut test = client.start(connection.clone(), config).await.unwrap();
    loop {
        let event = tokio::time::timeout(Duration::from_secs(2), test.next())
            .await
            .unwrap()
            .unwrap();
        if matches!(event, NetBenchEvent::DownloadStarted) {
            break;
        }
    }

    assert!(!test.is_finished());
    test.cancel();
    tokio::time::timeout(Duration::from_secs(2), test.abort_and_wait())
        .await
        .unwrap()
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), connection.closed())
            .await
            .is_err()
    );
    connection.close(0_u8.into(), b"test complete");

    router.shutdown().await.unwrap();
    client_endpoint.close().await;
}

#[tokio::test]
async fn overall_timeout_reports_stage_without_closing_connection() {
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let router = Router::builder(server.clone())
        .accept(NETBENCH_ALPN, NetBenchProtocol::default())
        .spawn();
    let client_endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let connection = connect(&client_endpoint, server.addr()).await;
    let client = NetBenchInitiator::new();

    let mut config = NetBenchConfig::quick();
    config.overall_timeout = Duration::from_secs(1);
    config.path_stabilization_timeout = Duration::ZERO;
    config.latency_duration = Duration::from_secs(10);
    let result = client.run(connection.clone(), config).await;
    assert!(matches!(
        result,
        Err(Error::Timeout {
            stage: "idle latency"
        })
    ));
    assert!(
        tokio::time::timeout(Duration::from_millis(100), connection.closed())
            .await
            .is_err()
    );
    connection.close(0_u8.into(), b"test complete");

    router.shutdown().await.unwrap();
    client_endpoint.close().await;
}

#[tokio::test]
async fn dropping_run_future_cancels_the_internal_task() {
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let router = Router::builder(server.clone())
        .accept(NETBENCH_ALPN, NetBenchProtocol::default())
        .spawn();
    let client_endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let connection = connect(&client_endpoint, server.addr()).await;
    let client = NetBenchInitiator::new();

    let mut config = NetBenchConfig::quick();
    config.overall_timeout = Duration::from_secs(20);
    config.path_stabilization_timeout = Duration::ZERO;
    config.latency_duration = Duration::from_secs(10);
    let outer = tokio::time::timeout(
        Duration::from_millis(300),
        client.run(connection.clone(), config),
    )
    .await;
    assert!(outer.is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(100), connection.closed())
            .await
            .is_err()
    );
    connection.close(0_u8.into(), b"test complete");

    router.shutdown().await.unwrap();
    client_endpoint.close().await;
}

#[tokio::test]
async fn denied_throughput_fails_early_but_probe_only_still_completes() {
    let server = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let protocol = NetBenchProtocol::builder()
        .throughput_policy(ThroughputPolicy::Deny)
        .build();
    let router = Router::builder(server.clone())
        .accept(NETBENCH_ALPN, protocol)
        .spawn();
    let client_endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await
        .unwrap();
    let client = NetBenchInitiator::new();

    let full_connection = connect(&client_endpoint, server.addr()).await;
    let mut full_test = client
        .start(full_connection.clone(), NetBenchConfig::quick())
        .await
        .unwrap();
    let mut measurement_started = false;
    while let Some(event) = full_test.next().await {
        measurement_started |= matches!(
            event,
            NetBenchEvent::LatencyStarted
                | NetBenchEvent::LossStarted
                | NetBenchEvent::DownloadWarmupStarted
                | NetBenchEvent::DownloadStarted
                | NetBenchEvent::UploadWarmupStarted
                | NetBenchEvent::UploadStarted
        );
    }
    assert!(!measurement_started);
    assert!(matches!(
        full_test.result().await,
        Err(Error::ThroughputDeniedByPeer)
    ));

    let mut probe_config = NetBenchProbeConfig::quick();
    probe_config.path_stabilization_timeout = Duration::from_millis(200);
    probe_config.latency_duration = Duration::from_millis(200);
    probe_config.latency_interval = Duration::from_millis(25);
    probe_config.loss_duration = Duration::from_millis(200);
    probe_config.loss_rate_per_second = 20;
    probe_config.probe_timeout = Duration::from_millis(300);
    let probe_connection = connect(&client_endpoint, server.addr()).await;
    let report = client
        .run_probes(probe_connection.clone(), probe_config)
        .await
        .unwrap();
    assert_eq!(report.protocol_version, PROTOCOL_VERSION);
    assert!(report.idle_latency.samples > 0);
    assert!(report.loss.sent > 0);
    full_connection.close(0_u8.into(), b"test complete");
    probe_connection.close(0_u8.into(), b"test complete");

    router.shutdown().await.unwrap();
    client_endpoint.close().await;
}
