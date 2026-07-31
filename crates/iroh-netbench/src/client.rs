//! Client-side benchmark lifecycle.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use iroh::{
    Endpoint, EndpointAddr, TransportAddr,
    endpoint::{Connection, PathStats, RecvStream, SendStream},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    ConnectionInfo, ConnectionReport, ConnectionStage, Error, LatencySample, LoadedLatencyReport,
    LossReport, LossSample, NETBENCH_ALPN, NetBenchConfig, NetBenchEvent, NetBenchProbeConfig,
    NetBenchProbeReport, NetBenchReport, PROTOCOL_VERSION, PathKind, Result, SCHEMA_VERSION,
    SessionMonitor, ThroughputDirection, ThroughputReport, ThroughputSample, TransportReport,
    statistics::latency_report,
    wire::{
        Capabilities, ControlMessage, PROBE_MAGIC, Probe, ProbeKind, read_control, write_control,
    },
};

type ConnectionObserver = Arc<dyn Fn(Connection) + Send + Sync>;

/// Starts benchmark connections from an existing long-lived endpoint.
#[derive(Clone)]
pub struct NetBenchClient {
    endpoint: Endpoint,
    connection_observer: Option<ConnectionObserver>,
}

impl std::fmt::Debug for NetBenchClient {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchClient")
            .field("endpoint", &self.endpoint)
            .field(
                "has_connection_observer",
                &self.connection_observer.is_some(),
            )
            .finish()
    }
}

impl NetBenchClient {
    /// Creates a client without taking ownership of the application's endpoint lifecycle.
    #[must_use]
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            connection_observer: None,
        }
    }

    /// Observes each benchmark connection after its QUIC handshake succeeds.
    #[must_use]
    pub fn on_connection(mut self, observer: impl Fn(Connection) + Send + Sync + 'static) -> Self {
        self.connection_observer = Some(Arc::new(observer));
        self
    }

    /// Registers every created benchmark connection with a reusable session monitor.
    #[must_use]
    pub fn with_monitor(self, monitor: SessionMonitor) -> Self {
        self.on_connection(move |connection| {
            monitor.track("netbench-client", connection);
        })
    }

    /// Verifies that the peer accepts the `NetBench` ALPN and wire version.
    ///
    /// # Errors
    ///
    /// Returns a connection, negotiation, or peer error.
    pub async fn check(&self, peer: EndpointAddr) -> Result<ConnectionInfo> {
        self.check_with_progress(peer, |_| {}).await
    }

    /// Performs [`Self::check`] while reporting every connection milestone.
    ///
    /// # Errors
    ///
    /// Returns a connection, negotiation, or peer error.
    pub async fn check_with_progress(
        &self,
        peer: EndpointAddr,
        progress: impl FnMut(ConnectionStage),
    ) -> Result<ConnectionInfo> {
        self.check_with_progress_timeout(peer, Duration::from_secs(3), progress)
            .await
    }

    /// Performs [`Self::check`] with a caller-selected path stabilization timeout.
    ///
    /// A zero timeout is useful when the hosting endpoint deliberately disables direct
    /// transports and therefore must remain on its configured relay.
    ///
    /// # Errors
    ///
    /// Returns a connection, negotiation, or peer error.
    pub async fn check_with_progress_timeout(
        &self,
        peer: EndpointAddr,
        path_stabilization_timeout: Duration,
        mut progress: impl FnMut(ConnectionStage),
    ) -> Result<ConnectionInfo> {
        progress(ConnectionStage::PreparingAddress);
        let started = Instant::now();
        progress(ConnectionStage::QuicHandshakeStarted);
        let connection = self
            .endpoint
            .connect(peer, NETBENCH_ALPN)
            .await
            .map_err(Error::network)?;
        if let Some(observer) = &self.connection_observer {
            observer(connection.clone());
        }
        let connect_time = started.elapsed();
        progress(ConnectionStage::QuicHandshakeCompleted {
            elapsed: connect_time,
        });
        let (mut send, mut recv) = connection.open_bi().await.map_err(Error::network)?;
        progress(ConnectionStage::ControlStreamOpened);
        write_control(
            &mut send,
            &ControlMessage::ClientHello {
                protocol_versions: vec![PROTOCOL_VERSION],
                capabilities: Capabilities {
                    datagram_probes: true,
                    loaded_latency: true,
                    path_stats: true,
                },
            },
        )
        .await?;
        progress(ConnectionStage::ClientHelloSent);
        match read_control(&mut recv).await? {
            ControlMessage::ServerHello {
                selected_version, ..
            } if selected_version == PROTOCOL_VERSION => {
                progress(ConnectionStage::ServerHelloReceived {
                    version: selected_version,
                });
            }
            message => return Err(peer_or_protocol_error(message)),
        }
        let path = observe_path_for_check(
            &connection,
            started,
            path_stabilization_timeout,
            &mut progress,
        )
        .await;
        send.finish().map_err(Error::network)?;
        let info = ConnectionInfo { connect_time, path };
        connection.close(0_u8.into(), b"connectivity check complete");
        Ok(info)
    }

    /// Runs a benchmark and returns only its final result.
    ///
    /// # Errors
    ///
    /// Returns a connection, negotiation, measurement, timeout, cancellation, or peer error.
    pub async fn run(&self, peer: EndpointAddr, config: NetBenchConfig) -> Result<NetBenchReport> {
        self.start(peer, config).await?.result().await
    }

    /// Runs only the low-bandwidth latency and loss probes.
    ///
    /// This does not open upload or download throughput streams and remains available when the
    /// peer denies bandwidth-saturating measurements.
    ///
    /// # Errors
    ///
    /// Returns a connection, negotiation, measurement, or peer error.
    pub async fn run_probes(
        &self,
        peer: EndpointAddr,
        config: NetBenchProbeConfig,
    ) -> Result<NetBenchProbeReport> {
        let (event_tx, _event_rx) = mpsc::channel(128);
        run_probe_benchmark(
            self.endpoint.clone(),
            peer,
            config,
            &EventSink(event_tx),
            self.connection_observer.as_ref(),
        )
        .await
    }

    /// Starts a benchmark and returns an event stream plus final result handle.
    ///
    /// # Errors
    ///
    /// This method currently only fails if the benchmark task cannot be initialized.
    #[allow(
        clippy::unused_async,
        reason = "keeps the documented start(...).await API"
    )]
    pub async fn start(&self, peer: EndpointAddr, config: NetBenchConfig) -> Result<NetBenchTest> {
        let (event_tx, event_rx) = mpsc::channel(128);
        let (result_tx, result_rx) = oneshot::channel();
        let endpoint = self.endpoint.clone();
        let connection_observer = self.connection_observer.clone();
        let events = EventSink(event_tx);
        tokio::spawn(async move {
            let result = run_benchmark(
                endpoint,
                peer,
                config,
                &events,
                connection_observer.as_ref(),
            )
            .await;
            if let Ok(report) = &result {
                events.send(NetBenchEvent::Finished(report.clone()));
            }
            let _ = result_tx.send(result);
        });

        Ok(NetBenchTest {
            events: event_rx,
            result: result_rx,
        })
    }
}

/// A running benchmark.
#[derive(Debug)]
pub struct NetBenchTest {
    events: mpsc::Receiver<NetBenchEvent>,
    result: oneshot::Receiver<Result<NetBenchReport>>,
}

impl NetBenchTest {
    /// Waits for the next progress event.
    pub async fn next(&mut self) -> Option<NetBenchEvent> {
        self.events.recv().await
    }

    /// Waits for the final report.
    ///
    /// # Errors
    ///
    /// Returns the terminal error from the benchmark task.
    pub async fn result(self) -> Result<NetBenchReport> {
        self.result
            .await
            .map_err(|_| Error::Protocol("benchmark task ended without a result".to_owned()))?
    }
}

#[derive(Clone)]
struct EventSink(mpsc::Sender<NetBenchEvent>);

impl EventSink {
    fn send(&self, event: NetBenchEvent) {
        let _ = self.0.try_send(event);
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "linear ordering documents the benchmark state machine"
)]
async fn run_benchmark(
    endpoint: Endpoint,
    peer: EndpointAddr,
    config: NetBenchConfig,
    events: &EventSink,
    connection_observer: Option<&ConnectionObserver>,
) -> Result<NetBenchReport> {
    let total_started = Instant::now();
    events.send(NetBenchEvent::Connecting);
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::PreparingAddress,
    ));
    let connect_started = Instant::now();
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::QuicHandshakeStarted,
    ));
    let connection = endpoint
        .connect(peer, NETBENCH_ALPN)
        .await
        .map_err(Error::network)?;
    if let Some(observer) = connection_observer {
        observer(connection.clone());
    }
    let connect_time = connect_started.elapsed();
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::QuicHandshakeCompleted {
            elapsed: connect_time,
        },
    ));
    let peer_id = connection.remote_id();

    let (mut control_send, mut control_recv) =
        connection.open_bi().await.map_err(Error::network)?;
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::ControlStreamOpened,
    ));
    write_control(
        &mut control_send,
        &ControlMessage::ClientHello {
            protocol_versions: vec![PROTOCOL_VERSION],
            capabilities: Capabilities {
                datagram_probes: true,
                loaded_latency: true,
                path_stats: true,
            },
        },
    )
    .await?;
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::ClientHelloSent,
    ));

    let hello = read_control(&mut control_recv).await?;
    let first_control_message_time = connect_started.elapsed();
    let ControlMessage::ServerHello {
        selected_version,
        limits,
        capabilities,
    } = hello
    else {
        return Err(peer_or_protocol_error(hello));
    };
    if selected_version != PROTOCOL_VERSION {
        return Err(Error::UnsupportedProtocolVersion);
    }
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::ServerHelloReceived {
            version: selected_version,
        },
    ));
    if !capabilities.datagram_probes {
        return Err(Error::Protocol(
            "peer does not support QUIC Datagram probes".to_owned(),
        ));
    }
    validate_against_server_limits(&config, limits)?;

    let (initial_path, time_to_direct) = stabilize_path(
        &connection,
        config.path_stabilization_timeout,
        connect_started,
        events,
    )
    .await;
    events.send(NetBenchEvent::Connected(ConnectionInfo {
        connect_time,
        path: initial_path,
    }));
    let path_monitor = spawn_path_monitor(connection.clone(), events.clone());

    let before = TransportSnapshot::capture(&connection);
    let mut test_id = 1_u64;

    events.send(NetBenchEvent::LatencyStarted);
    write_control(
        &mut control_send,
        &ControlMessage::StartLatency {
            test_id,
            duration_ms: duration_ms(config.latency_duration),
            interval_ms: duration_ms(config.latency_interval),
        },
    )
    .await?;
    let idle = measure_probes(
        connection.clone(),
        test_id,
        config.latency_duration,
        config.latency_interval,
        config.probe_timeout,
        ProbeEventMode::Latency(events.clone()),
    )
    .await?;
    write_control(&mut control_send, &ControlMessage::StopTest { test_id }).await?;
    let idle_latency = latency_report(&idle.rtts);
    test_id += 1;

    events.send(NetBenchEvent::LossStarted);
    write_control(
        &mut control_send,
        &ControlMessage::StartLoss {
            test_id,
            duration_ms: duration_ms(config.loss_duration),
            rate_per_second: config.loss_rate_per_second,
            timeout_ms: duration_ms(config.probe_timeout),
        },
    )
    .await?;
    let loss_interval =
        Duration::from_secs_f64(1.0 / f64::from(config.loss_rate_per_second.max(1)));
    let loss_samples = measure_probes(
        connection.clone(),
        test_id,
        config.loss_duration,
        loss_interval,
        config.probe_timeout,
        ProbeEventMode::Loss(events.clone()),
    )
    .await?;
    write_control(&mut control_send, &ControlMessage::StopTest { test_id }).await?;
    let loss = loss_samples.loss_report();
    test_id += 1;

    if !config.download_warmup.is_zero() {
        events.send(NetBenchEvent::DownloadWarmupStarted);
        let _ = download_phase(
            &connection,
            &mut control_send,
            &mut control_recv,
            test_id,
            config.download_warmup,
            config.parallel_streams,
            config.chunk_size,
            None,
        )
        .await?;
        test_id += 1;
    }

    events.send(NetBenchEvent::DownloadStarted);
    let download_future = download_phase(
        &connection,
        &mut control_send,
        &mut control_recv,
        test_id,
        config.download_duration,
        config.parallel_streams,
        config.chunk_size,
        Some(events.clone()),
    );
    let download_probe_future = measure_probes(
        connection.clone(),
        test_id,
        config.download_duration,
        Duration::from_millis(200),
        config.probe_timeout,
        ProbeEventMode::Latency(events.clone()),
    );
    let (download, download_loaded) = tokio::try_join!(download_future, download_probe_future)?;
    test_id += 1;

    if !config.upload_warmup.is_zero() {
        events.send(NetBenchEvent::UploadWarmupStarted);
        if !matches!(
            selected_path_kind(&connection),
            PathKind::Direct | PathKind::DirectIpv4 | PathKind::DirectIpv6
        ) {
            let _ = stabilize_path(
                &connection,
                config.path_stabilization_timeout,
                Instant::now(),
                events,
            )
            .await;
        }
        let _ = upload_phase(
            &connection,
            &mut control_send,
            &mut control_recv,
            test_id,
            config.upload_warmup,
            config.parallel_streams,
            config.chunk_size,
            None,
        )
        .await?;
        test_id += 1;
    }

    events.send(NetBenchEvent::UploadStarted);
    let upload_future = upload_phase(
        &connection,
        &mut control_send,
        &mut control_recv,
        test_id,
        config.upload_duration,
        config.parallel_streams,
        config.chunk_size,
        Some(events.clone()),
    );
    let upload_probe_future = measure_probes(
        connection.clone(),
        test_id,
        config.upload_duration,
        Duration::from_millis(200),
        config.probe_timeout,
        ProbeEventMode::Latency(events.clone()),
    );
    let (upload, upload_loaded) = tokio::try_join!(upload_future, upload_probe_future)?;

    let after = TransportSnapshot::capture(&connection);
    path_monitor.abort();
    let final_path = selected_path_kind(&connection);
    let path = if initial_path == final_path {
        final_path
    } else {
        PathKind::Mixed
    };
    let download_p50 = latency_report(&download_loaded.rtts).p50;
    let upload_p50 = latency_report(&upload_loaded.rtts).p50;
    let loaded_latency = LoadedLatencyReport {
        idle_p50: idle_latency.p50,
        download_p50,
        download_increase: download_p50.saturating_sub(idle_latency.p50),
        upload_p50,
        upload_increase: upload_p50.saturating_sub(idle_latency.p50),
    };

    control_send.finish().map_err(Error::network)?;
    connection.close(0_u8.into(), b"netbench complete");

    Ok(NetBenchReport {
        schema_version: SCHEMA_VERSION,
        protocol_version: selected_version,
        peer_id,
        total_duration: total_started.elapsed(),
        connection: ConnectionReport {
            connect_time,
            first_control_message_time,
            path,
            became_direct: time_to_direct.is_some(),
            time_to_direct,
        },
        idle_latency,
        loss,
        download,
        upload,
        loaded_latency,
        transport: after.delta(before),
    })
}

#[allow(
    clippy::too_many_lines,
    reason = "mirrors the full benchmark negotiation while omitting all throughput phases"
)]
async fn run_probe_benchmark(
    endpoint: Endpoint,
    peer: EndpointAddr,
    config: NetBenchProbeConfig,
    events: &EventSink,
    connection_observer: Option<&ConnectionObserver>,
) -> Result<NetBenchProbeReport> {
    let total_started = Instant::now();
    events.send(NetBenchEvent::Connecting);
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::PreparingAddress,
    ));
    let connect_started = Instant::now();
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::QuicHandshakeStarted,
    ));
    let connection = endpoint
        .connect(peer, NETBENCH_ALPN)
        .await
        .map_err(Error::network)?;
    if let Some(observer) = connection_observer {
        observer(connection.clone());
    }
    let connect_time = connect_started.elapsed();
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::QuicHandshakeCompleted {
            elapsed: connect_time,
        },
    ));
    let peer_id = connection.remote_id();

    let (mut control_send, mut control_recv) =
        connection.open_bi().await.map_err(Error::network)?;
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::ControlStreamOpened,
    ));
    write_control(
        &mut control_send,
        &ControlMessage::ClientHello {
            protocol_versions: vec![PROTOCOL_VERSION],
            capabilities: Capabilities {
                datagram_probes: true,
                loaded_latency: false,
                path_stats: true,
            },
        },
    )
    .await?;
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::ClientHelloSent,
    ));

    let hello = read_control(&mut control_recv).await?;
    let first_control_message_time = connect_started.elapsed();
    let ControlMessage::ServerHello {
        selected_version,
        limits,
        capabilities,
    } = hello
    else {
        return Err(peer_or_protocol_error(hello));
    };
    if selected_version != PROTOCOL_VERSION {
        return Err(Error::UnsupportedProtocolVersion);
    }
    events.send(NetBenchEvent::ConnectionStage(
        ConnectionStage::ServerHelloReceived {
            version: selected_version,
        },
    ));
    if !capabilities.datagram_probes {
        return Err(Error::Protocol(
            "peer does not support QUIC Datagram probes".to_owned(),
        ));
    }
    validate_probes_against_server_limits(&config, limits)?;

    let (initial_path, time_to_direct) = stabilize_path(
        &connection,
        config.path_stabilization_timeout,
        connect_started,
        events,
    )
    .await;
    events.send(NetBenchEvent::Connected(ConnectionInfo {
        connect_time,
        path: initial_path,
    }));
    let path_monitor = spawn_path_monitor(connection.clone(), events.clone());
    let before = TransportSnapshot::capture(&connection);

    let mut test_id = 1_u64;
    events.send(NetBenchEvent::LatencyStarted);
    write_control(
        &mut control_send,
        &ControlMessage::StartLatency {
            test_id,
            duration_ms: duration_ms(config.latency_duration),
            interval_ms: duration_ms(config.latency_interval),
        },
    )
    .await?;
    let idle = measure_probes(
        connection.clone(),
        test_id,
        config.latency_duration,
        config.latency_interval,
        config.probe_timeout,
        ProbeEventMode::Latency(events.clone()),
    )
    .await?;
    write_control(&mut control_send, &ControlMessage::StopTest { test_id }).await?;
    let idle_latency = latency_report(&idle.rtts);
    test_id += 1;

    events.send(NetBenchEvent::LossStarted);
    write_control(
        &mut control_send,
        &ControlMessage::StartLoss {
            test_id,
            duration_ms: duration_ms(config.loss_duration),
            rate_per_second: config.loss_rate_per_second,
            timeout_ms: duration_ms(config.probe_timeout),
        },
    )
    .await?;
    let loss_interval =
        Duration::from_secs_f64(1.0 / f64::from(config.loss_rate_per_second.max(1)));
    let loss_samples = measure_probes(
        connection.clone(),
        test_id,
        config.loss_duration,
        loss_interval,
        config.probe_timeout,
        ProbeEventMode::Loss(events.clone()),
    )
    .await?;
    write_control(&mut control_send, &ControlMessage::StopTest { test_id }).await?;
    let loss = loss_samples.loss_report();

    let after = TransportSnapshot::capture(&connection);
    path_monitor.abort();
    let final_path = selected_path_kind(&connection);
    let path = if initial_path == final_path {
        final_path
    } else {
        PathKind::Mixed
    };
    control_send.finish().map_err(Error::network)?;
    connection.close(0_u8.into(), b"netbench probes complete");

    Ok(NetBenchProbeReport {
        schema_version: SCHEMA_VERSION,
        protocol_version: selected_version,
        peer_id,
        total_duration: total_started.elapsed(),
        connection: ConnectionReport {
            connect_time,
            first_control_message_time,
            path,
            became_direct: time_to_direct.is_some(),
            time_to_direct,
        },
        idle_latency,
        loss,
        transport: after.delta(before),
    })
}

fn validate_against_server_limits(
    config: &NetBenchConfig,
    limits: crate::wire::ServerLimits,
) -> Result<()> {
    if limits.max_parallel_streams == 0 {
        return Err(Error::ThroughputDeniedByPeer);
    }
    let maximum = Duration::from_millis(u64::from(limits.max_test_duration_ms));
    for duration in [
        config.latency_duration,
        config.loss_duration,
        config.download_duration,
        config.upload_duration,
        config.download_warmup,
        config.upload_warmup,
    ] {
        if duration > maximum {
            return Err(Error::DurationLimitExceeded {
                requested: duration,
                maximum,
            });
        }
    }
    if config.parallel_streams > limits.max_parallel_streams {
        return Err(Error::Protocol(format!(
            "requested {} streams but server allows {}",
            config.parallel_streams, limits.max_parallel_streams
        )));
    }
    if config.chunk_size > limits.max_chunk_size {
        return Err(Error::Protocol(format!(
            "requested {} byte chunks but server allows {}",
            config.chunk_size, limits.max_chunk_size
        )));
    }
    Ok(())
}

fn validate_probes_against_server_limits(
    config: &NetBenchProbeConfig,
    limits: crate::wire::ServerLimits,
) -> Result<()> {
    let maximum = Duration::from_millis(u64::from(limits.max_test_duration_ms));
    for duration in [config.latency_duration, config.loss_duration] {
        if duration > maximum {
            return Err(Error::DurationLimitExceeded {
                requested: duration,
                maximum,
            });
        }
    }
    Ok(())
}

fn peer_or_protocol_error(message: ControlMessage) -> Error {
    match message {
        ControlMessage::Error {
            code,
            message: peer_message,
        } => Error::Peer {
            code: code as u16,
            message: peer_message,
        },
        other => Error::Protocol(format!("expected ServerHello, received {other:?}")),
    }
}

async fn stabilize_path(
    connection: &Connection,
    timeout: Duration,
    connect_started: Instant,
    events: &EventSink,
) -> (PathKind, Option<Duration>) {
    let deadline = Instant::now() + timeout;
    let mut previous = None;
    loop {
        let kind = selected_path_kind(connection);
        if previous != Some(kind) {
            events.send(NetBenchEvent::ConnectionStage(
                ConnectionStage::PathObserved { path: kind },
            ));
            previous = Some(kind);
        }
        if matches!(
            kind,
            PathKind::Direct | PathKind::DirectIpv4 | PathKind::DirectIpv6
        ) {
            let elapsed = connect_started.elapsed();
            events.send(NetBenchEvent::ConnectionStage(
                ConnectionStage::DirectPathSelected { elapsed },
            ));
            return (kind, Some(elapsed));
        }
        if Instant::now() >= deadline {
            if kind == PathKind::Relay {
                events.send(NetBenchEvent::ConnectionStage(
                    ConnectionStage::RelayFallback { waited: timeout },
                ));
            }
            return (kind, None);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn observe_path_for_check(
    connection: &Connection,
    started: Instant,
    timeout: Duration,
    progress: &mut impl FnMut(ConnectionStage),
) -> PathKind {
    let deadline = Instant::now() + timeout;
    let mut previous = None;
    loop {
        let path = selected_path_kind(connection);
        if previous != Some(path) {
            progress(ConnectionStage::PathObserved { path });
            previous = Some(path);
        }
        if matches!(
            path,
            PathKind::Direct | PathKind::DirectIpv4 | PathKind::DirectIpv6
        ) {
            progress(ConnectionStage::DirectPathSelected {
                elapsed: started.elapsed(),
            });
            return path;
        }
        if Instant::now() >= deadline {
            if path == PathKind::Relay {
                progress(ConnectionStage::RelayFallback { waited: timeout });
            }
            return path;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[derive(Clone)]
enum ProbeEventMode {
    Latency(EventSink),
    Loss(EventSink),
}

struct ProbeResults {
    sent: u64,
    received: u64,
    duplicated: u64,
    reordered: u64,
    rtts: Vec<Duration>,
}

impl ProbeResults {
    #[allow(
        clippy::cast_precision_loss,
        reason = "ratios are intentionally reported as f64"
    )]
    fn loss_report(&self) -> LossReport {
        let timed_out = self.sent.saturating_sub(self.received);
        LossReport {
            sent: self.sent,
            received: self.received,
            timed_out,
            duplicated: self.duplicated,
            reordered: self.reordered,
            timeout_ratio: if self.sent == 0 {
                0.0
            } else {
                timed_out as f64 / self.sent as f64
            },
        }
    }
}

async fn measure_probes(
    connection: Connection,
    test_id: u64,
    duration: Duration,
    interval: Duration,
    timeout: Duration,
    mode: ProbeEventMode,
) -> Result<ProbeResults> {
    if connection.max_datagram_size().is_none() {
        return Err(Error::Protocol(
            "QUIC Datagram is unavailable on this connection".to_owned(),
        ));
    }

    let started = Instant::now();
    let send_deadline = started + duration;
    let final_deadline = send_deadline + timeout;
    let mut ticker = tokio::time::interval(interval.max(Duration::from_millis(1)));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    let mut sent_at = HashMap::<u64, Instant>::new();
    let mut received = HashSet::<u64>::new();
    let mut sequence = 0_u64;
    let mut duplicated = 0_u64;
    let mut reordered = 0_u64;
    let mut highest_received = None::<u64>;
    let mut rtts = Vec::new();

    loop {
        if Instant::now() >= final_deadline {
            break;
        }
        tokio::select! {
            _ = ticker.tick(), if Instant::now() < send_deadline => {
                let probe = Probe {
                    magic: PROBE_MAGIC,
                    test_id,
                    sequence,
                    kind: ProbeKind::Request,
                };
                let payload = postcard::to_allocvec(&probe)?;
                connection
                    .send_datagram_wait(Bytes::from(payload))
                    .await
                    .map_err(Error::network)?;
                sent_at.insert(sequence, Instant::now());
                sequence += 1;
            }
            datagram = connection.read_datagram() => {
                let bytes = datagram.map_err(Error::network)?;
                let Ok(probe) = postcard::from_bytes::<Probe>(&bytes) else {
                    continue;
                };
                if probe.magic != PROBE_MAGIC || probe.test_id != test_id || probe.kind != ProbeKind::Response {
                    continue;
                }
                let Some(sent) = sent_at.get(&probe.sequence) else {
                    continue;
                };
                if !received.insert(probe.sequence) {
                    duplicated += 1;
                    continue;
                }
                if highest_received.is_some_and(|highest| probe.sequence < highest) {
                    reordered += 1;
                }
                highest_received = Some(highest_received.map_or(probe.sequence, |value| value.max(probe.sequence)));
                let rtt = sent.elapsed();
                rtts.push(rtt);
                match &mode {
                    ProbeEventMode::Latency(events) => events.send(NetBenchEvent::LatencySample(
                        LatencySample { sequence: probe.sequence, rtt }
                    )),
                    ProbeEventMode::Loss(events) => events.send(NetBenchEvent::LossSample(
                        LossSample {
                            sent: sequence,
                            received: received.len() as u64,
                            timed_out: sequence.saturating_sub(received.len() as u64),
                        }
                    )),
                }
            }
            () = tokio::time::sleep_until(final_deadline) => break,
        }

        if Instant::now() >= send_deadline && received.len() == sent_at.len() {
            break;
        }
    }

    Ok(ProbeResults {
        sent: sequence,
        received: received.len() as u64,
        duplicated,
        reordered,
        rtts,
    })
}

#[allow(clippy::too_many_arguments)]
async fn download_phase(
    connection: &Connection,
    control_send: &mut SendStream,
    control_recv: &mut RecvStream,
    test_id: u64,
    duration: Duration,
    streams: u16,
    chunk_size: u32,
    events: Option<EventSink>,
) -> Result<ThroughputReport> {
    write_control(
        control_send,
        &ControlMessage::StartDownload {
            test_id,
            duration_ms: duration_ms(duration),
            streams,
            chunk_size,
        },
    )
    .await?;

    let total = Arc::new(AtomicU64::new(0));
    let mut download_streams = Vec::with_capacity(usize::from(streams));
    for _ in 0..streams {
        let mut stream = connection.accept_uni().await.map_err(Error::network)?;
        let mut magic = [0_u8; 1];
        stream
            .read_exact(&mut magic)
            .await
            .map_err(Error::network)?;
        if magic[0] != 0x44 {
            return Err(Error::Protocol(
                "download stream has an invalid pre-measurement header".to_owned(),
            ));
        }
        download_streams.push(stream);
    }
    expect_ready(control_recv, test_id).await?;
    write_control(control_send, &ControlMessage::TestReady { test_id }).await?;

    let started = Instant::now();
    let sample_task = spawn_sampler(
        Arc::clone(&total),
        started,
        ThroughputDirection::Download,
        events,
    );
    let mut tasks = JoinSet::new();
    for mut stream in download_streams {
        let total = Arc::clone(&total);
        tasks.spawn(async move {
            let mut buffer = vec![0_u8; 64 * 1024];
            while let Some(read) = stream.read(&mut buffer).await.map_err(Error::network)? {
                total.fetch_add(read as u64, Ordering::Relaxed);
            }
            Result::<()>::Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.map_err(Error::network)??;
    }
    sample_task.abort();
    let elapsed = started.elapsed();
    let bytes = total.load(Ordering::Relaxed);
    expect_finished(control_recv, test_id).await?;
    Ok(throughput_report(bytes, elapsed, streams))
}

#[allow(
    clippy::cast_precision_loss,
    clippy::too_many_arguments,
    clippy::too_many_lines,
    reason = "receiver-confirmed live samples keep the upload state machine explicit"
)]
async fn upload_phase(
    connection: &Connection,
    control_send: &mut SendStream,
    control_recv: &mut RecvStream,
    test_id: u64,
    duration: Duration,
    streams: u16,
    chunk_size: u32,
    events: Option<EventSink>,
) -> Result<ThroughputReport> {
    write_control(
        control_send,
        &ControlMessage::StartUpload {
            test_id,
            duration_ms: duration_ms(duration),
            streams,
            chunk_size,
        },
    )
    .await?;

    let mut upload_streams = Vec::with_capacity(usize::from(streams));
    for _ in 0..streams {
        let mut stream = connection.open_uni().await.map_err(Error::network)?;
        stream.write_all(&[0x55]).await.map_err(Error::network)?;
        upload_streams.push(stream);
    }
    expect_ready(control_recv, test_id).await?;

    let started = Instant::now();
    let deadline = started + duration;
    let chunk = Arc::new(vec![
        0x5A;
        usize::try_from(chunk_size).map_err(Error::network)?
    ]);
    let mut tasks = JoinSet::new();
    for mut stream in upload_streams {
        let chunk = Arc::clone(&chunk);
        tasks.spawn(async move {
            while Instant::now() < deadline {
                match tokio::time::timeout_at(deadline, stream.write_all(&chunk)).await {
                    Ok(result) => result.map_err(Error::network)?,
                    Err(_) => break,
                }
            }
            stream.finish().map_err(Error::network)
        });
    }
    let mut progress = VecDeque::<(Duration, u64)>::from([(Duration::ZERO, 0)]);
    let (received_bytes, receiver_duration) = loop {
        tokio::select! {
            result = tasks.join_next(), if !tasks.is_empty() => {
                if let Some(result) = result {
                    result.map_err(Error::network)??;
                }
            }
            message = read_control(control_recv) => {
                match message? {
                    ControlMessage::ThroughputProgress {
                        test_id: received_test_id,
                        received_bytes,
                        duration_ns,
                    } if received_test_id == test_id => {
                        let receiver_elapsed = Duration::from_nanos(duration_ns);
                        progress.push_back((receiver_elapsed, received_bytes));
                        while progress.len() > 5 {
                            progress.pop_front();
                        }
                        let (old_elapsed, old_bytes) = progress.front().copied().unwrap_or_default();
                        let sample_duration = receiver_elapsed.saturating_sub(old_elapsed);
                        let interval_bps = if sample_duration.is_zero() {
                            0.0
                        } else {
                            received_bytes.saturating_sub(old_bytes) as f64 * 8.0
                                / sample_duration.as_secs_f64()
                        };
                        if let Some(events) = &events {
                            events.send(NetBenchEvent::UploadSample(ThroughputSample {
                                direction: ThroughputDirection::Upload,
                                elapsed: receiver_elapsed,
                                received_bytes,
                                interval_bps,
                            }));
                        }
                    }
                    ControlMessage::TestFinished {
                        test_id: received_test_id,
                        received_bytes,
                        duration_ns,
                    } if received_test_id == test_id => {
                        break (received_bytes, Duration::from_nanos(duration_ns));
                    }
                    ControlMessage::Error { code, message } => {
                        return Err(Error::Peer {
                            code: code as u16,
                            message,
                        });
                    }
                    message => {
                        return Err(Error::Protocol(format!(
                            "expected upload progress or completion, received {message:?}"
                        )));
                    }
                }
            }
        }
    };
    while let Some(result) = tasks.join_next().await {
        result.map_err(Error::network)??;
    }
    Ok(throughput_report(
        received_bytes,
        receiver_duration,
        streams,
    ))
}

async fn expect_ready(control_recv: &mut RecvStream, expected_test_id: u64) -> Result<()> {
    match read_control(control_recv).await? {
        ControlMessage::TestReady { test_id } if test_id == expected_test_id => Ok(()),
        ControlMessage::TestReady { test_id } => Err(Error::Protocol(format!(
            "received readiness for test {test_id}, expected {expected_test_id}"
        ))),
        ControlMessage::Error { code, message } => Err(Error::Peer {
            code: code as u16,
            message,
        }),
        message => Err(Error::Protocol(format!(
            "expected TestReady, received {message:?}"
        ))),
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "throughput samples are intentionally reported as f64"
)]
fn spawn_sampler(
    total: Arc<AtomicU64>,
    started: Instant,
    direction: ThroughputDirection,
    events: Option<EventSink>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let Some(events) = events else {
            return;
        };
        let mut samples = VecDeque::<(Instant, u64)>::from([(started, 0)]);
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let now = Instant::now();
            let bytes = total.load(Ordering::Relaxed);
            samples.push_back((now, bytes));
            while samples.len() > 5 {
                samples.pop_front();
            }
            let (previous_time, previous_bytes) = samples.front().copied().unwrap_or((now, bytes));
            let delta_bytes = bytes.saturating_sub(previous_bytes);
            let delta_seconds = now.duration_since(previous_time).as_secs_f64();
            let interval_bps = if delta_seconds > 0.0 {
                delta_bytes as f64 * 8.0 / delta_seconds
            } else {
                0.0
            };
            events.send(match direction {
                ThroughputDirection::Download => NetBenchEvent::DownloadSample(ThroughputSample {
                    direction,
                    elapsed: started.elapsed(),
                    received_bytes: bytes,
                    interval_bps,
                }),
                ThroughputDirection::Upload => NetBenchEvent::UploadSample(ThroughputSample {
                    direction,
                    elapsed: started.elapsed(),
                    received_bytes: bytes,
                    interval_bps,
                }),
            });
        }
    })
}

fn spawn_path_monitor(connection: Connection, events: EventSink) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut previous = selected_path_kind(&connection);
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = ticker.tick() => {}
                _ = connection.closed() => break,
            }
            let current = selected_path_kind(&connection);
            if current != previous {
                events.send(NetBenchEvent::ConnectionStage(
                    ConnectionStage::PathObserved { path: current },
                ));
                previous = current;
            }
        }
    })
}

async fn expect_finished(recv: &mut RecvStream, expected_test_id: u64) -> Result<(u64, Duration)> {
    match read_control(recv).await? {
        ControlMessage::TestFinished {
            test_id,
            received_bytes,
            duration_ns,
        } if test_id == expected_test_id => Ok((received_bytes, Duration::from_nanos(duration_ns))),
        ControlMessage::Error { code, message } => Err(Error::Peer {
            code: code as u16,
            message,
        }),
        other => Err(Error::Protocol(format!(
            "expected TestFinished({expected_test_id}), received {other:?}"
        ))),
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "throughput is intentionally reported as f64"
)]
fn throughput_report(bytes: u64, duration: Duration, streams: u16) -> ThroughputReport {
    ThroughputReport {
        received_bytes: bytes,
        measurement_duration: duration,
        bits_per_second: if duration.is_zero() {
            0.0
        } else {
            bytes as f64 * 8.0 / duration.as_secs_f64()
        },
        streams,
    }
}

fn selected_path_kind(connection: &Connection) -> PathKind {
    let paths = connection.paths();
    let selected = paths.iter().find(iroh::endpoint::Path::is_selected);
    let Some(path) = selected else {
        return PathKind::Unknown;
    };
    match path.remote_addr() {
        TransportAddr::Relay(_) => PathKind::Relay,
        TransportAddr::Ip(address) if address.is_ipv4() => PathKind::DirectIpv4,
        TransportAddr::Ip(address) if address.is_ipv6() => PathKind::DirectIpv6,
        TransportAddr::Ip(_) => PathKind::Direct,
        _ => PathKind::Unknown,
    }
}

#[derive(Clone, Copy, Default)]
struct TransportSnapshot {
    connection_lost_packets: u64,
    connection_lost_bytes: u64,
    udp_rx_datagrams: u64,
    udp_tx_datagrams: u64,
    congestion_events: u64,
    black_holes_detected: u64,
    current_mtu: u16,
    rtt: Duration,
}

impl TransportSnapshot {
    fn capture(connection: &Connection) -> Self {
        let stats = connection.stats();
        let selected: Option<PathStats> = connection
            .paths()
            .iter()
            .find(iroh::endpoint::Path::is_selected)
            .map(|path| path.stats());
        Self {
            connection_lost_packets: stats.lost_packets,
            connection_lost_bytes: stats.lost_bytes,
            udp_rx_datagrams: stats.udp_rx.datagrams,
            udp_tx_datagrams: stats.udp_tx.datagrams,
            congestion_events: selected.map_or(0, |path| path.congestion_events),
            black_holes_detected: selected.map_or(0, |path| path.black_holes_detected),
            current_mtu: selected.map_or(0, |path| path.current_mtu),
            rtt: selected.map_or(Duration::ZERO, |path| path.rtt),
        }
    }

    fn delta(self, before: Self) -> TransportReport {
        TransportReport {
            lost_packets: self
                .connection_lost_packets
                .saturating_sub(before.connection_lost_packets),
            lost_bytes: self
                .connection_lost_bytes
                .saturating_sub(before.connection_lost_bytes),
            congestion_events: self
                .congestion_events
                .saturating_sub(before.congestion_events),
            udp_rx_datagrams: self
                .udp_rx_datagrams
                .saturating_sub(before.udp_rx_datagrams),
            udp_tx_datagrams: self
                .udp_tx_datagrams
                .saturating_sub(before.udp_tx_datagrams),
            current_mtu: self.current_mtu,
            black_holes_detected: self
                .black_holes_detected
                .saturating_sub(before.black_holes_detected),
            final_rtt: self.rtt,
        }
    }
}

fn duration_ms(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}
