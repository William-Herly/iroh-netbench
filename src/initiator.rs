//! Initiator-side benchmark lifecycle.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::mpsc,
    task::{AbortHandle, JoinHandle, JoinSet},
    time::{Instant, MissedTickBehavior},
};

use crate::{
    Error, FlowInfo, FlowStage, LatencySample, LoadedLatencyReport, LossReport, LossSample,
    MeasurementPathReport, NetBenchConfig, NetBenchEvent, NetBenchFlow, NetBenchProbeConfig,
    NetBenchProbeReport, NetBenchReceiveStream, NetBenchReport, NetBenchSendStream,
    NetBenchSession, NetBenchTelemetry, PROTOCOL_VERSION, PathKind, Result, SCHEMA_VERSION,
    ThroughputDirection, ThroughputReport, ThroughputSample, TransportReport,
    config::{LOADED_LATENCY_INTERVAL, MAX_PROBE_RATE_PER_SECOND, THROUGHPUT_SAMPLE_INTERVAL},
    statistics::latency_report,
    wire::{
        Capabilities, ControlMessage, PROBE_MAGIC, Probe, ProbeKind, read_control, write_control,
    },
};

const THROUGHPUT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
type SendStream = Box<dyn NetBenchSendStream>;
type RecvStream = Box<dyn NetBenchReceiveStream>;

#[derive(Clone)]
struct Connection(Arc<dyn NetBenchSession>);

impl Connection {
    fn remote_id(&self) -> String {
        self.0.remote_peer_id()
    }

    fn telemetry(&self) -> NetBenchTelemetry {
        self.0.telemetry()
    }

    fn max_datagram_size(&self) -> Option<usize> {
        self.0.max_datagram_size()
    }

    async fn open_uni(&self) -> Result<SendStream> {
        Ok(self.0.open_bi().await?.into_split().0)
    }

    async fn accept_uni(&self) -> Result<RecvStream> {
        Ok(self.0.accept_bi().await?.into_split().1)
    }

    async fn send_datagram_wait(&self, bytes: Vec<u8>) -> Result<()> {
        self.0.send_datagram(bytes).await
    }

    async fn read_datagram(&self) -> Result<Vec<u8>> {
        self.0.read_datagram().await
    }
}

/// Starts benchmark business flows on a caller-owned authenticated session.
#[derive(Clone)]
pub struct NetBenchInitiator;

impl std::fmt::Debug for NetBenchInitiator {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.debug_struct("NetBenchInitiator").finish()
    }
}

impl Default for NetBenchInitiator {
    fn default() -> Self {
        Self::new()
    }
}

impl NetBenchInitiator {
    /// Creates an initiator which never dials or owns an endpoint/connection lifecycle.
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Runs a benchmark and returns only its final result.
    ///
    /// # Errors
    ///
    /// Returns a negotiation, measurement, timeout, cancellation, or peer error.
    pub async fn run(&self, flow: NetBenchFlow, config: NetBenchConfig) -> Result<NetBenchReport> {
        self.start(flow, config).await?.result().await
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
        flow: NetBenchFlow,
        config: NetBenchProbeConfig,
    ) -> Result<NetBenchProbeReport> {
        let (event_tx, _event_rx) = mpsc::channel(128);
        let overall_timeout = config.overall_timeout;
        tokio::time::timeout(
            overall_timeout,
            run_probe_benchmark(flow, config, &EventSink(event_tx)),
        )
        .await
        .map_err(|_| Error::Timeout {
            stage: "probe benchmark",
        })?
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
    pub async fn start(&self, flow: NetBenchFlow, config: NetBenchConfig) -> Result<NetBenchTest> {
        let (event_tx, event_rx) = mpsc::channel(128);
        let events = EventSink(event_tx);
        let overall_timeout = config.overall_timeout;
        let stage = BenchmarkStage::default();
        let task_stage = stage.clone();
        let task = tokio::spawn(async move {
            let result = if let Ok(result) = tokio::time::timeout(
                overall_timeout,
                run_benchmark(flow, config, &events, &task_stage),
            )
            .await
            {
                result
            } else {
                Err(Error::Timeout {
                    stage: task_stage.current(),
                })
            };
            if let Ok(report) = &result {
                events.send(NetBenchEvent::Finished(report.clone()));
            }
            result
        });
        let abort = task.abort_handle();

        Ok(NetBenchTest {
            events: event_rx,
            task: Some(task),
            abort,
        })
    }
}

/// A running benchmark.
#[derive(Debug)]
pub struct NetBenchTest {
    events: mpsc::Receiver<NetBenchEvent>,
    task: Option<JoinHandle<Result<NetBenchReport>>>,
    abort: AbortHandle,
}

impl NetBenchTest {
    /// Waits for the next progress event.
    pub async fn next(&mut self) -> Option<NetBenchEvent> {
        self.events.recv().await
    }

    /// Cancels the benchmark task without closing the caller-owned connection.
    ///
    /// Call [`Self::abort_and_wait`] when the caller must prove that the internal task has exited.
    pub fn cancel(&self) {
        self.abort.abort();
    }

    /// Returns whether the internal benchmark task has finished.
    #[must_use]
    pub fn is_finished(&self) -> bool {
        self.task.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Cancels the benchmark and waits until its internal task has exited.
    ///
    /// # Errors
    ///
    /// Returns an error if the task panicked while cancellation was racing with completion.
    pub async fn abort_and_wait(mut self) -> Result<()> {
        self.cancel();
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        match task.await {
            Ok(_) => Ok(()),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(Error::Protocol(format!(
                "benchmark task failed while being cancelled: {error}"
            ))),
        }
    }

    /// Waits for the final report.
    ///
    /// # Errors
    ///
    /// Returns the terminal error from the benchmark task.
    pub async fn result(mut self) -> Result<NetBenchReport> {
        let task = self
            .task
            .take()
            .ok_or_else(|| Error::Protocol("benchmark result was already consumed".to_owned()))?;
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Err(Error::Cancelled),
            Err(error) => Err(Error::Protocol(format!(
                "benchmark task ended without a result: {error}"
            ))),
        }
    }
}

impl Drop for NetBenchTest {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[derive(Clone, Debug)]
struct BenchmarkStage(Arc<Mutex<&'static str>>);

impl Default for BenchmarkStage {
    fn default() -> Self {
        Self(Arc::new(Mutex::new("flow setup")))
    }
}

impl BenchmarkStage {
    fn set(&self, stage: &'static str) {
        *lock_unpoisoned(&self.0) = stage;
    }

    fn current(&self) -> &'static str {
        *lock_unpoisoned(&self.0)
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    flow: NetBenchFlow,
    config: NetBenchConfig,
    events: &EventSink,
    stage: &BenchmarkStage,
) -> Result<NetBenchReport> {
    let (session, mut control_send, mut control_recv) = flow.into_parts();
    let connection = Connection(session);
    let total_started = Instant::now();
    let connect_started = Instant::now();
    let peer_id = connection.remote_id();

    stage.set("protocol negotiation");
    events.send(NetBenchEvent::FlowStage(FlowStage::ControlStreamReady));
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
    events.send(NetBenchEvent::FlowStage(FlowStage::ClientHelloSent));

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
    events.send(NetBenchEvent::FlowStage(FlowStage::ServerHelloReceived {
        version: selected_version,
    }));
    if !capabilities.datagram_probes {
        return finish_with_error(
            &mut control_send,
            &mut control_recv,
            Error::Protocol("peer does not support Datagram probes".to_owned()),
        )
        .await;
    }
    if let Err(error) = validate_against_server_limits(&config, limits) {
        return finish_with_error(&mut control_send, &mut control_recv, error).await;
    }

    stage.set("path stabilization");
    let path_at_flow_start = selected_path_kind(&connection);
    let (initial_path, stabilization_time_to_direct) = stabilize_path(
        &connection,
        config.path_stabilization_timeout,
        connect_started,
        events,
    )
    .await;
    events.send(NetBenchEvent::Ready(FlowInfo {
        negotiation_time: first_control_message_time,
        path: initial_path,
    }));
    let (path_monitor, path_monitor_state) =
        spawn_path_monitor(connection.clone(), events.clone(), connect_started);

    let before = TransportSnapshot::capture(&connection);
    let mut test_id = 1_u64;

    stage.set("idle latency");
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

    stage.set("loss probes");
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
        stage.set("download warm-up");
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

    stage.set("download throughput");
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
        LOADED_LATENCY_INTERVAL,
        config.probe_timeout,
        ProbeEventMode::Latency(events.clone()),
    );
    let (download, download_loaded) = tokio::try_join!(download_future, download_probe_future)?;
    test_id += 1;

    if !config.upload_warmup.is_zero() {
        stage.set("upload warm-up");
        events.send(NetBenchEvent::UploadWarmupStarted);
        if !matches!(
            selected_path_kind(&connection),
            PathKind::Direct | PathKind::DirectIpv4 | PathKind::DirectIpv6
        ) {
            let _ = stabilize_path(
                &connection,
                config.path_stabilization_timeout,
                connect_started,
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

    stage.set("upload throughput");
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
        LOADED_LATENCY_INTERVAL,
        config.probe_timeout,
        ProbeEventMode::Latency(events.clone()),
    );
    let (upload, upload_loaded) = tokio::try_join!(upload_future, upload_probe_future)?;

    stage.set("finalization");
    let after = TransportSnapshot::capture(&connection);
    path_monitor.abort_and_wait().await;
    let final_path = selected_path_kind(&connection);
    let path_monitor_state = lock_unpoisoned(&path_monitor_state).clone();
    let time_to_direct = if path_at_flow_start == PathKind::Relay {
        stabilization_time_to_direct.or(path_monitor_state.time_to_direct)
    } else {
        None
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

    finish_flow(&mut control_send, &mut control_recv).await?;

    Ok(NetBenchReport {
        schema_version: SCHEMA_VERSION,
        protocol_version: selected_version,
        peer_id,
        total_duration: total_started.elapsed(),
        path: MeasurementPathReport {
            negotiation_time: first_control_message_time,
            initial_path,
            final_path,
            path_changed: path_at_flow_start != initial_path
                || initial_path != final_path
                || path_monitor_state.path_changed,
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
    flow: NetBenchFlow,
    config: NetBenchProbeConfig,
    events: &EventSink,
) -> Result<NetBenchProbeReport> {
    let (session, mut control_send, mut control_recv) = flow.into_parts();
    let connection = Connection(session);
    let total_started = Instant::now();
    let connect_started = Instant::now();
    let peer_id = connection.remote_id();

    events.send(NetBenchEvent::FlowStage(FlowStage::ControlStreamReady));
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
    events.send(NetBenchEvent::FlowStage(FlowStage::ClientHelloSent));

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
    events.send(NetBenchEvent::FlowStage(FlowStage::ServerHelloReceived {
        version: selected_version,
    }));
    if !capabilities.datagram_probes {
        return finish_with_error(
            &mut control_send,
            &mut control_recv,
            Error::Protocol("peer does not support Datagram probes".to_owned()),
        )
        .await;
    }
    if let Err(error) = validate_probes_against_server_limits(&config, limits) {
        return finish_with_error(&mut control_send, &mut control_recv, error).await;
    }

    let path_at_flow_start = selected_path_kind(&connection);
    let (initial_path, stabilization_time_to_direct) = stabilize_path(
        &connection,
        config.path_stabilization_timeout,
        connect_started,
        events,
    )
    .await;
    events.send(NetBenchEvent::Ready(FlowInfo {
        negotiation_time: first_control_message_time,
        path: initial_path,
    }));
    let (path_monitor, path_monitor_state) =
        spawn_path_monitor(connection.clone(), events.clone(), connect_started);
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
    path_monitor.abort_and_wait().await;
    let final_path = selected_path_kind(&connection);
    let path_monitor_state = lock_unpoisoned(&path_monitor_state).clone();
    let time_to_direct = if path_at_flow_start == PathKind::Relay {
        stabilization_time_to_direct.or(path_monitor_state.time_to_direct)
    } else {
        None
    };
    finish_flow(&mut control_send, &mut control_recv).await?;

    Ok(NetBenchProbeReport {
        schema_version: SCHEMA_VERSION,
        protocol_version: selected_version,
        peer_id,
        total_duration: total_started.elapsed(),
        path: MeasurementPathReport {
            negotiation_time: first_control_message_time,
            initial_path,
            final_path,
            path_changed: path_at_flow_start != initial_path
                || initial_path != final_path
                || path_monitor_state.path_changed,
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
    validate_probe_parameters(
        config.latency_duration,
        config.latency_interval,
        config.loss_duration,
        config.loss_rate_per_second,
    )?;
    if limits.parallel_streams == 0 {
        return Err(Error::ThroughputDeniedByPeer);
    }
    if config.parallel_streams == 0 {
        return Err(Error::Protocol(
            "parallel stream count must be greater than zero".to_owned(),
        ));
    }
    if config.chunk_size == 0 {
        return Err(Error::Protocol(
            "throughput chunk size must be greater than zero".to_owned(),
        ));
    }
    if duration_ms(config.download_duration) == 0 || duration_ms(config.upload_duration) == 0 {
        return Err(Error::Protocol(
            "throughput durations must be at least one millisecond".to_owned(),
        ));
    }
    let maximum = Duration::from_millis(u64::from(limits.test_duration_ms));
    for duration in [
        config.latency_duration,
        config.loss_duration,
        config.download_duration,
        config.upload_duration,
        config.download_warmup,
        config.upload_warmup,
    ] {
        if !duration.is_zero() && duration_ms(duration) == 0 {
            return Err(Error::Protocol(
                "phase durations must be zero or at least one millisecond".to_owned(),
            ));
        }
        if duration > maximum {
            return Err(Error::DurationLimitExceeded {
                requested: duration,
                maximum,
            });
        }
    }
    if config.parallel_streams > limits.parallel_streams {
        return Err(Error::Protocol(format!(
            "requested {} streams but server allows {}",
            config.parallel_streams, limits.parallel_streams
        )));
    }
    if config.chunk_size > limits.chunk_size {
        return Err(Error::Protocol(format!(
            "requested {} byte chunks but server allows {}",
            config.chunk_size, limits.chunk_size
        )));
    }
    Ok(())
}

fn validate_probes_against_server_limits(
    config: &NetBenchProbeConfig,
    limits: crate::wire::ServerLimits,
) -> Result<()> {
    validate_probe_parameters(
        config.latency_duration,
        config.latency_interval,
        config.loss_duration,
        config.loss_rate_per_second,
    )?;
    let maximum = Duration::from_millis(u64::from(limits.test_duration_ms));
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

fn validate_probe_parameters(
    latency_duration: Duration,
    latency_interval: Duration,
    loss_duration: Duration,
    loss_rate_per_second: u32,
) -> Result<()> {
    if duration_ms(latency_duration) == 0 || duration_ms(loss_duration) == 0 {
        return Err(Error::Protocol(
            "probe durations must be at least one millisecond".to_owned(),
        ));
    }
    if duration_ms(latency_interval) == 0 {
        return Err(Error::Protocol(
            "latency probe interval must be at least one millisecond".to_owned(),
        ));
    }
    if !(1..=MAX_PROBE_RATE_PER_SECOND).contains(&loss_rate_per_second) {
        return Err(Error::Protocol(format!(
            "loss probe rate must be within 1..={MAX_PROBE_RATE_PER_SECOND} per second"
        )));
    }
    Ok(())
}

async fn finish_with_error<T>(
    send: &mut SendStream,
    recv: &mut RecvStream,
    error: Error,
) -> Result<T> {
    finish_flow(send, recv).await?;
    Err(error)
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
            events.send(NetBenchEvent::FlowStage(FlowStage::PathObserved {
                path: kind,
            }));
            previous = Some(kind);
        }
        if matches!(
            kind,
            PathKind::Direct | PathKind::DirectIpv4 | PathKind::DirectIpv6
        ) {
            let elapsed = connect_started.elapsed();
            events.send(NetBenchEvent::FlowStage(FlowStage::DirectPathSelected {
                elapsed,
            }));
            return (kind, Some(elapsed));
        }
        if Instant::now() >= deadline {
            if kind == PathKind::Relay {
                events.send(NetBenchEvent::FlowStage(FlowStage::RelayRetained {
                    waited: timeout,
                }));
            }
            return (kind, None);
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
                match tokio::time::timeout_at(
                    send_deadline,
                    connection.send_datagram_wait(payload),
                )
                .await
                {
                    Ok(result) => result.map_err(Error::network)?,
                    Err(_) => break,
                }
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
                            outstanding: sequence.saturating_sub(received.len() as u64),
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
        let mut stream = connection.accept_uni().await?;
        let mut magic = [0_u8; 1];
        stream.read_exact(&mut magic).await?;
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
    let deadline = started + duration;
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
            while Instant::now() < deadline {
                match tokio::time::timeout_at(deadline, stream.read(&mut buffer)).await {
                    Ok(Ok(read)) if read > 0 && Instant::now() <= deadline => {
                        total.fetch_add(read as u64, Ordering::Relaxed);
                    }
                    Ok(Ok(_)) | Err(_) => break,
                    Ok(Err(error)) => return Err(error),
                }
            }
            stream.cancel();
            Result::<()>::Ok(())
        });
    }
    while let Some(result) = tasks.join_next().await {
        result.map_err(Error::network)??;
    }
    tokio::time::sleep_until(deadline).await;
    sample_task.abort_and_wait().await;
    let bytes = total.load(Ordering::Relaxed);
    tracing::debug!(
        test_id,
        direction = "download",
        cleanup_timeout = ?THROUGHPUT_CLEANUP_TIMEOUT,
        "waiting for throughput completion on the prioritized control stream"
    );
    tokio::time::timeout(
        THROUGHPUT_CLEANUP_TIMEOUT,
        expect_finished(control_recv, test_id),
    )
    .await
    .map_err(|_| Error::Timeout {
        stage: "download cleanup",
    })??;
    Ok(throughput_report(bytes, duration, streams))
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
        let mut stream = connection.open_uni().await?;
        stream.write_all(&[0x55]).await?;
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
                match tokio::time::timeout_at(deadline, stream.write(&chunk)).await {
                    Ok(Ok(_)) => {}
                    Ok(Err(Error::FlowStopped)) | Err(_) => break,
                    Ok(Err(error)) => return Err(error),
                }
            }
            stream.cancel();
            Result::<()>::Ok(())
        });
    }
    let mut progress = VecDeque::<(Duration, u64)>::from([(Duration::ZERO, 0)]);
    let cleanup_deadline = deadline + THROUGHPUT_CLEANUP_TIMEOUT;
    tracing::debug!(
        test_id,
        direction = "upload",
        cleanup_timeout = ?THROUGHPUT_CLEANUP_TIMEOUT,
        "waiting for throughput completion on the prioritized control stream"
    );
    let received_bytes = loop {
        let message = tokio::time::timeout_at(cleanup_deadline, read_control(control_recv))
            .await
            .map_err(|_| Error::Timeout {
                stage: "upload cleanup",
            })??;
        match message {
            ControlMessage::ThroughputProgress {
                test_id: received_test_id,
                received_bytes,
                duration_ns,
            } if received_test_id == test_id => {
                let receiver_elapsed = Duration::from_nanos(duration_ns).min(duration);
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
                ..
            } if received_test_id == test_id => break received_bytes,
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
    };
    tokio::time::timeout_at(cleanup_deadline, async {
        while let Some(result) = tasks.join_next().await {
            result.map_err(Error::network)??;
        }
        Result::<()>::Ok(())
    })
    .await
    .map_err(|_| Error::Timeout {
        stage: "upload stream cleanup",
    })??;
    Ok(throughput_report(received_bytes, duration, streams))
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
) -> AbortOnDropTask<()> {
    AbortOnDropTask::new(tokio::spawn(async move {
        let Some(events) = events else {
            return;
        };
        let mut samples = VecDeque::<(Instant, u64)>::from([(started, 0)]);
        let mut ticker = tokio::time::interval(THROUGHPUT_SAMPLE_INTERVAL);
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
    }))
}

#[derive(Clone, Debug, Default)]
struct PathMonitorState {
    path_changed: bool,
    time_to_direct: Option<Duration>,
}

fn spawn_path_monitor(
    connection: Connection,
    events: EventSink,
    flow_started: Instant,
) -> (AbortOnDropTask<()>, Arc<Mutex<PathMonitorState>>) {
    let state = Arc::new(Mutex::new(PathMonitorState::default()));
    let task_state = Arc::clone(&state);
    let task = AbortOnDropTask::new(tokio::spawn(async move {
        let mut previous = selected_path_kind(&connection);
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            ticker.tick().await;
            let current = selected_path_kind(&connection);
            if current != previous {
                let mut state = lock_unpoisoned(&task_state);
                state.path_changed = true;
                if previous == PathKind::Relay
                    && matches!(
                        current,
                        PathKind::Direct | PathKind::DirectIpv4 | PathKind::DirectIpv6
                    )
                    && state.time_to_direct.is_none()
                {
                    state.time_to_direct = Some(flow_started.elapsed());
                }
                drop(state);
                events.send(NetBenchEvent::FlowStage(FlowStage::PathObserved {
                    path: current,
                }));
                previous = current;
            }
        }
    }));
    (task, state)
}

#[derive(Debug)]
struct AbortOnDropTask<T>(Option<JoinHandle<T>>);

impl<T> AbortOnDropTask<T> {
    fn new(task: JoinHandle<T>) -> Self {
        Self(Some(task))
    }

    async fn abort_and_wait(mut self) {
        if let Some(task) = self.0.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl<T> Drop for AbortOnDropTask<T> {
    fn drop(&mut self) {
        if let Some(task) = &self.0 {
            task.abort();
        }
    }
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

async fn finish_flow(send: &mut SendStream, recv: &mut RecvStream) -> Result<()> {
    write_control(send, &ControlMessage::FlowFinished).await?;
    match read_control(recv).await? {
        ControlMessage::FlowFinishedAck => {
            send.finish().map_err(Error::network)?;
            Ok(())
        }
        ControlMessage::Error { code, message } => Err(Error::Peer {
            code: code as u16,
            message,
        }),
        other => Err(Error::Protocol(format!(
            "expected FlowFinishedAck, received {other:?}"
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
    connection.telemetry().path
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
        let telemetry = connection.telemetry();
        Self {
            connection_lost_packets: telemetry.lost_packets,
            connection_lost_bytes: telemetry.lost_bytes,
            udp_rx_datagrams: telemetry.rx_datagrams,
            udp_tx_datagrams: telemetry.tx_datagrams,
            congestion_events: telemetry.congestion_events,
            black_holes_detected: telemetry.black_holes_detected,
            current_mtu: telemetry.current_mtu,
            rtt: telemetry.rtt,
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
