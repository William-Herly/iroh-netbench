//! Server-side `NetBench` protocol handler.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use iroh::{
    EndpointId,
    endpoint::{Connection, ReadError, RecvStream, SendStream, VarInt, WriteError},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    Error, PROTOCOL_VERSION, Result, SessionMonitor,
    scheduling::{
        THROUGHPUT_CLEANUP_TIMEOUT, deprioritize_throughput_stream, prioritize_control_stream,
    },
    wire::{
        Capabilities, ControlMessage, ErrorCode, Probe, ProbeKind, ServerLimits, read_control,
        write_control,
    },
};

const DEFAULT_MAX_CHUNK_SIZE: u32 = 1024 * 1024;
const DOWNLOAD_STREAM_MAGIC: u8 = 0x44;
const UPLOAD_STREAM_MAGIC: u8 = 0x55;
const THROUGHPUT_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
// QUIC stream application code used only to mark a normal measurement-window cutoff.
const MEASUREMENT_COMPLETE_CODE: u32 = 0x4E42;

type ConnectionObserver = Arc<dyn Fn(Connection) + Send + Sync>;

/// Receiver-side policy for bandwidth-saturating upload and download measurements.
///
/// This policy does not affect connectivity checks or low-bandwidth latency and loss probes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ThroughputPolicy {
    /// Accept throughput measurements, subject to the configured server limits.
    #[default]
    Allow,
    /// Reject throughput measurements while continuing to serve low-bandwidth probes.
    Deny,
}

/// Server-side benchmark protocol configuration.
#[allow(
    clippy::struct_field_names,
    reason = "max_* names mirror the public builder API and server policy"
)]
#[derive(Clone)]
pub struct NetBenchProtocol {
    max_concurrent_tests: usize,
    max_test_duration: Duration,
    max_parallel_streams: u16,
    max_chunk_size: u32,
    throughput_allowed: Arc<AtomicBool>,
    permits: Arc<Semaphore>,
    allowed_peer: Option<EndpointId>,
    connection_observer: Option<ConnectionObserver>,
}

impl std::fmt::Debug for NetBenchProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchProtocol")
            .field("max_concurrent_tests", &self.max_concurrent_tests)
            .field("max_test_duration", &self.max_test_duration)
            .field("max_parallel_streams", &self.max_parallel_streams)
            .field("throughput_policy", &self.throughput_policy())
            .field("max_chunk_size", &self.max_chunk_size)
            .field("allowed_peer", &self.allowed_peer)
            .field(
                "has_connection_observer",
                &self.connection_observer.is_some(),
            )
            .finish_non_exhaustive()
    }
}

impl NetBenchProtocol {
    /// Starts configuring a protocol handler.
    #[must_use]
    pub fn builder() -> NetBenchProtocolBuilder {
        NetBenchProtocolBuilder::default()
    }

    /// Configured simultaneous-test limit.
    #[must_use]
    pub const fn max_concurrent_tests(&self) -> usize {
        self.max_concurrent_tests
    }

    /// Configured per-phase duration limit.
    #[must_use]
    pub const fn max_test_duration(&self) -> Duration {
        self.max_test_duration
    }

    /// Configured parallel-stream limit.
    #[must_use]
    pub const fn max_parallel_streams(&self) -> u16 {
        self.max_parallel_streams
    }

    /// Current receiver-side policy for upload and download throughput measurements.
    #[must_use]
    pub fn throughput_policy(&self) -> ThroughputPolicy {
        if self.throughput_allowed.load(Ordering::Relaxed) {
            ThroughputPolicy::Allow
        } else {
            ThroughputPolicy::Deny
        }
    }

    /// Changes the receiver-side throughput policy for future measurement phases.
    ///
    /// Existing low-bandwidth latency and loss probes remain available under either policy.
    pub fn set_throughput_policy(&self, policy: ThroughputPolicy) {
        self.throughput_allowed
            .store(policy == ThroughputPolicy::Allow, Ordering::Relaxed);
    }

    fn limits(&self) -> ServerLimits {
        ServerLimits {
            max_test_duration_ms: duration_ms(self.max_test_duration),
            // Version 1 clients already understand this field. Zero is used as the
            // backwards-compatible signal that no throughput streams are accepted.
            max_parallel_streams: if self.throughput_policy() == ThroughputPolicy::Allow {
                self.max_parallel_streams
            } else {
                0
            },
            max_chunk_size: self.max_chunk_size,
        }
    }

    fn validate_phase(&self, duration_ms: u32) -> Result<Duration> {
        let requested = Duration::from_millis(u64::from(duration_ms));
        if requested.is_zero() || requested > self.max_test_duration {
            return Err(Error::DurationLimitExceeded {
                requested,
                maximum: self.max_test_duration,
            });
        }
        Ok(requested)
    }

    fn validate_throughput(
        &self,
        duration_ms: u32,
        streams: u16,
        chunk_size: u32,
    ) -> Result<Duration> {
        if self.throughput_policy() == ThroughputPolicy::Deny || self.max_parallel_streams == 0 {
            return Err(Error::ThroughputDeniedByPeer);
        }
        let duration = self.validate_phase(duration_ms)?;
        if streams == 0 || streams > self.max_parallel_streams {
            return Err(Error::Protocol(format!(
                "stream count {streams} is outside 1..={}",
                self.max_parallel_streams
            )));
        }
        if chunk_size == 0 || chunk_size > self.max_chunk_size {
            return Err(Error::Protocol(format!(
                "chunk size {chunk_size} is outside 1..={}",
                self.max_chunk_size
            )));
        }
        Ok(duration)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "linear command dispatch is easier to audit"
    )]
    async fn handle(&self, connection: Connection) -> Result<()> {
        let permit = Arc::clone(&self.permits).try_acquire_owned();
        let (mut control_send, mut control_recv) =
            connection.accept_bi().await.map_err(Error::network)?;
        prioritize_control_stream(&control_send)?;

        let Ok(_permit) = permit else {
            write_control(
                &mut control_send,
                &ControlMessage::Error {
                    code: ErrorCode::Busy,
                    message: "server concurrency limit reached".to_owned(),
                },
            )
            .await?;
            control_send.finish().map_err(Error::network)?;
            return Ok(());
        };

        let hello = read_control(&mut control_recv).await?;
        let ControlMessage::ClientHello {
            protocol_versions,
            capabilities: _,
        } = hello
        else {
            return Err(Error::Protocol(
                "first control message must be ClientHello".to_owned(),
            ));
        };
        if !protocol_versions.contains(&PROTOCOL_VERSION) {
            write_control(
                &mut control_send,
                &ControlMessage::Error {
                    code: ErrorCode::UnsupportedVersion,
                    message: "no mutually supported protocol version".to_owned(),
                },
            )
            .await?;
            control_send.finish().map_err(Error::network)?;
            return Ok(());
        }

        write_control(
            &mut control_send,
            &ControlMessage::ServerHello {
                selected_version: PROTOCOL_VERSION,
                limits: self.limits(),
                capabilities: Capabilities {
                    datagram_probes: connection.max_datagram_size().is_some(),
                    loaded_latency: true,
                    path_stats: true,
                },
            },
        )
        .await?;

        let mut background_tasks = JoinSet::new();
        background_tasks.spawn(echo_datagrams(connection.clone()));

        loop {
            let message = match read_control(&mut control_recv).await {
                Ok(message) => message,
                Err(Error::Network(message))
                    if message.contains("closed") || message.contains("reset") =>
                {
                    break;
                }
                Err(error) => return Err(error),
            };

            let result = match message {
                ControlMessage::StartLatency { duration_ms, .. }
                | ControlMessage::StartLoss { duration_ms, .. } => {
                    self.validate_phase(duration_ms).map(|_| ())
                }
                ControlMessage::StopTest { .. } => Ok(()),
                ControlMessage::StartDownload {
                    test_id,
                    duration_ms,
                    streams,
                    chunk_size,
                } => {
                    let duration = match self.validate_throughput(duration_ms, streams, chunk_size)
                    {
                        Ok(duration) => duration,
                        Err(error) => {
                            send_error_best_effort(
                                &mut control_send,
                                ErrorCode::InvalidRequest,
                                error.to_string(),
                            )
                            .await;
                            return Err(error);
                        }
                    };
                    let phase_timeout = duration
                        .saturating_add(THROUGHPUT_SETUP_TIMEOUT)
                        .saturating_add(THROUGHPUT_CLEANUP_TIMEOUT);
                    tokio::time::timeout(phase_timeout, async {
                        let download_streams =
                            open_download_streams(connection.clone(), streams).await?;
                        write_control(&mut control_send, &ControlMessage::TestReady { test_id })
                            .await?;
                        expect_test_ready(&mut control_recv, test_id).await?;
                        let (bytes, elapsed) = send_download(
                            download_streams,
                            duration,
                            usize::try_from(chunk_size).map_err(Error::network)?,
                        )
                        .await?;
                        tracing::debug!(
                            test_id,
                            direction = "download",
                            received_bytes = bytes,
                            measurement_duration = ?elapsed,
                            "throughput data tasks completed; queueing completion on the prioritized control stream"
                        );
                        write_control(
                            &mut control_send,
                            &ControlMessage::TestFinished {
                                test_id,
                                received_bytes: bytes,
                                duration_ns: duration_ns(elapsed),
                            },
                        )
                        .await
                    })
                    .await
                    .map_err(|_| Error::Timeout {
                        stage: "download phase",
                    })?
                }
                ControlMessage::StartUpload {
                    test_id,
                    duration_ms,
                    streams,
                    chunk_size,
                } => {
                    let duration = match self.validate_throughput(duration_ms, streams, chunk_size)
                    {
                        Ok(duration) => duration,
                        Err(error) => {
                            send_error_best_effort(
                                &mut control_send,
                                ErrorCode::InvalidRequest,
                                error.to_string(),
                            )
                            .await;
                            return Err(error);
                        }
                    };
                    let phase_timeout = duration
                        .saturating_add(THROUGHPUT_SETUP_TIMEOUT)
                        .saturating_add(THROUGHPUT_CLEANUP_TIMEOUT);
                    tokio::time::timeout(phase_timeout, async {
                        let upload_streams =
                            accept_upload_streams(connection.clone(), streams).await?;
                        write_control(&mut control_send, &ControlMessage::TestReady { test_id })
                            .await?;
                        let (bytes, elapsed) =
                            receive_upload(upload_streams, &mut control_send, test_id, duration)
                                .await?;
                        tracing::debug!(
                            test_id,
                            direction = "upload",
                            received_bytes = bytes,
                            measurement_duration = ?elapsed,
                            "throughput data tasks completed; queueing completion on the prioritized control stream"
                        );
                        write_control(
                            &mut control_send,
                            &ControlMessage::TestFinished {
                                test_id,
                                received_bytes: bytes,
                                duration_ns: duration_ns(elapsed),
                            },
                        )
                        .await
                    })
                    .await
                    .map_err(|_| Error::Timeout {
                        stage: "upload phase",
                    })?
                }
                ControlMessage::ClientHello { .. }
                | ControlMessage::ServerHello { .. }
                | ControlMessage::TestReady { .. }
                | ControlMessage::ThroughputProgress { .. }
                | ControlMessage::TestFinished { .. }
                | ControlMessage::Error { .. } => Err(Error::Protocol(
                    "message is invalid in server command state".to_owned(),
                )),
            };

            if let Err(error) = result {
                send_error_best_effort(
                    &mut control_send,
                    ErrorCode::InvalidRequest,
                    error.to_string(),
                )
                .await;
                return Err(error);
            }
        }

        background_tasks.shutdown().await;
        Ok(())
    }
}

impl ProtocolHandler for NetBenchProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        if self
            .allowed_peer
            .is_some_and(|allowed| connection.remote_id() != allowed)
        {
            connection.close(0_u8.into(), b"netbench peer is not authorized");
            return Err(AcceptError::from_err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "netbench peer is not authorized",
            )));
        }
        if let Some(observer) = &self.connection_observer {
            observer(connection.clone());
        }
        let result = self
            .handle(connection.clone())
            .await
            .map_err(|error| AcceptError::from_err(std::io::Error::other(error.to_string())));
        connection.close(0_u8.into(), b"netbench server task ended");
        result
    }
}

impl Default for NetBenchProtocol {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Builder for [`NetBenchProtocol`].
#[allow(
    clippy::struct_field_names,
    reason = "max_* names mirror the public builder API and server policy"
)]
#[derive(Clone)]
pub struct NetBenchProtocolBuilder {
    max_concurrent_tests: usize,
    max_test_duration: Duration,
    max_parallel_streams: u16,
    max_chunk_size: u32,
    throughput_policy: ThroughputPolicy,
    allowed_peer: Option<EndpointId>,
    connection_observer: Option<ConnectionObserver>,
}

impl std::fmt::Debug for NetBenchProtocolBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchProtocolBuilder")
            .field("max_concurrent_tests", &self.max_concurrent_tests)
            .field("max_test_duration", &self.max_test_duration)
            .field("max_parallel_streams", &self.max_parallel_streams)
            .field("throughput_policy", &self.throughput_policy)
            .field("max_chunk_size", &self.max_chunk_size)
            .field("allowed_peer", &self.allowed_peer)
            .field(
                "has_connection_observer",
                &self.connection_observer.is_some(),
            )
            .finish()
    }
}

impl NetBenchProtocolBuilder {
    /// Sets the number of simultaneous benchmark connections.
    #[must_use]
    pub const fn max_concurrent_tests(mut self, value: usize) -> Self {
        self.max_concurrent_tests = value;
        self
    }

    /// Sets the maximum accepted duration for an individual phase.
    #[must_use]
    pub const fn max_test_duration(mut self, value: Duration) -> Self {
        self.max_test_duration = value;
        self
    }

    /// Sets the maximum stream count per throughput direction.
    #[must_use]
    pub const fn max_parallel_streams(mut self, value: u16) -> Self {
        self.max_parallel_streams = value;
        self
    }

    /// Sets whether peers may run bandwidth-saturating upload and download measurements.
    #[must_use]
    pub const fn throughput_policy(mut self, value: ThroughputPolicy) -> Self {
        self.throughput_policy = value;
        self
    }

    /// Restricts benchmark connections to one TLS-authenticated iroh endpoint identity.
    #[must_use]
    pub const fn allowed_peer(mut self, endpoint_id: EndpointId) -> Self {
        self.allowed_peer = Some(endpoint_id);
        self
    }

    /// Observes each accepted benchmark connection.
    #[must_use]
    pub fn on_connection(mut self, observer: impl Fn(Connection) + Send + Sync + 'static) -> Self {
        self.connection_observer = Some(Arc::new(observer));
        self
    }

    /// Registers every accepted benchmark connection with a reusable session monitor.
    #[must_use]
    pub fn with_monitor(self, monitor: SessionMonitor) -> Self {
        self.on_connection(move |connection| {
            monitor.track("netbench-server", connection);
        })
    }

    /// Builds the server policy.
    #[must_use]
    pub fn build(self) -> NetBenchProtocol {
        NetBenchProtocol {
            max_concurrent_tests: self.max_concurrent_tests,
            max_test_duration: self.max_test_duration,
            max_parallel_streams: self.max_parallel_streams,
            max_chunk_size: self.max_chunk_size,
            throughput_allowed: Arc::new(AtomicBool::new(matches!(
                self.throughput_policy,
                ThroughputPolicy::Allow
            ))),
            permits: Arc::new(Semaphore::new(self.max_concurrent_tests)),
            allowed_peer: self.allowed_peer,
            connection_observer: self.connection_observer,
        }
    }
}

impl Default for NetBenchProtocolBuilder {
    fn default() -> Self {
        Self {
            max_concurrent_tests: 2,
            max_test_duration: Duration::from_secs(30),
            max_parallel_streams: 8,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            throughput_policy: ThroughputPolicy::Allow,
            allowed_peer: None,
            connection_observer: None,
        }
    }
}

async fn echo_datagrams(connection: Connection) {
    while let Ok(bytes) = connection.read_datagram().await {
        let Ok(mut probe) = postcard::from_bytes::<Probe>(&bytes) else {
            continue;
        };
        if probe.kind != ProbeKind::Request {
            continue;
        }
        probe.kind = ProbeKind::Response;
        let Ok(payload) = postcard::to_allocvec(&probe) else {
            continue;
        };
        if connection
            .send_datagram_wait(Bytes::from(payload))
            .await
            .is_err()
        {
            break;
        }
    }
}

async fn send_error_best_effort(control_send: &mut SendStream, code: ErrorCode, message: String) {
    let _ = tokio::time::timeout(
        THROUGHPUT_CLEANUP_TIMEOUT,
        write_control(control_send, &ControlMessage::Error { code, message }),
    )
    .await;
}

async fn send_download(
    streams: Vec<SendStream>,
    duration: Duration,
    chunk_size: usize,
) -> Result<(u64, Duration)> {
    let total = Arc::new(AtomicU64::new(0));
    let started = Instant::now();
    let deadline = started + duration;
    let chunk = Arc::new(vec![0xA5; chunk_size]);
    let mut tasks = JoinSet::new();

    for mut stream in streams {
        let total = Arc::clone(&total);
        let chunk = Arc::clone(&chunk);
        tasks.spawn(async move {
            while Instant::now() < deadline {
                match tokio::time::timeout_at(deadline, stream.write(&chunk)).await {
                    Ok(Ok(written)) => {
                        total.fetch_add(written as u64, Ordering::Relaxed);
                    }
                    Ok(Err(WriteError::Stopped(code))) if code == measurement_complete_code() => {
                        break;
                    }
                    Ok(Err(error)) => return Err(Error::network(error)),
                    Err(_) => break,
                }
            }
            let _ = stream.reset(measurement_complete_code());
            Result::<()>::Ok(())
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.map_err(Error::network)??;
    }
    tokio::time::sleep_until(deadline).await;
    Ok((total.load(Ordering::Relaxed), duration))
}

async fn open_download_streams(
    connection: Connection,
    streams: u16,
) -> Result<Vec<iroh::endpoint::SendStream>> {
    let mut opened = Vec::with_capacity(usize::from(streams));
    for _ in 0..streams {
        let mut stream = connection.open_uni().await.map_err(Error::network)?;
        deprioritize_throughput_stream(&stream)?;
        stream
            .write_all(&[DOWNLOAD_STREAM_MAGIC])
            .await
            .map_err(Error::network)?;
        opened.push(stream);
    }
    Ok(opened)
}

async fn expect_test_ready(recv: &mut RecvStream, expected_test_id: u64) -> Result<()> {
    match read_control(recv).await? {
        ControlMessage::TestReady { test_id } if test_id == expected_test_id => Ok(()),
        ControlMessage::TestReady { test_id } => Err(Error::Protocol(format!(
            "received readiness for test {test_id}, expected {expected_test_id}"
        ))),
        message => Err(Error::Protocol(format!(
            "expected TestReady, received {message:?}"
        ))),
    }
}

async fn accept_upload_streams(connection: Connection, streams: u16) -> Result<Vec<RecvStream>> {
    let mut accepted = Vec::with_capacity(usize::from(streams));
    for _ in 0..streams {
        let mut stream = connection.accept_uni().await.map_err(Error::network)?;
        let mut magic = [0_u8; 1];
        stream
            .read_exact(&mut magic)
            .await
            .map_err(Error::network)?;
        if magic[0] != UPLOAD_STREAM_MAGIC {
            return Err(Error::Protocol(
                "upload stream has an invalid pre-measurement header".to_owned(),
            ));
        }
        accepted.push(stream);
    }
    Ok(accepted)
}

async fn receive_upload(
    streams: Vec<RecvStream>,
    control_send: &mut SendStream,
    test_id: u64,
    duration: Duration,
) -> Result<(u64, Duration)> {
    let total = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();

    let started = Instant::now();
    let deadline = started + duration;
    for mut stream in streams {
        let total = Arc::clone(&total);
        tasks.spawn(async move {
            let mut buffer = vec![0_u8; 64 * 1024];
            while Instant::now() < deadline {
                match tokio::time::timeout_at(deadline, stream.read(&mut buffer)).await {
                    Ok(Ok(Some(read))) if Instant::now() <= deadline => {
                        total.fetch_add(read as u64, Ordering::Relaxed);
                    }
                    Ok(Ok(Some(_) | None)) | Err(_) => break,
                    Ok(Err(ReadError::Reset(code))) if code == measurement_complete_code() => {
                        break;
                    }
                    Ok(Err(error)) => return Err(Error::network(error)),
                }
            }
            let _ = stream.stop(measurement_complete_code());
            Result::<()>::Ok(())
        });
    }
    let mut ticker = tokio::time::interval(Duration::from_millis(250));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;
    while !tasks.is_empty() {
        tokio::select! {
            result = tasks.join_next() => {
                if let Some(result) = result {
                    result.map_err(Error::network)??;
                }
            }
            _ = ticker.tick() => {
                tokio::time::timeout_at(
                    deadline + THROUGHPUT_CLEANUP_TIMEOUT,
                    write_control(
                        control_send,
                        &ControlMessage::ThroughputProgress {
                            test_id,
                            received_bytes: total.load(Ordering::Relaxed),
                            duration_ns: duration_ns(started.elapsed().min(duration)),
                        },
                    ),
                )
                .await
                .map_err(|_| Error::Timeout {
                    stage: "upload progress",
                })??;
            }
        }
    }
    tokio::time::sleep_until(deadline).await;
    Ok((total.load(Ordering::Relaxed), duration))
}

fn measurement_complete_code() -> VarInt {
    MEASUREMENT_COMPLETE_CODE.into()
}

fn duration_ms(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};

    const DOWNLOAD_DEADLINE_ALPN: &[u8] = b"/iroh/netbench/test/download-deadline";
    const UPLOAD_DEADLINE_ALPN: &[u8] = b"/iroh/netbench/test/upload-deadline";

    #[derive(Debug, Clone)]
    struct DownloadDeadlineProtocol {
        completed: tokio::sync::mpsc::UnboundedSender<Duration>,
    }

    impl ProtocolHandler for DownloadDeadlineProtocol {
        async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
            let stream = connection.open_uni().await?;
            let started = Instant::now();
            send_download(vec![stream], Duration::from_millis(100), 64 * 1024)
                .await
                .map_err(AcceptError::from_err)?;
            let _ = self.completed.send(started.elapsed());
            tokio::time::sleep(Duration::from_millis(500)).await;
            connection.close(0_u8.into(), b"download deadline test complete");
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct UploadDeadlineProtocol {
        completed: tokio::sync::mpsc::UnboundedSender<Duration>,
    }

    impl ProtocolHandler for UploadDeadlineProtocol {
        async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
            let (mut control_send, _control_recv) = connection.accept_bi().await?;
            let stream = connection.accept_uni().await?;
            let started = Instant::now();
            receive_upload(
                vec![stream],
                &mut control_send,
                1,
                Duration::from_millis(100),
            )
            .await
            .map_err(AcceptError::from_err)?;
            let _ = self.completed.send(started.elapsed());
            tokio::time::sleep(Duration::from_millis(500)).await;
            connection.close(0_u8.into(), b"upload deadline test complete");
            Ok(())
        }
    }

    #[test]
    fn denied_throughput_is_advertised_and_enforced_without_blocking_probes() {
        let protocol = NetBenchProtocol::builder()
            .throughput_policy(ThroughputPolicy::Deny)
            .build();

        assert_eq!(protocol.limits().max_parallel_streams, 0);
        assert!(protocol.validate_phase(1_000).is_ok());
        assert!(matches!(
            protocol.validate_throughput(1_000, 1, 1_024),
            Err(Error::ThroughputDeniedByPeer)
        ));
    }

    #[tokio::test]
    async fn download_deadline_resets_queued_data_instead_of_draining() {
        let server = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let (completed_tx, mut completed_rx) = tokio::sync::mpsc::unbounded_channel();
        let router = Router::builder(server.clone())
            .accept(
                DOWNLOAD_DEADLINE_ALPN,
                DownloadDeadlineProtocol {
                    completed: completed_tx,
                },
            )
            .spawn();
        let client = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let connection = client
            .connect(server.addr(), DOWNLOAD_DEADLINE_ALPN)
            .await
            .unwrap();
        let mut stream = connection.accept_uni().await.unwrap();

        let elapsed = tokio::time::timeout(Duration::from_secs(1), completed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(elapsed < Duration::from_millis(300));
        let mut buffer = vec![0_u8; 64 * 1024];
        let reset_code = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                match stream.read(&mut buffer).await {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("download ended with graceful EOF instead of reset"),
                    Err(ReadError::Reset(code)) => break code,
                    Err(error) => panic!("unexpected download reader error: {error}"),
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(reset_code, measurement_complete_code());

        connection.close(0_u8.into(), b"download deadline assertion complete");
        router.shutdown().await.unwrap();
        client.close().await;
    }

    #[tokio::test]
    async fn upload_deadline_stops_a_sender_that_never_finishes() {
        let server = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let (completed_tx, mut completed_rx) = tokio::sync::mpsc::unbounded_channel();
        let router = Router::builder(server.clone())
            .accept(
                UPLOAD_DEADLINE_ALPN,
                UploadDeadlineProtocol {
                    completed: completed_tx,
                },
            )
            .spawn();
        let client = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let connection = client
            .connect(server.addr(), UPLOAD_DEADLINE_ALPN)
            .await
            .unwrap();
        let (mut control_send, _control_recv) = connection.open_bi().await.unwrap();
        control_send.write_all(&[0]).await.unwrap();
        let mut stream = connection.open_uni().await.unwrap();
        let writer = tokio::spawn(async move {
            let chunk = vec![0x5A; 64 * 1024];
            loop {
                match stream.write(&chunk).await {
                    Ok(_) => {}
                    Err(WriteError::Stopped(code)) => break code,
                    Err(error) => panic!("unexpected upload writer error: {error}"),
                }
            }
        });

        let elapsed = tokio::time::timeout(Duration::from_secs(1), completed_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(elapsed < Duration::from_millis(300));
        let stop_code = tokio::time::timeout(Duration::from_secs(1), writer)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(stop_code, measurement_complete_code());

        connection.close(0_u8.into(), b"upload deadline assertion complete");
        router.shutdown().await.unwrap();
        client.close().await;
    }
}
