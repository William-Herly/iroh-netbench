//! Server-side `NetBench` protocol handler.

use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use bytes::Bytes;
use iroh::{
    endpoint::{Connection, RecvStream},
    protocol::{AcceptError, ProtocolHandler},
};
use tokio::{
    sync::Semaphore,
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};

use crate::{
    Error, PROTOCOL_VERSION, Result, SessionMonitor,
    wire::{
        Capabilities, ControlMessage, ErrorCode, Probe, ProbeKind, ServerLimits, read_control,
        write_control,
    },
};

const DEFAULT_MAX_CHUNK_SIZE: u32 = 1024 * 1024;
const DOWNLOAD_STREAM_MAGIC: u8 = 0x44;
const UPLOAD_STREAM_MAGIC: u8 = 0x55;

type ConnectionObserver = Arc<dyn Fn(Connection) + Send + Sync>;

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
    permits: Arc<Semaphore>,
    connection_observer: Option<ConnectionObserver>,
}

impl std::fmt::Debug for NetBenchProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchProtocol")
            .field("max_concurrent_tests", &self.max_concurrent_tests)
            .field("max_test_duration", &self.max_test_duration)
            .field("max_parallel_streams", &self.max_parallel_streams)
            .field("max_chunk_size", &self.max_chunk_size)
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

    fn limits(&self) -> ServerLimits {
        ServerLimits {
            max_test_duration_ms: duration_ms(self.max_test_duration),
            max_parallel_streams: self.max_parallel_streams,
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

        let echo_task = tokio::spawn(echo_datagrams(connection.clone()));

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
                    let duration = self.validate_throughput(duration_ms, streams, chunk_size)?;
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
                    write_control(
                        &mut control_send,
                        &ControlMessage::TestFinished {
                            test_id,
                            received_bytes: bytes,
                            duration_ns: duration_ns(elapsed),
                        },
                    )
                    .await
                }
                ControlMessage::StartUpload {
                    test_id,
                    duration_ms,
                    streams,
                    chunk_size,
                } => {
                    let _duration = self.validate_throughput(duration_ms, streams, chunk_size)?;
                    let upload_streams = accept_upload_streams(connection.clone(), streams).await?;
                    write_control(&mut control_send, &ControlMessage::TestReady { test_id })
                        .await?;
                    let (bytes, elapsed) =
                        receive_upload(upload_streams, &mut control_send, test_id).await?;
                    write_control(
                        &mut control_send,
                        &ControlMessage::TestFinished {
                            test_id,
                            received_bytes: bytes,
                            duration_ns: duration_ns(elapsed),
                        },
                    )
                    .await
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
                let _ = write_control(
                    &mut control_send,
                    &ControlMessage::Error {
                        code: ErrorCode::InvalidRequest,
                        message: error.to_string(),
                    },
                )
                .await;
                echo_task.abort();
                return Err(error);
            }
        }

        echo_task.abort();
        Ok(())
    }
}

impl ProtocolHandler for NetBenchProtocol {
    async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
        if let Some(observer) = &self.connection_observer {
            observer(connection.clone());
        }
        self.handle(connection)
            .await
            .map_err(|error| AcceptError::from_err(std::io::Error::other(error.to_string())))
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
    connection_observer: Option<ConnectionObserver>,
}

impl std::fmt::Debug for NetBenchProtocolBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchProtocolBuilder")
            .field("max_concurrent_tests", &self.max_concurrent_tests)
            .field("max_test_duration", &self.max_test_duration)
            .field("max_parallel_streams", &self.max_parallel_streams)
            .field("max_chunk_size", &self.max_chunk_size)
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
            permits: Arc::new(Semaphore::new(self.max_concurrent_tests)),
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

async fn send_download(
    streams: Vec<iroh::endpoint::SendStream>,
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
                match tokio::time::timeout_at(deadline, stream.write_all(&chunk)).await {
                    Ok(result) => result.map_err(Error::network)?,
                    Err(_) => break,
                }
                total.fetch_add(chunk.len() as u64, Ordering::Relaxed);
            }
            stream.finish().map_err(Error::network)
        });
    }

    while let Some(result) = tasks.join_next().await {
        result.map_err(Error::network)??;
    }
    Ok((total.load(Ordering::Relaxed), started.elapsed()))
}

async fn open_download_streams(
    connection: Connection,
    streams: u16,
) -> Result<Vec<iroh::endpoint::SendStream>> {
    let mut opened = Vec::with_capacity(usize::from(streams));
    for _ in 0..streams {
        let mut stream = connection.open_uni().await.map_err(Error::network)?;
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
    control_send: &mut iroh::endpoint::SendStream,
    test_id: u64,
) -> Result<(u64, Duration)> {
    let total = Arc::new(AtomicU64::new(0));
    let mut tasks = JoinSet::new();

    let started = Instant::now();
    for mut stream in streams {
        let total = Arc::clone(&total);
        tasks.spawn(async move {
            let mut buffer = vec![0_u8; 64 * 1024];
            while let Some(read) = stream.read(&mut buffer).await.map_err(Error::network)? {
                total.fetch_add(read as u64, Ordering::Relaxed);
            }
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
                write_control(
                    control_send,
                    &ControlMessage::ThroughputProgress {
                        test_id,
                        received_bytes: total.load(Ordering::Relaxed),
                        duration_ns: duration_ns(started.elapsed()),
                    },
                )
                .await?;
            }
        }
    }
    Ok((total.load(Ordering::Relaxed), started.elapsed()))
}

fn duration_ms(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}
