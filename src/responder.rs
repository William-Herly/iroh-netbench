//! Responder-side benchmark protocol handler.

use std::{
    collections::HashSet,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Duration,
};

use tokio::{
    sync::Semaphore,
    task::{JoinHandle, JoinSet},
    time::{Instant, MissedTickBehavior},
};

use crate::{
    Error, NetBenchFlow, NetBenchReceiveStream, NetBenchSendStream, NetBenchSession,
    PROTOCOL_VERSION, Result,
    config::{LOADED_LATENCY_INTERVAL, MAX_PROBE_RATE_PER_SECOND, THROUGHPUT_SAMPLE_INTERVAL},
    wire::{
        Capabilities, ControlMessage, ErrorCode, PROBE_MAGIC, Probe, ProbeKind, ServerLimits,
        read_control, write_control,
    },
};

const DEFAULT_MAX_CHUNK_SIZE: u32 = 1024 * 1024;
const DOWNLOAD_STREAM_MAGIC: u8 = 0x44;
const UPLOAD_STREAM_MAGIC: u8 = 0x55;
const THROUGHPUT_SETUP_TIMEOUT: Duration = Duration::from_secs(5);
const THROUGHPUT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);
type SendStream = Box<dyn NetBenchSendStream>;
type RecvStream = Box<dyn NetBenchReceiveStream>;

struct ProbeEchoTask {
    test_id: u64,
    task: Option<JoinHandle<Result<()>>>,
}

impl ProbeEchoTask {
    fn spawn(
        session: Arc<dyn NetBenchSession>,
        test_id: u64,
        lifetime: Duration,
        max_requests: u64,
    ) -> Self {
        Self {
            test_id,
            task: Some(tokio::spawn(echo_datagrams(
                session,
                test_id,
                lifetime,
                max_requests,
            ))),
        }
    }

    async fn stop(mut self) -> Result<()> {
        let Some(task) = self.task.take() else {
            return Ok(());
        };
        if !task.is_finished() {
            task.abort();
        }
        match task.await {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(Error::Protocol(format!(
                "probe echo task ended unexpectedly: {error}"
            ))),
        }
    }
}

impl Drop for ProbeEchoTask {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Clone)]
struct Connection(Arc<dyn NetBenchSession>);

impl Connection {
    fn max_datagram_size(&self) -> Option<usize> {
        self.0.max_datagram_size()
    }

    async fn open_uni(&self) -> Result<SendStream> {
        Ok(self.0.open_bi().await?.into_split().0)
    }

    async fn accept_uni(&self) -> Result<RecvStream> {
        Ok(self.0.accept_bi().await?.into_split().1)
    }
}

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
#[derive(Clone)]
pub struct NetBenchResponder {
    concurrent_tests: usize,
    test_duration: Duration,
    parallel_streams: u16,
    chunk_size: u32,
    throughput_allowed: Arc<AtomicBool>,
    permits: Arc<Semaphore>,
}

impl std::fmt::Debug for NetBenchResponder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchResponder")
            .field("max_concurrent_tests", &self.concurrent_tests)
            .field("max_test_duration", &self.test_duration)
            .field("max_parallel_streams", &self.parallel_streams)
            .field("throughput_policy", &self.throughput_policy())
            .field("max_chunk_size", &self.chunk_size)
            .finish_non_exhaustive()
    }
}

impl NetBenchResponder {
    /// Starts configuring a protocol handler.
    #[must_use]
    pub fn builder() -> NetBenchResponderBuilder {
        NetBenchResponderBuilder::default()
    }

    /// Configured simultaneous-test limit.
    #[must_use]
    pub const fn max_concurrent_tests(&self) -> usize {
        self.concurrent_tests
    }

    /// Configured per-phase duration limit.
    #[must_use]
    pub const fn max_test_duration(&self) -> Duration {
        self.test_duration
    }

    /// Configured parallel-stream limit.
    #[must_use]
    pub const fn max_parallel_streams(&self) -> u16 {
        self.parallel_streams
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
            test_duration_ms: duration_ms(self.test_duration),
            // Zero is the protocol-v1 signal that no throughput streams are accepted.
            parallel_streams: if self.throughput_policy() == ThroughputPolicy::Allow {
                self.parallel_streams
            } else {
                0
            },
            chunk_size: self.chunk_size,
        }
    }

    fn validate_phase(&self, duration_ms: u32) -> Result<Duration> {
        let requested = Duration::from_millis(u64::from(duration_ms));
        if requested.is_zero() || requested > self.test_duration {
            return Err(Error::DurationLimitExceeded {
                requested,
                maximum: self.test_duration,
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
        if self.throughput_policy() == ThroughputPolicy::Deny || self.parallel_streams == 0 {
            return Err(Error::ThroughputDeniedByPeer);
        }
        let duration = self.validate_phase(duration_ms)?;
        if streams == 0 || streams > self.parallel_streams {
            return Err(Error::Protocol(format!(
                "stream count {streams} is outside 1..={}",
                self.parallel_streams
            )));
        }
        if chunk_size == 0 || chunk_size > self.chunk_size {
            return Err(Error::Protocol(format!(
                "chunk size {chunk_size} is outside 1..={}",
                self.chunk_size
            )));
        }
        Ok(duration)
    }

    #[allow(
        clippy::too_many_lines,
        reason = "linear command dispatch is easier to audit"
    )]
    /// Serves one host-admitted benchmark flow.
    ///
    /// # Errors
    ///
    /// Returns a flow-local negotiation, policy, measurement, timeout, or transport error.
    pub async fn serve(&self, flow: NetBenchFlow) -> Result<()> {
        let (session, mut control_send, mut control_recv) = flow.into_parts();
        let connection = Connection(Arc::clone(&session));
        let permit = Arc::clone(&self.permits).try_acquire_owned();

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

        let mut active_probe = None::<ProbeEchoTask>;

        loop {
            let message = read_control(&mut control_recv).await?;

            let result = match message {
                ControlMessage::StartLatency {
                    test_id,
                    duration_ms,
                    interval_ms,
                } => self
                    .validate_phase(duration_ms)
                    .and_then(|_| latency_probe_budget(duration_ms, interval_ms))
                    .and_then(|max_requests| {
                        start_probe_echo(
                            &mut active_probe,
                            Arc::clone(&session),
                            test_id,
                            self.test_duration,
                            max_requests,
                        )
                    }),
                ControlMessage::StartLoss {
                    test_id,
                    duration_ms,
                    rate_per_second,
                    timeout_ms: _,
                } => self
                    .validate_phase(duration_ms)
                    .and_then(|_| loss_probe_budget(duration_ms, rate_per_second))
                    .and_then(|max_requests| {
                        start_probe_echo(
                            &mut active_probe,
                            Arc::clone(&session),
                            test_id,
                            self.test_duration,
                            max_requests,
                        )
                    }),
                ControlMessage::StopTest { test_id } => match active_probe.take() {
                    None => Err(Error::Protocol(format!(
                        "received StopTest for inactive test {test_id}"
                    ))),
                    Some(probe) if probe.test_id != test_id => {
                        let active_test_id = probe.test_id;
                        active_probe = Some(probe);
                        Err(Error::Protocol(format!(
                            "received StopTest for test {test_id}, active test is {active_test_id}"
                        )))
                    }
                    Some(probe) => probe.stop().await,
                },
                ControlMessage::FlowFinished => {
                    if let Some(probe) = &active_probe {
                        Err(Error::Protocol(format!(
                            "flow finished while probe test {} is still active",
                            probe.test_id
                        )))
                    } else {
                        write_control(&mut control_send, &ControlMessage::FlowFinishedAck).await?;
                        break;
                    }
                }
                ControlMessage::StartDownload {
                    test_id,
                    duration_ms,
                    streams,
                    chunk_size,
                } => {
                    if let Some(probe) = &active_probe {
                        let error = Error::Protocol(format!(
                            "cannot start download test {test_id} while probe test {} is active",
                            probe.test_id
                        ));
                        send_error_best_effort(
                            &mut control_send,
                            ErrorCode::InvalidRequest,
                            error.to_string(),
                        )
                        .await;
                        return Err(error);
                    }
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
                        let probe = ProbeEchoTask::spawn(
                            Arc::clone(&session),
                            test_id,
                            duration.saturating_add(THROUGHPUT_CLEANUP_TIMEOUT),
                            loaded_probe_budget(duration),
                        );
                        let download = send_download(
                            download_streams,
                            duration,
                            usize::try_from(chunk_size).map_err(Error::network)?,
                        )
                        .await;
                        let probe_result = probe.stop().await;
                        let (bytes, elapsed) = download?;
                        probe_result?;
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
                    if let Some(probe) = &active_probe {
                        let error = Error::Protocol(format!(
                            "cannot start upload test {test_id} while probe test {} is active",
                            probe.test_id
                        ));
                        send_error_best_effort(
                            &mut control_send,
                            ErrorCode::InvalidRequest,
                            error.to_string(),
                        )
                        .await;
                        return Err(error);
                    }
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
                        let probe = ProbeEchoTask::spawn(
                            Arc::clone(&session),
                            test_id,
                            duration.saturating_add(THROUGHPUT_CLEANUP_TIMEOUT),
                            loaded_probe_budget(duration),
                        );
                        let upload =
                            receive_upload(upload_streams, &mut control_send, test_id, duration)
                                .await;
                        let probe_result = probe.stop().await;
                        let (bytes, elapsed) = upload?;
                        probe_result?;
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
                | ControlMessage::FlowFinishedAck
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

        Ok(())
    }
}

impl Default for NetBenchResponder {
    fn default() -> Self {
        Self::builder().build()
    }
}

/// Builder for [`NetBenchResponder`].
#[derive(Clone)]
pub struct NetBenchResponderBuilder {
    concurrent_tests: usize,
    test_duration: Duration,
    parallel_streams: u16,
    chunk_size: u32,
    throughput_policy: ThroughputPolicy,
}

impl std::fmt::Debug for NetBenchResponderBuilder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchResponderBuilder")
            .field("max_concurrent_tests", &self.concurrent_tests)
            .field("max_test_duration", &self.test_duration)
            .field("max_parallel_streams", &self.parallel_streams)
            .field("throughput_policy", &self.throughput_policy)
            .field("max_chunk_size", &self.chunk_size)
            .finish()
    }
}

impl NetBenchResponderBuilder {
    /// Sets the number of simultaneous benchmark flows.
    #[must_use]
    pub const fn max_concurrent_tests(mut self, value: usize) -> Self {
        self.concurrent_tests = value;
        self
    }

    /// Sets the maximum accepted duration for an individual phase.
    #[must_use]
    pub const fn max_test_duration(mut self, value: Duration) -> Self {
        self.test_duration = value;
        self
    }

    /// Sets the maximum stream count per throughput direction.
    #[must_use]
    pub const fn max_parallel_streams(mut self, value: u16) -> Self {
        self.parallel_streams = value;
        self
    }

    /// Sets whether peers may run bandwidth-saturating upload and download measurements.
    #[must_use]
    pub const fn throughput_policy(mut self, value: ThroughputPolicy) -> Self {
        self.throughput_policy = value;
        self
    }

    /// Builds the server policy.
    #[must_use]
    pub fn build(self) -> NetBenchResponder {
        NetBenchResponder {
            concurrent_tests: self.concurrent_tests,
            test_duration: self.test_duration,
            parallel_streams: self.parallel_streams,
            chunk_size: self.chunk_size,
            throughput_allowed: Arc::new(AtomicBool::new(matches!(
                self.throughput_policy,
                ThroughputPolicy::Allow
            ))),
            permits: Arc::new(Semaphore::new(self.concurrent_tests)),
        }
    }
}

impl Default for NetBenchResponderBuilder {
    fn default() -> Self {
        Self {
            concurrent_tests: 2,
            test_duration: Duration::from_secs(30),
            parallel_streams: 8,
            chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            throughput_policy: ThroughputPolicy::Allow,
        }
    }
}

fn start_probe_echo(
    active: &mut Option<ProbeEchoTask>,
    session: Arc<dyn NetBenchSession>,
    test_id: u64,
    lifetime: Duration,
    max_requests: u64,
) -> Result<()> {
    if let Some(probe) = active {
        return Err(Error::Protocol(format!(
            "cannot start test {test_id} while probe test {} is active",
            probe.test_id
        )));
    }
    *active = Some(ProbeEchoTask::spawn(
        session,
        test_id,
        lifetime,
        max_requests,
    ));
    Ok(())
}

fn latency_probe_budget(duration_ms: u32, interval_ms: u32) -> Result<u64> {
    if interval_ms == 0 {
        return Err(Error::Protocol(
            "latency probe interval must be at least one millisecond".to_owned(),
        ));
    }
    Ok(u64::from(duration_ms).div_ceil(u64::from(interval_ms)))
}

fn loss_probe_budget(duration_ms: u32, rate_per_second: u32) -> Result<u64> {
    if !(1..=MAX_PROBE_RATE_PER_SECOND).contains(&rate_per_second) {
        return Err(Error::Protocol(format!(
            "loss probe rate must be within 1..={MAX_PROBE_RATE_PER_SECOND} per second"
        )));
    }
    Ok((u64::from(duration_ms) * u64::from(rate_per_second)).div_ceil(1_000))
}

fn loaded_probe_budget(duration: Duration) -> u64 {
    let requests = duration
        .as_nanos()
        .div_ceil(LOADED_LATENCY_INTERVAL.as_nanos());
    u64::try_from(requests).unwrap_or(u64::MAX)
}

async fn echo_datagrams(
    session: Arc<dyn NetBenchSession>,
    test_id: u64,
    lifetime: Duration,
    max_requests: u64,
) -> Result<()> {
    let deadline = Instant::now() + lifetime;
    let mut echoed = HashSet::<u64>::new();
    let mut echoed_count = 0_u64;
    while echoed_count < max_requests {
        let bytes = match tokio::time::timeout_at(deadline, session.read_datagram()).await {
            Ok(result) => result?,
            Err(_) => break,
        };
        let Ok(mut probe) = postcard::from_bytes::<Probe>(&bytes) else {
            continue;
        };
        if probe.magic != PROBE_MAGIC
            || probe.test_id != test_id
            || probe.kind != ProbeKind::Request
            || !echoed.insert(probe.sequence)
        {
            continue;
        }
        echoed_count += 1;
        probe.kind = ProbeKind::Response;
        let payload = postcard::to_allocvec(&probe)?;
        match tokio::time::timeout_at(deadline, session.send_datagram(payload)).await {
            Ok(result) => result?,
            Err(_) => break,
        }
    }
    Ok(())
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
                    Ok(Err(Error::FlowStopped)) | Err(_) => break,
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
    Ok((total.load(Ordering::Relaxed), duration))
}

async fn open_download_streams(connection: Connection, streams: u16) -> Result<Vec<SendStream>> {
    let mut opened = Vec::with_capacity(usize::from(streams));
    for _ in 0..streams {
        let mut stream = connection.open_uni().await?;
        stream.write_all(&[DOWNLOAD_STREAM_MAGIC]).await?;
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
        let mut stream = connection.accept_uni().await?;
        let mut magic = [0_u8; 1];
        stream.read_exact(&mut magic).await?;
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
    let mut ticker = tokio::time::interval(THROUGHPUT_SAMPLE_INTERVAL);
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

fn duration_ms(duration: Duration) -> u32 {
    u32::try_from(duration.as_millis()).unwrap_or(u32::MAX)
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, future::pending, sync::Mutex};

    use async_trait::async_trait;

    use super::*;
    use crate::{NetBenchBidirectionalStream, NetBenchTelemetry};

    struct PeerStoppedSend;

    #[async_trait]
    impl NetBenchSendStream for PeerStoppedSend {
        async fn write_all(&mut self, _bytes: &[u8]) -> Result<()> {
            Err(Error::FlowStopped)
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        fn cancel(&mut self) {}
    }

    #[derive(Default)]
    struct ProbeSession {
        datagrams: Mutex<VecDeque<Vec<u8>>>,
        sent: Mutex<Vec<Vec<u8>>>,
    }

    #[async_trait]
    impl NetBenchSession for ProbeSession {
        fn remote_peer_id(&self) -> String {
            "peer".to_owned()
        }

        fn telemetry(&self) -> NetBenchTelemetry {
            NetBenchTelemetry::default()
        }

        fn max_datagram_size(&self) -> Option<usize> {
            Some(1_200)
        }

        async fn open_bi(&self) -> Result<Box<dyn NetBenchBidirectionalStream>> {
            Err(Error::Protocol("stream operation is unused".to_owned()))
        }

        async fn accept_bi(&self) -> Result<Box<dyn NetBenchBidirectionalStream>> {
            Err(Error::Protocol("stream operation is unused".to_owned()))
        }

        async fn send_datagram(&self, bytes: Vec<u8>) -> Result<()> {
            lock_unpoisoned(&self.sent).push(bytes);
            Ok(())
        }

        async fn read_datagram(&self) -> Result<Vec<u8>> {
            if let Some(bytes) = lock_unpoisoned(&self.datagrams).pop_front() {
                return Ok(bytes);
            }
            pending().await
        }
    }

    struct SinkSend;

    #[async_trait]
    impl NetBenchSendStream for SinkSend {
        async fn write_all(&mut self, _bytes: &[u8]) -> Result<()> {
            Ok(())
        }

        fn finish(&mut self) -> Result<()> {
            Ok(())
        }

        fn cancel(&mut self) {}
    }

    struct ScriptedReceive {
        bytes: VecDeque<u8>,
        terminal: Option<Error>,
    }

    #[async_trait]
    impl NetBenchReceiveStream for ScriptedReceive {
        async fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
            if self.bytes.is_empty() {
                return Err(self.terminal.take().unwrap_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "script exhausted")
                        .into()
                }));
            }
            let count = bytes.len().min(self.bytes.len());
            for byte in &mut bytes[..count] {
                *byte = self.bytes.pop_front().expect("length checked");
            }
            Ok(count)
        }

        async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
            for byte in bytes {
                let Some(next) = self.bytes.pop_front() else {
                    return Err(self.terminal.take().unwrap_or_else(|| {
                        std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "script exhausted")
                            .into()
                    }));
                };
                *byte = next;
            }
            Ok(())
        }

        fn cancel(&mut self) {}
    }

    fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        mutex
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn control_frame(message: &ControlMessage) -> VecDeque<u8> {
        let payload = postcard::to_allocvec(message).unwrap();
        let mut frame = VecDeque::from(u32::try_from(payload.len()).unwrap().to_be_bytes());
        frame.extend(payload);
        frame
    }

    fn probe_bytes(magic: [u8; 4], test_id: u64, sequence: u64, kind: ProbeKind) -> Vec<u8> {
        postcard::to_allocvec(&Probe {
            magic,
            test_id,
            sequence,
            kind,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn receiver_deadline_stop_is_a_clean_download_terminal() {
        let result = send_download(
            vec![Box::new(PeerStoppedSend)],
            Duration::from_millis(1),
            1024,
        )
        .await
        .unwrap();
        assert_eq!(result.0, 0);
    }

    #[test]
    fn probe_budgets_match_the_declared_cadence() {
        assert_eq!(latency_probe_budget(2_000, 100).unwrap(), 20);
        assert_eq!(loss_probe_budget(2_000, 100).unwrap(), 200);
        assert!(latency_probe_budget(2_000, 0).is_err());
        assert!(loss_probe_budget(2_000, MAX_PROBE_RATE_PER_SECOND + 1).is_err());
    }

    #[tokio::test]
    async fn probe_echo_is_scoped_deduplicated_and_bounded() {
        let session = Arc::new(ProbeSession {
            datagrams: Mutex::new(VecDeque::from([
                probe_bytes(*b"NOPE", 42, 0, ProbeKind::Request),
                probe_bytes(PROBE_MAGIC, 42, 0, ProbeKind::Request),
                probe_bytes(PROBE_MAGIC, 42, 0, ProbeKind::Request),
                probe_bytes(PROBE_MAGIC, 7, 1, ProbeKind::Request),
                probe_bytes(PROBE_MAGIC, 42, 1, ProbeKind::Response),
                probe_bytes(PROBE_MAGIC, 42, 1, ProbeKind::Request),
            ])),
            sent: Mutex::new(Vec::new()),
        });

        echo_datagrams(
            Arc::clone(&session) as Arc<dyn NetBenchSession>,
            42,
            Duration::from_secs(1),
            2,
        )
        .await
        .unwrap();

        let sent = lock_unpoisoned(&session.sent);
        assert_eq!(sent.len(), 2);
        for (sequence, bytes) in sent.iter().enumerate() {
            let probe: Probe = postcard::from_bytes(bytes).unwrap();
            assert_eq!(probe.magic, PROBE_MAGIC);
            assert_eq!(probe.test_id, 42);
            assert_eq!(probe.sequence, sequence as u64);
            assert_eq!(probe.kind, ProbeKind::Response);
        }
    }

    #[tokio::test]
    async fn responder_propagates_control_session_errors() {
        let hello = ControlMessage::ClientHello {
            protocol_versions: vec![PROTOCOL_VERSION],
            capabilities: Capabilities::default(),
        };
        let flow = NetBenchFlow::new(
            Arc::new(ProbeSession::default()),
            Box::new(SinkSend),
            Box::new(ScriptedReceive {
                bytes: control_frame(&hello),
                terminal: Some(Error::Network("dispatcher closed".to_owned())),
            }),
        );

        let error = NetBenchResponder::default().serve(flow).await.unwrap_err();
        assert!(matches!(
            error,
            Error::Network(message) if message == "dispatcher closed"
        ));
    }
}
