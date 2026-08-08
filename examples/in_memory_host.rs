//! Complete host adapter using in-memory Streams and Datagram queues.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use iroh_netbench::{
    Error, NetBenchBidirectionalStream, NetBenchConfig, NetBenchFlow, NetBenchInitiator,
    NetBenchReceiveStream, NetBenchResponder, NetBenchSendStream, NetBenchSession,
    NetBenchTelemetry, PathKind, Result,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt, DuplexStream, ReadHalf, WriteHalf, duplex, split},
    sync::{Mutex, mpsc},
};

struct MemorySession {
    peer_id: String,
    stream_tx: mpsc::Sender<DuplexStream>,
    stream_rx: Mutex<mpsc::Receiver<DuplexStream>>,
    datagram_tx: mpsc::Sender<Vec<u8>>,
    datagram_rx: Mutex<mpsc::Receiver<Vec<u8>>>,
}

#[async_trait]
impl NetBenchSession for MemorySession {
    fn remote_peer_id(&self) -> String {
        self.peer_id.clone()
    }

    fn telemetry(&self) -> NetBenchTelemetry {
        NetBenchTelemetry {
            path: PathKind::Direct,
            current_mtu: 1_200,
            ..NetBenchTelemetry::default()
        }
    }

    fn max_datagram_size(&self) -> Option<usize> {
        Some(1_200)
    }

    async fn open_bi(&self) -> Result<Box<dyn NetBenchBidirectionalStream>> {
        let (local, remote) = duplex(1024 * 1024);
        self.stream_tx
            .send(remote)
            .await
            .map_err(|_| Error::Network("peer Stream dispatcher closed".to_owned()))?;
        Ok(Box::new(MemoryBi(local)))
    }

    async fn accept_bi(&self) -> Result<Box<dyn NetBenchBidirectionalStream>> {
        let stream = self
            .stream_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| Error::Network("peer Stream dispatcher closed".to_owned()))?;
        Ok(Box::new(MemoryBi(stream)))
    }

    async fn send_datagram(&self, bytes: Vec<u8>) -> Result<()> {
        self.datagram_tx
            .send(bytes)
            .await
            .map_err(|_| Error::Network("peer Datagram dispatcher closed".to_owned()))
    }

    async fn read_datagram(&self) -> Result<Vec<u8>> {
        self.datagram_rx
            .lock()
            .await
            .recv()
            .await
            .ok_or_else(|| Error::Network("peer Datagram dispatcher closed".to_owned()))
    }
}

struct MemoryBi(DuplexStream);

impl NetBenchBidirectionalStream for MemoryBi {
    fn into_split(
        self: Box<Self>,
    ) -> (Box<dyn NetBenchSendStream>, Box<dyn NetBenchReceiveStream>) {
        let (recv, send) = split(self.0);
        (
            Box::new(MemorySend(Some(send))),
            Box::new(MemoryReceive(Some(recv))),
        )
    }
}

struct MemorySend(Option<WriteHalf<DuplexStream>>);

#[async_trait]
impl NetBenchSendStream for MemorySend {
    async fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        self.0
            .as_mut()
            .ok_or_else(|| Error::Network("Stream send half is closed".to_owned()))?
            .write(bytes)
            .await
            .map_err(map_memory_write_error)
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.0
            .as_mut()
            .ok_or_else(|| Error::Network("Stream send half is closed".to_owned()))?
            .write_all(bytes)
            .await
            .map_err(map_memory_write_error)
    }

    fn finish(&mut self) -> Result<()> {
        self.0.take();
        Ok(())
    }

    fn cancel(&mut self) {
        self.0.take();
    }
}

fn map_memory_write_error(error: std::io::Error) -> Error {
    match error.kind() {
        std::io::ErrorKind::BrokenPipe
        | std::io::ErrorKind::ConnectionReset
        | std::io::ErrorKind::NotConnected => Error::FlowStopped,
        _ => Error::Io(error),
    }
}

struct MemoryReceive(Option<ReadHalf<DuplexStream>>);

#[async_trait]
impl NetBenchReceiveStream for MemoryReceive {
    async fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        self.0
            .as_mut()
            .ok_or_else(|| Error::Network("Stream receive half is closed".to_owned()))?
            .read(bytes)
            .await
            .map_err(Error::Io)
    }

    async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        self.0
            .as_mut()
            .ok_or_else(|| Error::Network("Stream receive half is closed".to_owned()))?
            .read_exact(bytes)
            .await
            .map(|_| ())
            .map_err(Error::Io)
    }

    fn cancel(&mut self) {
        self.0.take();
    }
}

fn session_pair() -> (Arc<MemorySession>, Arc<MemorySession>) {
    let (stream_ab_tx, stream_ab_rx) = mpsc::channel(16);
    let (stream_ba_tx, stream_ba_rx) = mpsc::channel(16);
    let (datagram_ab_tx, datagram_ab_rx) = mpsc::channel(256);
    let (datagram_ba_tx, datagram_ba_rx) = mpsc::channel(256);
    (
        Arc::new(MemorySession {
            peer_id: "peer-b".to_owned(),
            stream_tx: stream_ab_tx,
            stream_rx: Mutex::new(stream_ba_rx),
            datagram_tx: datagram_ab_tx,
            datagram_rx: Mutex::new(datagram_ba_rx),
        }),
        Arc::new(MemorySession {
            peer_id: "peer-a".to_owned(),
            stream_tx: stream_ba_tx,
            stream_rx: Mutex::new(stream_ab_rx),
            datagram_tx: datagram_ba_tx,
            datagram_rx: Mutex::new(datagram_ab_rx),
        }),
    )
}

fn flow(session: Arc<MemorySession>, control: DuplexStream) -> NetBenchFlow {
    let (recv, send) = split(control);
    NetBenchFlow::new(
        session,
        Box::new(MemorySend(Some(send))),
        Box::new(MemoryReceive(Some(recv))),
    )
}

fn example_config() -> NetBenchConfig {
    NetBenchConfig {
        overall_timeout: Duration::from_secs(5),
        path_stabilization_timeout: Duration::ZERO,
        latency_duration: Duration::from_millis(100),
        latency_interval: Duration::from_millis(10),
        loss_duration: Duration::from_millis(100),
        loss_rate_per_second: 100,
        probe_timeout: Duration::from_millis(100),
        download_warmup: Duration::from_millis(20),
        download_duration: Duration::from_millis(100),
        upload_warmup: Duration::from_millis(20),
        upload_duration: Duration::from_millis(100),
        parallel_streams: 2,
        chunk_size: 4 * 1024,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let (initiator_session, responder_session) = session_pair();
    let (initiator_control, responder_control) = duplex(64 * 1024);
    let initiator_flow = flow(initiator_session, initiator_control);
    let responder_flow = flow(responder_session, responder_control);

    let responder =
        tokio::spawn(async move { NetBenchResponder::default().serve(responder_flow).await });
    let initiator_runtime = NetBenchInitiator::new();
    let initiator = initiator_runtime.run(initiator_flow, example_config());
    let (report, responder) = tokio::join!(initiator, responder);
    let report = report?;
    responder.map_err(|error| Error::Network(format!("responder task failed: {error}")))??;

    println!(
        "idle p50={:?}, download={:.2} Mbps, upload={:.2} Mbps",
        report.idle_latency.p50,
        report.download.bits_per_second / 1_000_000.0,
        report.upload.bits_per_second / 1_000_000.0,
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh_netbench::{NetBenchEvent, NetBenchProbeConfig, ThroughputPolicy};

    async fn join_responder(task: tokio::task::JoinHandle<Result<()>>) -> Result<()> {
        task.await
            .map_err(|error| Error::Network(format!("responder task failed: {error}")))?
    }

    #[tokio::test]
    async fn full_flow_finishes_without_closing_the_host_session() -> Result<()> {
        let (initiator_session, responder_session) = session_pair();
        let (initiator_control, responder_control) = duplex(64 * 1024);
        let initiator_flow = flow(Arc::clone(&initiator_session), initiator_control);
        let responder_flow = flow(Arc::clone(&responder_session), responder_control);

        let responder =
            tokio::spawn(async move { NetBenchResponder::default().serve(responder_flow).await });
        NetBenchInitiator::new()
            .run(initiator_flow, example_config())
            .await?;
        join_responder(responder).await?;

        let local = initiator_session.open_bi().await?;
        let remote = responder_session.accept_bi().await?;
        drop((local, remote));
        Ok(())
    }

    #[tokio::test]
    async fn throughput_denial_finishes_the_flow_cleanly() -> Result<()> {
        let (initiator_session, responder_session) = session_pair();
        let (initiator_control, responder_control) = duplex(64 * 1024);
        let initiator_flow = flow(initiator_session, initiator_control);
        let responder_flow = flow(responder_session, responder_control);
        let responder_runtime = NetBenchResponder::builder()
            .throughput_policy(ThroughputPolicy::Deny)
            .build();

        let responder = tokio::spawn(async move { responder_runtime.serve(responder_flow).await });
        let result = NetBenchInitiator::new()
            .run(initiator_flow, example_config())
            .await;

        assert!(matches!(result, Err(Error::ThroughputDeniedByPeer)));
        join_responder(responder).await
    }

    #[tokio::test]
    async fn probe_only_remains_available_when_throughput_is_denied() -> Result<()> {
        let (initiator_session, responder_session) = session_pair();
        let (initiator_control, responder_control) = duplex(64 * 1024);
        let initiator_flow = flow(initiator_session, initiator_control);
        let responder_flow = flow(responder_session, responder_control);
        let responder_runtime = NetBenchResponder::builder()
            .throughput_policy(ThroughputPolicy::Deny)
            .build();

        let responder = tokio::spawn(async move { responder_runtime.serve(responder_flow).await });
        let report = NetBenchInitiator::new()
            .run_probes(initiator_flow, NetBenchProbeConfig::from(&example_config()))
            .await?;

        assert!(report.idle_latency.samples > 0);
        join_responder(responder).await
    }

    #[tokio::test]
    async fn cancellation_stops_active_throughput_without_closing_the_host_session() -> Result<()> {
        let (initiator_session, responder_session) = session_pair();
        let (initiator_control, responder_control) = duplex(64 * 1024);
        let initiator_flow = flow(Arc::clone(&initiator_session), initiator_control);
        let responder_flow = flow(Arc::clone(&responder_session), responder_control);
        let responder =
            tokio::spawn(async move { NetBenchResponder::default().serve(responder_flow).await });
        let mut config = example_config();
        config.download_warmup = Duration::ZERO;
        config.download_duration = Duration::from_secs(2);
        config.upload_warmup = Duration::ZERO;
        let mut test = NetBenchInitiator::new()
            .start(initiator_flow, config)
            .await?;

        let mut download_sampled = false;
        while let Some(event) = test.next().await {
            if matches!(event, NetBenchEvent::DownloadSample(_)) {
                download_sampled = true;
                break;
            }
        }
        assert!(download_sampled);
        test.abort_and_wait().await?;

        let responder_result = tokio::time::timeout(Duration::from_secs(2), responder)
            .await
            .map_err(|_| Error::Timeout {
                stage: "cancelled responder cleanup",
            })?
            .map_err(|error| Error::Network(format!("responder task failed: {error}")))?;
        assert!(responder_result.is_err());

        let local = initiator_session.open_bi().await?;
        let remote = responder_session.accept_bi().await?;
        drop((local, remote));
        Ok(())
    }

    #[tokio::test]
    async fn overall_timeout_reports_the_current_stage_and_releases_the_flow() -> Result<()> {
        let (initiator_session, responder_session) = session_pair();
        let (initiator_control, responder_control) = duplex(64 * 1024);
        let initiator_flow = flow(Arc::clone(&initiator_session), initiator_control);
        let mut config = example_config();
        config.overall_timeout = Duration::from_millis(50);

        let result = NetBenchInitiator::new().run(initiator_flow, config).await;
        assert!(matches!(
            result,
            Err(Error::Timeout {
                stage: "protocol negotiation"
            })
        ));
        drop(responder_control);

        let local = initiator_session.open_bi().await?;
        let remote = responder_session.accept_bi().await?;
        drop((local, remote));
        Ok(())
    }
}
