//! Host-owned session and stream boundary used by benchmark flows.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;

use crate::{PathKind, Result};

/// A cumulative counter from one host-owned connection or path generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterSnapshot {
    /// Opaque identity of this counter's lifetime. Change it on replacement or reset.
    ///
    /// The host must not reuse an identity during a benchmark flow. Path counters must
    /// change scope when their path changes, even if the high-level path kind is unchanged.
    pub scope: u64,
    /// Cumulative count within this scope.
    pub value: u64,
}

/// One transport telemetry snapshot supplied by the host session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NetBenchTelemetry {
    /// Currently selected transport path.
    pub path: PathKind,
    /// Connection-wide QUIC packets declared lost.
    pub lost_packets: Option<CounterSnapshot>,
    /// Connection-wide QUIC bytes declared lost.
    pub lost_bytes: Option<CounterSnapshot>,
    /// Congestion events on the selected path.
    pub congestion_events: Option<CounterSnapshot>,
    /// Received transport datagrams.
    pub rx_datagrams: Option<CounterSnapshot>,
    /// Transmitted transport datagrams.
    pub tx_datagrams: Option<CounterSnapshot>,
    /// Current path MTU, or `None` when unavailable.
    pub current_mtu: Option<u16>,
    /// Path black-hole detections.
    pub black_holes_detected: Option<CounterSnapshot>,
    /// Current smoothed transport RTT.
    pub rtt: Option<Duration>,
}

impl Default for NetBenchTelemetry {
    fn default() -> Self {
        Self {
            path: PathKind::Unknown,
            lost_packets: None,
            lost_bytes: None,
            congestion_events: None,
            rx_datagrams: None,
            tx_datagrams: None,
            current_mtu: None,
            black_holes_detected: None,
            rtt: None,
        }
    }
}

/// Sending half of one host-routed reliable stream.
#[async_trait]
pub trait NetBenchSendStream: Send {
    /// Writes one buffer and returns its complete length.
    async fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        self.write_all(bytes).await?;
        Ok(bytes.len())
    }
    /// Writes the complete buffer.
    async fn write_all(&mut self, bytes: &[u8]) -> Result<()>;
    /// Finishes the sending half without closing the host session.
    ///
    /// # Errors
    ///
    /// Returns the host adapter's flow-local stream error.
    fn finish(&mut self) -> Result<()>;
    /// Cancels this stream's pending write without closing the host session.
    fn cancel(&mut self);
}

#[async_trait]
impl<T: NetBenchSendStream + ?Sized> NetBenchSendStream for Box<T> {
    async fn write(&mut self, bytes: &[u8]) -> Result<usize> {
        (**self).write(bytes).await
    }

    async fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        (**self).write_all(bytes).await
    }

    fn finish(&mut self) -> Result<()> {
        (**self).finish()
    }

    fn cancel(&mut self) {
        (**self).cancel();
    }
}

/// Receiving half of one host-routed reliable stream.
#[async_trait]
pub trait NetBenchReceiveStream: Send {
    /// Reads available bytes, returning zero after the peer finishes.
    async fn read(&mut self, bytes: &mut [u8]) -> Result<usize>;
    /// Reads exactly the supplied buffer length.
    async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()>;
    /// Cancels this stream's pending read without closing the host session.
    fn cancel(&mut self);
}

#[async_trait]
impl<T: NetBenchReceiveStream + ?Sized> NetBenchReceiveStream for Box<T> {
    async fn read(&mut self, bytes: &mut [u8]) -> Result<usize> {
        (**self).read(bytes).await
    }

    async fn read_exact(&mut self, bytes: &mut [u8]) -> Result<()> {
        (**self).read_exact(bytes).await
    }

    fn cancel(&mut self) {
        (**self).cancel();
    }
}

/// One host-routed reliable bidirectional stream.
pub trait NetBenchBidirectionalStream: Send {
    /// Splits the stream so sending and receiving can run independently.
    fn into_split(self: Box<Self>)
    -> (Box<dyn NetBenchSendStream>, Box<dyn NetBenchReceiveStream>);
}

/// A flow-scoped view of one caller-owned authenticated peer session.
///
/// Implementations perform typed Stream and Datagram demultiplexing. Therefore `accept_bi` and
/// `read_datagram` must never return traffic belonging to another host business flow.
#[async_trait]
pub trait NetBenchSession: Send + Sync {
    /// Stable, non-secret remote identity for the report.
    fn remote_peer_id(&self) -> String;
    /// Samples path and transport counters without active probing.
    fn telemetry(&self) -> NetBenchTelemetry;
    /// Current maximum application Datagram payload.
    fn max_datagram_size(&self) -> Option<usize>;
    /// Opens one stream already typed as part of this netbench flow.
    async fn open_bi(&self) -> Result<Box<dyn NetBenchBidirectionalStream>>;
    /// Accepts the next stream already routed to this netbench flow.
    async fn accept_bi(&self) -> Result<Box<dyn NetBenchBidirectionalStream>>;
    /// Sends one flow-scoped unreliable Datagram.
    async fn send_datagram(&self, bytes: Vec<u8>) -> Result<()>;
    /// Reads the next Datagram routed to this flow.
    async fn read_datagram(&self) -> Result<Vec<u8>>;
}

/// One admitted benchmark business flow.
pub struct NetBenchFlow {
    session: Arc<dyn NetBenchSession>,
    control_send: Box<dyn NetBenchSendStream>,
    control_recv: Box<dyn NetBenchReceiveStream>,
}

impl std::fmt::Debug for NetBenchFlow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("NetBenchFlow")
            .field("remote_peer_id", &self.session.remote_peer_id())
            .finish_non_exhaustive()
    }
}

impl NetBenchFlow {
    /// Creates a flow after the host has authenticated/admitted the peer and routed its control
    /// stream and Datagram namespace.
    #[must_use]
    pub fn new(
        session: Arc<dyn NetBenchSession>,
        control_send: Box<dyn NetBenchSendStream>,
        control_recv: Box<dyn NetBenchReceiveStream>,
    ) -> Self {
        Self {
            session,
            control_send,
            control_recv,
        }
    }

    pub(crate) fn into_parts(
        self,
    ) -> (
        Arc<dyn NetBenchSession>,
        Box<dyn NetBenchSendStream>,
        Box<dyn NetBenchReceiveStream>,
    ) {
        (self.session, self.control_send, self.control_recv)
    }
}
