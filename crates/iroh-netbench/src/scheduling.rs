//! QUIC stream scheduling shared by the client and server.

use std::time::Duration;

use iroh::endpoint::SendStream;

use crate::{Error, Result};

// Keep the control plane ahead of bandwidth-saturating streams. Only two priority levels are
// used so the scheduler does not have to maintain an unnecessarily fragmented priority tree.
const CONTROL_STREAM_PRIORITY: i32 = 10;
const THROUGHPUT_STREAM_PRIORITY: i32 = -10;
const _: () = assert!(CONTROL_STREAM_PRIORITY > THROUGHPUT_STREAM_PRIORITY);

// This remains a hard bound. Priority isolation is the primary fix; the additional grace absorbs
// Relay and impaired-path scheduling jitter without returning to unbounded stream draining.
pub(crate) const THROUGHPUT_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn prioritize_control_stream(stream: &SendStream) -> Result<()> {
    stream
        .set_priority(CONTROL_STREAM_PRIORITY)
        .map_err(Error::network)
}

pub(crate) fn deprioritize_throughput_stream(stream: &SendStream) -> Result<()> {
    stream
        .set_priority(THROUGHPUT_STREAM_PRIORITY)
        .map_err(Error::network)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{
        Endpoint, RelayMode,
        endpoint::{Connection, presets},
        protocol::{AcceptError, ProtocolHandler, Router},
    };

    const PRIORITY_TEST_ALPN: &[u8] = b"/iroh/netbench/test/stream-priority";

    #[derive(Debug, Clone)]
    struct PriorityProtocol {
        observed: tokio::sync::mpsc::UnboundedSender<(i32, i32)>,
    }

    impl ProtocolHandler for PriorityProtocol {
        async fn accept(&self, connection: Connection) -> std::result::Result<(), AcceptError> {
            let (control_send, _control_recv) = connection.accept_bi().await?;
            prioritize_control_stream(&control_send).map_err(AcceptError::from_err)?;
            let throughput_send = connection.open_uni().await?;
            deprioritize_throughput_stream(&throughput_send).map_err(AcceptError::from_err)?;
            let priorities = (
                control_send.priority().map_err(AcceptError::from_err)?,
                throughput_send.priority().map_err(AcceptError::from_err)?,
            );
            let _ = self.observed.send(priorities);
            connection.close(0_u8.into(), b"stream priority test complete");
            Ok(())
        }
    }

    #[tokio::test]
    async fn priorities_are_applied_to_live_quic_streams() {
        let server = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let (observed_tx, mut observed_rx) = tokio::sync::mpsc::unbounded_channel();
        let router = Router::builder(server.clone())
            .accept(
                PRIORITY_TEST_ALPN,
                PriorityProtocol {
                    observed: observed_tx,
                },
            )
            .spawn();
        let client = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let connection = client
            .connect(server.addr(), PRIORITY_TEST_ALPN)
            .await
            .unwrap();
        let (mut control_send, _control_recv) = connection.open_bi().await.unwrap();
        prioritize_control_stream(&control_send).unwrap();
        assert_eq!(control_send.priority().unwrap(), CONTROL_STREAM_PRIORITY);
        control_send.write_all(&[0]).await.unwrap();

        let (server_control_priority, server_throughput_priority) =
            tokio::time::timeout(Duration::from_secs(1), observed_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(server_control_priority, CONTROL_STREAM_PRIORITY);
        assert_eq!(server_throughput_priority, THROUGHPUT_STREAM_PRIORITY);

        router.shutdown().await.unwrap();
        client.close().await;
    }
}
