//! Minimal one-file-per-connection transfer protocol.

use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};

use iroh::{
    Endpoint, EndpointAddr,
    endpoint::{RecvStream, SendStream},
    protocol::{AcceptError, ProtocolHandler},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use tokio::fs::OpenOptions;
use tokio::io::AsyncWriteExt as _;

/// ALPN used by the demo's file-transfer connection.
pub const FILE_ALPN: &[u8] = b"/demo/file/1";
const FILE_PROTOCOL_VERSION: u16 = 1;
const MAX_HEADER_SIZE: usize = 64 * 1024;
const PROGRESS_INTERVAL: Duration = Duration::from_millis(250);

/// Direction of a file-transfer progress event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileTransferDirection {
    /// This endpoint is sending the file.
    Sending,
    /// This endpoint is receiving the file.
    Receiving,
}

/// One cumulative file-transfer progress observation.
#[derive(Debug, Clone, PartialEq)]
pub struct FileTransferProgress {
    /// Local transfer direction.
    pub direction: FileTransferDirection,
    /// Base file name without a local directory.
    pub file_name: String,
    /// Confirmed local stream bytes processed so far.
    pub transferred_bytes: u64,
    /// Declared total file size.
    pub total_bytes: u64,
    /// Time since payload transfer began.
    pub elapsed: Duration,
    /// Effective payload rate over the most recent sample interval.
    pub interval_bps: f64,
    /// The receiver has committed the file and acknowledged completion.
    pub completed: bool,
}

type ProgressHandler = Arc<dyn Fn(FileTransferProgress) + Send + Sync>;
type ConnectionObserver = Arc<dyn Fn(iroh::endpoint::Connection) + Send + Sync>;

/// File transfer failure.
#[derive(Debug, thiserror::Error)]
pub enum FileError {
    /// The peer or local transport failed.
    #[error("network error: {0}")]
    Network(String),
    /// Header or response serialization failed.
    #[error("codec error: {0}")]
    Codec(#[from] postcard::Error),
    /// A local filesystem operation failed.
    #[error("filesystem error: {0}")]
    Io(#[from] std::io::Error),
    /// The receiver rejected the transfer.
    #[error("receiver rejected the file: {0}")]
    Rejected(String),
    /// The protocol exchange was invalid.
    #[error("protocol error: {0}")]
    Protocol(String),
}

/// Header sent before file bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileHeader {
    /// File wire-protocol version.
    pub version: u16,
    /// Base name only; receivers reject path separators.
    pub file_name: String,
    /// Declared payload length.
    pub file_size: u64,
}

/// Receiver response on the bidirectional file stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FileResponse {
    /// Header was accepted; sender may stream bytes.
    Accept,
    /// Header was rejected.
    Reject {
        /// User-facing rejection reason.
        reason: String,
    },
    /// Temporary file was verified and committed.
    Completed {
        /// Bytes read from the stream.
        received_bytes: u64,
    },
}

/// Receiver policy for one-file-per-connection transfers.
#[derive(Clone)]
pub struct FileProtocol {
    download_dir: PathBuf,
    max_file_size: u64,
    progress_handler: Option<ProgressHandler>,
    connection_observer: Option<ConnectionObserver>,
}

impl std::fmt::Debug for FileProtocol {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("FileProtocol")
            .field("download_dir", &self.download_dir)
            .field("max_file_size", &self.max_file_size)
            .field("has_progress_handler", &self.progress_handler.is_some())
            .field(
                "has_connection_observer",
                &self.connection_observer.is_some(),
            )
            .finish()
    }
}

impl FileProtocol {
    /// Creates a receiver with a conservative 1 GiB default limit.
    #[must_use]
    pub fn new(download_dir: impl Into<PathBuf>) -> Self {
        Self {
            download_dir: download_dir.into(),
            max_file_size: 1024 * 1024 * 1024,
            progress_handler: None,
            connection_observer: None,
        }
    }

    /// Overrides the maximum accepted file size.
    #[must_use]
    pub const fn max_file_size(mut self, max_file_size: u64) -> Self {
        self.max_file_size = max_file_size;
        self
    }

    /// Installs a non-blocking observer for receiver-side progress.
    #[must_use]
    pub fn on_progress(
        mut self,
        handler: impl Fn(FileTransferProgress) + Send + Sync + 'static,
    ) -> Self {
        self.progress_handler = Some(Arc::new(handler));
        self
    }

    /// Observes each accepted file-transfer connection.
    #[must_use]
    pub fn on_connection(
        mut self,
        observer: impl Fn(iroh::endpoint::Connection) + Send + Sync + 'static,
    ) -> Self {
        self.connection_observer = Some(Arc::new(observer));
        self
    }

    /// Receiver directory.
    #[must_use]
    pub fn download_dir(&self) -> &Path {
        &self.download_dir
    }

    async fn handle(&self, connection: iroh::endpoint::Connection) -> Result<(), FileError> {
        if let Some(observer) = &self.connection_observer {
            observer(connection.clone());
        }
        let (mut send, mut recv) = connection.accept_bi().await.map_err(network)?;
        let header: FileHeader = read_message(&mut recv).await?;
        let file_name = match validate_header(&header, self.max_file_size) {
            Ok(name) => name,
            Err(reason) => {
                write_message(&mut send, &FileResponse::Reject { reason }).await?;
                send.finish().map_err(network)?;
                return Ok(());
            }
        };

        tokio::fs::create_dir_all(&self.download_dir).await?;
        let final_path = self.download_dir.join(&file_name);
        let part_path = self.download_dir.join(format!(".{file_name}.part"));
        if tokio::fs::try_exists(&final_path).await? || tokio::fs::try_exists(&part_path).await? {
            write_message(
                &mut send,
                &FileResponse::Reject {
                    reason: "目标文件或临时文件已存在".to_owned(),
                },
            )
            .await?;
            send.finish().map_err(network)?;
            return Ok(());
        }

        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&part_path)
            .await?;
        write_message(&mut send, &FileResponse::Accept).await?;

        let receive_result = receive_exact_file(
            &mut recv,
            &mut output,
            &file_name,
            header.file_size,
            self.progress_handler.as_deref(),
        )
        .await;
        if let Err(error) = receive_result {
            drop(output);
            let _ = tokio::fs::remove_file(&part_path).await;
            return Err(error);
        }
        output.sync_all().await?;
        drop(output);
        tokio::fs::rename(&part_path, &final_path).await?;
        if let Some(progress) = self.progress_handler.as_deref() {
            progress(file_progress(
                FileTransferDirection::Receiving,
                &file_name,
                header.file_size,
                header.file_size,
                Instant::now(),
                0.0,
                true,
            ));
        }
        write_message(
            &mut send,
            &FileResponse::Completed {
                received_bytes: header.file_size,
            },
        )
        .await?;
        send.finish().map_err(network)?;
        match tokio::time::timeout(Duration::from_secs(30), send.stopped()).await {
            Ok(Ok(None)) => {}
            Ok(Ok(Some(code))) => tracing::warn!(
                target: "transfer_demo::file",
                ?code,
                "sender stopped the completion response stream"
            ),
            Ok(Err(error)) => return Err(network(error)),
            Err(_) => {
                tracing::warn!(
                    target: "transfer_demo::file",
                    "timed out waiting for sender to acknowledge the completion response"
                );
            }
        }
        Ok(())
    }
}

impl ProtocolHandler for FileProtocol {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), AcceptError> {
        self.handle(connection)
            .await
            .map_err(|error| AcceptError::from_err(std::io::Error::other(error.to_string())))
    }
}

/// Sends one file over a dedicated file-transfer connection.
///
/// # Errors
///
/// Returns a validation, filesystem, connection, rejection, or protocol error.
pub async fn send_file(
    endpoint: &Endpoint,
    peer: EndpointAddr,
    path: &Path,
) -> Result<FileTransferReport, FileError> {
    send_file_with_progress(endpoint, peer, path, |_| {}).await
}

/// Sends one file while reporting cumulative payload progress.
///
/// # Errors
///
/// Returns a validation, filesystem, connection, rejection, or protocol error.
#[allow(
    clippy::too_many_lines,
    reason = "the linear file handshake keeps commit and acknowledgement ordering auditable"
)]
pub async fn send_file_with_progress(
    endpoint: &Endpoint,
    peer: EndpointAddr,
    path: &Path,
    progress: impl FnMut(FileTransferProgress),
) -> Result<FileTransferReport, FileError> {
    send_file_with_progress_observed(endpoint, peer, path, progress, |_| {}).await
}

#[allow(
    clippy::too_many_lines,
    reason = "the linear file handshake keeps commit and acknowledgement ordering auditable"
)]
pub(crate) async fn send_file_with_progress_observed(
    endpoint: &Endpoint,
    peer: EndpointAddr,
    path: &Path,
    mut progress: impl FnMut(FileTransferProgress),
    observe_connection: impl FnOnce(iroh::endpoint::Connection),
) -> Result<FileTransferReport, FileError> {
    let metadata = tokio::fs::metadata(path).await?;
    if !metadata.is_file() {
        return Err(FileError::Protocol(
            "source path is not a regular file".to_owned(),
        ));
    }
    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| FileError::Protocol("file name is not valid UTF-8".to_owned()))?
        .to_owned();
    let header = FileHeader {
        version: FILE_PROTOCOL_VERSION,
        file_name,
        file_size: metadata.len(),
    };

    let started = Instant::now();
    let connection = endpoint.connect(peer, FILE_ALPN).await.map_err(network)?;
    observe_connection(connection.clone());
    let (mut send, mut recv) = connection.open_bi().await.map_err(network)?;
    write_message(&mut send, &header).await?;
    match read_message::<FileResponse>(&mut recv).await? {
        FileResponse::Accept => {}
        FileResponse::Reject { reason } => return Err(FileError::Rejected(reason)),
        FileResponse::Completed { .. } => {
            return Err(FileError::Protocol(
                "received Completed before file payload".to_owned(),
            ));
        }
    }

    let mut input = tokio::fs::File::open(path).await?;
    let payload_started = Instant::now();
    let mut sampler = ProgressSampler::new(payload_started);
    let mut sent_bytes = 0_u64;
    let mut buffer = vec![0_u8; 64 * 1024];
    progress(file_progress(
        FileTransferDirection::Sending,
        &header.file_name,
        0,
        header.file_size,
        payload_started,
        0.0,
        false,
    ));
    loop {
        use tokio::io::AsyncReadExt as _;
        let read = input.read(&mut buffer).await?;
        if read == 0 {
            break;
        }
        send.write_all(&buffer[..read]).await.map_err(network)?;
        sent_bytes += read as u64;
        if let Some(interval_bps) = sampler.sample(sent_bytes, sent_bytes == header.file_size) {
            progress(file_progress(
                FileTransferDirection::Sending,
                &header.file_name,
                sent_bytes,
                header.file_size,
                payload_started,
                interval_bps,
                false,
            ));
        }
    }
    let finish_error = send.finish().err().map(|error| error.to_string());
    if sent_bytes != header.file_size {
        return Err(FileError::Protocol(format!(
            "read {sent_bytes} bytes but metadata declared {}",
            header.file_size
        )));
    }

    let FileResponse::Completed { received_bytes } =
        read_message::<FileResponse>(&mut recv).await?
    else {
        return Err(FileError::Protocol(
            "receiver did not confirm completion".to_owned(),
        ));
    };
    if received_bytes != sent_bytes {
        return Err(FileError::Protocol(format!(
            "receiver confirmed {received_bytes} of {sent_bytes} bytes"
        )));
    }
    if recv.read(&mut [0_u8; 1]).await.map_err(network)?.is_some() {
        return Err(FileError::Protocol(
            "receiver sent trailing bytes after completion".to_owned(),
        ));
    }
    if let Some(error) = finish_error {
        tracing::warn!(
            target: "transfer_demo::file",
            %error,
            "sender stream finish reported an error after receiver confirmed the complete file"
        );
    }
    progress(file_progress(
        FileTransferDirection::Sending,
        &header.file_name,
        received_bytes,
        header.file_size,
        payload_started,
        0.0,
        true,
    ));
    connection.close(0_u8.into(), b"file transfer complete");

    Ok(FileTransferReport {
        path: path.to_owned(),
        received_bytes,
        duration: started.elapsed(),
    })
}

/// Confirmed result of a completed file transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileTransferReport {
    /// Source path.
    pub path: PathBuf,
    /// Receiver-confirmed byte count.
    pub received_bytes: u64,
    /// End-to-end transfer duration.
    pub duration: Duration,
}

fn validate_header(header: &FileHeader, max_file_size: u64) -> Result<String, String> {
    if header.version != FILE_PROTOCOL_VERSION {
        return Err(format!("不支持文件协议版本 {}", header.version));
    }
    if header.file_size > max_file_size {
        return Err(format!(
            "文件大小 {} 超过限制 {max_file_size}",
            header.file_size
        ));
    }
    let path = Path::new(&header.file_name);
    if header.file_name.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err("文件名必须是不含路径的普通文件名".to_owned());
    }
    Ok(header.file_name.clone())
}

async fn receive_exact_file(
    recv: &mut RecvStream,
    output: &mut tokio::fs::File,
    file_name: &str,
    expected: u64,
    progress: Option<&(dyn Fn(FileTransferProgress) + Send + Sync)>,
) -> Result<(), FileError> {
    let mut remaining = expected;
    let mut received = 0_u64;
    let started = Instant::now();
    let mut sampler = ProgressSampler::new(started);
    if let Some(progress) = progress {
        progress(file_progress(
            FileTransferDirection::Receiving,
            file_name,
            0,
            expected,
            started,
            0.0,
            false,
        ));
    }
    let mut buffer = vec![0_u8; 64 * 1024];
    while remaining > 0 {
        let limit = usize::try_from(remaining.min(buffer.len() as u64)).map_err(network)?;
        let Some(read) = recv.read(&mut buffer[..limit]).await.map_err(network)? else {
            return Err(FileError::Protocol(format!(
                "stream ended with {remaining} bytes missing"
            )));
        };
        output.write_all(&buffer[..read]).await?;
        remaining -= read as u64;
        received += read as u64;
        if let Some(interval_bps) = sampler.sample(received, remaining == 0)
            && let Some(progress) = progress
        {
            progress(file_progress(
                FileTransferDirection::Receiving,
                file_name,
                received,
                expected,
                started,
                interval_bps,
                false,
            ));
        }
    }
    if recv.read(&mut [0_u8; 1]).await.map_err(network)?.is_some() {
        return Err(FileError::Protocol(
            "sender transmitted more bytes than declared".to_owned(),
        ));
    }
    Ok(())
}

struct ProgressSampler {
    last_time: Instant,
    last_bytes: u64,
}

impl ProgressSampler {
    fn new(started: Instant) -> Self {
        Self {
            last_time: started,
            last_bytes: 0,
        }
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "interactive transfer rates are intentionally approximate"
    )]
    fn sample(&mut self, bytes: u64, force: bool) -> Option<f64> {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_time);
        if !force && elapsed < PROGRESS_INTERVAL {
            return None;
        }
        let delta = bytes.saturating_sub(self.last_bytes);
        let bps = if elapsed.is_zero() {
            0.0
        } else {
            delta as f64 * 8.0 / elapsed.as_secs_f64()
        };
        self.last_time = now;
        self.last_bytes = bytes;
        Some(bps)
    }
}

fn file_progress(
    direction: FileTransferDirection,
    file_name: &str,
    transferred_bytes: u64,
    total_bytes: u64,
    started: Instant,
    interval_bps: f64,
    completed: bool,
) -> FileTransferProgress {
    FileTransferProgress {
        direction,
        file_name: file_name.to_owned(),
        transferred_bytes,
        total_bytes,
        elapsed: started.elapsed(),
        interval_bps,
        completed,
    }
}

async fn write_message<T: Serialize>(send: &mut SendStream, message: &T) -> Result<(), FileError> {
    let payload = postcard::to_allocvec(message)?;
    if payload.len() > MAX_HEADER_SIZE {
        return Err(FileError::Protocol("message exceeds 64 KiB".to_owned()));
    }
    let length = u32::try_from(payload.len()).map_err(network)?;
    send.write_all(&length.to_be_bytes())
        .await
        .map_err(network)?;
    send.write_all(&payload).await.map_err(network)
}

async fn read_message<T: DeserializeOwned>(recv: &mut RecvStream) -> Result<T, FileError> {
    let mut length = [0_u8; 4];
    recv.read_exact(&mut length).await.map_err(network)?;
    let length = u32::from_be_bytes(length) as usize;
    if length > MAX_HEADER_SIZE {
        return Err(FileError::Protocol("message exceeds 64 KiB".to_owned()));
    }
    let mut payload = vec![0_u8; length];
    recv.read_exact(&mut payload).await.map_err(network)?;
    Ok(postcard::from_bytes(&payload)?)
}

fn network(error: impl std::fmt::Display) -> FileError {
    FileError::Network(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::{RelayMode, endpoint::presets, protocol::Router};

    #[test]
    fn rejects_path_components() {
        for name in ["", "../secret", "dir/file", "/absolute"] {
            let header = FileHeader {
                version: 1,
                file_name: name.to_owned(),
                file_size: 1,
            };
            assert!(validate_header(&header, 10).is_err(), "{name}");
        }
    }

    #[test]
    fn accepts_plain_file_name() {
        let header = FileHeader {
            version: 1,
            file_name: "report.txt".to_owned(),
            file_size: 10,
        };
        assert_eq!(validate_header(&header, 10).unwrap(), "report.txt");
    }

    #[tokio::test]
    async fn confirmed_file_transfer_reports_progress_and_commits_file() {
        let unique = format!(
            "iroh-file-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        );
        let root = std::env::temp_dir().join(unique);
        let receive_dir = root.join("received");
        tokio::fs::create_dir_all(&receive_dir).await.unwrap();
        let source = root.join("payload.bin");
        let payload = vec![0xA5_u8; 512 * 1024];
        tokio::fs::write(&source, &payload).await.unwrap();

        let (receiver_events, received_progress) = std::sync::mpsc::channel();
        let server = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let router = Router::builder(server.clone())
            .accept(
                FILE_ALPN,
                FileProtocol::new(&receive_dir).on_progress(move |progress| {
                    let _ = receiver_events.send(progress);
                }),
            )
            .spawn();
        let client = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let (sender_events, sender_progress) = std::sync::mpsc::channel();

        let report = send_file_with_progress(&client, server.addr(), &source, move |progress| {
            let _ = sender_events.send(progress);
        })
        .await
        .unwrap();
        assert_eq!(report.received_bytes, payload.len() as u64);
        assert_eq!(
            tokio::fs::read(receive_dir.join("payload.bin"))
                .await
                .unwrap(),
            payload
        );
        assert!(
            sender_progress
                .try_iter()
                .any(|progress| progress.completed)
        );
        assert!(
            received_progress
                .try_iter()
                .any(|progress| progress.completed)
        );

        router.shutdown().await.unwrap();
        client.close().await;
        tokio::fs::remove_dir_all(&root).await.unwrap();
    }
}
