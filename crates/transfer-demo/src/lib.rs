//! Minimal single-file transfer protocol used to host the benchmark demo.

#[cfg(feature = "gui")]
mod app;
#[cfg(feature = "gui")]
mod logging;
mod protocol;

#[cfg(feature = "gui")]
pub use app::DemoApp;
#[cfg(feature = "gui")]
pub use logging::{desktop_log_path, init_desktop_file_logging};
pub use protocol::{
    FILE_ALPN, FileError, FileHeader, FileProtocol, FileResponse, FileTransferDirection,
    FileTransferProgress, FileTransferReport, send_file, send_file_with_progress,
};
