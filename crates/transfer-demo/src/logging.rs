//! Persistent desktop diagnostics stored beside the distributed program.

use std::{
    fs::{self, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{Mutex, OnceLock},
};

use tracing_subscriber::EnvFilter;

static LOG_FILE: OnceLock<PathBuf> = OnceLock::new();

/// Initializes the desktop tracing subscriber and returns `log/client.log`.
///
/// On macOS the directory is created beside `Iroh Transfer Demo.app`, rather than inside the
/// signed bundle. On other platforms it is created beside the executable.
///
/// # Errors
///
/// Returns an error when the executable directory cannot be resolved, the log directory or
/// file cannot be created, permissions cannot be applied, or another global subscriber exists.
pub fn init_desktop_file_logging() -> io::Result<PathBuf> {
    if let Some(path) = LOG_FILE.get() {
        return Ok(path.clone());
    }

    let log_dir = desktop_log_dir()?;
    fs::create_dir_all(&log_dir)?;
    set_directory_permissions(&log_dir)?;
    let log_file = log_dir.join("client.log");
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log_file)?;
    set_file_permissions(&log_file)?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info,quinn=warn,rustls=warn,hickory=warn"))
        .with_ansi(false)
        .with_target(true)
        .with_thread_names(true)
        .with_writer(Mutex::new(file))
        .try_init()
        .map_err(|error| io::Error::other(format!("initialize file logger: {error}")))?;

    let _ = LOG_FILE.set(log_file.clone());
    tracing::info!(
        target: "transfer_demo::diagnostics",
        application_version = env!("CARGO_PKG_VERSION"),
        protocol_version = iroh_netbench::PROTOCOL_VERSION,
        os = std::env::consts::OS,
        arch = std::env::consts::ARCH,
        "desktop diagnostics started"
    );
    Ok(log_file)
}

/// Returns the initialized desktop log path.
#[must_use]
pub fn desktop_log_path() -> Option<&'static Path> {
    LOG_FILE.get().map(PathBuf::as_path)
}

fn desktop_log_dir() -> io::Result<PathBuf> {
    let executable = std::env::current_exe()?;
    let executable_dir = executable
        .parent()
        .ok_or_else(|| io::Error::other("executable has no parent directory"))?;

    #[cfg(target_os = "macos")]
    if executable_dir
        .file_name()
        .is_some_and(|name| name == "MacOS")
        && let Some(contents_dir) = executable_dir.parent()
        && contents_dir
            .file_name()
            .is_some_and(|name| name == "Contents")
        && let Some(app_dir) = contents_dir.parent()
        && let Some(install_dir) = app_dir.parent()
    {
        return Ok(install_dir.join("log"));
    }

    Ok(executable_dir.join("log"))
}

#[cfg(unix)]
fn set_directory_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o700))
}

#[cfg(not(unix))]
fn set_directory_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}

#[cfg(unix)]
fn set_file_permissions(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(path, fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_file_permissions(_path: &Path) -> io::Result<()> {
    Ok(())
}
