//! Headless `NetBench` and file-transfer server.

use std::{
    fs::OpenOptions,
    io::Write,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    str::FromStr,
    time::Duration,
};

use data_encoding::HEXLOWER;
use iroh::{
    Endpoint, RelayConfig, RelayMap, RelayMode, RelayUrl, SecretKey, endpoint::presets,
    protocol::Router,
};
use iroh_netbench::{NETBENCH_ALPN, NetBenchProtocol};
use tracing_subscriber::EnvFilter;
use transfer_demo::{FILE_ALPN, FileProtocol};

#[derive(Debug)]
struct Options {
    bind: SocketAddr,
    public_ip: Option<IpAddr>,
    key_file: PathBuf,
    download_dir: PathBuf,
    max_file_size: u64,
    relay_url: Option<String>,
    qad_port: u16,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:5000".parse().expect("static socket address"),
            public_ip: None,
            key_file: PathBuf::from("./netbench-server.key"),
            download_dir: PathBuf::from("./downloads"),
            max_file_size: 1024 * 1024 * 1024,
            relay_url: None,
            qad_port: 4000,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();
    let options = parse_args(std::env::args().skip(1))?;
    let secret_key = load_or_create_key(&options.key_file)?;
    tokio::fs::create_dir_all(&options.download_dir).await?;

    let relay_url = options
        .relay_url
        .as_deref()
        .ok_or("--relay-url is required; public Relay fallback is intentionally disabled")?
        .parse::<RelayUrl>()?;
    if relay_url.scheme() != "https" {
        return Err("private Relay URL must use https://".into());
    }
    let relay_token = std::env::var("IROH_NETBENCH_RELAY_TOKEN")
        .map_err(|_| "IROH_NETBENCH_RELAY_TOKEN is required")?;
    if relay_token.len() < 32 {
        return Err("IROH_NETBENCH_RELAY_TOKEN must contain at least 32 characters".into());
    }
    let mut relay_config = RelayConfig::from(relay_url);
    relay_config
        .quic
        .as_mut()
        .expect("RelayConfig::from enables QAD")
        .port = options.qad_port;
    let relay_config = relay_config.with_auth_token(relay_token);
    let relay_mode = RelayMode::Custom(RelayMap::from(relay_config));
    let mut builder = Endpoint::builder(presets::Minimal)
        .clear_address_lookup()
        .relay_mode(relay_mode)
        .secret_key(secret_key)
        .clear_ip_transports()
        .bind_addr(options.bind)?;
    if let Some(public_ip) = options.public_ip {
        builder = builder.external_addr(SocketAddr::new(public_ip, options.bind.port()));
    }
    let endpoint = builder.bind().await?;
    let protocol = NetBenchProtocol::builder()
        .max_concurrent_tests(2)
        .max_test_duration(Duration::from_secs(30))
        .max_parallel_streams(8)
        .build();
    let router = Router::builder(endpoint.clone())
        .accept(
            FILE_ALPN,
            FileProtocol::new(&options.download_dir).max_file_size(options.max_file_size),
        )
        .accept(NETBENCH_ALPN, protocol)
        .spawn();

    let online = tokio::time::timeout(Duration::from_secs(30), endpoint.online())
        .await
        .is_ok();
    if !online {
        router.shutdown().await?;
        return Err(
            "private Relay/QAD did not become online within 30 seconds; refusing to start without the required fallback"
                .into(),
        );
    }
    println!("Iroh Transfer Demo server is running");
    println!("Endpoint ID: {}", endpoint.id());
    println!("UDP bind: {}", options.bind);
    println!(
        "Relay: {}",
        options.relay_url.as_deref().expect("validated Relay URL")
    );
    println!("QAD UDP port: {}", options.qad_port);
    println!("Public address lookup: disabled");
    println!("Relay online: {online}");
    println!("Downloads: {}", options.download_dir.display());
    println!("Share this EndpointAddr JSON with clients:");
    println!("{}", serde_json::to_string(&endpoint.addr())?);
    println!("Press Ctrl+C to stop.");

    tokio::signal::ctrl_c().await?;
    router.shutdown().await?;
    Ok(())
}

fn parse_args(
    mut args: impl Iterator<Item = String>,
) -> Result<Options, Box<dyn std::error::Error>> {
    let mut options = Options::default();
    while let Some(flag) = args.next() {
        let mut value = || {
            args.next()
                .ok_or_else(|| format!("{flag} requires a value"))
        };
        match flag.as_str() {
            "--bind" => options.bind = value()?.parse()?,
            "--public-ip" => options.public_ip = Some(value()?.parse()?),
            "--key-file" => options.key_file = PathBuf::from(value()?),
            "--download-dir" => options.download_dir = PathBuf::from(value()?),
            "--max-file-size" => options.max_file_size = value()?.parse()?,
            "--relay-url" => options.relay_url = Some(value()?),
            "--qad-port" => options.qad_port = value()?.parse()?,
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            unknown => return Err(format!("unknown argument: {unknown}").into()),
        }
    }
    Ok(options)
}

fn print_help() {
    println!(
        "\
Usage: netbench-server [OPTIONS]

Options:
  --bind <ADDR>             UDP bind address (default: 0.0.0.0:5000)
  --public-ip <IP>          Public IP advertised for direct connections
  --key-file <PATH>         Persistent endpoint secret (default: ./netbench-server.key)
  --download-dir <PATH>     Received files (default: ./downloads)
  --max-file-size <BYTES>   Maximum accepted file size (default: 1073741824)
  --relay-url <HTTPS_URL>   Required private Relay URL
  --qad-port <PORT>         Private Relay QAD UDP port (default: 4000)
  -h, --help                Show this help"
    );
}

fn load_or_create_key(path: &Path) -> Result<SecretKey, Box<dyn std::error::Error>> {
    match std::fs::read_to_string(path) {
        Ok(value) => Ok(SecretKey::from_str(value.trim())?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::create_dir_all(parent)?;
            }
            let key = SecretKey::generate();
            let encoded = HEXLOWER.encode(&key.to_bytes());
            let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
            file.write_all(encoded.as_bytes())?;
            file.write_all(b"\n")?;
            set_private_permissions(path)?;
            Ok(key)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(unix)]
fn set_private_permissions(path: &Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_private_permissions(_path: &Path) -> Result<(), std::io::Error> {
    Ok(())
}
