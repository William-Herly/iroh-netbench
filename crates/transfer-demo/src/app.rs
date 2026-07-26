//! Minimal egui desktop interface backed by a long-lived iroh endpoint.

use std::{
    collections::{HashMap, VecDeque},
    fs::{self, OpenOptions},
    io::Write,
    path::PathBuf,
    str::FromStr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use data_encoding::HEXLOWER;
use eframe::egui::{self, Color32, RichText};
use iroh::{
    EndpointAddr, RelayConfig, RelayMap, RelayMode, RelayUrl, SecretKey, TransportAddr,
    address_lookup::{AddrFilter, PkarrPublisher, PkarrResolver},
    endpoint::{Connection, RecvStream, SendStream, presets},
    protocol::{AcceptError, ProtocolHandler, Router},
    tls::CaTlsConfig,
};
use iroh_netbench::{
    ConnectionInfo, ConnectionStage, NetBenchClient, NetBenchConfig, NetBenchEvent,
    NetBenchProtocol, NetBenchReport, PathKind, SessionMonitor,
    wire::{PROBE_MAGIC, Probe, ProbeKind},
};
use rustls_pki_types::{CertificateDer, pem::PemObject};
use serde::Deserialize;

use crate::{
    FileProtocol, FileTransferDirection, FileTransferProgress, desktop_log_path,
    protocol::send_file_with_progress_observed,
};

const MAX_LOG_LINES: usize = 100;
const SESSION_ALPN: &[u8] = b"/demo/session/1";
const SESSION_VERSION: u8 = 1;
static NEXT_SESSION_PROBE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
struct PrivateInfraConfig {
    discovery_url: url::Url,
    relay_mode: RelayMode,
    ca_tls_config: CaTlsConfig,
    connection_policy: ConnectionPolicy,
}

#[derive(Debug, Deserialize)]
struct BundledInfraConfig {
    discovery_url: String,
    relay_url: String,
    qad_port: u16,
    relay_token: String,
    ca_file: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnectionPolicy {
    Automatic,
    DirectOnly,
    RelayOnly,
}

impl ConnectionPolicy {
    const fn label(self) -> &'static str {
        match self {
            Self::Automatic => "普通模式（Direct 优先，失败回退 Relay）",
            Self::DirectOnly => "Direct Only（禁止 Relay）",
            Self::RelayOnly => "Relay Only（禁止 Direct/打洞）",
        }
    }

    const fn short_label(self) -> &'static str {
        match self {
            Self::Automatic => "普通模式",
            Self::DirectOnly => "Direct Only",
            Self::RelayOnly => "Relay Only",
        }
    }

    const fn requires_relay(self) -> bool {
        !matches!(self, Self::DirectOnly)
    }

    const fn path_stabilization_timeout(self) -> Duration {
        match self {
            Self::RelayOnly => Duration::ZERO,
            Self::Automatic | Self::DirectOnly => Duration::from_secs(3),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ConnectionState {
    Disconnected,
    Connecting,
    Connected { peer: String },
    Failed(String),
}

impl ConnectionState {
    fn label(&self) -> &str {
        match self {
            Self::Disconnected => "未连接",
            Self::Connecting => "正在连接…",
            Self::Connected { .. } => "已连接",
            Self::Failed(_) => "连接失败",
        }
    }

    const fn color(&self) -> Color32 {
        match self {
            Self::Disconnected => Color32::GRAY,
            Self::Connecting => Color32::YELLOW,
            Self::Connected { .. } => Color32::LIGHT_GREEN,
            Self::Failed(_) => Color32::LIGHT_RED,
        }
    }

    const fn is_connected(&self) -> bool {
        matches!(self, Self::Connected { .. })
    }

    const fn is_busy(&self) -> bool {
        matches!(self, Self::Connecting)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchPreset {
    Quick,
    Standard,
}

impl BenchPreset {
    const fn label(self) -> &'static str {
        match self {
            Self::Quick => "Quick（约 22 秒）",
            Self::Standard => "Standard（约 37 秒）",
        }
    }

    fn config(self) -> NetBenchConfig {
        match self {
            Self::Quick => NetBenchConfig::quick(),
            Self::Standard => NetBenchConfig::standard(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchPhase {
    Connecting,
    Latency,
    Loss,
    DownloadWarmup,
    Download,
    UploadWarmup,
    Upload,
    Finishing,
}

impl BenchPhase {
    const fn label(self) -> &'static str {
        match self {
            Self::Connecting => "建立测速连接",
            Self::Latency => "Datagram 空闲延迟",
            Self::Loss => "Datagram 探测超时",
            Self::DownloadWarmup => "Stream 下载预热",
            Self::Download => "Stream 下载测速",
            Self::UploadWarmup => "Stream 上传预热",
            Self::Upload => "Stream 上传测速",
            Self::Finishing => "生成报告",
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum BenchState {
    Idle,
    Running {
        phase: BenchPhase,
        phase_started: Instant,
        config: NetBenchConfig,
    },
    Finished,
    Failed(String),
}

impl BenchState {
    const fn is_running(&self) -> bool {
        matches!(self, Self::Running { .. })
    }

    fn progress(&self) -> Option<(f32, &'static str)> {
        let Self::Running {
            phase,
            phase_started,
            config,
        } = self
        else {
            return None;
        };
        let phases = [
            (BenchPhase::Connecting, config.path_stabilization_timeout),
            (BenchPhase::Latency, config.latency_duration),
            (
                BenchPhase::Loss,
                config.loss_duration + config.probe_timeout,
            ),
            (BenchPhase::DownloadWarmup, config.download_warmup),
            (BenchPhase::Download, config.download_duration),
            (BenchPhase::UploadWarmup, config.upload_warmup),
            (BenchPhase::Upload, config.upload_duration),
        ];
        let total = phases
            .iter()
            .map(|(_, duration)| *duration)
            .sum::<Duration>()
            .as_secs_f32()
            .max(0.001);
        let completed = phases
            .iter()
            .take_while(|(candidate, _)| candidate != phase)
            .map(|(_, duration)| *duration)
            .sum::<Duration>();
        let phase_duration = phases
            .iter()
            .find(|(candidate, _)| candidate == phase)
            .map_or(Duration::ZERO, |(_, duration)| *duration);
        let progress = if *phase == BenchPhase::Finishing {
            0.99
        } else {
            (completed + phase_started.elapsed().min(phase_duration)).as_secs_f32() / total
        };
        Some((progress.clamp(0.0, 0.99), phase.label()))
    }
}

#[derive(Debug, Clone)]
enum FileTransferState {
    Idle,
    Running(FileTransferProgress),
    Finished(FileTransferProgress),
    Failed(String),
}

impl FileTransferState {
    const fn is_running(&self) -> bool {
        matches!(self, Self::Running(_))
    }
}

#[derive(Debug, Default)]
struct LiveMetrics {
    path: Option<PathKind>,
    latest_rtt: Option<Duration>,
    probe_sent: u64,
    probe_received: u64,
    probe_timed_out: u64,
    download_bps: Option<f64>,
    upload_bps: Option<f64>,
    quic_lost_packets: u64,
    quic_tx_datagrams: u64,
    active_connections: usize,
}

impl LiveMetrics {
    fn reset_probe_measurements(&mut self) {
        self.probe_sent = 0;
        self.probe_received = 0;
        self.probe_timed_out = 0;
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "live UI ratios are intentionally approximate f64 values"
    )]
    fn timeout_ratio(&self) -> Option<f64> {
        (self.probe_sent != 0).then(|| self.probe_timed_out as f64 / self.probe_sent as f64)
    }

    #[allow(
        clippy::cast_precision_loss,
        reason = "live UI ratios are intentionally approximate f64 values"
    )]
    fn quic_loss_ratio(&self) -> Option<f64> {
        (self.quic_tx_datagrams != 0).then(|| {
            (self.quic_lost_packets as f64 / self.quic_tx_datagrams as f64).clamp(0.0, 1.0)
        })
    }
}

enum WorkerCommand {
    Connect(String),
    Disconnect,
    Benchmark {
        peer: String,
        config: NetBenchConfig,
    },
    SendFile {
        peer: String,
        path: PathBuf,
    },
    Shutdown,
}

enum WorkerEvent {
    Ready {
        endpoint_id: String,
        download_dir: PathBuf,
    },
    ConnectionProgress(ConnectionStage),
    Connected {
        peer: String,
        info: ConnectionInfo,
    },
    InboundConnected {
        peer: String,
        path: PathKind,
    },
    InboundPathChanged {
        peer: String,
        path: PathKind,
    },
    SessionClosed {
        peer: String,
    },
    SessionProbeStats {
        peer: String,
        latest_rtt: Option<Duration>,
        sent: u64,
        received: u64,
        timed_out: u64,
    },
    SessionTraffic {
        download_bps: f64,
        upload_bps: f64,
        lost_packets: u64,
        tx_datagrams: u64,
        active_connections: usize,
    },
    ConnectFailed(String),
    BenchmarkEvent(NetBenchEvent),
    BenchmarkFinished(NetBenchReport),
    BenchmarkFailed(String),
    FileProgress(FileTransferProgress),
    FileSent {
        bytes: u64,
        duration: Duration,
    },
    FileFailed(String),
    Fatal(String),
}

/// Lightweight desktop UI for connection, one-file transfer, and `NetBench` actions.
pub struct DemoApp {
    worker_started: bool,
    discovery_url: String,
    relay_url: String,
    qad_port: String,
    relay_token: String,
    ca_cert_path: String,
    bundled_infra: bool,
    infrastructure_status: String,
    connection_policy: ConnectionPolicy,
    own_addr: String,
    identity_copy_status: String,
    download_dir: PathBuf,
    peer_addr: String,
    file_path: String,
    connection: ConnectionState,
    preset: BenchPreset,
    benchmark: BenchState,
    file_transfer: FileTransferState,
    live: LiveMetrics,
    last_report: Option<NetBenchReport>,
    logs: VecDeque<String>,
    commands: mpsc::Sender<WorkerCommand>,
    events: mpsc::Receiver<WorkerEvent>,
}

impl DemoApp {
    /// Creates the UI. The network endpoint starts only after Relay selection.
    #[must_use]
    pub fn new(context: &eframe::CreationContext<'_>) -> Self {
        install_cjk_font(&context.egui_ctx);
        context.egui_ctx.set_theme(egui::Theme::Light);
        let mut app = Self::default();
        match load_bundled_infrastructure() {
            Ok((config, config_dir)) => {
                app.discovery_url = config.discovery_url;
                app.relay_url = config.relay_url;
                app.qad_port = config.qad_port.to_string();
                app.relay_token = config.relay_token;
                app.ca_cert_path = config_dir.join(config.ca_file).display().to_string();
                app.bundled_infra = true;
                "已从客户端配置安全加载".clone_into(&mut app.infrastructure_status);
                app.push_log("已加载随客户端分发的私有基础设施配置");
            }
            Err(error) => {
                app.infrastructure_status = format!("未找到客户端配置：{error}");
                app.push_log(app.infrastructure_status.clone());
            }
        }
        if let Some(path) = desktop_log_path() {
            app.push_log(format!("诊断日志：{}", path.display()));
        }
        app
    }

    fn push_log(&mut self, message: impl Into<String>) {
        let mut message = message.into();
        if !self.relay_token.is_empty() {
            message = message.replace(&self.relay_token, "<redacted>");
        }
        tracing::info!(target: "transfer_demo::ui", message = %message);
        if self.logs.len() == MAX_LOG_LINES {
            self.logs.pop_front();
        }
        self.logs.push_back(message);
    }

    fn connect_clicked(&mut self) {
        let peer = self.peer_addr.trim().to_owned();
        if peer.is_empty() {
            self.connection = ConnectionState::Failed("请输入 Endpoint ID".to_owned());
            self.push_log("连接失败：Endpoint ID 不能为空");
            return;
        }
        self.connection = ConnectionState::Connecting;
        self.live.path = None;
        self.push_log("正在验证对端 NetBench 协议…");
        if self.commands.send(WorkerCommand::Connect(peer)).is_err() {
            self.connection = ConnectionState::Failed("网络线程已停止".to_owned());
        }
    }

    fn start_endpoint_clicked(&mut self) {
        let discovery_value = self.discovery_url.trim().trim_end_matches('/');
        let discovery_url = match url::Url::parse(discovery_value) {
            Ok(url) => url,
            Err(error) => {
                self.push_log(format!("启动失败：Discovery URL 无效：{error}"));
                return;
            }
        };
        if discovery_url.scheme() != "https" || !discovery_url.path().ends_with("/pkarr") {
            self.push_log("启动失败：Discovery 必须是 https://.../pkarr");
            return;
        }
        let ca_cert_path = PathBuf::from(self.ca_cert_path.trim());
        if ca_cert_path.as_os_str().is_empty() {
            self.push_log("启动失败：请选择私有 CA 根证书（ca.crt）");
            return;
        }
        let ca_tls_config = match load_private_ca(&ca_cert_path) {
            Ok(config) => config,
            Err(error) => {
                self.push_log(format!("启动失败：{error}"));
                return;
            }
        };
        let (relay_mode, relay_summary) = if self.connection_policy.requires_relay() {
            let relay_url = match self.relay_url.trim().parse::<RelayUrl>() {
                Ok(url) => url,
                Err(error) => {
                    self.push_log(format!("启动失败：Relay URL 无效：{error}"));
                    return;
                }
            };
            if relay_url.scheme() != "https" {
                self.push_log("启动失败：安全模式只接受 https:// Relay URL");
                return;
            }
            let qad_port = match self.qad_port.trim().parse::<u16>() {
                Ok(port) if port != 0 => port,
                _ => {
                    self.push_log("启动失败：QAD UDP 端口必须是 1–65535");
                    return;
                }
            };
            let token = self.relay_token.trim();
            if token.len() < 32 {
                self.push_log("启动失败：Relay 访问令牌至少需要 32 个字符");
                return;
            }
            let mut relay_config = RelayConfig::from(relay_url);
            relay_config
                .quic
                .as_mut()
                .expect("RelayConfig::from enables QAD")
                .port = qad_port;
            let relay_config = relay_config.with_auth_token(token);
            (
                RelayMode::Custom(RelayMap::from(relay_config)),
                format!("{}，QAD UDP={qad_port}", self.relay_url.trim()),
            )
        } else {
            (RelayMode::Disabled, "已禁用".to_owned())
        };
        let infra = PrivateInfraConfig {
            discovery_url,
            relay_mode,
            ca_tls_config,
            connection_policy: self.connection_policy,
        };

        let (command_tx, command_rx) = mpsc::channel();
        let (event_tx, event_rx) = mpsc::channel();
        self.commands = command_tx;
        self.events = event_rx;
        self.worker_started = true;
        self.push_log(format!(
            "策略={}；Discovery={}，Relay={}，CA={}；公共基础设施已禁用",
            self.connection_policy.short_label(),
            discovery_value,
            relay_summary,
            ca_cert_path.display()
        ));
        spawn_network_worker(command_rx, event_tx, infra);
    }

    fn disconnect_clicked(&mut self) {
        let _ = self.commands.send(WorkerCommand::Disconnect);
        self.connection = ConnectionState::Disconnected;
        self.benchmark = BenchState::Idle;
        self.last_report = None;
        self.live = LiveMetrics::default();
        self.push_log("已清除当前对端；本机 Endpoint 继续运行");
    }

    fn send_clicked(&mut self) {
        let Some(peer) = self.connected_peer() else {
            return;
        };
        let path = PathBuf::from(self.file_path.trim());
        if path.as_os_str().is_empty() {
            self.push_log("发送失败：请先输入文件路径");
            return;
        }
        let file_name = path
            .file_name()
            .map_or_else(|| "<unknown>".into(), |name| name.to_string_lossy());
        self.push_log(format!("开始发送文件 {file_name}"));
        self.file_transfer = FileTransferState::Idle;
        let _ = self.commands.send(WorkerCommand::SendFile { peer, path });
    }

    fn benchmark_clicked(&mut self) {
        let Some(peer) = self.connected_peer() else {
            return;
        };
        let mut config = self.preset.config();
        if self.connection_policy == ConnectionPolicy::RelayOnly {
            config.path_stabilization_timeout = Duration::ZERO;
        }
        self.last_report = None;
        self.live.reset_probe_measurements();
        self.benchmark = BenchState::Running {
            phase: BenchPhase::Connecting,
            phase_started: Instant::now(),
            config: config.clone(),
        };
        self.push_log(format!(
            "开始 {}：{} 条 Stream，{} KiB Chunk",
            self.preset.label(),
            config.parallel_streams,
            config.chunk_size / 1024
        ));
        let _ = self
            .commands
            .send(WorkerCommand::Benchmark { peer, config });
    }

    fn connected_peer(&self) -> Option<String> {
        match &self.connection {
            ConnectionState::Connected { peer } => Some(peer.clone()),
            _ => None,
        }
    }

    fn apply_inbound_connected(&mut self, peer: String, path: PathKind) {
        self.peer_addr.clone_from(&peer);
        self.live.path = Some(path);
        self.connection = ConnectionState::Connected { peer };
        self.push_log(format!("对端已连接本机：当前路径 {path:?}"));
    }

    fn apply_inbound_path_changed(&mut self, peer: &str, path: PathKind) {
        if self.connected_peer().as_deref() == Some(peer) {
            self.live.path = Some(path);
            self.push_log(format!("连接路径已切换为 {path:?}"));
        }
    }

    fn apply_session_closed(&mut self, peer: &str) {
        if self.connected_peer().as_deref() == Some(peer) {
            self.connection = ConnectionState::Disconnected;
            self.live = LiveMetrics::default();
            self.push_log("对端已断开连接");
        }
    }

    fn set_benchmark_phase(&mut self, phase: BenchPhase) {
        if let BenchState::Running {
            phase: current,
            phase_started,
            ..
        } = &mut self.benchmark
            && *current != phase
        {
            *current = phase;
            *phase_started = Instant::now();
            self.push_log(format!("测速阶段：{}", phase.label()));
        }
    }

    #[allow(
        clippy::too_many_lines,
        reason = "the exhaustive UI event dispatcher is clearest as one match"
    )]
    fn drain_worker_events(&mut self) {
        while let Ok(event) = self.events.try_recv() {
            match event {
                WorkerEvent::Ready {
                    endpoint_id,
                    download_dir,
                } => {
                    self.own_addr = endpoint_id;
                    self.download_dir = download_dir;
                    self.push_log("本机 Endpoint 已启动，地址正发布到私有 Discovery");
                }
                WorkerEvent::Connected { peer, info } => {
                    self.live.path = Some(info.path);
                    self.connection = ConnectionState::Connected { peer };
                    self.push_log(format!(
                        "连接成功：{:?}，耗时 {:.1} ms",
                        info.path,
                        info.connect_time.as_secs_f64() * 1000.0
                    ));
                }
                WorkerEvent::InboundConnected { peer, path } => {
                    self.apply_inbound_connected(peer, path);
                }
                WorkerEvent::InboundPathChanged { peer, path } => {
                    self.apply_inbound_path_changed(&peer, path);
                }
                WorkerEvent::SessionClosed { peer } => {
                    self.apply_session_closed(&peer);
                }
                WorkerEvent::SessionProbeStats {
                    peer,
                    latest_rtt,
                    sent,
                    received,
                    timed_out,
                } => {
                    if self.connected_peer().as_deref() == Some(peer.as_str()) {
                        if let Some(rtt) = latest_rtt {
                            self.live.latest_rtt = Some(rtt);
                        }
                        if !self.benchmark.is_running() {
                            self.live.probe_sent = sent;
                            self.live.probe_received = received;
                            self.live.probe_timed_out = timed_out;
                        }
                    }
                }
                WorkerEvent::SessionTraffic {
                    download_bps,
                    upload_bps,
                    lost_packets,
                    tx_datagrams,
                    active_connections,
                } => {
                    if self.connection.is_connected() {
                        self.live.download_bps = Some(download_bps);
                        self.live.upload_bps = Some(upload_bps);
                        self.live.quic_lost_packets = lost_packets;
                        self.live.quic_tx_datagrams = tx_datagrams;
                        self.live.active_connections = active_connections;
                    }
                }
                WorkerEvent::ConnectionProgress(stage) => {
                    self.apply_connection_stage(&stage);
                    self.push_log(connection_stage_text(&stage, self.connection_policy));
                }
                WorkerEvent::ConnectFailed(reason) => {
                    self.connection = ConnectionState::Failed(reason.clone());
                    self.push_log(format!("连接失败：{reason}"));
                }
                WorkerEvent::BenchmarkEvent(event) => {
                    self.apply_benchmark_event(&event);
                    match &event {
                        NetBenchEvent::DownloadSample(sample) => tracing::info!(
                            target: "transfer_demo::throughput",
                            direction = "download",
                            elapsed_ms = sample.elapsed.as_millis(),
                            bytes = sample.received_bytes,
                            interval_mbps = sample.interval_bps / 1_000_000.0,
                            "Stream throughput sample"
                        ),
                        NetBenchEvent::UploadSample(sample) => tracing::info!(
                            target: "transfer_demo::throughput",
                            direction = "upload",
                            elapsed_ms = sample.elapsed.as_millis(),
                            bytes = sample.received_bytes,
                            interval_mbps = sample.interval_bps / 1_000_000.0,
                            "Stream throughput sample"
                        ),
                        _ => {}
                    }
                    if let NetBenchEvent::ConnectionStage(connection_stage) = &event {
                        self.push_log(connection_stage_text(
                            connection_stage,
                            self.connection_policy,
                        ));
                    }
                    let phase = match event {
                        NetBenchEvent::Connecting => Some(BenchPhase::Connecting),
                        NetBenchEvent::LatencyStarted => Some(BenchPhase::Latency),
                        NetBenchEvent::LossStarted => Some(BenchPhase::Loss),
                        NetBenchEvent::DownloadWarmupStarted => Some(BenchPhase::DownloadWarmup),
                        NetBenchEvent::DownloadStarted => Some(BenchPhase::Download),
                        NetBenchEvent::UploadWarmupStarted => Some(BenchPhase::UploadWarmup),
                        NetBenchEvent::UploadStarted => Some(BenchPhase::Upload),
                        NetBenchEvent::Finished(_) => Some(BenchPhase::Finishing),
                        NetBenchEvent::ConnectionStage(_)
                        | NetBenchEvent::Connected(_)
                        | NetBenchEvent::LatencySample(_)
                        | NetBenchEvent::LossSample(_)
                        | NetBenchEvent::DownloadSample(_)
                        | NetBenchEvent::UploadSample(_) => None,
                    };
                    if let Some(phase) = phase {
                        self.set_benchmark_phase(phase);
                    }
                }
                WorkerEvent::BenchmarkFinished(report) => {
                    self.live.path = Some(report.connection.path);
                    self.live.latest_rtt = Some(report.idle_latency.p50);
                    self.live.probe_sent = report.loss.sent;
                    self.live.probe_received = report.loss.received;
                    self.live.probe_timed_out = report.loss.timed_out;
                    self.push_log(format!(
                        "测速完成：下载 {:.1} Mbps，上传 {:.1} Mbps",
                        report.download.bits_per_second / 1_000_000.0,
                        report.upload.bits_per_second / 1_000_000.0
                    ));
                    self.last_report = Some(report);
                    self.benchmark = BenchState::Finished;
                }
                WorkerEvent::BenchmarkFailed(reason) => {
                    self.benchmark = BenchState::Failed(reason.clone());
                    self.push_log(format!("测速失败：{reason}"));
                }
                WorkerEvent::FileProgress(progress) => {
                    tracing::info!(
                        target: "transfer_demo::file",
                        direction = ?progress.direction,
                        file_name = %progress.file_name,
                        bytes = progress.transferred_bytes,
                        total_bytes = progress.total_bytes,
                        interval_mbps = progress.interval_bps / 1_000_000.0,
                        completed = progress.completed,
                        "file transfer progress"
                    );
                    self.file_transfer = if progress.completed {
                        FileTransferState::Finished(progress)
                    } else {
                        FileTransferState::Running(progress)
                    };
                }
                WorkerEvent::FileSent { bytes, duration } => {
                    self.push_log(format!(
                        "文件发送完成：{bytes} bytes，{:.2} 秒",
                        duration.as_secs_f64()
                    ));
                }
                WorkerEvent::FileFailed(reason) => {
                    self.file_transfer = FileTransferState::Failed(reason.clone());
                    self.push_log(format!("文件发送失败：{reason}"));
                }
                WorkerEvent::Fatal(reason) => {
                    self.worker_started = false;
                    self.connection = ConnectionState::Failed(reason.clone());
                    self.push_log(format!("Endpoint 启动失败：{reason}"));
                }
            }
        }
    }

    fn apply_connection_stage(&mut self, stage: &ConnectionStage) {
        match stage {
            ConnectionStage::PathObserved { path } => self.live.path = Some(*path),
            ConnectionStage::DirectPathSelected { .. } => {
                self.live.path = Some(PathKind::Direct);
            }
            ConnectionStage::RelayFallback { .. } => {
                self.live.path = Some(PathKind::Relay);
            }
            _ => {}
        }
    }

    fn apply_benchmark_event(&mut self, event: &NetBenchEvent) {
        match event {
            NetBenchEvent::Connecting => self.live.reset_probe_measurements(),
            NetBenchEvent::ConnectionStage(stage) => self.apply_connection_stage(stage),
            NetBenchEvent::Connected(info) => self.live.path = Some(info.path),
            NetBenchEvent::LatencySample(sample) => self.live.latest_rtt = Some(sample.rtt),
            NetBenchEvent::LossSample(sample) => {
                self.live.probe_sent = sample.sent;
                self.live.probe_received = sample.received;
                self.live.probe_timed_out = sample.timed_out;
            }
            NetBenchEvent::Finished(report) => {
                self.live.path = Some(report.connection.path);
            }
            NetBenchEvent::DownloadWarmupStarted
            | NetBenchEvent::DownloadStarted
            | NetBenchEvent::DownloadSample(_)
            | NetBenchEvent::UploadWarmupStarted
            | NetBenchEvent::UploadStarted
            | NetBenchEvent::UploadSample(_)
            | NetBenchEvent::LatencyStarted
            | NetBenchEvent::LossStarted => {}
        }
    }

    fn identity_panel(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("本机节点");
            if self.own_addr.is_empty() {
                ui.spinner();
                ui.label("正在启动 Endpoint…");
            } else {
                ui.add(
                    egui::TextEdit::singleline(&mut self.own_addr)
                        .desired_width(f32::INFINITY)
                        .interactive(false),
                );
                if ui
                    .add_sized([140.0, 32.0], egui::Button::new("复制本机 ID"))
                    .clicked()
                {
                    ui.ctx().copy_text(self.own_addr.clone());
                    "已复制到剪贴板".clone_into(&mut self.identity_copy_status);
                    self.push_log("本机 Endpoint ID 已复制到剪贴板");
                }
                if !self.identity_copy_status.is_empty() {
                    ui.colored_label(Color32::DARK_GREEN, &self.identity_copy_status);
                }
                ui.small("这是稳定的 Endpoint ID；把它通过可信渠道发给对端。");
                ui.label(format!(
                    "连接策略：{}",
                    self.connection_policy.short_label()
                ));
                ui.label(format!("接收文件目录：{}", self.download_dir.display()));
            }
        });
    }

    fn relay_panel(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("启动私有网络");
            ui.colored_label(
                Color32::DARK_GREEN,
                "只使用指定的私有 Discovery、Relay 和 QAD，不访问 n0/iroh.link。",
            );
            ui.label("连接策略");
            egui::ComboBox::from_id_salt("connection_policy")
                .selected_text(self.connection_policy.label())
                .width(ui.available_width())
                .show_ui(ui, |ui| {
                    ui.selectable_value(
                        &mut self.connection_policy,
                        ConnectionPolicy::Automatic,
                        ConnectionPolicy::Automatic.label(),
                    );
                    ui.selectable_value(
                        &mut self.connection_policy,
                        ConnectionPolicy::DirectOnly,
                        ConnectionPolicy::DirectOnly.label(),
                    );
                    ui.selectable_value(
                        &mut self.connection_policy,
                        ConnectionPolicy::RelayOnly,
                        ConnectionPolicy::RelayOnly.label(),
                    );
                });
            ui.label("私有 Discovery HTTPS URL");
            ui.add(
                egui::TextEdit::singleline(&mut self.discovery_url)
                    .hint_text("https://203.0.113.10:3478/pkarr")
                    .interactive(!self.bundled_infra)
                    .desired_width(f32::INFINITY),
            );
            ui.add_enabled_ui(self.connection_policy.requires_relay(), |ui| {
                ui.label("自建 Relay HTTPS URL");
                ui.add(
                    egui::TextEdit::singleline(&mut self.relay_url)
                        .hint_text("https://203.0.113.10:3479")
                        .interactive(!self.bundled_infra)
                        .desired_width(f32::INFINITY),
                );
                ui.horizontal(|ui| {
                    ui.label("QAD UDP 端口");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.qad_port)
                            .interactive(!self.bundled_infra)
                            .desired_width(100.0),
                    );
                });
                if self.bundled_infra {
                    ui.label("Relay 访问令牌：已配置（明文不显示）");
                } else {
                    ui.label("Relay 访问令牌");
                    ui.add(
                        egui::TextEdit::singleline(&mut self.relay_token)
                            .password(true)
                            .hint_text("至少 32 个随机字符")
                            .desired_width(f32::INFINITY),
                    );
                }
            });
            ui.label("私有 CA 根证书路径");
            ui.add(
                egui::TextEdit::singleline(&mut self.ca_cert_path)
                    .hint_text("/path/to/ca.crt")
                    .interactive(!self.bundled_infra)
                    .desired_width(f32::INFINITY),
            );
            ui.colored_label(
                if self.bundled_infra {
                    Color32::DARK_GREEN
                } else {
                    Color32::DARK_RED
                },
                &self.infrastructure_status,
            );
            ui.small(
                "无域名时证书必须包含服务器公网 IP 的 SAN。客户端只信任这个 CA；不会关闭 TLS 校验。",
            );
            ui.small(match self.connection_policy {
                ConnectionPolicy::Automatic => {
                    "普通模式：连接优先 Direct，打洞失败只回退此私有 Relay。"
                }
                ConnectionPolicy::DirectOnly => {
                    "Direct Only：不连接任何 Relay；NAT 无法直连时连接会失败。"
                }
                ConnectionPolicy::RelayOnly => {
                    "Relay Only：关闭本机 IP 传输与打洞；业务流量只能经过此私有 Relay。"
                }
            });
            if ui.button("启动 Endpoint").clicked() {
                self.start_endpoint_clicked();
            }
        });
    }

    fn connection_panel(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("连接");
            ui.horizontal(|ui| {
                ui.label("状态：");
                ui.colored_label(
                    self.connection.color(),
                    RichText::new(self.connection.label()).strong(),
                );
                if let ConnectionState::Failed(reason) = &self.connection {
                    ui.label(reason);
                }
            });
            ui.add(
                egui::TextEdit::singleline(&mut self.peer_addr)
                    .hint_text("粘贴对端 Endpoint ID")
                    .desired_width(f32::INFINITY),
            );
            ui.horizontal(|ui| {
                let can_connect = !self.connection.is_busy() && !self.connection.is_connected();
                if ui
                    .add_enabled(can_connect, egui::Button::new("连接"))
                    .clicked()
                {
                    self.connect_clicked();
                }
                if ui
                    .add_enabled(
                        self.connection.is_connected(),
                        egui::Button::new("断开连接"),
                    )
                    .clicked()
                {
                    self.disconnect_clicked();
                }
            });
        });
    }

    fn file_panel(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("单文件传输");
            ui.label(format!("收到的文件保存到：{}", self.download_dir.display()));
            ui.horizontal(|ui| {
                ui.label("文件");
                ui.add(
                    egui::TextEdit::singleline(&mut self.file_path)
                        .hint_text("/path/to/file")
                        .desired_width(f32::INFINITY),
                );
            });
            let can_send = self.connection.is_connected()
                && !self.file_path.trim().is_empty()
                && !self.file_transfer.is_running();
            if ui
                .add_enabled(can_send, egui::Button::new("发送文件"))
                .clicked()
            {
                self.send_clicked();
            }
            match &self.file_transfer {
                FileTransferState::Idle => {}
                FileTransferState::Running(progress) | FileTransferState::Finished(progress) => {
                    #[allow(
                        clippy::cast_precision_loss,
                        reason = "egui progress is intentionally represented as f32"
                    )]
                    let fraction = if progress.total_bytes == 0 {
                        1.0
                    } else {
                        progress.transferred_bytes as f32 / progress.total_bytes as f32
                    };
                    let direction = match progress.direction {
                        FileTransferDirection::Sending => "发送",
                        FileTransferDirection::Receiving => "接收",
                    };
                    let status = if progress.completed {
                        format!("{direction}完成：{}", progress.file_name)
                    } else {
                        format!(
                            "{direction} {} / {} · {:.1} Mbps",
                            format_bytes(progress.transferred_bytes),
                            format_bytes(progress.total_bytes),
                            progress.interval_bps / 1_000_000.0
                        )
                    };
                    ui.add(
                        egui::ProgressBar::new(fraction.clamp(0.0, 1.0))
                            .show_percentage()
                            .text(status),
                    );
                }
                FileTransferState::Failed(reason) => {
                    ui.colored_label(Color32::LIGHT_RED, format!("传输失败：{reason}"));
                }
            }
        });
    }

    fn live_metrics_panel(&self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("实时连接指标");
            egui::Grid::new("live-metrics-grid")
                .num_columns(2)
                .striped(true)
                .show(ui, |ui| {
                    live_metric(
                        ui,
                        "路径",
                        self.live
                            .path
                            .map_or_else(|| "—".to_owned(), |path| path_label(path).to_owned()),
                    );
                    live_metric(
                        ui,
                        "Datagram 实时 RTT",
                        self.live.latest_rtt.map_or_else(
                            || "—".to_owned(),
                            |rtt| format!("{:.1} ms", rtt.as_secs_f64() * 1000.0),
                        ),
                    );
                    live_metric(
                        ui,
                        "会话总下载速率",
                        format_live_rate(self.live.download_bps),
                    );
                    live_metric(
                        ui,
                        "会话总上传速率",
                        format_live_rate(self.live.upload_bps),
                    );
                    live_metric(
                        ui,
                        "会话 QUIC 丢包率",
                        self.live.quic_loss_ratio().map_or_else(
                            || "—".to_owned(),
                            |ratio| format!("{:.2}%", ratio * 100.0),
                        ),
                    );
                    live_metric(
                        ui,
                        "活动 QUIC Connection",
                        self.live.active_connections.to_string(),
                    );
                    live_metric(
                        ui,
                        "Datagram 超时率",
                        self.live.timeout_ratio().map_or_else(
                            || "—".to_owned(),
                            |ratio| format!("{:.2}%", ratio * 100.0),
                        ),
                    );
                    live_metric(
                        ui,
                        "Datagram 探测包",
                        if self.live.probe_sent == 0 {
                            "—".to_owned()
                        } else {
                            format!(
                                "{}/{}（超时 {}）",
                                self.live.probe_received,
                                self.live.probe_sent,
                                self.live.probe_timed_out
                            )
                        },
                    );
                });
            ui.small(
                "会话速率按最近约 1 秒的所有 QUIC Connection 聚合，包含文件、测速和协议开销；延迟来自持续 Datagram 探测。",
            );
        });
    }

    fn benchmark_panel(&mut self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("线路测试");
            ui.horizontal(|ui| {
                ui.selectable_value(
                    &mut self.preset,
                    BenchPreset::Quick,
                    BenchPreset::Quick.label(),
                );
                ui.selectable_value(
                    &mut self.preset,
                    BenchPreset::Standard,
                    BenchPreset::Standard.label(),
                );
            });
            let can_start = self.connection.is_connected() && !self.benchmark.is_running();
            if ui
                .add_enabled(can_start, egui::Button::new("开始线路测试"))
                .clicked()
            {
                self.benchmark_clicked();
            }

            match &self.benchmark {
                BenchState::Idle => {
                    ui.small("连接成功后可运行独立的 NetBench Connection。");
                }
                BenchState::Running { .. } => {
                    if let Some((progress, stage)) = self.benchmark.progress() {
                        ui.add(
                            egui::ProgressBar::new(progress)
                                .show_percentage()
                                .text(stage),
                        );
                    }
                }
                BenchState::Finished => {
                    ui.colored_label(Color32::LIGHT_GREEN, "测速完成");
                }
                BenchState::Failed(reason) => {
                    ui.colored_label(Color32::LIGHT_RED, reason);
                }
            }

            if let Some(report) = &self.last_report {
                egui::Grid::new("report-grid")
                    .num_columns(2)
                    .striped(true)
                    .show(ui, |ui| {
                        metric(ui, "路径", format!("{:?}", report.connection.path));
                        metric(
                            ui,
                            "Datagram P50 RTT",
                            format!("{:.1} ms", report.idle_latency.p50.as_secs_f64() * 1000.0),
                        );
                        metric(
                            ui,
                            "Datagram P95 RTT",
                            format!("{:.1} ms", report.idle_latency.p95.as_secs_f64() * 1000.0),
                        );
                        metric(
                            ui,
                            "Datagram 探测超时",
                            format!("{:.2}%", report.loss.timeout_ratio * 100.0),
                        );
                        metric(
                            ui,
                            "Stream 下载",
                            format!("{:.1} Mbps", report.download.bits_per_second / 1_000_000.0),
                        );
                        metric(
                            ui,
                            "Stream 上传",
                            format!("{:.1} Mbps", report.upload.bits_per_second / 1_000_000.0),
                        );
                    });
            }
        });
    }

    fn log_panel(&self, ui: &mut egui::Ui) {
        ui.group(|ui| {
            ui.set_width(ui.available_width());
            ui.heading("事件");
            egui::ScrollArea::vertical()
                .stick_to_bottom(true)
                .max_height(150.0)
                .show(ui, |ui| {
                    if self.logs.is_empty() {
                        ui.weak("等待操作…");
                    }
                    for line in &self.logs {
                        ui.monospace(line);
                    }
                });
        });
    }
}

impl Default for DemoApp {
    fn default() -> Self {
        let (commands, _) = mpsc::channel();
        let (_, events) = mpsc::channel();
        Self {
            worker_started: false,
            discovery_url: String::new(),
            relay_url: String::new(),
            qad_port: "4000".to_owned(),
            relay_token: String::new(),
            ca_cert_path: String::new(),
            bundled_infra: false,
            infrastructure_status: "等待加载客户端配置".to_owned(),
            connection_policy: ConnectionPolicy::Automatic,
            own_addr: String::new(),
            identity_copy_status: String::new(),
            download_dir: default_download_dir(),
            peer_addr: String::new(),
            file_path: String::new(),
            connection: ConnectionState::Disconnected,
            preset: BenchPreset::Standard,
            benchmark: BenchState::Idle,
            file_transfer: FileTransferState::Idle,
            live: LiveMetrics::default(),
            last_report: None,
            logs: VecDeque::new(),
            commands,
            events,
        }
    }
}

impl eframe::App for DemoApp {
    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        // Keep the application readable even when the OS switches to dark mode at runtime.
        ui.ctx().set_theme(egui::Theme::Light);
        *ui.visuals_mut() = egui::Visuals::light();
        ui.painter()
            .rect_filled(ui.max_rect(), 0.0, Color32::from_rgb(248, 249, 251));
        self.drain_worker_events();
        if self.connection.is_connected()
            || self.benchmark.is_running()
            || self.file_transfer.is_running()
        {
            ui.ctx().request_repaint_after(Duration::from_millis(100));
        }

        egui::ScrollArea::vertical().show(ui, |ui| {
            ui.heading("Iroh 文件传输与线路测试");
            ui.label("一个 Endpoint，Session / File / NetBench 独立 Connection");
            ui.add_space(10.0);
            if !self.worker_started {
                self.relay_panel(ui);
                ui.add_space(8.0);
                self.log_panel(ui);
                return;
            }
            self.identity_panel(ui);
            ui.add_space(8.0);
            self.connection_panel(ui);
            ui.add_space(8.0);
            self.live_metrics_panel(ui);
            ui.add_space(8.0);
            self.file_panel(ui);
            ui.add_space(8.0);
            self.benchmark_panel(ui);
            ui.add_space(8.0);
            self.log_panel(ui);
        });
    }
}

impl Drop for DemoApp {
    fn drop(&mut self) {
        let _ = self.commands.send(WorkerCommand::Shutdown);
    }
}

fn load_bundled_infrastructure() -> Result<(BundledInfraConfig, PathBuf), String> {
    let mut directories = Vec::new();
    if let Ok(executable) = std::env::current_exe()
        && let Some(executable_dir) = executable.parent()
    {
        #[cfg(target_os = "macos")]
        if executable_dir
            .file_name()
            .is_some_and(|name| name == "MacOS")
            && let Some(contents_dir) = executable_dir.parent()
        {
            directories.push(contents_dir.join("Resources"));
        }
        directories.push(executable_dir.to_owned());
    }
    if let Ok(current_dir) = std::env::current_dir() {
        directories.push(current_dir);
    }

    for directory in directories {
        let path = directory.join("private-infra.json");
        if !path.is_file() {
            continue;
        }
        let bytes =
            fs::read(&path).map_err(|error| format!("无法读取 {}：{error}", path.display()))?;
        let config = serde_json::from_slice(&bytes)
            .map_err(|error| format!("{} 格式无效：{error}", path.display()))?;
        return Ok((config, directory));
    }
    Err("private-infra.json 不在程序资源目录".to_owned())
}

fn load_private_ca(path: &std::path::Path) -> Result<CaTlsConfig, String> {
    let pem =
        fs::read(path).map_err(|error| format!("无法读取私有 CA {}：{error}", path.display()))?;
    let certificates = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("私有 CA PEM 格式无效：{error}"))?;
    if certificates.is_empty() {
        return Err("私有 CA 文件中没有 CERTIFICATE PEM 块".to_owned());
    }
    Ok(CaTlsConfig::custom_roots(certificates))
}

fn connection_stage_text(stage: &ConnectionStage, policy: ConnectionPolicy) -> String {
    match stage {
        ConnectionStage::PreparingAddress => {
            "连接 1/8：通过私有 Discovery 解析 Endpoint ID".to_owned()
        }
        ConnectionStage::QuicHandshakeStarted => match policy {
            ConnectionPolicy::Automatic => {
                "连接 2/8：开始 QUIC/TLS 握手，尝试私有 Relay 与 Direct 路径".to_owned()
            }
            ConnectionPolicy::DirectOnly => {
                "连接 2/8：开始 QUIC/TLS 握手，仅允许 Direct 路径".to_owned()
            }
            ConnectionPolicy::RelayOnly => {
                "连接 2/8：开始 QUIC/TLS 握手，仅允许私有 Relay 路径".to_owned()
            }
        },
        ConnectionStage::QuicHandshakeCompleted { elapsed } => format!(
            "连接 3/8：QUIC/TLS 身份验证完成（{:.1} ms）",
            elapsed.as_secs_f64() * 1000.0
        ),
        ConnectionStage::ControlStreamOpened => "连接 4/8：控制 Stream 已打开".to_owned(),
        ConnectionStage::ClientHelloSent => "连接 5/8：ClientHello 已发送".to_owned(),
        ConnectionStage::ServerHelloReceived { version } => {
            format!("连接 6/8：ServerHello 已收到，协议版本 {version}")
        }
        ConnectionStage::PathObserved { path } => {
            format!("连接 7/8：当前选中路径 {path:?}，等待 Direct 稳定")
        }
        ConnectionStage::DirectPathSelected { elapsed } => format!(
            "连接 8/8：打洞成功，已切换 Direct（{:.1} ms）",
            elapsed.as_secs_f64() * 1000.0
        ),
        ConnectionStage::RelayFallback { waited } => {
            if policy == ConnectionPolicy::RelayOnly {
                "连接 8/8：Relay Only 策略已锁定私有 Relay，未尝试打洞".to_owned()
            } else {
                format!(
                    "连接 8/8：等待 Direct {:.1} 秒后保持 Relay，连接继续可用",
                    waited.as_secs_f64()
                )
            }
        }
    }
}

fn metric(ui: &mut egui::Ui, label: &str, value: String) {
    ui.label(label);
    ui.strong(value);
    ui.end_row();
}

fn live_metric(ui: &mut egui::Ui, label: &str, value: String) {
    ui.label(label);
    ui.strong(value);
    ui.end_row();
}

fn path_label(path: PathKind) -> &'static str {
    match path {
        PathKind::Direct => "Direct",
        PathKind::DirectIpv4 => "Direct IPv4",
        PathKind::DirectIpv6 => "Direct IPv6",
        PathKind::Relay => "Relay",
        PathKind::Mixed => "Mixed",
        PathKind::Unknown => "Unknown",
    }
}

fn format_live_rate(rate: Option<f64>) -> String {
    rate.map_or_else(
        || "—".to_owned(),
        |bits_per_second| format!("{:.1} Mbps", bits_per_second / 1_000_000.0),
    )
}

#[allow(
    clippy::cast_precision_loss,
    reason = "human-readable byte counts are intentionally approximate"
)]
fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("{:.2} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("{:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("{:.1} KiB", bytes / KIB)
    } else {
        format!("{bytes:.0} B")
    }
}

fn spawn_network_worker(
    commands: mpsc::Receiver<WorkerCommand>,
    events: mpsc::Sender<WorkerEvent>,
    infra: PrivateInfraConfig,
) {
    std::thread::Builder::new()
        .name("iroh-network".to_owned())
        .spawn(move || {
            let runtime = match tokio::runtime::Builder::new_multi_thread()
                .worker_threads(4)
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(error) => {
                    let _ = events.send(WorkerEvent::Fatal(error.to_string()));
                    return;
                }
            };
            runtime.block_on(network_worker(commands, events, infra));
        })
        .expect("failed to spawn iroh network thread");
}

#[allow(
    clippy::too_many_lines,
    reason = "the worker keeps serialized UI commands in one auditable loop"
)]
async fn network_worker(
    commands: mpsc::Receiver<WorkerCommand>,
    events: mpsc::Sender<WorkerEvent>,
    infra: PrivateInfraConfig,
) {
    let secret_key = match load_or_create_desktop_key() {
        Ok(key) => key,
        Err(error) => {
            let _ = events.send(WorkerEvent::Fatal(error));
            return;
        }
    };
    let PrivateInfraConfig {
        discovery_url,
        relay_mode,
        ca_tls_config,
        connection_policy,
    } = infra;
    let builder = iroh::Endpoint::builder(presets::Minimal)
        .ca_tls_config(ca_tls_config)
        .clear_address_lookup()
        .address_lookup(PkarrPublisher::builder(discovery_url.clone()))
        .address_lookup(PkarrResolver::builder(discovery_url))
        .relay_mode(relay_mode)
        .secret_key(secret_key);
    let builder = match connection_policy {
        ConnectionPolicy::Automatic => builder,
        ConnectionPolicy::DirectOnly => builder.addr_filter(AddrFilter::ip_only()),
        ConnectionPolicy::RelayOnly => builder
            .clear_ip_transports()
            .addr_filter(AddrFilter::relay_only()),
    };
    let endpoint = match builder.bind().await {
        Ok(endpoint) => endpoint,
        Err(error) => {
            let _ = events.send(WorkerEvent::Fatal(error.to_string()));
            return;
        }
    };
    let download_dir = default_download_dir();
    let inbound_sessions = Arc::new(Mutex::new(Vec::<Connection>::new()));
    let session_monitor = SessionMonitor::default();
    let file_events = events.clone();
    let inbound_file_monitor = session_monitor.clone();
    let router = Router::builder(endpoint.clone())
        .accept(
            SESSION_ALPN,
            PeerSessionProtocol::new(
                events.clone(),
                Arc::clone(&inbound_sessions),
                session_monitor.clone(),
            ),
        )
        .accept(
            crate::FILE_ALPN,
            FileProtocol::new(&download_dir)
                .max_file_size(1024 * 1024 * 1024)
                .on_connection(move |connection| {
                    inbound_file_monitor.track("file-server", connection);
                })
                .on_progress(move |progress| {
                    let _ = file_events.send(WorkerEvent::FileProgress(progress));
                }),
        )
        .accept(
            iroh_netbench::NETBENCH_ALPN,
            NetBenchProtocol::builder()
                .with_monitor(session_monitor.clone())
                .build(),
        )
        .spawn();
    let mut traffic_updates = session_monitor.subscribe();
    let traffic_events = events.clone();
    let traffic_monitor = tokio::spawn(async move {
        while traffic_updates.changed().await.is_ok() {
            let metrics = traffic_updates.borrow_and_update().clone();
            if traffic_events
                .send(WorkerEvent::SessionTraffic {
                    download_bps: metrics.download_bps,
                    upload_bps: metrics.upload_bps,
                    lost_packets: metrics.recent_lost_packets,
                    tx_datagrams: metrics.recent_tx_datagrams,
                    active_connections: metrics.active_connections,
                })
                .is_err()
            {
                break;
            }
        }
    });

    if connection_policy.requires_relay()
        && tokio::time::timeout(Duration::from_secs(15), endpoint.online())
            .await
            .is_err()
    {
        let _ = events.send(WorkerEvent::Fatal(
            "15 秒内 Relay 未上线；请检查 Relay URL、TLS 证书和网络后重试".to_owned(),
        ));
        let _ = router.shutdown().await;
        return;
    }
    let _ = events.send(WorkerEvent::Ready {
        endpoint_id: endpoint.id().to_string(),
        download_dir,
    });

    let mut peer_session: Option<PeerSession> = None;
    let mut benchmark_task: Option<tokio::task::JoinHandle<()>> = None;
    let mut file_task: Option<tokio::task::JoinHandle<()>> = None;
    loop {
        if benchmark_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            benchmark_task.take();
        }
        if file_task
            .as_ref()
            .is_some_and(tokio::task::JoinHandle::is_finished)
        {
            file_task.take();
        }
        let command = match commands.try_recv() {
            Ok(command) => command,
            Err(mpsc::TryRecvError::Empty) => {
                tokio::time::sleep(Duration::from_millis(20)).await;
                continue;
            }
            Err(mpsc::TryRecvError::Disconnected) => break,
        };
        match command {
            WorkerCommand::Connect(peer_json) => {
                let result = parse_peer(&peer_json);
                match result {
                    Ok(peer) => {
                        let progress_events = events.clone();
                        let check_monitor = session_monitor.clone();
                        match NetBenchClient::new(endpoint.clone())
                            .with_monitor(check_monitor)
                            .check_with_progress_timeout(
                                peer.clone(),
                                connection_policy.path_stabilization_timeout(),
                                move |stage| {
                                    let _ = progress_events
                                        .send(WorkerEvent::ConnectionProgress(stage));
                                },
                            )
                            .await
                        {
                            Ok(info) => {
                                if let Some(previous) = peer_session.take() {
                                    previous.close();
                                }
                                match open_peer_session(
                                    &endpoint,
                                    peer,
                                    &peer_json,
                                    events.clone(),
                                    session_monitor.clone(),
                                )
                                .await
                                {
                                    Ok(session) => {
                                        peer_session = Some(session);
                                        let _ = events.send(WorkerEvent::Connected {
                                            peer: peer_json,
                                            info,
                                        });
                                    }
                                    Err(error) => {
                                        let _ = events.send(WorkerEvent::ConnectFailed(error));
                                    }
                                }
                            }
                            Err(error) => {
                                let _ = events.send(WorkerEvent::ConnectFailed(error.to_string()));
                            }
                        }
                    }
                    Err(error) => {
                        let _ = events.send(WorkerEvent::ConnectFailed(error));
                    }
                }
            }
            WorkerCommand::Disconnect => {
                if let Some(task) = benchmark_task.take() {
                    task.abort();
                }
                if let Some(task) = file_task.take() {
                    task.abort();
                }
                if let Some(session) = peer_session.take() {
                    session.close();
                }
                let sessions = inbound_sessions
                    .lock()
                    .map(|mut sessions| sessions.drain(..).collect::<Vec<_>>())
                    .unwrap_or_default();
                for connection in sessions {
                    connection.close(0_u8.into(), b"user disconnected");
                }
                session_monitor.clear();
            }
            WorkerCommand::Benchmark { peer, config } => {
                if benchmark_task.is_some() {
                    let _ = events.send(WorkerEvent::BenchmarkFailed(
                        "已有线路测试正在运行".to_owned(),
                    ));
                    continue;
                }
                let task_endpoint = endpoint.clone();
                let task_events = events.clone();
                let task_monitor = session_monitor.clone();
                benchmark_task = Some(tokio::spawn(async move {
                    let result = async {
                        let peer = parse_peer(&peer).map_err(std::io::Error::other)?;
                        let mut test = NetBenchClient::new(task_endpoint)
                            .with_monitor(task_monitor)
                            .start(peer, config)
                            .await
                            .map_err(|error| std::io::Error::other(error.to_string()))?;
                        while let Some(event) = test.next().await {
                            let finished = matches!(event, NetBenchEvent::Finished(_));
                            let _ = task_events.send(WorkerEvent::BenchmarkEvent(event));
                            if finished {
                                break;
                            }
                        }
                        test.result()
                            .await
                            .map_err(|error| std::io::Error::other(error.to_string()))
                    }
                    .await;
                    match result {
                        Ok(report) => {
                            let _ = task_events.send(WorkerEvent::BenchmarkFinished(report));
                        }
                        Err(error) => {
                            let _ =
                                task_events.send(WorkerEvent::BenchmarkFailed(error.to_string()));
                        }
                    }
                }));
            }
            WorkerCommand::SendFile { peer, path } => {
                if file_task.is_some() {
                    let _ = events.send(WorkerEvent::FileFailed("已有文件正在传输".to_owned()));
                    continue;
                }
                let task_endpoint = endpoint.clone();
                let task_events = events.clone();
                let task_monitor = session_monitor.clone();
                file_task = Some(tokio::spawn(async move {
                    let result = async {
                        let peer = parse_peer(&peer).map_err(std::io::Error::other)?;
                        let progress_events = task_events.clone();
                        send_file_with_progress_observed(
                            &task_endpoint,
                            peer,
                            &path,
                            move |progress| {
                                let _ = progress_events.send(WorkerEvent::FileProgress(progress));
                            },
                            move |connection| {
                                task_monitor.track("file-client", connection);
                            },
                        )
                        .await
                        .map_err(|error| std::io::Error::other(error.to_string()))
                    }
                    .await;
                    match result {
                        Ok(report) => {
                            let _ = task_events.send(WorkerEvent::FileSent {
                                bytes: report.received_bytes,
                                duration: report.duration,
                            });
                        }
                        Err(error) => {
                            let _ = task_events.send(WorkerEvent::FileFailed(error.to_string()));
                        }
                    }
                }));
            }
            WorkerCommand::Shutdown => {
                if let Some(task) = benchmark_task.take() {
                    task.abort();
                }
                if let Some(task) = file_task.take() {
                    task.abort();
                }
                break;
            }
        }
    }

    traffic_monitor.abort();
    let _ = router.shutdown().await;
}

struct PeerSession {
    connection: Connection,
    _send: SendStream,
    _recv: RecvStream,
    probe_task: tokio::task::JoinHandle<()>,
}

impl PeerSession {
    fn close(self) {
        self.probe_task.abort();
        self.connection.close(0_u8.into(), b"user disconnected");
    }
}

impl Drop for PeerSession {
    fn drop(&mut self) {
        self.probe_task.abort();
    }
}

#[derive(Debug, Clone)]
struct PeerSessionProtocol {
    events: mpsc::Sender<WorkerEvent>,
    sessions: Arc<Mutex<Vec<Connection>>>,
    session_monitor: SessionMonitor,
}

impl PeerSessionProtocol {
    fn new(
        events: mpsc::Sender<WorkerEvent>,
        sessions: Arc<Mutex<Vec<Connection>>>,
        session_monitor: SessionMonitor,
    ) -> Self {
        Self {
            events,
            sessions,
            session_monitor,
        }
    }
}

impl ProtocolHandler for PeerSessionProtocol {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.session_monitor
            .track("session-server", connection.clone());
        let peer = connection.remote_id().to_string();
        let (mut send, mut recv) = connection.accept_bi().await.map_err(session_accept_error)?;
        let mut hello = [0_u8; 1];
        recv.read_exact(&mut hello)
            .await
            .map_err(session_accept_error)?;
        if hello[0] != SESSION_VERSION {
            connection.close(1_u8.into(), b"unsupported session version");
            return Err(session_accept_error("unsupported session version"));
        }
        send.write_all(&[SESSION_VERSION])
            .await
            .map_err(session_accept_error)?;

        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.push(connection.clone());
        }
        let mut previous_path = selected_path_kind(&connection);
        let _ = self.events.send(WorkerEvent::InboundConnected {
            peer: peer.clone(),
            path: previous_path,
        });
        let probe_task = tokio::spawn(run_session_probes(
            connection.clone(),
            peer.clone(),
            self.events.clone(),
        ));
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    let path = selected_path_kind(&connection);
                    if path != previous_path {
                        previous_path = path;
                        let _ = self.events.send(WorkerEvent::InboundPathChanged {
                            peer: peer.clone(),
                            path,
                        });
                    }
                }
                _ = connection.closed() => break,
            }
        }
        probe_task.abort();
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.retain(|session| session.remote_id() != connection.remote_id());
        }
        let _ = self.events.send(WorkerEvent::SessionClosed { peer });
        Ok(())
    }
}

async fn open_peer_session(
    endpoint: &iroh::Endpoint,
    peer: EndpointAddr,
    peer_text: &str,
    events: mpsc::Sender<WorkerEvent>,
    session_monitor: SessionMonitor,
) -> Result<PeerSession, String> {
    let connection = endpoint
        .connect(peer, SESSION_ALPN)
        .await
        .map_err(|error| format!("建立持续会话失败：{error}"))?;
    session_monitor.track("session-client", connection.clone());
    let (mut send, mut recv) = connection
        .open_bi()
        .await
        .map_err(|error| format!("打开会话控制流失败：{error}"))?;
    send.write_all(&[SESSION_VERSION])
        .await
        .map_err(|error| format!("发送会话握手失败：{error}"))?;
    let mut response = [0_u8; 1];
    recv.read_exact(&mut response)
        .await
        .map_err(|error| format!("读取会话握手失败：{error}"))?;
    if response[0] != SESSION_VERSION {
        return Err("对端会话协议版本不兼容".to_owned());
    }

    let probe_task = tokio::spawn(run_session_probes(
        connection.clone(),
        peer_text.to_owned(),
        events.clone(),
    ));
    let watched_connection = connection.clone();
    let watched_peer = peer_text.to_owned();
    tokio::spawn(async move {
        let _ = watched_connection.closed().await;
        let _ = events.send(WorkerEvent::SessionClosed { peer: watched_peer });
    });
    Ok(PeerSession {
        connection,
        _send: send,
        _recv: recv,
        probe_task,
    })
}

async fn run_session_probes(
    connection: Connection,
    peer: String,
    events: mpsc::Sender<WorkerEvent>,
) {
    if connection.max_datagram_size().is_none() {
        return;
    }
    let test_id = NEXT_SESSION_PROBE_ID.fetch_add(1, Ordering::Relaxed);
    let mut sequence = 0_u64;
    let mut received = 0_u64;
    let mut timed_out = 0_u64;
    let mut latest_rtt = None;
    let mut outstanding = HashMap::<u64, Instant>::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let probe = Probe {
                    magic: PROBE_MAGIC,
                    test_id,
                    sequence,
                    kind: ProbeKind::Request,
                };
                let Ok(payload) = postcard::to_allocvec(&probe) else {
                    break;
                };
                if connection.send_datagram_wait(Bytes::from(payload)).await.is_err() {
                    break;
                }
                outstanding.insert(sequence, Instant::now());
                sequence = sequence.wrapping_add(1);
                let before = outstanding.len();
                outstanding.retain(|_, sent| sent.elapsed() < Duration::from_secs(3));
                timed_out = timed_out.saturating_add(
                    u64::try_from(before.saturating_sub(outstanding.len())).unwrap_or(u64::MAX),
                );
                let _ = events.send(WorkerEvent::SessionProbeStats {
                    peer: peer.clone(),
                    latest_rtt,
                    sent: sequence,
                    received,
                    timed_out,
                });
            }
            datagram = connection.read_datagram() => {
                let Ok(bytes) = datagram else {
                    break;
                };
                let Ok(mut probe) = postcard::from_bytes::<Probe>(&bytes) else {
                    continue;
                };
                if probe.magic != PROBE_MAGIC {
                    continue;
                }
                match probe.kind {
                    ProbeKind::Request => {
                        probe.kind = ProbeKind::Response;
                        let Ok(payload) = postcard::to_allocvec(&probe) else {
                            continue;
                        };
                        if connection.send_datagram_wait(Bytes::from(payload)).await.is_err() {
                            break;
                        }
                    }
                    ProbeKind::Response if probe.test_id == test_id => {
                        if let Some(sent) = outstanding.remove(&probe.sequence) {
                            received = received.saturating_add(1);
                            latest_rtt = Some(sent.elapsed());
                            let _ = events.send(WorkerEvent::SessionProbeStats {
                                peer: peer.clone(),
                                latest_rtt,
                                sent: sequence,
                                received,
                                timed_out,
                            });
                        }
                    }
                    ProbeKind::Response => {}
                }
            }
            _ = connection.closed() => break,
        }
    }
}

fn session_accept_error(error: impl std::fmt::Display) -> AcceptError {
    AcceptError::from_err(std::io::Error::other(error.to_string()))
}

fn selected_path_kind(connection: &Connection) -> PathKind {
    let paths = connection.paths();
    let Some(path) = paths.iter().find(iroh::endpoint::Path::is_selected) else {
        return PathKind::Unknown;
    };
    match path.remote_addr() {
        TransportAddr::Relay(_) => PathKind::Relay,
        TransportAddr::Ip(address) if address.is_ipv4() => PathKind::DirectIpv4,
        TransportAddr::Ip(address) if address.is_ipv6() => PathKind::DirectIpv6,
        TransportAddr::Ip(_) => PathKind::Direct,
        _ => PathKind::Unknown,
    }
}

fn parse_peer(value: &str) -> Result<EndpointAddr, String> {
    let value = value.trim();
    if value.starts_with('{') {
        return serde_json::from_str(value)
            .map_err(|error| format!("EndpointAddr JSON 无效：{error}"));
    }
    value
        .parse::<iroh::EndpointId>()
        .map(EndpointAddr::from)
        .map_err(|error| format!("Endpoint ID 无效：{error}"))
}

fn load_or_create_desktop_key() -> Result<SecretKey, String> {
    let path = endpoint_key_path();
    migrate_legacy_desktop_key(&path)?;
    match std::fs::read_to_string(&path) {
        Ok(value) => SecretKey::from_str(value.trim())
            .map_err(|error| format!("读取本机 Endpoint 身份失败：{error}")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)
                    .map_err(|error| format!("创建身份目录失败：{error}"))?;
            }
            let key = SecretKey::generate();
            let encoded = HEXLOWER.encode(&key.to_bytes());
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)
                .map_err(|error| format!("创建 Endpoint 身份失败：{error}"))?;
            file.write_all(encoded.as_bytes())
                .and_then(|()| file.write_all(b"\n"))
                .map_err(|error| format!("保存 Endpoint 身份失败：{error}"))?;
            set_key_permissions(&path)
                .map_err(|error| format!("保护 Endpoint 身份失败：{error}"))?;
            Ok(key)
        }
        Err(error) => Err(format!("读取 Endpoint 身份失败：{error}")),
    }
}

fn endpoint_key_path() -> PathBuf {
    desktop_data_dir("Iroh Transfer Demo").join("endpoint.key")
}

fn legacy_endpoint_key_path() -> PathBuf {
    desktop_data_dir("Iroh NetBench").join("endpoint.key")
}

fn desktop_data_dir(application_name: &str) -> PathBuf {
    #[cfg(target_os = "windows")]
    let base = std::env::var_os("LOCALAPPDATA").map(PathBuf::from);
    #[cfg(target_os = "macos")]
    let base = std::env::var_os("HOME")
        .map(PathBuf::from)
        .map(|home| home.join("Library").join("Application Support"));
    #[cfg(not(any(target_os = "windows", target_os = "macos")))]
    let base = std::env::var_os("XDG_DATA_HOME")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|home| home.join(".local").join("share"))
        });
    base.unwrap_or_else(|| PathBuf::from("."))
        .join(application_name)
}

fn migrate_legacy_desktop_key(path: &std::path::Path) -> Result<(), String> {
    if path.exists() {
        return Ok(());
    }
    let legacy = legacy_endpoint_key_path();
    if !legacy.is_file() {
        return Ok(());
    }
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|error| format!("创建身份目录失败：{error}"))?;
    }
    std::fs::rename(&legacy, path)
        .map_err(|error| format!("迁移旧版 Endpoint 身份失败：{error}"))?;
    set_key_permissions(path).map_err(|error| format!("保护 Endpoint 身份失败：{error}"))
}

#[cfg(unix)]
fn set_key_permissions(path: &std::path::Path) -> Result<(), std::io::Error> {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
}

#[cfg(not(unix))]
fn set_key_permissions(_path: &std::path::Path) -> Result<(), std::io::Error> {
    Ok(())
}

fn default_download_dir() -> PathBuf {
    let home = std::env::var_os("HOME")
        .or_else(|| std::env::var_os("USERPROFILE"))
        .map_or_else(
            || std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")),
            PathBuf::from,
        );
    home.join("Downloads").join("Iroh Transfer Demo")
}

fn install_cjk_font(context: &egui::Context) {
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        "noto-sans-cjk-sc".to_owned(),
        egui::FontData::from_static(include_bytes!(
            "../../../assets/fonts/NotoSansCJKsc-Regular.otf"
        ))
        .into(),
    );
    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        fonts
            .families
            .entry(family)
            .or_default()
            .insert(0, "noto-sans-cjk-sc".to_owned());
    }
    context.set_fonts(fonts);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_peer_is_rejected_by_ui() {
        let mut app = DemoApp::default();
        app.connect_clicked();
        assert!(matches!(app.connection, ConnectionState::Failed(_)));
    }

    #[test]
    fn connected_state_enables_connection_dependent_actions() {
        let connection = ConnectionState::Connected {
            peer: "example".to_owned(),
        };
        assert!(connection.is_connected());
        assert!(!connection.is_busy());
    }

    #[test]
    fn endpoint_id_becomes_discovery_address() {
        let endpoint_id = SecretKey::generate().public();
        let parsed = parse_peer(&endpoint_id.to_string()).expect("valid Endpoint ID");
        assert_eq!(parsed.id, endpoint_id);
        assert!(parsed.addrs.is_empty());
    }

    #[test]
    fn standard_preset_preserves_four_streams() {
        assert_eq!(BenchPreset::Standard.config().parallel_streams, 4);
        assert_eq!(BenchPreset::Quick.config().chunk_size, 64 * 1024);
    }

    #[test]
    fn standard_upload_progress_starts_near_three_quarters() {
        let state = BenchState::Running {
            phase: BenchPhase::Upload,
            phase_started: Instant::now(),
            config: NetBenchConfig::standard(),
        };
        let (progress, label) = state.progress().unwrap();
        assert!((0.70..0.80).contains(&progress), "{progress}");
        assert_eq!(label, "Stream 上传测速");
    }

    #[test]
    fn log_is_bounded() {
        let mut app = DemoApp::default();
        for index in 0..=MAX_LOG_LINES {
            app.push_log(index.to_string());
        }
        assert_eq!(app.logs.len(), MAX_LOG_LINES);
        assert_eq!(app.logs.front().map(String::as_str), Some("1"));
    }

    #[test]
    fn live_timeout_ratio_uses_cumulative_probe_counts() {
        let live = LiveMetrics {
            probe_sent: 100,
            probe_received: 97,
            probe_timed_out: 3,
            ..LiveMetrics::default()
        };
        assert_eq!(live.timeout_ratio(), Some(0.03));
    }

    #[test]
    fn connection_policies_require_expected_infrastructure() {
        assert!(ConnectionPolicy::Automatic.requires_relay());
        assert!(!ConnectionPolicy::DirectOnly.requires_relay());
        assert!(ConnectionPolicy::RelayOnly.requires_relay());
        assert_eq!(
            ConnectionPolicy::RelayOnly.path_stabilization_timeout(),
            Duration::ZERO
        );
    }

    #[test]
    fn bundled_infrastructure_schema_is_stable() {
        let config: BundledInfraConfig = serde_json::from_str(
            r#"{
                "discovery_url": "https://203.0.113.10:3478/pkarr",
                "relay_url": "https://203.0.113.10:3479",
                "qad_port": 4000,
                "relay_token": "01234567890123456789012345678901",
                "ca_file": "private-ca.crt"
            }"#,
        )
        .expect("valid bundled config");
        assert_eq!(config.qad_port, 4000);
        assert_eq!(config.ca_file, "private-ca.crt");
    }

    #[tokio::test]
    async fn persistent_session_notifies_receiver_and_close() {
        let server = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let client = iroh::Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .bind()
            .await
            .unwrap();
        let (events, received) = mpsc::channel();
        let sessions = Arc::new(Mutex::new(Vec::new()));
        let router = Router::builder(server.clone())
            .accept(
                SESSION_ALPN,
                PeerSessionProtocol::new(
                    events.clone(),
                    Arc::clone(&sessions),
                    SessionMonitor::default(),
                ),
            )
            .spawn();

        let session = open_peer_session(
            &client,
            server.addr(),
            &server.id().to_string(),
            events,
            SessionMonitor::default(),
        )
        .await
        .unwrap();
        let connected = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(event) = received.try_recv()
                    && matches!(event, WorkerEvent::InboundConnected { .. })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(connected.is_ok());

        session.close();
        let closed = tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if let Ok(event) = received.try_recv()
                    && matches!(event, WorkerEvent::SessionClosed { .. })
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(closed.is_ok());

        router.shutdown().await.unwrap();
        client.close().await;
    }
}
