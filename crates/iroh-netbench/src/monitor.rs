//! Reusable live monitoring for one or more iroh QUIC connections.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use iroh::{
    EndpointId, TransportAddr,
    endpoint::{Connection, Path},
};
use serde::{Deserialize, Serialize};
use tokio::{
    sync::watch,
    task::JoinHandle,
    time::{Instant, MissedTickBehavior},
};

use crate::PathKind;

/// Stable monitor-local identifier for a tracked QUIC connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MonitoredConnectionId(u64);

impl MonitoredConnectionId {
    /// Numeric identifier suitable for logs and UI keys.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Sampling and smoothing policy for [`SessionMonitor`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionMonitorConfig {
    /// Frequency at which local QUIC statistics are read.
    pub sample_interval: Duration,
    /// Sliding window used for upload and download rates.
    pub rate_window: Duration,
    /// Sliding window used for the recent QUIC loss ratio.
    pub loss_window: Duration,
}

impl Default for SessionMonitorConfig {
    fn default() -> Self {
        Self {
            sample_interval: Duration::from_millis(250),
            rate_window: Duration::from_secs(1),
            loss_window: Duration::from_secs(5),
        }
    }
}

/// Whether a tracked QUIC connection is currently usable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MonitoredConnectionState {
    /// The transport has not reported a terminal close reason.
    Active,
    /// The transport has closed.
    Closed,
}

/// Latest live metrics for one tracked QUIC connection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConnectionMetrics {
    /// Monitor-local connection identifier.
    pub id: MonitoredConnectionId,
    /// Host-provided purpose label such as `session`, `file`, or `netbench`.
    pub label: String,
    /// Authenticated remote endpoint identity.
    pub peer_id: EndpointId,
    /// Current transport state.
    pub state: MonitoredConnectionState,
    /// Terminal close reason when available.
    pub close_reason: Option<String>,
    /// Currently selected Direct or Relay path.
    pub path: PathKind,
    /// Current selected-path transport RTT.
    pub rtt: Option<Duration>,
    /// Smoothed received wire bits per second.
    pub download_bps: f64,
    /// Smoothed transmitted wire bits per second.
    pub upload_bps: f64,
    /// Recent QUIC packets declared lost.
    pub recent_lost_packets: u64,
    /// Recent transmitted UDP datagrams used as the loss-ratio denominator.
    pub recent_tx_datagrams: u64,
    /// Recent QUIC packet loss ratio, clamped to `0.0..=1.0`.
    pub recent_loss_ratio: f64,
    /// Cumulative received UDP bytes for this connection.
    pub total_rx_bytes: u64,
    /// Cumulative transmitted UDP bytes for this connection.
    pub total_tx_bytes: u64,
}

/// Aggregated live metrics for all connections registered in one logical session.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMetrics {
    /// Monotonically increasing sample generation.
    pub generation: u64,
    /// Active connection count.
    pub active_connections: usize,
    /// Smoothed aggregate received wire bits per second.
    pub download_bps: f64,
    /// Smoothed aggregate transmitted wire bits per second.
    pub upload_bps: f64,
    /// Recent aggregate QUIC packets declared lost.
    pub recent_lost_packets: u64,
    /// Recent aggregate transmitted datagrams.
    pub recent_tx_datagrams: u64,
    /// Recent aggregate QUIC packet loss ratio, clamped to `0.0..=1.0`.
    pub recent_loss_ratio: f64,
    /// Cumulative received UDP bytes over tracked connections.
    pub total_rx_bytes: u64,
    /// Cumulative transmitted UDP bytes over tracked connections.
    pub total_tx_bytes: u64,
    /// Per-connection metrics in stable identifier order.
    pub connections: Vec<ConnectionMetrics>,
}

impl Default for SessionMetrics {
    fn default() -> Self {
        Self {
            generation: 0,
            active_connections: 0,
            download_bps: 0.0,
            upload_bps: 0.0,
            recent_lost_packets: 0,
            recent_tx_datagrams: 0,
            recent_loss_ratio: 0.0,
            total_rx_bytes: 0,
            total_tx_bytes: 0,
            connections: Vec::new(),
        }
    }
}

impl SessionMetrics {
    /// Looks up the latest metrics for one tracked connection.
    #[must_use]
    pub fn connection(&self, id: MonitoredConnectionId) -> Option<&ConnectionMetrics> {
        self.connections.iter().find(|metrics| metrics.id == id)
    }

    /// Iterates over connections authenticated as one remote endpoint.
    pub fn connections_for_peer(
        &self,
        peer_id: EndpointId,
    ) -> impl Iterator<Item = &ConnectionMetrics> {
        self.connections
            .iter()
            .filter(move |metrics| metrics.peer_id == peer_id)
    }
}

#[derive(Clone)]
struct RegisteredConnection {
    label: String,
    connection: Connection,
}

struct MonitorInner {
    connections: Arc<Mutex<HashMap<MonitoredConnectionId, RegisteredConnection>>>,
    updates: watch::Sender<SessionMetrics>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for MonitorInner {
    fn drop(&mut self) {
        if let Ok(mut task) = self.task.lock()
            && let Some(task) = task.take()
        {
            task.abort();
        }
    }
}

/// Tracks local wire statistics for all QUIC connections in one logical application session.
///
/// Register any `Connection` created or accepted by the host. Sampling reads local iroh
/// statistics only and does not itself send probes or application traffic.
#[derive(Clone)]
pub struct SessionMonitor {
    inner: Arc<MonitorInner>,
}

impl std::fmt::Debug for SessionMonitor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("SessionMonitor")
            .field("tracked_connections", &self.tracked_connection_count())
            .finish_non_exhaustive()
    }
}

impl SessionMonitor {
    /// Starts a monitor task on the current Tokio runtime.
    ///
    /// # Panics
    ///
    /// Panics when called outside a Tokio runtime.
    #[must_use]
    pub fn spawn(config: SessionMonitorConfig) -> Self {
        let config = normalized_config(config);
        let connections = Arc::new(Mutex::new(HashMap::new()));
        let (updates, _) = watch::channel(SessionMetrics::default());
        let task_connections = Arc::clone(&connections);
        let task_updates = updates.clone();
        let task = tokio::spawn(async move {
            run_monitor(config, task_connections, task_updates).await;
        });
        Self {
            inner: Arc::new(MonitorInner {
                connections,
                updates,
                task: Mutex::new(Some(task)),
            }),
        }
    }

    /// Registers or relabels a QUIC connection and returns its stable monitor identifier.
    pub fn track(&self, label: impl Into<String>, connection: Connection) -> MonitoredConnectionId {
        let id = monitored_id(&connection);
        if let Ok(mut connections) = self.inner.connections.lock() {
            connections.insert(
                id,
                RegisteredConnection {
                    label: label.into(),
                    connection,
                },
            );
        }
        id
    }

    /// Stops exposing one connection in future session snapshots.
    #[must_use]
    pub fn untrack(&self, id: MonitoredConnectionId) -> bool {
        self.inner
            .connections
            .lock()
            .is_ok_and(|mut connections| connections.remove(&id).is_some())
    }

    /// Removes all tracked connections and resets the published snapshot.
    pub fn clear(&self) {
        if let Ok(mut connections) = self.inner.connections.lock() {
            connections.clear();
        }
        self.inner.updates.send_replace(SessionMetrics::default());
    }

    /// Removes connections whose transport has already closed.
    pub fn prune_closed(&self) {
        if let Ok(mut connections) = self.inner.connections.lock() {
            connections.retain(|_, entry| entry.connection.close_reason().is_none());
        }
    }

    /// Returns the most recently published aggregate and per-connection metrics.
    #[must_use]
    pub fn latest(&self) -> SessionMetrics {
        self.inner.updates.borrow().clone()
    }

    /// Subscribes to future aggregate and per-connection snapshots.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<SessionMetrics> {
        self.inner.updates.subscribe()
    }

    /// Number of connections currently retained by the monitor.
    #[must_use]
    pub fn tracked_connection_count(&self) -> usize {
        self.inner
            .connections
            .lock()
            .map_or(0, |connections| connections.len())
    }
}

impl Default for SessionMonitor {
    fn default() -> Self {
        Self::spawn(SessionMonitorConfig::default())
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Counters {
    rx_bytes: u64,
    tx_bytes: u64,
    lost_packets: u64,
    tx_datagrams: u64,
}

impl Counters {
    fn capture(connection: &Connection) -> Self {
        let stats = connection.stats();
        Self {
            rx_bytes: stats.udp_rx.bytes,
            tx_bytes: stats.udp_tx.bytes,
            lost_packets: stats.lost_packets,
            tx_datagrams: stats.udp_tx.datagrams,
        }
    }

    fn delta(self, previous: Self) -> Self {
        Self {
            rx_bytes: self.rx_bytes.saturating_sub(previous.rx_bytes),
            tx_bytes: self.tx_bytes.saturating_sub(previous.tx_bytes),
            lost_packets: self.lost_packets.saturating_sub(previous.lost_packets),
            tx_datagrams: self.tx_datagrams.saturating_sub(previous.tx_datagrams),
        }
    }

    fn add_assign(&mut self, other: Self) {
        self.rx_bytes = self.rx_bytes.saturating_add(other.rx_bytes);
        self.tx_bytes = self.tx_bytes.saturating_add(other.tx_bytes);
        self.lost_packets = self.lost_packets.saturating_add(other.lost_packets);
        self.tx_datagrams = self.tx_datagrams.saturating_add(other.tx_datagrams);
    }
}

#[derive(Default)]
struct ConnectionSampleState {
    previous: Option<Counters>,
    rate_window: VecDeque<(Duration, Counters)>,
    loss_window: VecDeque<(Duration, Counters)>,
}

async fn run_monitor(
    config: SessionMonitorConfig,
    connections: Arc<Mutex<HashMap<MonitoredConnectionId, RegisteredConnection>>>,
    updates: watch::Sender<SessionMetrics>,
) {
    let mut states = HashMap::<MonitoredConnectionId, ConnectionSampleState>::new();
    let mut generation = 0_u64;
    let mut previous_time = Instant::now();
    let mut ticker = tokio::time::interval(config.sample_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        ticker.tick().await;
        let now = Instant::now();
        let elapsed = now.duration_since(previous_time);
        previous_time = now;
        let registered = connections
            .lock()
            .map(|items| {
                items
                    .iter()
                    .map(|(id, entry)| (*id, entry.clone()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let retained = registered.iter().map(|(id, _)| *id).collect::<HashSet<_>>();
        states.retain(|id, _| retained.contains(id));

        let mut connection_metrics = Vec::with_capacity(registered.len());
        for (id, entry) in registered {
            let current = Counters::capture(&entry.connection);
            let state = states.entry(id).or_default();
            let delta = state
                .previous
                .replace(current)
                .map_or_else(Counters::default, |previous| current.delta(previous));
            push_window(&mut state.rate_window, elapsed, delta, config.rate_window);
            push_window(&mut state.loss_window, elapsed, delta, config.loss_window);
            let (rate_duration, rate_counters) = sum_window(&state.rate_window);
            let (_, loss_counters) = sum_window(&state.loss_window);
            connection_metrics.push(connection_snapshot(
                id,
                &entry,
                current,
                rate_duration,
                rate_counters,
                loss_counters,
            ));
        }
        connection_metrics.sort_by_key(|metrics| metrics.id.get());
        generation = generation.wrapping_add(1);
        updates.send_replace(session_snapshot(generation, connection_metrics));
    }
}

fn normalized_config(mut config: SessionMonitorConfig) -> SessionMonitorConfig {
    let minimum = Duration::from_millis(10);
    config.sample_interval = config.sample_interval.max(minimum);
    config.rate_window = config.rate_window.max(config.sample_interval);
    config.loss_window = config.loss_window.max(config.sample_interval);
    config
}

fn push_window(
    window: &mut VecDeque<(Duration, Counters)>,
    elapsed: Duration,
    counters: Counters,
    target: Duration,
) {
    window.push_back((elapsed, counters));
    let mut total = window
        .iter()
        .map(|(duration, _)| *duration)
        .sum::<Duration>();
    while window.len() > 1 {
        let Some((front_duration, _)) = window.front() else {
            break;
        };
        if total.saturating_sub(*front_duration) < target {
            break;
        }
        total = total.saturating_sub(*front_duration);
        window.pop_front();
    }
}

fn sum_window(window: &VecDeque<(Duration, Counters)>) -> (Duration, Counters) {
    let mut duration = Duration::ZERO;
    let mut counters = Counters::default();
    for (sample_duration, sample) in window {
        duration += *sample_duration;
        counters.add_assign(*sample);
    }
    (duration, counters)
}

#[allow(
    clippy::cast_precision_loss,
    reason = "interactive rates and ratios are intentionally approximate f64 values"
)]
fn connection_snapshot(
    id: MonitoredConnectionId,
    entry: &RegisteredConnection,
    current: Counters,
    rate_duration: Duration,
    rate: Counters,
    loss: Counters,
) -> ConnectionMetrics {
    let close_reason = entry
        .connection
        .close_reason()
        .map(|reason| reason.to_string());
    let selected = entry
        .connection
        .paths()
        .iter()
        .find(Path::is_selected)
        .map(|path| (path_kind(path.remote_addr()), path.stats().rtt));
    let rate_seconds = rate_duration.as_secs_f64();
    ConnectionMetrics {
        id,
        label: entry.label.clone(),
        peer_id: entry.connection.remote_id(),
        state: if close_reason.is_some() {
            MonitoredConnectionState::Closed
        } else {
            MonitoredConnectionState::Active
        },
        close_reason,
        path: selected.map_or(PathKind::Unknown, |(path, _)| path),
        rtt: selected.map(|(_, rtt)| rtt),
        download_bps: if rate_seconds > 0.0 {
            rate.rx_bytes as f64 * 8.0 / rate_seconds
        } else {
            0.0
        },
        upload_bps: if rate_seconds > 0.0 {
            rate.tx_bytes as f64 * 8.0 / rate_seconds
        } else {
            0.0
        },
        recent_lost_packets: loss.lost_packets,
        recent_tx_datagrams: loss.tx_datagrams,
        recent_loss_ratio: ratio(loss.lost_packets, loss.tx_datagrams),
        total_rx_bytes: current.rx_bytes,
        total_tx_bytes: current.tx_bytes,
    }
}

#[allow(
    clippy::cast_precision_loss,
    reason = "interactive ratios are intentionally approximate f64 values"
)]
fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        (numerator as f64 / denominator as f64).clamp(0.0, 1.0)
    }
}

fn session_snapshot(generation: u64, connections: Vec<ConnectionMetrics>) -> SessionMetrics {
    let active_connections = connections
        .iter()
        .filter(|metrics| metrics.state == MonitoredConnectionState::Active)
        .count();
    let download_bps = connections.iter().map(|metrics| metrics.download_bps).sum();
    let upload_bps = connections.iter().map(|metrics| metrics.upload_bps).sum();
    let recent_lost_packets = connections
        .iter()
        .map(|metrics| metrics.recent_lost_packets)
        .sum();
    let recent_tx_datagrams = connections
        .iter()
        .map(|metrics| metrics.recent_tx_datagrams)
        .sum();
    let received_bytes_total = connections
        .iter()
        .map(|metrics| metrics.total_rx_bytes)
        .sum();
    let transmitted_bytes_total = connections
        .iter()
        .map(|metrics| metrics.total_tx_bytes)
        .sum();
    SessionMetrics {
        generation,
        active_connections,
        download_bps,
        upload_bps,
        recent_lost_packets,
        recent_tx_datagrams,
        recent_loss_ratio: ratio(recent_lost_packets, recent_tx_datagrams),
        total_rx_bytes: received_bytes_total,
        total_tx_bytes: transmitted_bytes_total,
        connections,
    }
}

fn path_kind(address: &TransportAddr) -> PathKind {
    match address {
        TransportAddr::Relay(_) => PathKind::Relay,
        TransportAddr::Ip(address) if address.is_ipv4() => PathKind::DirectIpv4,
        TransportAddr::Ip(address) if address.is_ipv6() => PathKind::DirectIpv6,
        TransportAddr::Ip(_) => PathKind::Direct,
        _ => PathKind::Unknown,
    }
}

fn monitored_id(connection: &Connection) -> MonitoredConnectionId {
    MonitoredConnectionId(u64::try_from(connection.stable_id()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn monitor_config_is_normalized() {
        let config = normalized_config(SessionMonitorConfig {
            sample_interval: Duration::ZERO,
            rate_window: Duration::ZERO,
            loss_window: Duration::ZERO,
        });
        assert_eq!(config.sample_interval, Duration::from_millis(10));
        assert_eq!(config.rate_window, config.sample_interval);
        assert_eq!(config.loss_window, config.sample_interval);
    }

    #[test]
    fn ratio_is_bounded_and_handles_empty_input() {
        assert!(ratio(0, 0).abs() < f64::EPSILON);
        assert!((ratio(2, 1) - 1.0).abs() < f64::EPSILON);
        assert!((ratio(1, 2) - 0.5).abs() < f64::EPSILON);
    }

    #[test]
    fn window_keeps_at_least_one_sample() {
        let mut window = VecDeque::new();
        for _ in 0..8 {
            push_window(
                &mut window,
                Duration::from_millis(250),
                Counters::default(),
                Duration::from_secs(1),
            );
        }
        let (duration, _) = sum_window(&window);
        assert!(duration >= Duration::from_secs(1));
        assert!(duration <= Duration::from_millis(1250));
    }
}
