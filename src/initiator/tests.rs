use super::*;

struct TelemetryOnlySession(Mutex<crate::NetBenchTelemetry>);

#[async_trait::async_trait]
impl crate::NetBenchSession for TelemetryOnlySession {
    fn remote_peer_id(&self) -> String {
        "telemetry-test".to_owned()
    }
    fn telemetry(&self) -> crate::NetBenchTelemetry {
        *lock_unpoisoned(&self.0)
    }
    fn max_datagram_size(&self) -> Option<usize> {
        None
    }
    async fn open_bi(&self) -> Result<Box<dyn crate::NetBenchBidirectionalStream>> {
        unreachable!()
    }
    async fn accept_bi(&self) -> Result<Box<dyn crate::NetBenchBidirectionalStream>> {
        unreachable!()
    }
    async fn send_datagram(&self, _: Vec<u8>) -> Result<()> {
        unreachable!()
    }
    async fn read_datagram(&self) -> Result<Vec<u8>> {
        unreachable!()
    }
}

#[tokio::test]
async fn path_change_before_monitor_first_poll_is_retained() {
    let session = Arc::new(TelemetryOnlySession(Mutex::new(crate::NetBenchTelemetry {
        path: PathKind::Relay,
        ..crate::NetBenchTelemetry::default()
    })));
    let (sender, mut events) = mpsc::channel(16);
    let (task, state) = spawn_path_monitor(
        Connection(session.clone()),
        EventSink(sender),
        Instant::now(),
        PathKind::Relay,
    );
    lock_unpoisoned(&session.0).path = PathKind::DirectIpv4;
    let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
        .await
        .unwrap();
    assert!(matches!(
        event,
        Some(NetBenchEvent::FlowStage(FlowStage::PathObserved {
            path: PathKind::DirectIpv4
        }))
    ));
    task.abort_and_wait().await;
    assert!(
        lock_unpoisoned(&state).path_changed,
        "the initial path must be captured before the monitor task is scheduled"
    );
}

#[test]
fn preparation_convergence_is_separate_from_measurement_path_changes() {
    let state = PathMonitorState::new(PathKind::DirectIpv4);
    let report = state.report(
        PathKind::Relay,
        PathKind::DirectIpv4,
        Duration::from_millis(5),
        Some(Duration::from_millis(30)),
    );
    assert_eq!(report.preparation_path, PathKind::Relay);
    assert_eq!(report.observed_path(), PathKind::DirectIpv4);
    assert!(!report.path_changed);
    assert!(report.became_direct);
    assert_eq!(report.time_to_direct, Some(Duration::from_millis(30)));
}

#[tokio::test]
async fn observed_round_trip_is_mixed_even_when_final_path_matches_initial() {
    let session = Arc::new(TelemetryOnlySession(Mutex::new(crate::NetBenchTelemetry {
        path: PathKind::Relay,
        ..crate::NetBenchTelemetry::default()
    })));
    let (sender, mut events) = mpsc::channel(16);
    let (task, state) = spawn_path_monitor(
        Connection(session.clone()),
        EventSink(sender),
        Instant::now(),
        PathKind::Relay,
    );
    for path in [PathKind::DirectIpv4, PathKind::Relay] {
        lock_unpoisoned(&session.0).path = path;
        let event = tokio::time::timeout(Duration::from_secs(1), events.recv())
            .await
            .unwrap();
        assert!(
            matches!(event, Some(NetBenchEvent::FlowStage(FlowStage::PathObserved { path: observed })) if observed == path)
        );
    }
    task.abort_and_wait().await;
    let report = lock_unpoisoned(&state).clone().report(
        PathKind::Relay,
        PathKind::Relay,
        Duration::ZERO,
        None,
    );
    assert_eq!(report.initial_path, report.final_path);
    assert_eq!(report.observed_path(), PathKind::Mixed);
    assert!(report.became_direct);
}

#[test]
fn final_and_unknown_observations_are_retained() {
    for unknown_position in 0..3 {
        let mut paths = [PathKind::Relay, PathKind::DirectIpv4, PathKind::Relay];
        paths[unknown_position] = PathKind::Unknown;
        let mut state = PathMonitorState::new(paths[0]);
        state.observe(paths[1], Duration::from_millis(10));
        state.observe(paths[2], Duration::from_millis(20));
        let report = state.report(PathKind::Relay, paths[0], Duration::ZERO, None);
        assert_eq!(report.final_path, paths[2]);
        assert!(report.unknown_path_observed);
        assert_eq!(report.observed_path(), PathKind::Unknown);
    }
    // A final snapshot covers a change after the last periodic sample.
    let mut state = PathMonitorState::new(PathKind::Relay);
    state.observe(PathKind::DirectIpv6, Duration::from_millis(1));
    let report = state.report(PathKind::Relay, PathKind::Relay, Duration::ZERO, None);
    assert_eq!(report.final_path, PathKind::DirectIpv6);
    assert_eq!(report.observed_path(), PathKind::Mixed);
}

#[test]
fn transport_counters_distinguish_missing_reset_replacement_and_zero() {
    use crate::{CounterDelta, CounterSnapshot};
    let counter = |scope, value| Some(CounterSnapshot { scope, value });
    assert_eq!(
        counter_delta(counter(1, 20), counter(1, 23)),
        CounterDelta::Measured(3)
    );
    assert_eq!(
        counter_delta(counter(1, 20), counter(1, 20)),
        CounterDelta::Measured(0)
    );
    assert_eq!(
        counter_delta(counter(1, 20), counter(1, 3)),
        CounterDelta::Reset
    );
    assert_eq!(
        counter_delta(counter(1, 20), counter(2, 23)),
        CounterDelta::ScopeChanged
    );
    assert_eq!(
        counter_delta(counter(1, 20), counter(2, 3)),
        CounterDelta::ScopeChanged
    );
    for (before, after) in [(None, counter(1, 0)), (counter(1, 0), None), (None, None)] {
        assert_eq!(counter_delta(before, after), CounterDelta::Unavailable);
    }
}

#[test]
fn path_counter_replacement_does_not_invalidate_connection_counters() {
    use crate::{CounterDelta, CounterSnapshot};
    let before = TransportSnapshot(NetBenchTelemetry {
        lost_packets: Some(CounterSnapshot { scope: 1, value: 7 }),
        congestion_events: Some(CounterSnapshot {
            scope: 10,
            value: 4,
        }),
        ..NetBenchTelemetry::default()
    });
    let after = TransportSnapshot(NetBenchTelemetry {
        lost_packets: Some(CounterSnapshot { scope: 1, value: 9 }),
        congestion_events: Some(CounterSnapshot {
            scope: 11,
            value: 20,
        }),
        current_mtu: Some(1200),
        rtt: Some(Duration::from_millis(3)),
        ..NetBenchTelemetry::default()
    });
    let report = after.delta(before);
    assert_eq!(report.lost_packets, CounterDelta::Measured(2));
    assert_eq!(report.congestion_events, CounterDelta::ScopeChanged);
    assert_eq!(report.udp_tx_datagrams, CounterDelta::Unavailable);
    assert_eq!(report.current_mtu, Some(1200));
    assert_eq!(report.final_rtt, Some(Duration::from_millis(3)));
    let unknown = TransportSnapshot(NetBenchTelemetry::default()).delta(before);
    assert_eq!(unknown.current_mtu, None);
    assert_eq!(unknown.final_rtt, None);
}

#[test]
fn loss_progress_separates_confirmed_timeout_from_outstanding() {
    let now = Instant::now();
    let sent_at = HashMap::from([
        (0, now - Duration::from_secs(2)),
        (1, now - Duration::from_millis(100)),
        (2, now - Duration::from_millis(200)),
    ]);
    let received = HashSet::from([2]);

    let sample = loss_progress(&sent_at, &received, now, Duration::from_secs(1));
    assert_eq!(sample.sent, 3);
    assert_eq!(sample.received, 1);
    assert_eq!(sample.timed_out, 1);
    assert_eq!(sample.outstanding, 1);
    assert!((sample.timeout_ratio - (1.0 / 3.0)).abs() < f64::EPSILON);
}
