//! Network quality measurements over a dedicated iroh QUIC connection.
//!
//! This crate is intentionally independent from file transfer and diagnostics.

mod client;
mod config;
mod error;
mod event;
mod monitor;
mod protocol;
mod report;
mod scheduling;
mod statistics;
pub mod wire;

pub use client::{NetBenchInitiator, NetBenchTest};
pub use config::{NetBenchConfig, NetBenchProbeConfig};
pub use error::{Error, Result};
pub use event::{
    FlowInfo, FlowStage, LatencySample, LossSample, NetBenchEvent, ThroughputDirection,
    ThroughputSample,
};
pub use monitor::{
    ConnectionMetrics, MonitoredConnectionId, MonitoredConnectionState, SessionMetrics,
    SessionMonitor, SessionMonitorConfig,
};
pub use protocol::{NetBenchProtocol, NetBenchProtocolBuilder, ThroughputPolicy};
pub use report::{
    LatencyReport, LoadedLatencyReport, LossReport, MeasurementPathReport, NetBenchProbeReport,
    NetBenchReport, PathKind, ThroughputReport, TransportReport,
};

/// ALPN used by the benchmark protocol.
pub const NETBENCH_ALPN: &[u8] = b"/iroh/netbench/1";

/// Current wire protocol version.
///
/// Increment this only for an incompatible change to encoded messages or their semantics.
pub const PROTOCOL_VERSION: u16 = 1;

/// Current JSON/report schema version.
///
/// While the crate is pre-1.0, report-shape changes replace the initial schema directly rather
/// than retaining development-only schema versions or compatibility decoders.
pub const SCHEMA_VERSION: u16 = 1;
