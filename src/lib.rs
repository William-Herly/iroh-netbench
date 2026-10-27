//! Network quality measurements inside a caller-owned authenticated peer session.
//!
//! This crate is intentionally independent from file transfer and diagnostics.

mod config;
mod error;
mod event;
mod flow;
mod initiator;
mod report;
mod responder;
mod statistics;
mod wire;

pub use config::{NetBenchConfig, NetBenchProbeConfig};
pub use error::{Error, Result};
pub use event::{
    FlowInfo, FlowStage, LatencySample, LossSample, NetBenchEvent, ThroughputDirection,
    ThroughputSample,
};
pub use flow::{
    CounterSnapshot, NetBenchBidirectionalStream, NetBenchFlow, NetBenchReceiveStream,
    NetBenchSendStream, NetBenchSession, NetBenchTelemetry,
};
pub use initiator::{NetBenchInitiator, NetBenchTest};
pub use report::{
    CounterDelta, LatencyReport, LoadedLatencyReport, LossReport, MeasurementPathReport,
    NetBenchProbeReport, NetBenchReport, PathKind, ThroughputReport, TransportReport,
};
pub use responder::{NetBenchResponder, NetBenchResponderBuilder, ThroughputPolicy};

/// Current wire protocol version.
///
/// Increment this only for an incompatible change to encoded messages or their semantics.
pub const PROTOCOL_VERSION: u16 = 1;

/// Current JSON/report schema version.
///
/// While the crate is pre-1.0, report-shape changes replace the initial schema directly rather
/// than retaining development-only schema versions or compatibility decoders.
pub const SCHEMA_VERSION: u16 = 1;
