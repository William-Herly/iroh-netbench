//! Versioned on-the-wire messages.

mod codec;
mod control;
mod probe;

pub use codec::{
    MAX_CONTROL_MESSAGE_SIZE, decode_control, encode_control, read_control, write_control,
};
pub use control::{Capabilities, ControlMessage, ErrorCode, ServerLimits};
pub use probe::{PROBE_MAGIC, Probe, ProbeKind};
