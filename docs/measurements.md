# Measurement semantics

Reports describe one admitted benchmark flow on one host-selected peer session. They do not select a
route or prove that another future flow will use the same path.

## Probe mode

`run_probes` is the low-bandwidth mode:

- **Idle latency** sends application Datagram echoes at the configured interval and reports min,
  mean, p50, p95, p99, maximum and adjacent-sample jitter.
- **Datagram timeout** reports sent, received, timed-out, duplicate and reordered application probes.
- **Path observation** records preparation separately from the measurement window, with initial,
  final, changed and unknown observations.
- **Transport delta** compares supported counters within the same connection or path generation.

Datagram timeout is not raw IP, UDP or QUIC packet loss. A timeout can also result from queueing,
application scheduling, flow demultiplexing or a deadline that is too short.

Each probe has its own configured response deadline. The current `quick` and `standard` presets use
two seconds: an echo arriving before the deadline is received, one still within the deadline is
outstanding, and one absent when the deadline expires is timed out. A late echo does not reverse the
completed observation.

Interactive callers receive bounded-cadence cumulative `LossSample` progress with sent, received,
individually confirmed timeout and still-outstanding counts. The live ratio uses only confirmed
timeouts; an in-flight probe is never prematurely classified as loss. One terminal `LossFinished`
result locks the value before a full run starts download warm-up, so later throughput phases keep
showing the completed Datagram result.

## Full mode

A full run includes Probe measurements and adds:

- fixed-window download application-payload throughput;
- fixed-window upload application-payload throughput;
- RTT samples while download and upload traffic are active;
- loaded-latency increase relative to idle p50.

Warm-up bytes and bytes queued after an accounting deadline are excluded. Rates therefore describe
payload delivered during the declared window, not link-layer capacity or provider billing bytes.

The receiver may stop a throughput Stream at the accounting deadline. `FlowStopped` is considered a
clean terminal condition only in those throughput loops; the same error elsewhere remains a failure.

## Telemetry supplied by the host

`TransportReport` can include lost packets/bytes, congestion events, sent/received Datagrams, current
MTU, black-hole detections and smoothed RTT. These values are only as complete and current as the
host's `NetBenchTelemetry` implementation. Unsupported values are `None`, and unavailable path kind
is `Unknown`; neither is replaced with a measured zero.

Each cumulative value is a `CounterSnapshot { scope, value }`. The host owns the scope identity and
changes it when the connection/path or counter lifetime changes; a path kind such as Direct IPv4
is not an identity. `CounterDelta` distinguishes `Measured(0)` from `Unavailable`, `Reset` (a decrease
within one advertised scope) and `ScopeChanged`. Counters must not silently reset and catch up under
the same scope. A path counter measures only that counter's scope, not the aggregate of all paths
used by a connection. Summing currently open paths cannot reconstruct retired-path history.

The observation window begins with one synchronous telemetry snapshot after initial stabilization.
It includes idle probes, loss probes, throughput warmups, both throughput directions and their
cleanup. At the end, the path monitor is stopped before the final telemetry snapshot is captured;
flow-finished acknowledgement is outside this window. Connection counters can include concurrent
non-benchmark traffic on the shared Session. They are not per-flow loss or throughput counters.
Each direction's payload throughput excludes warmup and uses its own declared accounting window.

`preparation_path` is sampled after negotiation, before initial stabilization. `initial_path` and
`final_path` share the transport-counter boundaries. `path_changed` describes differences observed
inside that window only; preparation convergence is separate. Periodic observations have a nominal
250 ms interval, with no replay of missed ticks. They retain observed round trips even when initial
and final path kinds match, but cannot prove that shorter migrations never occurred. Physical path
changes that retain the same high-level kind are also outside this classification.

`MeasurementPathReport::observed_path()` returns `Unknown` if any window sample was unknown,
otherwise `Mixed` for an observed change, or the initial kind for an unchanged observation. The
underlying changed/unknown flags and boundary paths remain available together. Relay-to-direct
timing describes observed preparation or in-window convergence; it does not change the classification
of an otherwise stable measurement window.

## Comparing results

For useful comparisons, keep the preset, responder policy, path kind, application version and host
adapter constant. Use `observed_path()` and retain incomplete observations; do not compare a mixed
or unknown window directly with a stable known-path run.

The report `schema_version` and negotiated `protocol_version` are separate from the Rust crate's
Semantic Version. The pre-1.0 implementation currently uses initial value `1` for both.
