# Measurement semantics

Reports describe one admitted benchmark flow on one host-selected peer session. They do not select a
route or prove that another future flow will use the same path.

## Probe mode

`run_probes` is the low-bandwidth mode:

- **Idle latency** sends application Datagram echoes at the configured interval and reports min,
  mean, p50, p95, p99, maximum and adjacent-sample jitter.
- **Datagram timeout** reports sent, received, timed-out, duplicate and reordered application probes.
- **Path observation** records the path at measurement start/end and any Relay-to-direct transition
  reported by the host telemetry.
- **Transport delta** subtracts the host telemetry snapshot taken before the run from the snapshot
  taken after it.

Datagram timeout is not raw IP, UDP or QUIC packet loss. A timeout can also result from queueing,
application scheduling, flow demultiplexing or a deadline that is too short.

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
host's `NetBenchTelemetry` implementation. Unsupported counters should remain zero and path kind
should be `Unknown` rather than inferred.

## Comparing results

For useful comparisons, keep the preset, responder policy, path kind, application version and host
adapter constant. Treat a Direct/Relay path change during a run as `Mixed`; do not compare its
throughput directly with a stable single-path run.

The report `schema_version` and negotiated `protocol_version` are separate from the Rust crate's
Semantic Version. The pre-1.0 implementation currently uses initial value `1` for both.
