# iroh-netbench

English | [中文](README.zh-CN.md)

[![crates.io](https://img.shields.io/crates/v/iroh-netbench.svg)](https://crates.io/crates/iroh-netbench)
[![docs.rs](https://img.shields.io/docsrs/iroh-netbench)](https://docs.rs/iroh-netbench)

A small embeddable Rust component for benchmarking and monitoring
[iroh](https://github.com/n0-computer/iroh) QUIC connections.

`iroh-netbench` runs benchmark business flows on a host-established iroh `Connection`. It does not
own an `Endpoint`, resolve peer addresses, dial, choose a path policy, or close the connection.

> Status: the maintained branch uses wire protocol v1 and report schema v2. The reusable library passed its
> Direct, Relay and impaired-path acceptance matrix. Rust APIs may still evolve while the crate
> remains below `1.0`.

## Release scope

The buildable workspace contains only the `iroh-netbench` library crate. The legacy
`transfer-demo` is excluded from the workspace and does not define or validate the new API.

The library does not create or own Relay, discovery, PKARR or certificate infrastructure. The host
application supplies a configured `Endpoint` and decides which peer addresses and paths are
eligible for a full throughput test.

## Measurements

| Measurement | Method |
|---|---|
| Latency and jitter | QUIC Datagram request/response RTT |
| Probe timeout | Timed-out, duplicate and reordered Datagrams |
| Download | Payload bytes received by the client |
| Upload | Payload bytes received and confirmed by the server |
| Loaded latency | Datagram RTT during download and upload |
| Transport | Path, QUIC loss, congestion, MTU and UDP counters |
| Live monitoring | Per-connection and aggregate throughput, RTT and loss |

## Add the crate

Add the published crate from [crates.io](https://crates.io/crates/iroh-netbench). API documentation
is available on [docs.rs](https://docs.rs/iroh-netbench):

```toml
[dependencies]
iroh = "1.0.3"
iroh-netbench = "0.1.0"
serde_json = "1.0"
```

The host application remains responsible for creating the `Endpoint` and configuring its address
discovery and Relay policy.

## Mount the protocol

`NetBenchProtocol` accepts benchmarks initiated by a peer. The benchmark responder must mount it
beside the application's own protocols:

```rust,ignore
use std::time::Duration;

use iroh::protocol::Router;
use iroh_netbench::{NETBENCH_ALPN, NetBenchProtocol};

let protocol = NetBenchProtocol::builder()
    .max_concurrent_tests(2)
    .max_test_duration(Duration::from_secs(30))
    .max_parallel_streams(8)
    .build();

let router = Router::builder(endpoint.clone())
    .accept(MY_APPLICATION_ALPN, my_protocol)
    .accept(NETBENCH_ALPN, protocol)
    .spawn();
```

For a benchmark initiated by A against B:

```text
A: iroh dial + NetBenchInitiator
B: NetBenchProtocol
```

A does not need to mount the protocol for this run; B does. If B must also initiate benchmarks
against A, both peers mount `NetBenchProtocol`, establish a connection, and create a
`NetBenchInitiator` when initiating a run.

## Run a benchmark

```rust,ignore
use iroh_netbench::{NETBENCH_ALPN, NetBenchConfig, NetBenchInitiator};

let connection = endpoint.connect(peer_addr, NETBENCH_ALPN).await?;
let report = NetBenchInitiator::new()
    .run(connection.clone(), NetBenchConfig::standard())
    .await?;

println!("{}", serde_json::to_string_pretty(&report)?);
```

For live progress:

```rust,ignore
let client = NetBenchInitiator::new();
let mut test = client.start(connection.clone(), NetBenchConfig::quick()).await?;

while let Some(event) = test.next().await {
    update_ui(event);
}

let report = test.result().await?;
```

The existing connection carries the netbench control stream, parallel upload/download streams and
Datagram probes. `report.path` describes the measurement window; the host records dialing facts.

### Bounded duration and cancellation

Throughput durations are fixed accounting windows. At the deadline, the sender resets its data
streams and the receiver stops unread data; queued QUIC data is not gracefully drained into the
reported duration. `ThroughputReport.measurement_duration` therefore remains the configured
window, while only payload received inside that window contributes to the rate.

Both peers give the bidirectional control stream a higher QUIC scheduling priority than throughput
streams, so `TestReady`, progress, errors and `TestFinished` do not compete at equal priority with
bandwidth-saturating payload. Per-phase cleanup has a five-second hard bound; this is grace for
control delivery, not extra measurement time.

`NetBenchConfig::quick()` has a 45-second overall hard limit and `standard()` has a 75-second
limit. `overall_timeout` includes protocol negotiation, path stabilization, measurements and
bounded cleanup. A timeout returns `Error::Timeout { stage }` without closing the caller-owned
connection. The receiver also applies the requested duration as a runtime limit; its
`max_test_duration` is not only request validation.

A running full benchmark can be stopped explicitly and joined:

```rust,ignore
let test = client.start(connection.clone(), NetBenchConfig::quick()).await?;

// Called when the user cancels or an outer operation is being torn down.
test.cancel();
test.abort_and_wait().await?;
```

Dropping `NetBenchTest` or a pending `run()` future aborts its internal tasks and streams but leaves
the caller-owned connection open. Use `abort_and_wait()` when the caller must prove task
termination before continuing.

### Low-bandwidth probes

Latency, jitter and Datagram timeout measurements can run without opening throughput streams:

```rust,ignore
use iroh_netbench::NetBenchProbeConfig;

let report = NetBenchInitiator::new()
    .run_probes(connection.clone(), NetBenchProbeConfig::quick())
    .await?;
```

The result is a `NetBenchProbeReport` with connection, latency, loss and transport data, but no
upload or download result. Probe presets have their own `overall_timeout` (15 seconds for Quick,
25 seconds for Standard) and remain separate from full-throughput success or failure.

### Receiver throughput policy

The receiving endpoint can allow or deny bandwidth-saturating upload and download measurements:

```rust,ignore
use iroh_netbench::{NetBenchProtocol, ThroughputPolicy};

let protocol = NetBenchProtocol::builder()
    .throughput_policy(ThroughputPolicy::Deny)
    .build();

// A retained clone can update the policy without restarting the endpoint.
protocol.set_throughput_policy(ThroughputPolicy::Allow);
```

`Allow` is the intentional standalone default. Under `Deny`, connectivity checks and
`run_probes` remain available, while Quick, Standard and other full throughput runs return
`Error::ThroughputDeniedByPeer` before measurements begin. The client does not silently downgrade
the requested test.

This reuses the existing protocol-v1 `ServerLimits` fields and does not change the wire protocol
version. Fixed throughput deadlines and cancellation likewise keep `PROTOCOL_VERSION = 1` and do
not add control-message variants. The server also enforces the policy when a throughput phase is
requested.

For a run initiated by A against B, the control roles remain fixed:

| Measurement | Data direction | Result accounting |
|---|---|---|
| Latency | A sends probes, B echoes | A calculates RTT |
| Download | B → A | A counts received payload |
| Upload | A → B | B counts and confirms received payload |

### Connection path selection

The host owns candidate selection, Direct/Relay policy and dialing. Netbench accepts neither an
`EndpointAddr` nor a dial policy. `report.path.initial_path`, `final_path`, and `path_changed`
describe only the benchmark business-flow window. `path_changed` covers transient changes even
when the final path equals the initial path. `became_direct` and `time_to_direct` are set only when
the flow entered on a non-Direct path and first reached Direct during stabilization or measurement.

Low-bandwidth probes are the recommended default diagnostic on unqualified or bandwidth-sensitive
paths. Full throughput should be opt-in and subject to the receiver's policy.

The `0.1.0` acceptance matrix passed private Relay-only Quick runs, Direct and Relay Quick,
receiver denial, Probe-only under denial, live events and active cancellation. Linux netns runs at
`12 Mbit/s + 50±10 ms` passed with `0%`, `0.5%` and `2%` packet loss. Five-second download and
upload windows stayed within ±250 ms, complete Quick runs stayed below 45 seconds, cleanup stopped
high-bandwidth traffic, and the application PeerSession remained usable.

## Monitor existing connections

`SessionMonitor` passively samples iroh transport statistics. Create one monitor per logical peer
session and register the connections that belong to it:

```rust,ignore
use iroh_netbench::{SessionMonitor, SessionMonitorConfig};

let monitor = SessionMonitor::spawn(SessionMonitorConfig::default());
let connection_id = monitor.track("application", connection.clone());
let mut updates = monitor.subscribe();

tokio::spawn(async move {
    while updates.changed().await.is_ok() {
        let session = updates.borrow_and_update();
        println!(
            "down={:.1} Mbps up={:.1} Mbps loss={:.2}%",
            session.download_bps / 1_000_000.0,
            session.upload_bps / 1_000_000.0,
            session.recent_loss_ratio * 100.0,
        );

        if let Some(connection) = session.connection(connection_id) {
            println!(
                "path={:?} rtt={:?} loss={:.2}%",
                connection.path,
                connection.rtt,
                connection.recent_loss_ratio * 100.0,
            );
        }
    }
});
```

Pass the same monitor to the benchmark client and protocol to include benchmark traffic in the
session total:

```rust,ignore
let protocol = NetBenchProtocol::builder()
    .with_monitor(monitor.clone())
    .build();

let client = NetBenchInitiator::new()
    .with_monitor(monitor.clone());
```

The monitor does not own connections, send heartbeats or perform active measurements.

## Metric semantics

Passive monitoring and active benchmarking use different counters:

| Value | Meaning |
|---|---|
| `SessionMetrics` / `ConnectionMetrics` throughput | Wire-level UDP byte deltas, including QUIC overhead |
| `ConnectionMetrics.rtt` | QUIC transport RTT from the selected path |
| `ConnectionMetrics.recent_loss_ratio` | Recent QUIC-declared loss |
| `NetBenchReport.download` | Client-received application payload |
| `NetBenchReport.upload` | Server-received application payload |
| `NetBenchProbeReport` | Low-bandwidth connection, latency, loss and transport results |
| `LatencyReport` | Application Datagram round-trip samples |
| `LossReport.timeout_ratio` | Probe timeout rate, not absolute physical packet loss |

Session throughput is the sum of all connections explicitly registered with the monitor.

## Presets

| Stage | Quick | Standard |
|---|---:|---:|
| Idle latency | 2 s | 3 s |
| Probe timeout | 2 s | 5 s |
| Download measurement | 5 s | 10 s |
| Upload measurement | 5 s | 10 s |
| Parallel streams | 4 | 4 |

Use `NetBenchConfig::quick()` for interactive checks and `NetBenchConfig::standard()` for a more
stable result.

`NetBenchProbeConfig::quick()` and `NetBenchProbeConfig::standard()` use the corresponding latency
and loss settings without running warm-up, upload, download or loaded-latency stages.

## Examples

Run a directly reachable test pair:

```bash
cargo run --example netbench-server
cargo run --example netbench-client -- '<EndpointAddr JSON printed by the server>'
```

The examples disable Relay and are intended for local/direct testing.

## Compatibility

- Rust 1.91 or newer
- ALPN: `/iroh/netbench/1`
- wire protocol: `1`
- report schema: `2`

The wire version increases only for incompatible changes to network messages or their semantics.
Local Rust API, UI and passive-monitoring changes do not bump it.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
cargo doc -p iroh-netbench --no-deps
cargo package -p iroh-netbench --list
```

See [CHANGELOG.md](CHANGELOG.md) for release notes.

## License

Licensed under either `MIT` or `Apache-2.0`, at your option.
