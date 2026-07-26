# iroh-netbench

English | [中文](README.zh-CN.md)

A small embeddable Rust component for benchmarking and monitoring
[iroh](https://github.com/n0-computer/iroh) QUIC connections.

`iroh-netbench` reuses an application's existing `Endpoint`. It mounts a benchmark protocol on
the application's Router and opens a dedicated connection only when a test is requested.

> Status: working MVP, wire protocol v1. The API is still evolving and the crate is not published
> to crates.io yet.

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

Until the crate is published, use a path or Git dependency:

```toml
[dependencies]
iroh = "1.0.3"
iroh-netbench = { path = "../iroh-netbench/crates/iroh-netbench" }
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
A: NetBenchClient
B: NetBenchProtocol
```

A does not need to mount the protocol for this run; B does. If B must also initiate benchmarks
against A, both peers mount `NetBenchProtocol` and create a `NetBenchClient` when initiating a run.

## Run a benchmark

```rust,ignore
use iroh_netbench::{NetBenchClient, NetBenchConfig};

let report = NetBenchClient::new(endpoint.clone())
    .run(peer_addr, NetBenchConfig::standard())
    .await?;

println!("{}", serde_json::to_string_pretty(&report)?);
```

For live progress:

```rust,ignore
let client = NetBenchClient::new(endpoint.clone());
let mut test = client.start(peer_addr, NetBenchConfig::quick()).await?;

while let Some(event) = test.next().await {
    update_ui(event);
}

let report = test.result().await?;
```

`NetBenchClient` uses the shared `Endpoint` but creates a dedicated `/iroh/netbench/1`
connection. One benchmark connection contains a control stream, parallel upload/download streams
and Datagram probes.

For a run initiated by A against B, the control roles remain fixed:

| Measurement | Data direction | Result accounting |
|---|---|---|
| Latency | A sends probes, B echoes | A calculates RTT |
| Download | B → A | A counts received payload |
| Upload | A → B | B counts and confirms received payload |

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

let client = NetBenchClient::new(endpoint.clone())
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
- report schema: `1`

The wire version increases only for incompatible changes to network messages or their semantics.
Local Rust API, UI and passive-monitoring changes do not bump it.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets
```

## License

Licensed under either `MIT` or `Apache-2.0`, at your option.
