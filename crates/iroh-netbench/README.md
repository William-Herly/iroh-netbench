# iroh-netbench

Embeddable latency, Datagram timeout, throughput and transport diagnostics for
[iroh](https://github.com/n0-computer/iroh) QUIC connections.

> The maintained branch uses wire protocol version `1` and report schema version `2`;
> Rust APIs may still evolve while the crate remains below `1.0`.

The responder mounts `NetBenchProtocol` on its Router. The host establishes an iroh `Connection`
and passes that same connection to `NetBenchInitiator`; netbench never dials or owns its lifecycle.

## Capabilities

- low-bandwidth latency, jitter and Datagram timeout probes;
- fixed-window download and upload measurements with loaded latency;
- selected-path and QUIC transport statistics;
- passive monitoring of existing application connections;
- receiver-controlled `Allow`/`Deny` policy for bandwidth-saturating tests;
- explicit cancellation plus bounded phase and whole-run deadlines.

## Add the crate

```toml
[dependencies]
iroh = "1.0.3"
iroh-netbench = "0.1.0"
```

## Minimal use

```rust,ignore
use iroh::protocol::Router;
use iroh_netbench::{NETBENCH_ALPN, NetBenchConfig, NetBenchInitiator, NetBenchProtocol};

let router = Router::builder(endpoint.clone())
    .accept(NETBENCH_ALPN, NetBenchProtocol::default())
    .spawn();

let connection = endpoint.connect(peer_addr, NETBENCH_ALPN).await?;
let report = NetBenchInitiator::new()
    .run(connection.clone(), NetBenchConfig::quick())
    .await?;
```

Full throughput consumes substantial bandwidth. A receiver can reject it without blocking normal
connectivity checks or low-bandwidth probes:

```rust,ignore
use iroh_netbench::{NetBenchProtocol, ThroughputPolicy};

let protocol = NetBenchProtocol::builder()
    .throughput_policy(ThroughputPolicy::Deny)
    .build();
```

The initiator then receives `Error::ThroughputDeniedByPeer`; the crate does not silently replace a
requested full test with a probe-only run.

## Runtime guarantees

Throughput uses fixed accounting windows followed by reset/stop cleanup. Control streams have a
higher QUIC scheduling priority than throughput streams, cleanup remains bounded, and Quick and
Standard have 45-second and 75-second whole-run deadlines. `NetBenchTest::abort_and_wait()` gives
callers an explicit cancellation and join boundary.

The host owns Direct/Relay policy and dialing. `report.path` describes only the initial/final path
and transitions observed during the benchmark measurement window.

## More documentation

- [Full English guide](https://github.com/William-Herly/iroh-netbench/blob/main/README.md)
- [中文指南](https://github.com/William-Herly/iroh-netbench/blob/main/README.zh-CN.md)

Licensed under either MIT or Apache-2.0, at your option.
