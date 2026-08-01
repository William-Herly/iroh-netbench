# iroh-netbench

Embeddable latency, Datagram timeout, throughput and transport diagnostics for
[iroh](https://github.com/n0-computer/iroh) QUIC connections.

> `0.1.0` is the first preview release. The wire protocol and report schema are version `1`;
> Rust APIs may still evolve while the crate remains below `1.0`.

`iroh-netbench` reuses an application's existing `iroh::Endpoint`. The responder mounts
`NetBenchProtocol` on its Router, while the initiator creates a `NetBenchClient`. Each run uses a
dedicated `/iroh/netbench/1` connection and does not take ownership of the host Endpoint.

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
use iroh_netbench::{NETBENCH_ALPN, NetBenchClient, NetBenchConfig, NetBenchProtocol};

let router = Router::builder(endpoint.clone())
    .accept(NETBENCH_ALPN, NetBenchProtocol::default())
    .spawn();

let report = NetBenchClient::new(endpoint.clone())
    .run(peer_addr, NetBenchConfig::quick())
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

The benchmark connection selects its own Direct or Relay path from the supplied `EndpointAddr`.
Callers that require a specific path should filter that address before dialing and inspect the
final `ConnectionReport.path`.

## More documentation

- [Full English guide](https://github.com/William-Herly/iroh-netbench/blob/main/README.md)
- [中文指南](https://github.com/William-Herly/iroh-netbench/blob/main/README.zh-CN.md)
- [Release process](https://github.com/William-Herly/iroh-netbench/blob/main/RELEASING.md)

Licensed under either MIT or Apache-2.0, at your option.
