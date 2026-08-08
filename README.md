# iroh-netbench

English | [中文](README.zh-CN.md)

`iroh-netbench` measures application-visible network quality inside a caller-owned authenticated
peer session. The host chooses and authenticates the physical connection, admits the benchmark
flow, and routes its reliable streams and unreliable Datagrams. The library never dials an
endpoint, registers an ALPN, or closes the host session.

The crate is designed for iroh and other QUIC-style multiplexed sessions. It has no direct iroh
dependency, so applications can integrate it without forcing a particular iroh release.

## Install

```toml
[dependencies]
iroh-netbench = "0.2"
```

MSRV is Rust `1.91`. The crate is pre-1.0; the current wire protocol and JSON report schema both
remain at their initial version `1`.

## Use an admitted flow

```rust,ignore
use iroh_netbench::{
    NetBenchConfig, NetBenchFlow, NetBenchInitiator, NetBenchReport, NetBenchResponder, Result,
};

pub async fn run(flow: NetBenchFlow) -> Result<NetBenchReport> {
    NetBenchInitiator::new()
        .run(flow, NetBenchConfig::quick())
        .await
}

pub async fn serve(flow: NetBenchFlow, responder: &NetBenchResponder) -> Result<()> {
    responder.serve(flow).await
}
```

The host constructs `NetBenchFlow` only after peer authentication and flow admission. See the
[complete in-memory host](examples/in_memory_host.rs) and the [integration guide](docs/integration.md)
for the required Stream and Datagram routing contract.

## Measurements

| Mode | Measurements |
| --- | --- |
| Probe | idle application RTT, jitter, Datagram timeout/reordering, path and transport deltas |
| Full | Probe measurements plus download/upload payload throughput and latency under load |

Datagram timeout is an application observation, not a claim about raw network packet loss.
Throughput is received application payload over fixed accounting windows. Detailed definitions and
limitations are in [measurement semantics](docs/measurements.md).

## Host contract

The host supplies:

- one `NetBenchSession` scoped to a single admitted benchmark flow;
- reliable bidirectional Stream adapters with flow-local finish and cancellation;
- flow-scoped Datagram send/receive;
- an opaque non-secret peer identity and a transport telemetry snapshot.

`accept_bi` and `read_datagram` must never return another benchmark flow or another application
protocol's traffic. Cancellation and normal completion release only benchmark resources; the host
continues to own the peer session.

The responder enforces concurrency, duration, stream-count, chunk-size, and runtime throughput
policy limits. It echoes only valid, unique probes for the currently active test and bounds replies
by the declared probe cadence. Applications should authenticate and authorize the peer before
constructing a flow.

## Documentation

- [Host and iroh integration](docs/integration.md)
- [Measurement semantics](docs/measurements.md)
- [Release history](CHANGELOG.md)
- [API documentation](https://docs.rs/iroh-netbench)

Licensed under either [MIT](LICENSE-MIT) or [Apache-2.0](LICENSE-APACHE), at your option.
