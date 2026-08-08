# Changelog

All notable changes to this project are documented in this file. The project follows Semantic
Versioning for the Rust crate; wire-protocol and report-schema versions are tracked independently.

## [Unreleased]

### Changed

- Datagram probe progress now reports sent, received, individually confirmed timeouts, outstanding
  responses and the current timeout ratio at a bounded cadence, including when every response times
  out. A terminal `LossFinished` event locks the result before throughput warm-up begins; the wire
  protocol and report schema remain at `1`.

## [0.2.0] - 2026-08-08

### Changed

- The single public crate now lives at the repository root instead of a one-member nested workspace.
- Netbench no longer dials, registers an ALPN or owns an iroh connection. The host passes an admitted
  `NetBenchFlow` backed by flow-scoped Stream and Datagram ports.
- Connection-establishment fields, standalone client/server examples, passive monitoring and internal
  stream scheduling were removed directly while the crate remains pre-1.0.
- The Rust crate moves to `0.2.0`. This release directly replaces the `0.1.0` preview; cross-version
  wire interoperability and report compatibility are unsupported, so wire protocol and report
  schema remain at their initial value `1`.
- Responder Datagram echo is scoped to the active test, rejects malformed or unrelated probes,
  deduplicates sequences and enforces the declared request budget.
- The unused `sample_interval` setting was removed; throughput progress uses a fixed internal
  250-millisecond cadence.
- Live loss progress reports sent, received and outstanding probes.

### Added

- Transport boundary traits for host-owned sessions and reliable Stream halves.
- Explicit flow-finished acknowledgement so completion does not depend on Connection EOF.
- Public integration and measurement-semantics documentation plus a complete in-memory host example.
- Minimal public CI for formatting, clippy, tests, docs, MSRV and package contents.

## [0.1.0] - 2026-08-02

First preview release of the reusable `iroh-netbench` library.

### Added

- Dedicated iroh ALPN with versioned control messages and structured reports.
- Quick and Standard latency, Datagram timeout, download, upload and loaded-latency measurements.
- Low-bandwidth Probe-only runs with independent whole-run deadlines.
- Receiver-controlled `Allow`/`Deny` policy for bandwidth-saturating tests.
- Passive per-connection and aggregate session monitoring.
- Explicit benchmark cancellation and awaitable task termination.
- Direct-path examples and end-to-end iroh integration tests.

### Reliability

- Throughput uses fixed accounting windows followed by reset/stop instead of graceful queued-data
  draining.
- Each throughput phase and complete run has a hard deadline.
- Control streams are scheduled above throughput streams, with bounded cleanup grace for completion
  messages on Relay and impaired paths.
- Protocol ALPN, wire version and report schema remain version `1`.

### Release scope

- `iroh-netbench` is the only public crate.
- Private Relay-only and Linux netns impaired-path acceptance passed, including `0%`, `0.5%` and
  `2%` packet-loss runs at `12 Mbit/s + 50±10 ms`.

[Unreleased]: https://github.com/William-Herly/iroh-netbench/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/William-Herly/iroh-netbench/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/William-Herly/iroh-netbench/releases/tag/v0.1.0
