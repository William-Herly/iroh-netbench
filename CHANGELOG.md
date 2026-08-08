# Changelog

All notable changes to this project are documented in this file. The project follows Semantic
Versioning for the Rust crate; wire-protocol and report-schema versions are tracked independently.

## [Unreleased]

### Changed

- Netbench no longer dials or owns an iroh connection; the host passes an already established
  `Connection` used by every measurement stream and Datagram.
- Connection-establishment fields were removed directly while the crate remains pre-1.0. Wire protocol,
  ALPN and report schema all remain version `1`; no development-only compatibility decoder is retained.

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

[Unreleased]: https://github.com/William-Herly/iroh-netbench/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/William-Herly/iroh-netbench/releases/tag/v0.1.0
