# Releasing iroh-netbench

This checklist applies to the reusable `iroh-netbench` crate. It does not authorize publishing or
publicly distributing `transfer-demo` desktop archives.

## 0.1.0 acceptance gate

- Private Relay-only Quick completes three consecutive runs within the 45-second whole-run limit.
- Linux netns Direct at `12 Mbit/s + 50±10 ms` completes the no-loss, `0.5%` and `2%` matrices, or
  returns a genuine network error rather than a local cleanup timeout.
- Reported five-second download and upload measurement windows remain within the agreed ±250 ms
  tolerance.
- Direct Quick, Probe-only, `ThroughputPolicy::Deny`, Deny plus Probe-only, timeout and explicit
  cancellation regressions pass.
- ALPN remains `/iroh/netbench/1`; `PROTOCOL_VERSION` and `SCHEMA_VERSION` remain `1` unless an
  intentionally incompatible change has been reviewed.

## Repository checks

Run from the workspace root with a clean worktree:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets --locked
cargo test --workspace --doc --locked
cargo doc -p iroh-netbench --no-deps
cargo +1.91.0 check -p iroh-netbench --all-targets --locked
cargo audit
git diff --check
```

The audit must report no known vulnerabilities. At the time of the `0.1.0` release candidate,
RustSec reports one allowed "unmaintained" warning for `paste`, inherited on Linux through
`iroh -> netwatch -> netlink-packet-core`; it is not a direct dependency or a known vulnerability
and should be tracked with the iroh dependency.

Set the release date in `CHANGELOG.md`, then inspect and verify the exact crate archive:

```bash
cargo package -p iroh-netbench --list --locked
cargo package -p iroh-netbench --locked
cargo publish -p iroh-netbench --dry-run --locked
```

The package list must contain the crate README, both license files, examples, tests and source. It
must not contain `private-pki`, certificates, tokens, application logs, desktop assets or
`transfer-demo`.

## Publish

Publishing and tagging are explicit maintainer actions; preparing the repository does neither.
After the acceptance gate and dry run pass:

```bash
git tag -a v0.1.0 -m "iroh-netbench 0.1.0"
git push origin v0.1.0
cargo publish -p iroh-netbench --locked
```

Create a GitHub release from the tag using the `0.1.0` changelog section. Crates.io versions are
immutable: use `cargo yank --vers 0.1.0 iroh-netbench` for a serious post-publication problem, then
fix forward with a new patch version.

## Private demo warning

The macOS and Windows demo scripts intentionally copy private infrastructure configuration and a
private CA into their output archives. Those archives are internal test artifacts. Never attach
them to the public library release, and never package Relay tokens or private keys.
