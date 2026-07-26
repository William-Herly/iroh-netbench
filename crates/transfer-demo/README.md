# transfer-demo

Internal desktop test application for exercising `iroh-netbench` with a minimal single-file
transfer protocol. It is not part of the reusable NetBench component API.

## Run

```bash
cargo run -p transfer-demo --bin transfer-demo
```

The application provides peer connection, disconnect, file selection, transfer progress,
benchmark controls and live session metrics.

Received files are stored in:

```text
~/Downloads/Iroh Transfer Demo/
```

The receiver first writes `.<filename>.part`, verifies the received byte count, and then renames
it to the final filename. Desktop diagnostics are written to `log/client.log` beside the
distributed application.
