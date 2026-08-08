# Host integration

`iroh-netbench` is a business-flow protocol, not a connection manager. A host must already own an
authenticated peer session before starting either side of a benchmark.

## Ownership

| Responsibility | Owner |
| --- | --- |
| Endpoint configuration, discovery, dialing, Relay and direct-path policy | Host |
| Peer authentication and benchmark authorization | Host |
| Physical connection and reconnection lifecycle | Host |
| Stream/Datagram admission and typed demultiplexing | Host |
| Benchmark negotiation, probes, throughput windows and report generation | iroh-netbench |
| Closing the peer session | Host |

The initiator and responder each receive one `NetBenchFlow`. A flow contains one reliable control
stream and an `Arc<dyn NetBenchSession>` whose remaining Streams and Datagrams are already scoped to
that flow.

## Required routing boundary

For a connection shared with other application protocols, the host needs an outer typed envelope:

```text
authenticated peer session
  -> host admission
  -> flow id + traffic kind
  -> netbench control stream
  -> netbench measurement streams
  -> netbench Datagram queue
```

`NetBenchSession::open_bi` must mark the new Stream as belonging to this flow before returning it.
The peer dispatcher must read that outer marker and route the Stream to the matching flow before
`accept_bi` can return it. Datagram routing follows the same rule. Netbench payloads remain opaque to
the outer dispatcher.

Do not let netbench call a connection-global `accept_bi` or `read_datagram` concurrently with another
consumer. That creates nondeterministic traffic theft even if peer authentication is correct.

## Using official iroh

There are two valid integration patterns:

1. **Application-shared Connection.** Keep the existing iroh `Connection`, add a host flow envelope
   and dispatcher, and adapt the routed send/receive halves plus telemetry to the netbench traits.
   This is the production pattern when Mesh, RPC, file transfer or other protocols share the same
   Connection.
2. **Benchmark-exclusive Connection.** The host may reserve an already-established iroh Connection
   for one benchmark flow and wrap its Streams and Datagrams directly. The host still owns dialing,
   ALPN selection, authentication and closure. This simpler pattern must not be used once another
   consumer shares that Connection.

The core crate intentionally does not publish an iroh-version-specific adapter. This keeps iroh
upgrades in the host and prevents a convenience wrapper from silently competing with the host's
dispatcher.

## Admission and responder limits

Authenticate the peer and authorize bandwidth use before constructing `NetBenchFlow`. The responder
then applies a second, flow-local resource boundary:

- maximum concurrent tests;
- maximum phase duration;
- maximum parallel throughput Streams;
- maximum write chunk size;
- runtime `ThroughputPolicy::Allow` or `Deny`.

Probe-only runs remain available when throughput is denied. The responder binds Datagram echo to
the active test id, rejects unrelated or malformed probes, deduplicates sequences and caps replies
at the count implied by the declared duration and cadence.

## Completion and cancellation

Normal completion uses an explicit flow-finished acknowledgement; it does not wait for Connection
EOF. Cancelling a `NetBenchTest` cancels only its control, measurement Streams and Datagram routing.
After responder cleanup returns, the host may immediately reuse the peer session for other flows.

An adapter should translate only flow-local stream-stop conditions to `Error::FlowStopped`. Session
closure, dispatcher shutdown and unrelated transport failures remain errors.
