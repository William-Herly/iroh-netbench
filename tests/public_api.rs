//! Public connection-bound API contracts.

use iroh_netbench::{NetBenchResponder, PROTOCOL_VERSION, SCHEMA_VERSION, ThroughputPolicy};

#[test]
fn initial_wire_and_report_versions_remain_one() {
    assert_eq!(PROTOCOL_VERSION, 1);
    assert_eq!(SCHEMA_VERSION, 1);
}

#[test]
fn responder_limits_and_runtime_policy_are_explicit() {
    let responder = NetBenchResponder::builder()
        .max_concurrent_tests(2)
        .max_parallel_streams(8)
        .throughput_policy(ThroughputPolicy::Deny)
        .build();
    assert_eq!(responder.max_concurrent_tests(), 2);
    assert_eq!(responder.max_parallel_streams(), 8);
    assert_eq!(responder.throughput_policy(), ThroughputPolicy::Deny);

    let retained = responder.clone();
    retained.set_throughput_policy(ThroughputPolicy::Allow);
    assert_eq!(responder.throughput_policy(), ThroughputPolicy::Allow);
}
