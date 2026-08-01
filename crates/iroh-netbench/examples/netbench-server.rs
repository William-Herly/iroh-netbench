//! Minimal direct-only `NetBench` server.

use std::{error::Error, time::Duration};

use iroh::{Endpoint, RelayMode, endpoint::presets, protocol::Router};
use iroh_netbench::{NETBENCH_ALPN, NetBenchProtocol};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await?;
    let protocol = NetBenchProtocol::builder()
        .max_concurrent_tests(2)
        .max_test_duration(Duration::from_secs(30))
        .max_parallel_streams(8)
        .build();
    let router = Router::builder(endpoint.clone())
        .accept(NETBENCH_ALPN, protocol)
        .spawn();

    println!("{}", serde_json::to_string(&endpoint.addr())?);
    println!("Copy the EndpointAddr JSON above, then press Ctrl-C to stop.");
    tokio::signal::ctrl_c().await?;

    router.shutdown().await?;
    Ok(())
}
