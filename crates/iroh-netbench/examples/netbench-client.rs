//! Minimal direct-only `NetBench` client accepting `EndpointAddr` JSON from the server example.

use std::{env, error::Error};

use iroh::{Endpoint, EndpointAddr, RelayMode, endpoint::presets};
use iroh_netbench::{NETBENCH_ALPN, NetBenchConfig, NetBenchInitiator};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let peer_json = env::args()
        .nth(1)
        .ok_or("usage: cargo run --example netbench-client -- '<EndpointAddr JSON>'")?;
    let peer: EndpointAddr = serde_json::from_str(&peer_json)?;
    let endpoint = Endpoint::builder(presets::Minimal)
        .relay_mode(RelayMode::Disabled)
        .bind()
        .await?;

    let connection = endpoint.connect(peer, NETBENCH_ALPN).await?;
    let report = NetBenchInitiator::new()
        .run(connection.clone(), NetBenchConfig::quick())
        .await?;
    println!("{}", serde_json::to_string_pretty(&report)?);

    connection.close(0_u8.into(), b"example complete");
    endpoint.close().await;
    Ok(())
}
