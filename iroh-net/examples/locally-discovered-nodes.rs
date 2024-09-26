//! A small example showing how to get a list of nodes that were discovered via [`iroh_net::discovery::LocalSwarmDiscovery`]. LocalSwarmDiscovery uses [`swarm-discovery`](https://crates.io/crates/swarm-discovery) to discover other nodes in the local network ala mDNS.
//!
//! This example creates an iroh endpoint, a few additional iroh endpoints to discover, waits a few seconds, and reports all of the iroh NodeIds (also called `[iroh_net::key::PublicKey]`s) it has discovered.
//!
//! This is an async, non-determinate process, so the number of NodeIDs discovered each time may be different. If you have other iroh endpoints or iroh nodes with [`LocalSwarmDiscovery`] enabled, it may discover those nodes as well.
use iroh_net::{
    discovery::local_swarm_discovery::LocalSwarmDiscovery, endpoint::Source, key::SecretKey,
    Endpoint,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    println!("locally discovered nodes example!\n");
    let key = SecretKey::generate();
    let id = key.public();
    println!("creating endpoint {id:?}\n");
    let ep = Endpoint::builder()
        .secret_key(key)
        .discovery(Box::new(LocalSwarmDiscovery::new(id)?))
        .bind()
        .await?;

    let num = 5;
    println!("creating {num} additional endpoints to discover locally:");
    let mut discoverable_eps = vec![];
    for _ in 0..num {
        let key = SecretKey::generate();
        let id = key.public();
        println!("\t{id:?}");
        let ep = Endpoint::builder()
            .secret_key(key)
            .discovery(Box::new(LocalSwarmDiscovery::new(id)?))
            .bind()
            .await?;
        discoverable_eps.push(ep);
    }

    let duration = Duration::from_secs(3);
    println!("\nwaiting {duration:?} to allow discovery to occur...\n");
    tokio::time::sleep(duration).await;

    // get a list of all the remote nodes this endpoint knows about
    let remotes = ep.remote_info_iter();
    // filter that list down to the nodes that have a `Source::Discovery` with the `service` name "local"
    // If you have a long running node and want to only get the nodes that were discovered recently, you can also filter on the `Duration` of the source, which indicates how long ago we got information from that source.
    let locally_discovered: Vec<_> = remotes
        .filter(|remote| {
            remote.sources().iter().any(|(source, _duration)| {
                if let Source::Discovery { service } = source {
                    service == "local"
                } else {
                    false
                }
            })
        })
        .map(|remote| remote.node_id)
        .collect();

    println!("found:");
    for id in locally_discovered {
        println!("\t{id:?}");
    }
    Ok(())
}
