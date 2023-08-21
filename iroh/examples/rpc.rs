//! An example that runs an iroh node that can be controlled via RPC.
use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

use clap::Parser;
use iroh::collection::IrohCollectionParser;
use iroh::rpc_protocol::{ProviderRequest, ProviderResponse};
use iroh::{bytes::util::runtime, rpc_protocol::ProviderService};
use iroh_bytes::baomap::Store;
use iroh_net::key::SecretKey;
use quic_rpc::transport::quinn::QuinnServerEndpoint;
use quic_rpc::ServiceEndpoint;
use tracing_subscriber::{prelude::*, EnvFilter};

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

const DEFAULT_RPC_PORT: u16 = 0x1337;
const RPC_ALPN: [u8; 17] = *b"n0/provider-rpc/1";

/// Makes a an RPC endpoint that uses a QUIC transport
fn make_rpc_endpoint(keypair: &SecretKey) -> anyhow::Result<impl ServiceEndpoint<ProviderService>> {
    let rpc_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, DEFAULT_RPC_PORT));
    let rpc_quinn_endpoint = quinn::Endpoint::server(
        iroh::node::make_server_config(keypair, 8, 1024, vec![RPC_ALPN.to_vec()])?,
        rpc_addr,
    )?;
    let rpc_endpoint =
        QuinnServerEndpoint::<ProviderRequest, ProviderResponse>::new(rpc_quinn_endpoint)?;
    Ok(rpc_endpoint)
}

async fn run(db: impl Store) -> anyhow::Result<()> {
    // create a new iroh runtime with 1 worker thread, reusing the existing tokio runtime
    let rt = runtime::Handle::from_currrent(1)?;
    // create a random keypair
    let keypair = SecretKey::generate();
    // create a rpc endpoint
    let rpc_endpoint = make_rpc_endpoint(&keypair)?;

    // create a new node
    // we must configure the iroh collection parser so the node understands iroh collections
    let node = iroh::node::Node::builder(db)
        .keypair(keypair)
        .collection_parser(IrohCollectionParser)
        .runtime(&rt)
        .rpc_endpoint(rpc_endpoint)
        .spawn()
        .await?;
    // print some info about the node
    let peer = node.peer_id();
    let addrs = node.local_endpoint_addresses().await?;
    println!("node PeerID:     {peer}");
    println!("node listening addresses:");
    for addr in addrs {
        println!("    {}", addr);
    }
    // wait for the node to finish, this will block indefinitely
    // stop with SIGINT (ctrl+c)
    node.await?;
    Ok(())
}

#[derive(Parser, Debug)]
struct Args {
    /// Path to use to store the iroh database.
    ///
    /// If this is not set, an in memory database will be used.
    #[clap(long)]
    path: Option<String>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();
    let rt = runtime::Handle::from_currrent(1)?;

    let args = Args::parse();
    match args.path {
        Some(path) => {
            tokio::fs::create_dir_all(&path).await?;
            let db = iroh::baomap::flat::Store::load(path.clone(), path, &rt).await?;
            run(db).await
        }
        None => {
            let db = iroh::baomap::mem::Store::new(rt);
            run(db).await
        }
    }
}
