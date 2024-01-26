//! The smallest possible example to spin up a node and serve a single blob.
//!
//! This is using an in memory database and a random node id.
//! run this example from the project root:
//!     $ cargo run --example hello-world-provide
use iroh_bytes::BlobFormat;
use tokio_util::task::LocalPoolHandle;
use tracing_subscriber::{prelude::*, EnvFilter};

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();
    println!("'Hello World' provide example!");
    // create a new, empty in memory database
    let mut db = iroh_bytes::store::readonly_mem::Store::default();
    // create an in-memory doc store (not used in the example)
    let doc_store = iroh_sync::store::memory::Store::default();
    // create a new iroh runtime with 1 worker thread, reusing the existing tokio runtime
    let lp = LocalPoolHandle::new(1);
    // add some data and remember the hash
    let hash = db.insert(b"Hello, world!");
    // create a new node
    let node = iroh::node::Node::builder(db, doc_store)
        .local_pool(&lp)
        .spawn()
        .await?;
    // create a ticket
    let ticket = node.ticket(hash, BlobFormat::Raw).await?;
    // print some info about the node
    println!("serving hash:    {}", ticket.hash());
    println!("node id:         {}", ticket.node_addr().node_id);
    println!("node listening addresses:");
    for addr in ticket.node_addr().direct_addresses() {
        println!("\t{:?}", addr);
    }
    println!(
        "node DERP server url: {:?}",
        ticket
            .node_addr()
            .derp_url()
            .expect("a default DERP url should be provided")
            .to_string()
    );
    // print the ticket, containing all the above information
    println!("\nin another terminal, run:");
    println!("\t cargo run --example hello-world-fetch {}", ticket);
    // wait for the node to finish, this will block indefinitely
    // stop with SIGINT (ctrl+c)
    node.await?;
    Ok(())
}
