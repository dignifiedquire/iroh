//! This example shows the shortest path to working with documents in iroh. This example creates a
//! document and sets an entry with key: "hello", value: "world". The document is completely local.
//!
//! The iroh node that creates the document is backed by an in-memory database and a random node ID
//!
//! run this example from the project root:
//!     $ cargo run --features=examples --example client
use indicatif::HumanBytes;
use iroh::{base::base32, client::docs::Entry, docs::store::Query, node::Node};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let node = Node::memory().enable_docs().spawn().await?;

    // Could also use `node` directly, as it derefs to the client.
    let client = node.client();

    let blobs = client.blobs();
    let doc = client.docs().create().await?;
    let author = client.docs().author_default().await?;

    doc.set_bytes(author, "hello", "world").await?;

    let mut stream = doc.get_many(Query::all()).await?;
    while let Some(entry) = stream.try_next().await? {
        println!("entry {}", fmt_entry(&entry));
        let content = blobs.read_to_bytes(entry.content_hash()).await?;
        println!("  content {}", std::str::from_utf8(&content)?)
    }

    Ok(())
}

fn fmt_entry(entry: &Entry) -> String {
    let id = entry.id();
    let key = std::str::from_utf8(id.key()).unwrap_or("<bad key>");
    let author = id.author().fmt_short();
    let hash = entry.content_hash();
    let hash = base32::fmt_short(hash.as_bytes());
    let len = HumanBytes(entry.content_len());
    format!("@{author}: {key} = {hash} ({len})",)
}
