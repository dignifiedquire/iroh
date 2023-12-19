//! An example how to download a single blob from a node and write it to stdout.
//!
//! This is using a helper method to stream the blob.
//! Run this example on any `iroh get` ticket. You can create a "hello world" ticket with:
//!     $ cargo run --example hello-world
//! hello-world will give you a "ticket" argument & example to use with `cargo run -- get --ticket`
//! copy that ticket & feed it to this example:
//!     $ cargo run --example dump-blob-stream <ticket>
use std::env::args;
use std::io;

use bao_tree::io::fsm::BaoContentItem;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use genawaiter::sync::Co;
use genawaiter::sync::Gen;
use iroh::dial::Options;
use iroh::ticket::BlobTicket;
use iroh_bytes::get::fsm::{AtInitial, BlobContentNext, ConnectedNext, EndBlobNext};
use iroh_bytes::protocol::GetRequest;
use iroh_net::key::SecretKey;
use tokio::io::AsyncWriteExt;
use tracing_subscriber::{prelude::*, EnvFilter};

// set the RUST_LOG env var to one of {debug,info,warn} to see logging info
pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

/// Stream the response for a request for a single blob.
///
/// If the request was for a part of the blob, this will stream just the requested
/// blocks.
///
/// This will stream the root blob and close the connection.
fn stream_blob(initial: AtInitial) -> impl Stream<Item = io::Result<Bytes>> + 'static {
    async fn inner(initial: AtInitial, co: &Co<io::Result<Bytes>>) -> io::Result<()> {
        // connect
        let connected = initial.next().await?;
        // read the first bytes
        let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
            return Err(io::Error::new(io::ErrorKind::Other, "expected start root"));
        };
        // move to the header
        let header = start_root.next();
        // get the size of the content
        let (mut content, _size) = header.next().await?;
        // manually loop over the content and yield all data
        let done = loop {
            match content.next().await {
                BlobContentNext::More((next, data)) => {
                    if let BaoContentItem::Leaf(leaf) = data? {
                        // yield the data
                        co.yield_(Ok(leaf.data)).await;
                    }
                    content = next;
                }
                BlobContentNext::Done(done) => {
                    // we are done with the root blob
                    break done;
                }
            }
        };
        // close the connection even if there is more data
        let closing = match done.next() {
            EndBlobNext::Closing(closing) => closing,
            EndBlobNext::MoreChildren(more) => more.finish(),
        };
        // close the connection
        let _stats = closing.next().await?;
        Ok(())
    }

    Gen::new(|co| async move {
        if let Err(e) = inner(initial, &co).await {
            co.yield_(Err(e)).await;
        }
    })
}

/// Stream the response for a request for an iroh collection and its children.
///
/// If the request was for a part of the children, this will stream just the requested
/// blocks.
///
/// The root blob is not streamed. It must be fully included in the response.
fn stream_children(initial: AtInitial) -> impl Stream<Item = io::Result<Bytes>> + 'static {
    async fn inner(initial: AtInitial, co: &Co<io::Result<Bytes>>) -> io::Result<()> {
        // connect
        let connected = initial.next().await?;
        // read the first bytes
        let ConnectedNext::StartRoot(start_root) = connected.next().await? else {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "failed to parse collection",
            ));
        };
        // check that we requestded the whole collection
        if !start_root.ranges().is_all() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "collection was not requested completely",
            ));
        }
        // move to the header
        let header: iroh_bytes::get::fsm::AtBlobHeader = start_root.next();
        let (root_end, links_bytes) = header.concatenate_into_vec().await?;
        let EndBlobNext::MoreChildren(at_meta) = root_end.next() else {
            return Err(io::Error::new(io::ErrorKind::Other, "missing meta blob"));
        };
        let links: Box<[iroh_bytes::Hash]> = postcard::from_bytes(&links_bytes)
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "failed to parse links"))?;
        let meta_link = *links
            .first()
            .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "missing meta link"))?;
        let (meta_end, _meta_bytes) = at_meta.next(meta_link).concatenate_into_vec().await?;
        let mut curr = meta_end.next();
        let closing = loop {
            match curr {
                EndBlobNext::MoreChildren(more) => {
                    let Some(hash) = links.get(more.child_offset() as usize) else {
                        break more.finish();
                    };
                    let header = more.next(*hash);
                    let (mut content, _size) = header.next().await?;
                    // manually loop over the content and yield all data
                    let done = loop {
                        match content.next().await {
                            BlobContentNext::More((next, data)) => {
                                if let BaoContentItem::Leaf(leaf) = data? {
                                    // yield the data
                                    co.yield_(Ok(leaf.data)).await;
                                }
                                content = next;
                            }
                            BlobContentNext::Done(done) => {
                                // we are done with the root blob
                                break done;
                            }
                        }
                    };
                    curr = done.next();
                }
                EndBlobNext::Closing(closing) => {
                    break closing;
                }
            }
        };
        // close the connection
        let _stats = closing.next().await?;
        Ok(())
    }

    Gen::new(|co| async move {
        if let Err(e) = inner(initial, &co).await {
            co.yield_(Err(e)).await;
        }
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    setup_logging();

    let ticket: BlobTicket = args().nth(1).expect("missing ticket").parse()?;

    // generate a transient secret key for this connection
    //
    // in real applications, it would be very much preferable to use a persistent secret key
    let secret_key = SecretKey::generate();
    let dial_options = Options {
        secret_key,
        peer: ticket.node_addr().clone(),
        keylog: false,
        derp_map: None,
    };

    // connect to the peer
    //
    // note that dial creates a new endpoint, so it should only be used for short lived command line tools
    let connection = iroh::dial::dial(dial_options).await?;

    let mut stream = if ticket.recursive() {
        // create a request for a single blob
        let request = GetRequest::all(ticket.hash());

        // create the initial state of the finite state machine
        let initial = iroh::bytes::get::fsm::start(connection, request);

        // create a stream that yields all the data of the blob
        stream_children(initial).boxed_local()
    } else {
        // create a request for a single blob
        let request = GetRequest::single(ticket.hash());

        // create the initial state of the finite state machine
        let initial = iroh::bytes::get::fsm::start(connection, request);

        // create a stream that yields all the data of the blob
        stream_blob(initial).boxed_local()
    };
    while let Some(item) = stream.next().await {
        let item = item?;
        tokio::io::stdout().write_all(&item).await?;
    }
    Ok(())
}
