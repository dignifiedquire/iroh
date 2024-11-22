use anyhow::{Context,Result};
use bytes::Bytes;
use clap::{Parser, Subcommand};
use futures_lite::StreamExt;
use iroh_net::{key::SecretKey, ticket::NodeTicket, Endpoint, NodeAddr, RelayMap, RelayMode, RelayUrl};
use std::time::{Duration, Instant};
use tracing::info;
use std::str::FromStr;

// Transfer ALPN that we are using to communicate over the `Endpoint`
const TRANSFER_ALPN: &[u8] = b"n0/iroh/transfer/example/0";

#[derive(Parser, Debug)]
#[command(name = "provide_fetch")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Provide {
        #[clap(long, default_value = "1G", value_parser = parse_byte_size)]
        size: u64,
        #[clap(long)]
        relay_url: Option<String>,
    },
    Fetch {
        #[arg(long)]
        ticket: String,
        #[clap(long)]
        relay_url: Option<String>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match &cli.command {
        Commands::Provide { size, relay_url } => provide(size.clone(), relay_url.clone()).await?,
        Commands::Fetch { ticket , relay_url} => fetch(&ticket, relay_url.clone()).await?,
    }

    Ok(())
}

async fn provide(size: u64, relay_url: Option<String>) -> anyhow::Result<()> {
    let secret_key = SecretKey::generate();
    let relay_mode = match relay_url {
        Some(relay_url) => {
            let relay_url = RelayUrl::from_str(&relay_url)?;
            let relay_map = RelayMap::from_url(relay_url);
            RelayMode::Custom(relay_map)
        }
        None => RelayMode::Default,
    };
    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![TRANSFER_ALPN.to_vec()])
        .relay_mode(relay_mode)
        .bind()
        .await?;

    let node_id = endpoint.node_id();

    for local_endpoint in endpoint
        .direct_addresses()
        .next()
        .await
        .context("no endpoints")?
    {
        println!("\t{}", local_endpoint.addr)
    }


    let relay_url = endpoint
        .home_relay()
        .expect("should be connected to a relay server");
    let local_addrs = endpoint
        .direct_addresses()
        .next()
        .await
        .context("no endpoints")?
        .into_iter()
        .map(|endpoint| {
            let addr = endpoint.addr;
            addr
        })
        .collect::<Vec<_>>();

    let node_addr = NodeAddr::from_parts(node_id, Some(relay_url), local_addrs);
    let ticket = NodeTicket::new(node_addr);

    println!("NodeTicket: {}", ticket);

    // accept incoming connections, returns a normal QUIC connection
    while let Some(incoming) = endpoint.accept().await {
        let mut connecting = match incoming.accept() {
            Ok(connecting) => connecting,
            Err(err) => {
                tracing::warn!("incoming connection failed: {err:#}");
                // we can carry on in these cases:
                // this can be caused by retransmitted datagrams
                continue;
            }
        };
        let alpn = connecting.alpn().await?;
        let conn = connecting.await?;
        let node_id = iroh_net::endpoint::get_remote_node_id(&conn)?;
        info!(
            "new connection from {node_id} with ALPN {} (coming from {})",
            String::from_utf8_lossy(&alpn),
            conn.remote_address()
        );

        // spawn a task to handle reading and writing off of the connection
        tokio::spawn(async move {
            // accept a bi-directional QUIC connection
            // use the `quinn` APIs to send and recv content
            let (mut send, mut recv) = conn.accept_bi().await?;
            tracing::debug!("accepted bi stream, waiting for data...");
            let message = recv.read_to_end(100).await?;
            let message = String::from_utf8(message)?;
            println!("received: {message}");

            send_data_on_stream(&mut send, size).await?;

            // We sent the last message, so wait for the client to close the connection once
            // it received this message.
            let res = tokio::time::timeout(Duration::from_secs(3), async move {
                let closed = conn.closed().await;
                if !matches!(closed, quinn::ConnectionError::ApplicationClosed(_)) {
                    println!("node {node_id} disconnected with an error: {closed:#}");
                }
            })
            .await;
            if res.is_err() {
                println!("node {node_id} did not disconnect within 3 seconds");
            }
            Ok::<_, anyhow::Error>(())
        });
    }

    // stop with SIGINT (ctrl-c)
    Ok(())
}

async fn fetch(ticket: &str, relay_url: Option<String>) -> anyhow::Result<()> {
    let ticket: NodeTicket = ticket.parse()?;
    let secret_key = SecretKey::generate();
    let relay_mode = match relay_url {
        Some(relay_url) => {
            let relay_url = RelayUrl::from_str(&relay_url)?;
            let relay_map = RelayMap::from_url(relay_url);
            RelayMode::Custom(relay_map)
        }
        None => RelayMode::Default,
    };
    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![TRANSFER_ALPN.to_vec()])
        .relay_mode(relay_mode)
        .bind()
        .await?;

    let start = Instant::now();

    let me = endpoint.node_id();
    println!("node id: {me}");
    println!("node listening addresses:");
    for local_endpoint in endpoint
        .direct_addresses()
        .next()
        .await
        .context("no endpoints")?
    {
        println!("\t{}", local_endpoint.addr)
    }

    let relay_url = endpoint
        .home_relay()
        .expect("should be connected to a relay server, try calling `endpoint.local_endpoints()` or `endpoint.connect()` first, to ensure the endpoint has actually attempted a connection before checking for the connected relay server");
    println!("node relay server url: {relay_url}\n");
    
    // Attempt to connect, over the given ALPN.
    // Returns a Quinn connection.
    let conn = endpoint.connect(ticket.node_addr().clone(), TRANSFER_ALPN).await?;
    info!("connected");

    // Use the Quinn API to send and recv content.
    let (mut send, mut recv) = conn.open_bi().await?;

    let message = format!("{me} is saying 'hello!'");
    send.write_all(message.as_bytes()).await?;

    // Call `finish` to close the send side of the connection gracefully.
    send.finish()?;

    let (len, dur, chnk) = drain_stream(&mut recv, false).await?;

    // We received the last message: close all connections and allow for the close
    // message to be sent.
    endpoint.close(0u8.into(), b"bye").await?;

    let duration = start.elapsed();
    println!("Received {} B in {:?}/{:?} in {} chunks", len, dur, duration, chnk);
    println!("Transferred {} B in {} seconds, {} B/s", len, duration.as_secs_f64(), len as f64 / duration.as_secs_f64());

    Ok(())
}

async fn drain_stream(
    stream: &mut iroh_net::endpoint::RecvStream,
    read_unordered: bool,
) -> Result<(usize, Duration, u64)> {
    let mut read = 0;

    let download_start = Instant::now();
    let mut first_byte = true;
    let mut ttfb = download_start.elapsed();

    let mut num_chunks: u64 = 0;

    if read_unordered {
        while let Some(chunk) = stream.read_chunk(usize::MAX, false).await? {
            if first_byte {
                ttfb = download_start.elapsed();
                first_byte = false;
            }
            read += chunk.bytes.len();
            num_chunks += 1;
        }
    } else {
        // These are 32 buffers, for reading approximately 32kB at once
        #[rustfmt::skip]
        let mut bufs = [
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
        ];

        while let Some(n) = stream.read_chunks(&mut bufs[..]).await? {
            if first_byte {
                ttfb = download_start.elapsed();
                first_byte = false;
            }
            read += bufs.iter().take(n).map(|buf| buf.len()).sum::<usize>();
            num_chunks += 1;
        }
    }

    Ok((read, ttfb, num_chunks))
}

async fn send_data_on_stream(stream: &mut iroh_net::endpoint::SendStream, stream_size: u64) -> Result<()> {
    const DATA: &[u8] = &[0xAB; 7*1024 * 1024];
    let bytes_data = Bytes::from_static(DATA);

    let full_chunks = stream_size / (DATA.len() as u64);
    let remaining = (stream_size % (DATA.len() as u64)) as usize;

    for _ in 0..full_chunks {
        stream
            .write_chunk(bytes_data.clone())
            .await
            .context("failed sending data")?;
    }

    if remaining != 0 {
        stream
            .write_chunk(bytes_data.slice(0..remaining))
            .await
            .context("failed sending data")?;
    }

    stream.finish().context("failed finishing stream")?;
    stream
        .stopped()
        .await
        .context("failed to wait for stream to be stopped")?;

    Ok(())
}

fn parse_byte_size(s: &str) -> Result<u64, std::num::ParseIntError> {
    let s = s.trim();

    let multiplier = match s.chars().last() {
        Some('T') => 1024 * 1024 * 1024 * 1024,
        Some('G') => 1024 * 1024 * 1024,
        Some('M') => 1024 * 1024,
        Some('k') => 1024,
        _ => 1,
    };

    let s = if multiplier != 1 {
        &s[..s.len() - 1]
    } else {
        s
    };

    let base: u64 = u64::from_str(s)?;

    Ok(base * multiplier)
}