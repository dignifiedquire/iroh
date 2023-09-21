use std::net::SocketAddr;

use clap::Parser;
use iroh_net::{
    defaults::{default_derp_map, TEST_REGION_ID},
    derp::DerpMap,
    key::{PublicKey, SecretKey},
    magic_endpoint::accept_conn,
    MagicEndpoint, PeerAddr,
};
use tracing::{debug, info};
use url::Url;

const EXAMPLE_ALPN: &[u8] = b"n0/iroh/examples/magic/0";

#[derive(Debug, Parser)]
struct Cli {
    #[clap(short, long)]
    secret: Option<String>,
    #[clap(short, long, default_value = "n0/iroh/examples/magic/0")]
    alpn: String,
    #[clap(short, long, default_value = "0")]
    bind_port: u16,
    #[clap(short, long)]
    derp_url: Option<Url>,
    #[clap(subcommand)]
    command: Command,
}

#[derive(Debug, Parser)]
enum Command {
    Listen,
    Connect {
        peer_id: String,
        addrs: Option<Vec<SocketAddr>>,
        derp_region: Option<u16>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();
    let args = Cli::parse();
    let secret_key = match args.secret {
        None => {
            let secret_key = SecretKey::generate();
            println!("our secret key: {}", fmt_secret(&secret_key));
            secret_key
        }
        Some(key) => parse_secret(&key)?,
    };

    let derp_map = match args.derp_url {
        None => default_derp_map(),
        // use `region_id` 65535, which is reserved for testing and experiments
        Some(url) => DerpMap::from_url(url, TEST_REGION_ID),
    };

    let endpoint = MagicEndpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![args.alpn.to_string().into_bytes()])
        .enable_derp(derp_map)
        .bind(args.bind_port)
        .await?;

    let me = endpoint.peer_id();
    let local_addr = endpoint.local_addr()?;
    println!("magic socket listening on {local_addr:?}");
    println!("our peer id: {me}");

    match args.command {
        Command::Listen => {
            while let Some(conn) = endpoint.accept().await {
                let (peer_id, alpn, conn) = accept_conn(conn).await?;
                info!(
                    "new connection from {peer_id} with ALPN {alpn} (coming from {})",
                    conn.remote_address()
                );
                tokio::spawn(async move {
                    let (mut send, mut recv) = conn.accept_bi().await?;
                    debug!("accepted bi stream, waiting for data...");
                    let message = recv.read_to_end(1000).await?;
                    let message = String::from_utf8(message)?;
                    println!("received: {message}");

                    let message = format!("hi! you connected to {me}. bye bye");
                    send.write_all(message.as_bytes()).await?;
                    send.finish().await?;

                    Ok::<_, anyhow::Error>(())
                });
            }
        }
        Command::Connect {
            peer_id,
            addrs,
            derp_region,
        } => {
            let peer_id: PublicKey = peer_id.parse()?;
            let addrs = addrs.unwrap_or_default();
            let peer_addr = PeerAddr::from_parts(peer_id, derp_region, addrs);
            let conn = endpoint.connect(peer_addr, EXAMPLE_ALPN).await?;
            info!("connected");

            let (mut send, mut recv) = conn.open_bi().await?;

            let message = format!("hello here's {me}");
            send.write_all(message.as_bytes()).await?;
            send.finish().await?;
            let message = recv.read_to_end(100).await?;
            let message = String::from_utf8(message)?;
            println!("received: {message}");
        }
    }
    Ok(())
}

fn fmt_secret(secret_key: &SecretKey) -> String {
    let mut text = data_encoding::BASE32_NOPAD.encode(&secret_key.to_bytes());
    text.make_ascii_lowercase();
    text
}
fn parse_secret(secret: &str) -> anyhow::Result<SecretKey> {
    let bytes: [u8; 32] = data_encoding::BASE32_NOPAD
        .decode(secret.to_ascii_uppercase().as_bytes())?
        .try_into()
        .map_err(|_| anyhow::anyhow!("Invalid secret"))?;
    let key = SecretKey::from(bytes);
    Ok(key)
}
