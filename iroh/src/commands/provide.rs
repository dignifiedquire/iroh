use std::{
    fmt,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
};

use anyhow::{ensure, Context, Result};
use iroh::{
    node::{Node, StaticTokenAuthHandler},
    rpc_protocol::{ProvideRequest, ProviderRequest, ProviderResponse, ProviderService},
};
use iroh_bytes::{
    database::{Database, FNAME_PATHS},
    protocol::RequestToken,
    provider::BaoReadonlyDb,
    util::runtime,
};
use iroh_net::{hp::derp::DerpMap, tls::Keypair};
use quic_rpc::{transport::quinn::QuinnServerEndpoint, ServiceEndpoint};

use crate::config::iroh_data_root;

use super::{
    add::{aggregate_add_response, print_add_response},
    MAX_RPC_CONNECTIONS, MAX_RPC_STREAMS, RPC_ALPN,
};

#[derive(Debug)]
pub struct ProvideOptions {
    pub addr: SocketAddr,
    pub rpc_port: ProviderRpcPort,
    pub keylog: bool,
    pub request_token: Option<RequestToken>,
    pub derp_map: Option<DerpMap>,
}

pub async fn run(rt: &runtime::Handle, path: Option<PathBuf>, opts: ProvideOptions) -> Result<()> {
    if let Some(ref path) = path {
        ensure!(
            path.exists(),
            "Cannot provide nonexistent path: {}",
            path.display()
        );
    }

    let iroh_data_root = iroh_data_root()?;
    let marker = iroh_data_root.join(FNAME_PATHS);
    let db = {
        if iroh_data_root.is_dir() && marker.exists() {
            // try to load db
            Database::load(&iroh_data_root).await.with_context(|| {
                format!(
                    "Failed to load iroh database from {}",
                    iroh_data_root.display()
                )
            })?
        } else {
            // directory does not exist, create an empty db
            Database::default()
        }
    };
    let key = Some(iroh_data_root.join("keypair"));
    let token = opts.request_token.clone();
    let provider = provide(db.clone(), rt, key, opts).await?;
    let controller = provider.controller();
    if let Some(t) = token.as_ref() {
        println!("Request token: {}", t);
    }

    // task that will add data to the provider, either from a file or from stdin
    let fut = {
        let provider = provider.clone();
        tokio::spawn(async move {
            let (path, tmp_path) = if let Some(path) = path {
                let absolute = path.canonicalize()?;
                println!("Adding {} as {}...", path.display(), absolute.display());
                (absolute, None)
            } else {
                // Store STDIN content into a temporary file
                let (file, path) = tempfile::NamedTempFile::new()?.into_parts();
                let mut file = tokio::fs::File::from_std(file);
                let path_buf = path.to_path_buf();
                // Copy from stdin to the file, until EOF
                tokio::io::copy(&mut tokio::io::stdin(), &mut file).await?;
                println!("Adding from stdin...");
                // return the TempPath to keep it alive
                (path_buf, Some(path))
            };
            // tell the provider to add the data
            let stream = controller.server_streaming(ProvideRequest { path }).await?;
            let (hash, entries) = aggregate_add_response(stream).await?;
            print_add_response(hash, entries);
            let ticket = provider.ticket(hash, token).await?;
            println!("All-in-one ticket: {ticket}");
            anyhow::Ok(tmp_path)
        })
    };

    let provider2 = provider.clone();
    tokio::select! {
        biased;
        _ = tokio::signal::ctrl_c() => {
            println!("Shutting down provider...");
            provider2.shutdown();
        }
        res = provider => {
            res?;
        }
    }
    // persist the db to disk.
    db.save(&iroh_data_root).await?;

    // the future holds a reference to the temp file, so we need to
    // keep it for as long as the provider is running. The drop(fut)
    // makes this explicit.
    fut.abort();
    drop(fut);
    Ok(())
}

async fn provide<D: BaoReadonlyDb>(
    db: D,
    rt: &runtime::Handle,
    key: Option<PathBuf>,
    opts: ProvideOptions,
) -> Result<Node<D>> {
    let keypair = get_keypair(key).await?;

    let mut builder = Node::builder(db)
        .custom_auth_handler(Arc::new(StaticTokenAuthHandler::new(opts.request_token)))
        .keylog(opts.keylog);
    if let Some(dm) = opts.derp_map {
        builder = builder.derp_map(dm);
    }
    let builder = builder.bind_addr(opts.addr).runtime(rt);

    let provider = if let Some(rpc_port) = opts.rpc_port.into() {
        let rpc_endpoint = make_rpc_endpoint(&keypair, rpc_port)?;
        builder
            .rpc_endpoint(rpc_endpoint)
            .keypair(keypair)
            .spawn()
            .await?
    } else {
        builder.keypair(keypair).spawn().await?
    };

    let eps = provider.local_endpoints().await?;
    println!("Listening addresses:");
    for ep in eps {
        println!("  {}", ep.addr);
    }
    println!("PeerID: {}", provider.peer_id());
    println!();
    Ok(provider)
}

async fn get_keypair(key: Option<PathBuf>) -> Result<Keypair> {
    match key {
        Some(key_path) => {
            if key_path.exists() {
                let keystr = tokio::fs::read(key_path).await?;
                let keypair = Keypair::try_from_openssh(keystr)?;
                Ok(keypair)
            } else {
                let keypair = Keypair::generate();
                let ser_key = keypair.to_openssh()?;
                if let Some(parent) = key_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                tokio::fs::write(key_path, ser_key).await?;
                Ok(keypair)
            }
        }
        None => {
            // No path provided, just generate one
            Ok(Keypair::generate())
        }
    }
}

/// Makes a an RPC endpoint that uses a QUIC transport
fn make_rpc_endpoint(
    keypair: &Keypair,
    rpc_port: u16,
) -> Result<impl ServiceEndpoint<ProviderService>> {
    let rpc_addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, rpc_port));
    let rpc_quinn_endpoint = quinn::Endpoint::server(
        iroh::node::make_server_config(
            keypair,
            MAX_RPC_STREAMS,
            MAX_RPC_CONNECTIONS,
            vec![RPC_ALPN.to_vec()],
        )?,
        rpc_addr,
    )?;
    let rpc_endpoint =
        QuinnServerEndpoint::<ProviderRequest, ProviderResponse>::new(rpc_quinn_endpoint)?;
    Ok(rpc_endpoint)
}

#[derive(Debug, Clone)]
pub enum ProviderRpcPort {
    Enabled(u16),
    Disabled,
}

impl From<ProviderRpcPort> for Option<u16> {
    fn from(value: ProviderRpcPort) -> Self {
        match value {
            ProviderRpcPort::Enabled(port) => Some(port),
            ProviderRpcPort::Disabled => None,
        }
    }
}

impl fmt::Display for ProviderRpcPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProviderRpcPort::Enabled(port) => write!(f, "{port}"),
            ProviderRpcPort::Disabled => write!(f, "disabled"),
        }
    }
}

impl FromStr for ProviderRpcPort {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "disabled" {
            Ok(ProviderRpcPort::Disabled)
        } else {
            Ok(ProviderRpcPort::Enabled(s.parse()?))
        }
    }
}
