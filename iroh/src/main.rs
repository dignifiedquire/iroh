use std::collections::{BTreeMap, HashMap};
use std::net::{Ipv4Addr, SocketAddrV4};
use std::time::Duration;
use std::{fmt, net::SocketAddr, path::PathBuf, str::FromStr};

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use console::{style, Emoji};
use futures::{Stream, StreamExt};
use indicatif::{HumanBytes, MultiProgress, ProgressBar, ProgressStyle};
use iroh::{
    bytes::{
        cid::Blake3Cid,
        get,
        get::io::{GetInteractive, StdWriter},
        provider::{Database, Ticket, FNAME_PATHS},
        Hash,
    },
    config::{iroh_config_path, iroh_data_root, Config, CONFIG_FILE_NAME, ENV_PREFIX},
    net::{
        client::create_quinn_client,
        hp::derp::DerpMap,
        tls::{Keypair, PeerId},
    },
    node::Node,
    rpc_protocol::*,
};
use quic_rpc::transport::quinn::{QuinnConnection, QuinnServerEndpoint};
use quic_rpc::{RpcClient, ServiceEndpoint};
use tracing_subscriber::{prelude::*, EnvFilter};

const DEFAULT_RPC_PORT: u16 = 0x1337;
const RPC_ALPN: [u8; 17] = *b"n0/provider-rpc/1";
const MAX_RPC_CONNECTIONS: u32 = 16;
const MAX_RPC_STREAMS: u64 = 1024;

/// Send data.
///
/// The iroh command line tool has two modes: provide and get.
///
/// The provide mode is a long-running process binding to a socket which the get mode
/// contacts to request data.  By default the provide process also binds to an RPC port
/// which allows adding additional data to be provided as well as a few other maintenance
/// commands.
///
/// The get mode retrieves data from the provider, for this it needs the hash, provider
/// address and PeerID as well as an authentication code.  The get-ticket subcommand is a
/// shortcut to provide all this information conveniently in a single ticket.
#[derive(Parser, Debug, Clone)]
#[clap(version)]
struct Cli {
    #[clap(subcommand)]
    command: Commands,
    /// Log SSL pre-master key to file in SSLKEYLOGFILE environment variable.
    #[clap(long)]
    keylog: bool,
    /// Bind address on which to serve Prometheus metrics
    #[cfg(feature = "metrics")]
    #[clap(long)]
    metrics_addr: Option<SocketAddr>,
    #[clap(long)]
    cfg: Option<PathBuf>,
}

#[derive(Debug, Clone)]
enum ProviderRpcPort {
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

#[derive(Subcommand, Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum Commands {
    /// Diagnostic commands for the derp relay protocol.
    Doctor {
        /// Commands for doctor - defined in the mod
        #[clap(subcommand)]
        command: iroh::doctor::Commands,
    },

    /// Serve data from the given path.
    ///
    /// If PATH is a folder all files in that folder will be served.  If no PATH is
    /// specified reads from STDIN.
    Provide {
        /// Path to initial file or directory to provide
        path: Option<PathBuf>,
        #[clap(long, short)]
        /// Listening address to bind to
        #[clap(long, short, default_value_t = SocketAddr::from(iroh::node::DEFAULT_BIND_ADDR))]
        addr: SocketAddr,
        /// RPC port, set to "disabled" to disable RPC
        #[clap(long, default_value_t = ProviderRpcPort::Enabled(DEFAULT_RPC_PORT))]
        rpc_port: ProviderRpcPort,
    },
    /// List availble content on the provider.
    #[clap(subcommand)]
    List(ListCommands),
    /// Validate hashes on the running provider.
    Validate {
        /// RPC port of the provider
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
    /// Shutdown provider.
    Shutdown {
        /// Shutdown mode.
        ///
        /// Hard shutdown will immediately terminate the process, soft shutdown will wait
        /// for all connections to close.
        #[clap(long, default_value_t = false)]
        force: bool,
        /// RPC port of the provider
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
    /// Identify the running provider.
    Id {
        /// RPC port of the provider
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
    /// Add data from PATH to the running provider's database.
    Add {
        /// The path to the file or folder to add
        path: PathBuf,
        /// RPC port
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
    /// Fetch the data identified by HASH from a provider.
    Get {
        /// The hash to retrieve, as a Blake3 CID
        hash: Blake3Cid,
        /// PeerId of the provider
        #[clap(long, short)]
        peer: PeerId,
        /// Addresses of the provider.
        #[clap(long, short)]
        addrs: Vec<SocketAddr>,
        /// Directory in which to save the file(s), defaults to writing to STDOUT
        #[clap(long, short)]
        out: Option<PathBuf>,
        /// True to download a single blob, false (default) to download a collection and its children.
        #[clap(long, default_value_t = false)]
        single: bool,
    },
    /// Fetch data from a provider using a ticket.
    ///
    /// The ticket contains all hash, authentication and connection information to connect
    /// to the provider.  It is a simpler, but slightly less flexible alternative to the
    /// `get` subcommand.
    GetTicket {
        /// Directory in which to save the file(s), defaults to writing to STDOUT
        #[clap(long, short)]
        out: Option<PathBuf>,
        /// Ticket containing everything to retrieve the data from a provider.
        ticket: Ticket,
    },
    /// List listening addresses of the provider.
    Addresses {
        /// RPC port
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
}

#[derive(Subcommand, Debug, Clone)]
enum ListCommands {
    /// List the available blobs on the running provider.
    Blobs {
        /// RPC port of the provider
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
    /// List the available collections on the running provider.
    Collections {
        /// RPC port of the provider
        #[clap(long, default_value_t = DEFAULT_RPC_PORT)]
        rpc_port: u16,
    },
}

async fn make_rpc_client(
    rpc_port: u16,
) -> anyhow::Result<RpcClient<ProviderService, QuinnConnection<ProviderResponse, ProviderRequest>>>
{
    let bind_addr = SocketAddrV4::new(Ipv4Addr::UNSPECIFIED, 0).into();
    let endpoint = create_quinn_client(bind_addr, None, vec![RPC_ALPN.to_vec()], false)?;
    let addr = SocketAddr::new(Ipv4Addr::LOCALHOST.into(), rpc_port);
    let server_name = "localhost".to_string();
    let connection = QuinnConnection::new(endpoint, addr, server_name);
    let client = RpcClient::<ProviderService, _>::new(connection);
    // Do a version request to check if the server is running.
    let _version = tokio::time::timeout(Duration::from_secs(1), client.rpc(VersionRequest))
        .await
        .context("iroh server is not running")??;
    Ok(client)
}

struct ProvideProgressState {
    mp: MultiProgress,
    pbs: HashMap<u64, ProgressBar>,
}

impl ProvideProgressState {
    fn new() -> Self {
        Self {
            mp: MultiProgress::new(),
            pbs: HashMap::new(),
        }
    }

    fn found(&mut self, name: String, id: u64, size: u64) {
        let pb = self.mp.add(ProgressBar::new(size));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {msg} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})").unwrap()
            .progress_chars("=>-"));
        pb.set_message(name);
        pb.set_length(size);
        pb.set_position(0);
        pb.enable_steady_tick(Duration::from_millis(500));
        self.pbs.insert(id, pb);
    }

    fn progress(&mut self, id: u64, progress: u64) {
        if let Some(pb) = self.pbs.get_mut(&id) {
            pb.set_position(progress);
        }
    }

    fn done(&mut self, id: u64, _hash: Hash) {
        if let Some(pb) = self.pbs.remove(&id) {
            pb.finish_and_clear();
            self.mp.remove(&pb);
        }
    }

    fn all_done(self) {
        self.mp.clear().ok();
    }

    fn error(self) {
        self.mp.clear().ok();
    }
}

struct ValidateProgressState {
    mp: MultiProgress,
    pbs: HashMap<u64, ProgressBar>,
    overall: ProgressBar,
    total: u64,
    errors: u64,
    successes: u64,
}

impl ValidateProgressState {
    fn new() -> Self {
        let mp = MultiProgress::new();
        let overall = mp.add(ProgressBar::new(0));
        overall.enable_steady_tick(Duration::from_millis(500));
        Self {
            mp,
            pbs: HashMap::new(),
            overall,
            total: 0,
            errors: 0,
            successes: 0,
        }
    }

    fn starting(&mut self, total: u64) {
        self.total = total;
        self.errors = 0;
        self.successes = 0;
        self.overall.set_position(0);
        self.overall.set_length(total);
        self.overall.set_style(
            ProgressStyle::default_bar()
                .template("{spinner:.green} [{bar:60.cyan/blue}] {msg}")
                .unwrap()
                .progress_chars("=>-"),
        );
    }

    fn add_entry(&mut self, id: u64, hash: Hash, path: Option<PathBuf>, size: u64) {
        let pb = self.mp.insert_before(&self.overall, ProgressBar::new(size));
        pb.set_style(ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:40.cyan/blue}] {msg} {bytes}/{total_bytes} ({bytes_per_sec}, eta {eta})").unwrap()
            .progress_chars("=>-"));
        let msg = if let Some(path) = path {
            path.display().to_string()
        } else {
            format!("outboard {}", Blake3Cid(hash))
        };
        pb.set_message(msg);
        pb.set_position(0);
        pb.set_length(size);
        pb.enable_steady_tick(Duration::from_millis(500));
        self.pbs.insert(id, pb);
    }

    fn progress(&mut self, id: u64, progress: u64) {
        if let Some(pb) = self.pbs.get_mut(&id) {
            pb.set_position(progress);
        }
    }

    fn abort(self, error: String) {
        let error_line = self.mp.add(ProgressBar::new(0));
        error_line.set_style(ProgressStyle::default_bar().template("{msg}").unwrap());
        error_line.set_message(error);
    }

    fn done(&mut self, id: u64, error: Option<String>) {
        if let Some(pb) = self.pbs.remove(&id) {
            let ok_char = style(Emoji("✔", "OK")).green();
            let fail_char = style(Emoji("✗", "Error")).red();
            let ok = error.is_none();
            let msg = match error {
                Some(error) => format!("{} {} {}", pb.message(), fail_char, error),
                None => format!("{} {}", pb.message(), ok_char),
            };
            if ok {
                self.successes += 1;
            } else {
                self.errors += 1;
            }
            self.overall.set_position(self.errors + self.successes);
            self.overall.set_message(format!(
                "Overall {} {}, {} {}",
                self.errors, fail_char, self.successes, ok_char
            ));
            if ok {
                pb.finish_and_clear();
            } else {
                pb.set_style(ProgressStyle::default_bar().template("{msg}").unwrap());
                pb.finish_with_message(msg);
            }
        }
    }
}

#[derive(Debug)]
struct ProvideResponseEntry {
    pub name: String,
    pub size: u64,
}

async fn aggregate_add_response<S, E>(
    stream: S,
) -> anyhow::Result<(Hash, Vec<ProvideResponseEntry>)>
where
    S: Stream<Item = std::result::Result<ProvideProgress, E>> + Unpin,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut stream = stream;
    let mut collection_hash = None;
    let mut collections = BTreeMap::<u64, (String, u64, Option<Hash>)>::new();
    let mut mp = Some(ProvideProgressState::new());
    while let Some(item) = stream.next().await {
        match item? {
            ProvideProgress::Found { name, id, size } => {
                tracing::info!("Found({id},{name},{size})");
                if let Some(mp) = mp.as_mut() {
                    mp.found(name.clone(), id, size);
                }
                collections.insert(id, (name, size, None));
            }
            ProvideProgress::Progress { id, offset } => {
                tracing::info!("Progress({id}, {offset})");
                if let Some(mp) = mp.as_mut() {
                    mp.progress(id, offset);
                }
            }
            ProvideProgress::Done { hash, id } => {
                tracing::info!("Done({id},{hash:?})");
                if let Some(mp) = mp.as_mut() {
                    mp.done(id, hash);
                }
                match collections.get_mut(&id) {
                    Some((_, _, ref mut h)) => {
                        *h = Some(hash);
                    }
                    None => {
                        anyhow::bail!("Got Done for unknown collection id {id}");
                    }
                }
            }
            ProvideProgress::AllDone { hash } => {
                tracing::info!("AllDone({hash:?})");
                if let Some(mp) = mp.take() {
                    mp.all_done();
                }
                collection_hash = Some(hash);
                break;
            }
            ProvideProgress::Abort(e) => {
                if let Some(mp) = mp.take() {
                    mp.error();
                }
                anyhow::bail!("Error while adding data: {e}");
            }
        }
    }
    let hash = collection_hash.context("Missing hash for collection")?;
    let entries = collections
        .into_iter()
        .map(|(_, (name, size, hash))| {
            let _hash = hash.context(format!("Missing hash for {name}"))?;
            Ok(ProvideResponseEntry { name, size })
        })
        .collect::<Result<Vec<_>>>()?;
    Ok((hash, entries))
}

fn print_add_response(hash: Hash, entries: Vec<ProvideResponseEntry>) {
    let mut total_size = 0;
    for ProvideResponseEntry { name, size, .. } in entries {
        total_size += size;
        println!("- {}: {}", name, HumanBytes(size));
    }
    println!("Total: {}", HumanBytes(total_size));
    println!();
    println!("Collection: {}", Blake3Cid::new(hash));
}

#[cfg(feature = "metrics")]
fn init_metrics_collection(
    metrics_addr: Option<SocketAddr>,
    rt: &iroh_bytes::runtime::Handle,
) -> Option<tokio::task::JoinHandle<()>> {
    iroh_metrics::metrics::init_metrics();
    // doesn't start the server if the address is None
    if let Some(metrics_addr) = metrics_addr {
        return Some(rt.main().spawn(async move {
            iroh_metrics::metrics::start_metrics_server(metrics_addr)
                .await
                .unwrap_or_else(|e| {
                    eprintln!("Failed to start metrics server: {e}");
                });
        }));
    }
    tracing::info!("Metrics server not started, no address provided");
    None
}

fn main() -> Result<()> {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .thread_name("main-runtime")
        .worker_threads(2)
        .enable_all()
        .build()?;
    rt.block_on(main_impl())?;
    // give the runtime some time to finish, but do not wait indefinitely.
    // there are cases where the a runtime thread is blocked doing io.
    // e.g. reading from stdin.
    rt.shutdown_timeout(Duration::from_millis(500));
    Ok(())
}

async fn main_impl() -> Result<()> {
    let tokio = tokio::runtime::Handle::current();
    let tpc = tokio_util::task::LocalPoolHandle::new(num_cpus::get());
    let rt = iroh::bytes::runtime::Handle::new(tokio, tpc);
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .init();
    let cli = Cli::parse();

    let config_path = iroh_config_path(CONFIG_FILE_NAME).context("invalid config path")?;
    let sources = [Some(config_path.as_path()), cli.cfg.as_deref()];
    let config = Config::load(
        // potential config files
        &sources,
        // env var prefix for this config
        ENV_PREFIX,
        // map of present command line arguments
        // args.make_overrides_map(),
        HashMap::<String, String>::new(),
    )?;

    #[cfg(feature = "metrics")]
    let metrics_fut = init_metrics_collection(cli.metrics_addr, &rt);

    let r = match cli.command {
        Commands::Get {
            hash,
            peer,
            addrs,
            out,
            single,
        } => {
            let opts = get::Options {
                addrs,
                peer_id: peer,
                keylog: cli.keylog,
                derp_map: config.derp_map(),
            };
            let get = GetInteractive::Hash {
                hash: *hash.as_hash(),
                opts,
                single,
            };
            tokio::select! {
                biased;
                res = get.get_interactive(&StdWriter::Stderr, out) => res,
                _ = tokio::signal::ctrl_c() => {
                    println!("Ending transfer early...");
                    Ok(())
                }
            }
        }
        Commands::GetTicket { out, ticket } => {
            let get = GetInteractive::Ticket {
                ticket,
                keylog: cli.keylog,
                derp_map: config.derp_map(),
            };
            tokio::select! {
                biased;
                res = get.get_interactive(&StdWriter::Stderr, out) => res,
                _ = tokio::signal::ctrl_c() => {
                    println!("Ending transfer early...");
                    Ok(())
                }
            }
        }
        Commands::Provide {
            path,
            addr,
            rpc_port,
        } => {
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

            let provider = provide(
                db.clone(),
                addr,
                key,
                cli.keylog,
                rpc_port.into(),
                config.derp_map(),
                &rt,
            )
            .await?;
            let controller = provider.controller();

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
                    let ticket = provider.ticket(hash).await?;
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
        Commands::List(ListCommands::Blobs { rpc_port }) => {
            let client = make_rpc_client(rpc_port).await?;
            let mut response = client.server_streaming(ListBlobsRequest).await?;
            while let Some(item) = response.next().await {
                let item = item?;
                println!(
                    "{} {} ({})",
                    item.path.display(),
                    Blake3Cid(item.hash),
                    HumanBytes(item.size),
                );
            }
            Ok(())
        }
        Commands::List(ListCommands::Collections { rpc_port }) => {
            let client = make_rpc_client(rpc_port).await?;
            let mut response = client.server_streaming(ListCollectionsRequest).await?;
            while let Some(collection) = response.next().await {
                let collection = collection?;
                println!(
                    "{}: {} {} ({})",
                    Blake3Cid(collection.hash),
                    collection.total_blobs_count,
                    if collection.total_blobs_count > 1 {
                        "blobs"
                    } else {
                        "blob"
                    },
                    HumanBytes(collection.total_blobs_size),
                );
            }
            Ok(())
        }
        Commands::Validate { rpc_port } => {
            let client = make_rpc_client(rpc_port).await?;
            let mut state = ValidateProgressState::new();
            let mut response = client.server_streaming(ValidateRequest).await?;

            while let Some(item) = response.next().await {
                match item? {
                    ValidateProgress::Starting { total } => {
                        state.starting(total);
                    }
                    ValidateProgress::Entry {
                        id,
                        hash,
                        path,
                        size,
                    } => {
                        state.add_entry(id, hash, path, size);
                    }
                    ValidateProgress::Progress { id, offset } => {
                        state.progress(id, offset);
                    }
                    ValidateProgress::Done { id, error } => {
                        state.done(id, error);
                    }
                    ValidateProgress::Abort(error) => {
                        state.abort(error.to_string());
                        break;
                    }
                    ValidateProgress::AllDone => {
                        break;
                    }
                }
            }
            Ok(())
        }
        Commands::Shutdown { force, rpc_port } => {
            let client = make_rpc_client(rpc_port).await?;
            client.rpc(ShutdownRequest { force }).await?;
            Ok(())
        }
        Commands::Id { rpc_port } => {
            let client = make_rpc_client(rpc_port).await?;
            let response = client.rpc(IdRequest).await?;

            println!("Listening address: {:#?}", response.listen_addrs);
            println!("PeerID: {}", response.peer_id);
            Ok(())
        }
        Commands::Add { path, rpc_port } => {
            let client = make_rpc_client(rpc_port).await?;
            let absolute = path.canonicalize()?;
            println!("Adding {} as {}...", path.display(), absolute.display());
            let stream = client
                .server_streaming(ProvideRequest { path: absolute })
                .await?;
            let (hash, entries) = aggregate_add_response(stream).await?;
            print_add_response(hash, entries);
            Ok(())
        }
        Commands::Addresses { rpc_port } => {
            let client = make_rpc_client(rpc_port).await?;
            let response = client.rpc(AddrsRequest).await?;
            println!("Listening addresses: {:?}", response.addrs);
            Ok(())
        }
        Commands::Doctor { command } => iroh::doctor::run(command, &config).await,
    };

    #[cfg(feature = "metrics")]
    if let Some(metrics_fut) = metrics_fut {
        metrics_fut.abort();
        drop(metrics_fut);
    }
    r
}

async fn provide(
    db: Database,
    addr: SocketAddr,
    key: Option<PathBuf>,
    keylog: bool,
    rpc_port: Option<u16>,
    dm: Option<DerpMap>,
    rt: &iroh::bytes::runtime::Handle,
) -> Result<Node> {
    let keypair = get_keypair(key).await?;

    let mut builder = Node::builder(db).keylog(keylog);
    if let Some(dm) = dm {
        builder = builder.derp_map(dm);
    }
    let builder = builder.bind_addr(addr).runtime(rt);

    let provider = if let Some(rpc_port) = rpc_port {
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
