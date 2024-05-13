//! Tool to get information about the current network environment of a node,
//! and to test connectivity to specific other nodes.
use std::{
    collections::HashMap,
    io,
    net::SocketAddr,
    num::NonZeroU16,
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::{Duration, Instant},
};

use crate::config::{iroh_data_root, NodeConfig};

use anyhow::Context;
use clap::Subcommand;
use console::style;
use derive_more::Display;
use futures_lite::StreamExt;
use indicatif::{HumanBytes, MultiProgress, ProgressBar};
use iroh::{
    base::ticket::{BlobTicket, Ticket},
    blobs::{
        store::{ReadableStore, Store as _},
        util::progress::{FlumeProgressSender, ProgressSender},
    },
    docs::{Capability, DocTicket},
    net::{
        defaults::DEFAULT_RELAY_STUN_PORT,
        discovery::{
            dns::DnsDiscovery, pkarr_publish::PkarrPublisher, ConcurrentDiscovery, Discovery,
        },
        dns::default_resolver,
        endpoint::{self, Connection, ConnectionTypeStream, RecvStream, SendStream},
        key::{PublicKey, SecretKey},
        netcheck, portmapper,
        relay::{RelayMap, RelayMode, RelayUrl},
        ticket::NodeTicket,
        util::CancelOnDrop,
        Endpoint, NodeAddr, NodeId,
    },
    util::{path::IrohPaths, progress::ProgressWriter},
};
use portable_atomic::AtomicU64;
use postcard::experimental::max_size::MaxSize;
use ratatui::backend::Backend;
use serde::{Deserialize, Serialize};
use tokio::{io::AsyncWriteExt, sync};

use iroh::net::metrics::MagicsockMetrics;
use iroh_metrics::core::Core;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyEventKind},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use rand::Rng;
use ratatui::{prelude::*, widgets::*};

#[derive(Debug, Clone, derive_more::Display)]
pub enum SecretKeyOption {
    /// Generate random secret key
    Random,
    /// Use local secret key
    Local,
    /// Explicitly specify a secret key
    Hex(String),
}

impl std::str::FromStr for SecretKeyOption {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s_lower = s.to_ascii_lowercase();
        Ok(if s_lower == "random" {
            SecretKeyOption::Random
        } else if s_lower == "local" {
            SecretKeyOption::Local
        } else {
            SecretKeyOption::Hex(s.to_string())
        })
    }
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Report on the current network environment, using either an explicitly provided stun host
    /// or the settings from the config file.
    Report {
        /// Explicitly provided stun host. If provided, this will disable relay and just do stun.
        #[clap(long)]
        stun_host: Option<String>,
        /// The port of the STUN server.
        #[clap(long, default_value_t = DEFAULT_RELAY_STUN_PORT)]
        stun_port: u16,
    },
    /// Wait for incoming requests from iroh doctor connect
    Accept {
        /// Our own secret key, in hex. If not specified, the locally configured key will be used.
        #[clap(long, default_value_t = SecretKeyOption::Local)]
        secret_key: SecretKeyOption,

        /// Number of bytes to send to the remote for each test
        #[clap(long, default_value_t = 1024 * 1024 * 16)]
        size: u64,

        /// Number of iterations to run the test for. If not specified, the test will run forever.
        #[clap(long)]
        iterations: Option<u64>,

        /// Use a local relay
        #[clap(long)]
        local_relay_server: bool,

        /// Do not allow the node to dial and be dialed by id only.
        ///
        /// This disables DNS discovery, which would allow the node to dial other nodes by id only.
        /// And it disables Pkarr Publishing, which would allow the node to announce its address for dns discovery.
        ///
        /// Default is `false`
        #[clap(long, default_value_t = false)]
        disable_discovery: bool,
    },
    /// Connect to an iroh doctor accept node.
    Connect {
        /// hex node id of the node to connect to
        dial: PublicKey,

        /// One or more remote endpoints to use when dialing
        #[clap(long)]
        remote_endpoint: Vec<SocketAddr>,

        /// Our own secret key, in hex. If not specified, a random key will be generated.
        #[clap(long, default_value_t = SecretKeyOption::Random)]
        secret_key: SecretKeyOption,

        /// Use a local relay
        ///
        /// Overrides the `relay_url` field.
        #[clap(long)]
        local_relay_server: bool,

        /// The relay url the peer you are dialing can be found on.
        ///
        /// If `local_relay_server` is true, this field is ignored.
        ///
        /// When `None`, or if attempting to dial an unknown url, no hole punching can occur.
        ///
        /// Default is `None`.
        #[clap(long)]
        relay_url: Option<RelayUrl>,

        /// Do not allow the node to dial and be dialed by id only.
        ///
        /// This disables DNS discovery, which would allow the node to dial other nodes by id only.
        /// And it disables Pkarr Publishing, which would allow the node to announce its address for dns discovery.
        ///
        /// Default is `false`
        #[clap(long, default_value_t = false)]
        disable_discovery: bool,
    },
    /// Probe the port mapping protocols.
    PortMapProbe {
        /// Whether to enable UPnP.
        #[clap(long)]
        enable_upnp: bool,
        /// Whether to enable PCP.
        #[clap(long)]
        enable_pcp: bool,
        /// Whether to enable NAT-PMP.
        #[clap(long)]
        enable_nat_pmp: bool,
    },
    /// Attempt to get a port mapping to the given local port.
    PortMap {
        /// Protocol to use for port mapping. One of ["upnp", "nat_pmp", "pcp"].
        protocol: String,
        /// Local port to get a mapping.
        local_port: NonZeroU16,
        /// How long to wait for an external port to be ready in seconds.
        #[clap(long, default_value_t = 10)]
        timeout_secs: u64,
    },
    /// Get the latencies of the different relay url
    ///
    /// Tests the latencies of the default relay url and nodes. To test custom urls or nodes,
    /// adjust the `Config`.
    RelayUrls {
        /// How often to execute.
        #[clap(long, default_value_t = 5)]
        count: usize,
    },
    /// Inspect a ticket.
    TicketInspect {
        ticket: String,
        #[clap(long)]
        zbase32: bool,
    },
    /// Perform a metadata consistency check on a blob store.
    BlobConsistencyCheck {
        /// Path of the blob store to validate. For iroh, this is the blobs subdirectory
        /// in the iroh data directory. But this can also be used for apps that embed
        /// just iroh-blobs.
        path: PathBuf,
        /// Try to get the store into a consistent state by removing orphaned data
        /// and broken entries.
        ///
        /// Caution, this might remove data.
        #[clap(long)]
        repair: bool,
    },
    /// Validate the actual content of a blob store.
    BlobValidate {
        /// Path of the blob store to validate. For iroh, this is the blobs subdirectory
        /// in the iroh data directory. But this can also be used for apps that embed
        /// just iroh-blobs.
        path: PathBuf,
        /// Try to get the store into a consistent state by downgrading entries from
        /// complete to partial if data is missing etc.
        #[clap(long)]
        repair: bool,
    },
    /// Plot metric counters
    Plot {
        /// How often to collect samples in milliseconds.
        #[clap(long, default_value_t = 500)]
        interval: u64,
        /// Which metrics to plot. Commas separated list of metric names.
        metrics: String,
        /// What the plotted time frame should be in seconds.
        #[clap(long, default_value_t = 60)]
        timeframe: usize,
        /// Endpoint to scrape for prometheus metrics
        #[clap(long, default_value = "http://localhost:9090")]
        scrape_url: String,
    },
}

#[derive(Debug, Serialize, Deserialize, MaxSize)]
enum TestStreamRequest {
    Echo { bytes: u64 },
    Drain { bytes: u64 },
    Send { bytes: u64, block_size: u32 },
}

#[derive(Debug, Clone, Copy)]
struct TestConfig {
    size: u64,
    iterations: Option<u64>,
}

fn update_pb(
    task: &'static str,
    pb: Option<ProgressBar>,
    total_bytes: u64,
    mut updates: sync::mpsc::Receiver<u64>,
) -> tokio::task::JoinHandle<()> {
    if let Some(pb) = pb {
        pb.set_message(task);
        pb.set_position(0);
        pb.set_length(total_bytes);
        tokio::spawn(async move {
            while let Some(position) = updates.recv().await {
                pb.set_position(position);
            }
        })
    } else {
        tokio::spawn(std::future::ready(()))
    }
}

/// handle a test stream request
async fn handle_test_request(
    mut send: SendStream,
    mut recv: RecvStream,
    gui: &Gui,
) -> anyhow::Result<()> {
    let mut buf = [0u8; TestStreamRequest::POSTCARD_MAX_SIZE];
    recv.read_exact(&mut buf).await?;
    let request: TestStreamRequest = postcard::from_bytes(&buf)?;
    let pb = Some(gui.pb.clone());
    match request {
        TestStreamRequest::Echo { bytes } => {
            // copy the stream back
            let (mut send, updates) = ProgressWriter::new(&mut send);
            let t0 = Instant::now();
            let progress = update_pb("echo", pb, bytes, updates);
            tokio::io::copy(&mut recv, &mut send).await?;
            let elapsed = t0.elapsed();
            drop(send);
            progress.await?;
            gui.set_echo(bytes, elapsed);
        }
        TestStreamRequest::Drain { bytes } => {
            // drain the stream
            let (mut send, updates) = ProgressWriter::new(tokio::io::sink());
            let progress = update_pb("recv", pb, bytes, updates);
            let t0 = Instant::now();
            tokio::io::copy(&mut recv, &mut send).await?;
            let elapsed = t0.elapsed();
            drop(send);
            progress.await?;
            gui.set_recv(bytes, elapsed);
        }
        TestStreamRequest::Send { bytes, block_size } => {
            // send the requested number of bytes, in blocks of the requested size
            let (mut send, updates) = ProgressWriter::new(&mut send);
            let progress = update_pb("send", pb, bytes, updates);
            let t0 = Instant::now();
            send_blocks(&mut send, bytes, block_size).await?;
            drop(send);
            let elapsed = t0.elapsed();
            progress.await?;
            gui.set_send(bytes, elapsed);
        }
    }
    send.finish().await?;
    Ok(())
}

async fn send_blocks(
    mut send: impl tokio::io::AsyncWrite + Unpin,
    total_bytes: u64,
    block_size: u32,
) -> anyhow::Result<()> {
    // send the requested number of bytes, in blocks of the requested size
    let buf = vec![0u8; block_size as usize];
    let mut remaining = total_bytes;
    while remaining > 0 {
        let n = remaining.min(block_size as u64);
        send.write_all(&buf[..n as usize]).await?;
        remaining -= n;
    }
    Ok(())
}

async fn report(
    stun_host: Option<String>,
    stun_port: u16,
    config: &NodeConfig,
) -> anyhow::Result<()> {
    let port_mapper = portmapper::Client::default();
    let dns_resolver = default_resolver().clone();
    let mut client = netcheck::Client::new(Some(port_mapper), dns_resolver)?;

    let dm = match stun_host {
        Some(host_name) => {
            let url = host_name.parse()?;
            // creating a relay map from host name and stun port
            RelayMap::default_from_node(url, stun_port)
        }
        None => config.relay_map()?.unwrap_or_else(RelayMap::empty),
    };
    println!("getting report using relay map {dm:#?}");

    let r = client.get_report(dm, None, None).await?;
    println!("{r:#?}");
    Ok(())
}

/// Contain all the gui state
struct Gui {
    #[allow(dead_code)]
    mp: MultiProgress,
    pb: ProgressBar,
    #[allow(dead_code)]
    counters: ProgressBar,
    send_pb: ProgressBar,
    recv_pb: ProgressBar,
    echo_pb: ProgressBar,
    #[allow(dead_code)]
    counter_task: Option<CancelOnDrop>,
}

impl Gui {
    fn new(endpoint: Endpoint, node_id: NodeId) -> Self {
        let mp = MultiProgress::new();
        mp.set_draw_target(indicatif::ProgressDrawTarget::stderr());
        let counters = mp.add(ProgressBar::hidden());
        let conn_info = mp.add(ProgressBar::hidden());
        let send_pb = mp.add(ProgressBar::hidden());
        let recv_pb = mp.add(ProgressBar::hidden());
        let echo_pb = mp.add(ProgressBar::hidden());
        let style = indicatif::ProgressStyle::default_bar()
            .template("{msg}")
            .unwrap();
        send_pb.set_style(style.clone());
        recv_pb.set_style(style.clone());
        echo_pb.set_style(style.clone());
        conn_info.set_style(style.clone());
        counters.set_style(style);
        let pb = mp.add(indicatif::ProgressBar::hidden());
        pb.enable_steady_tick(Duration::from_millis(100));
        pb.set_style(indicatif::ProgressStyle::default_bar()
            .template("{spinner:.green} [{bar:80.cyan/blue}] {msg} {bytes}/{total_bytes} ({bytes_per_sec})").unwrap()
            .progress_chars("█▉▊▋▌▍▎▏ "));
        let counters2 = counters.clone();
        let counter_task = tokio::spawn(async move {
            loop {
                Self::update_counters(&counters2);
                Self::update_connection_info(&conn_info, &endpoint, &node_id);
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        });
        Self {
            mp,
            pb,
            counters,
            send_pb,
            recv_pb,
            echo_pb,
            counter_task: Some(CancelOnDrop::new(
                "counter_task",
                counter_task.abort_handle(),
            )),
        }
    }

    fn update_connection_info(target: &ProgressBar, endpoint: &Endpoint, node_id: &NodeId) {
        let format_latency = |x: Option<Duration>| {
            x.map(|x| format!("{:.6}s", x.as_secs_f64()))
                .unwrap_or_else(|| "unknown".to_string())
        };
        let msg = match endpoint.connection_info(*node_id) {
            Some(endpoint::ConnectionInfo {
                relay_url,
                conn_type,
                latency,
                addrs,
                ..
            }) => {
                let relay_url = relay_url
                    .map(|x| x.relay_url.to_string())
                    .unwrap_or_else(|| "unknown".to_string());
                let latency = format_latency(latency);
                let addrs = addrs
                    .into_iter()
                    .map(|addr_info| {
                        format!("{} ({})", addr_info.addr, format_latency(addr_info.latency))
                    })
                    .collect::<Vec<_>>()
                    .join("; ");
                format!(
                    "relay url: {}, latency: {}, connection type: {}, addrs: [{}]",
                    relay_url, latency, conn_type, addrs
                )
            }
            None => "connection info unavailable".to_string(),
        };
        target.set_message(msg);
    }

    fn update_counters(target: &ProgressBar) {
        if let Some(core) = Core::get() {
            let metrics = core.get_collector::<MagicsockMetrics>().unwrap();
            let send_ipv4 = HumanBytes(metrics.send_ipv4.get());
            let send_ipv6 = HumanBytes(metrics.send_ipv6.get());
            let send_relay = HumanBytes(metrics.send_relay.get());
            let recv_data_relay = HumanBytes(metrics.recv_data_relay.get());
            let recv_data_ipv4 = HumanBytes(metrics.recv_data_ipv4.get());
            let recv_data_ipv6 = HumanBytes(metrics.recv_data_ipv6.get());
            let text = format!(
                r#"Counters

Relay:
  send: {send_relay}
  recv: {recv_data_relay}
Ipv4:
  send: {send_ipv4}
  recv: {recv_data_ipv4}
Ipv6:
  send: {send_ipv6}
  recv: {recv_data_ipv6}
"#,
            );
            target.set_message(text);
        }
    }

    fn set_send(&self, b: u64, d: Duration) {
        Self::set_bench_speed(&self.send_pb, "send", b, d);
    }

    fn set_recv(&self, b: u64, d: Duration) {
        Self::set_bench_speed(&self.recv_pb, "recv", b, d);
    }

    fn set_echo(&self, b: u64, d: Duration) {
        Self::set_bench_speed(&self.echo_pb, "echo", b, d);
    }

    fn set_bench_speed(pb: &ProgressBar, text: &str, b: u64, d: Duration) {
        pb.set_message(format!(
            "{}: {}/s",
            text,
            HumanBytes((b as f64 / d.as_secs_f64()) as u64)
        ));
    }

    fn clear(&self) {
        self.mp.clear().ok();
    }
}

async fn active_side(
    connection: Connection,
    config: &TestConfig,
    gui: Option<&Gui>,
) -> anyhow::Result<()> {
    let n = config.iterations.unwrap_or(u64::MAX);
    if let Some(gui) = gui {
        let pb = Some(&gui.pb);
        for _ in 0..n {
            let d = send_test(&connection, config, pb).await?;
            gui.set_send(config.size, d);
            let d = recv_test(&connection, config, pb).await?;
            gui.set_recv(config.size, d);
            let d = echo_test(&connection, config, pb).await?;
            gui.set_echo(config.size, d);
        }
    } else {
        let pb = None;
        for _ in 0..n {
            let _d = send_test(&connection, config, pb).await?;
            let _d = recv_test(&connection, config, pb).await?;
            let _d = echo_test(&connection, config, pb).await?;
        }
    }
    Ok(())
}

async fn send_test_request(
    send: &mut SendStream,
    request: &TestStreamRequest,
) -> anyhow::Result<()> {
    let mut buf = [0u8; TestStreamRequest::POSTCARD_MAX_SIZE];
    postcard::to_slice(&request, &mut buf)?;
    send.write_all(&buf).await?;
    Ok(())
}

async fn echo_test(
    connection: &Connection,
    config: &TestConfig,
    pb: Option<&indicatif::ProgressBar>,
) -> anyhow::Result<Duration> {
    let size = config.size;
    let (mut send, mut recv) = connection.open_bi().await?;
    send_test_request(&mut send, &TestStreamRequest::Echo { bytes: size }).await?;
    let (mut sink, updates) = ProgressWriter::new(tokio::io::sink());
    let copying = tokio::spawn(async move { tokio::io::copy(&mut recv, &mut sink).await });
    let progress = update_pb("echo", pb.cloned(), size, updates);
    let t0 = Instant::now();
    send_blocks(&mut send, size, 1024 * 1024).await?;
    send.finish().await?;
    let received = copying.await??;
    anyhow::ensure!(received == size);
    let duration = t0.elapsed();
    progress.await?;
    Ok(duration)
}

async fn send_test(
    connection: &Connection,
    config: &TestConfig,
    pb: Option<&indicatif::ProgressBar>,
) -> anyhow::Result<Duration> {
    let size = config.size;
    let (mut send, mut recv) = connection.open_bi().await?;
    send_test_request(&mut send, &TestStreamRequest::Drain { bytes: size }).await?;
    let (mut send_with_progress, updates) = ProgressWriter::new(&mut send);
    let copying =
        tokio::spawn(async move { tokio::io::copy(&mut recv, &mut tokio::io::sink()).await });
    let progress = update_pb("send", pb.cloned(), size, updates);
    let t0 = Instant::now();
    send_blocks(&mut send_with_progress, size, 1024 * 1024).await?;
    drop(send_with_progress);
    send.finish().await?;
    drop(send);
    let received = copying.await??;
    anyhow::ensure!(received == 0);
    let duration = t0.elapsed();
    progress.await?;
    Ok(duration)
}

async fn recv_test(
    connection: &Connection,
    config: &TestConfig,
    pb: Option<&indicatif::ProgressBar>,
) -> anyhow::Result<Duration> {
    let size = config.size;
    let (mut send, mut recv) = connection.open_bi().await?;
    let t0 = Instant::now();
    let (mut sink, updates) = ProgressWriter::new(tokio::io::sink());
    send_test_request(
        &mut send,
        &TestStreamRequest::Send {
            bytes: size,
            block_size: 1024 * 1024,
        },
    )
    .await?;
    let copying = tokio::spawn(async move { tokio::io::copy(&mut recv, &mut sink).await });
    let progress = update_pb("recv", pb.cloned(), size, updates);
    send.finish().await?;
    let received = copying.await??;
    anyhow::ensure!(received == size);
    let duration = t0.elapsed();
    progress.await?;
    Ok(duration)
}

/// Passive side that just accepts connections and answers requests (echo, drain or send)
async fn passive_side(gui: Gui, connection: Connection) -> anyhow::Result<()> {
    loop {
        match connection.accept_bi().await {
            Ok((send, recv)) => {
                if let Err(cause) = handle_test_request(send, recv, &gui).await {
                    eprintln!("Error handling test request {cause}");
                }
            }
            Err(cause) => {
                eprintln!("error accepting bidi stream {cause}");
                break Err(cause.into());
            }
        };
    }
}

fn configure_local_relay_map() -> RelayMap {
    let stun_port = DEFAULT_RELAY_STUN_PORT;
    let url = "http://localhost:3340".parse().unwrap();
    RelayMap::default_from_node(url, stun_port)
}

const DR_RELAY_ALPN: [u8; 11] = *b"n0/drderp/1";

async fn make_endpoint(
    secret_key: SecretKey,
    relay_map: Option<RelayMap>,
    discovery: Option<Box<dyn Discovery>>,
) -> anyhow::Result<Endpoint> {
    tracing::info!(
        "public key: {}",
        hex::encode(secret_key.public().as_bytes())
    );
    tracing::info!("relay map {:#?}", relay_map);

    let mut transport_config = endpoint::TransportConfig::default();
    transport_config.keep_alive_interval(Some(Duration::from_secs(5)));
    transport_config.max_idle_timeout(Some(Duration::from_secs(10).try_into().unwrap()));

    let endpoint = Endpoint::builder()
        .secret_key(secret_key)
        .alpns(vec![DR_RELAY_ALPN.to_vec()])
        .transport_config(transport_config);

    let endpoint = match discovery {
        Some(discovery) => endpoint.discovery(discovery),
        None => endpoint,
    };

    let endpoint = match relay_map {
        Some(relay_map) => endpoint.relay_mode(RelayMode::Custom(relay_map)),
        None => endpoint,
    };
    let endpoint = endpoint.bind(0).await?;

    tokio::time::timeout(Duration::from_secs(10), endpoint.local_endpoints().next())
        .await
        .context("wait for relay connection")?
        .context("no endpoints")?;

    Ok(endpoint)
}

async fn connect(
    node_id: NodeId,
    secret_key: SecretKey,
    direct_addresses: Vec<SocketAddr>,
    relay_url: Option<RelayUrl>,
    relay_map: Option<RelayMap>,
    discovery: Option<Box<dyn Discovery>>,
) -> anyhow::Result<()> {
    let endpoint = make_endpoint(secret_key, relay_map, discovery).await?;

    tracing::info!("dialing {:?}", node_id);
    let node_addr = NodeAddr::from_parts(node_id, relay_url, direct_addresses);
    let conn = endpoint.connect(node_addr, &DR_RELAY_ALPN).await;
    match conn {
        Ok(connection) => {
            let maybe_stream = endpoint.conn_type_stream(&node_id);
            let gui = Gui::new(endpoint, node_id);
            if let Ok(stream) = maybe_stream {
                log_connection_changes(gui.mp.clone(), node_id, stream);
            }

            if let Err(cause) = passive_side(gui, connection).await {
                eprintln!("error handling connection: {cause}");
            }
        }
        Err(cause) => {
            eprintln!("unable to connect to {node_id}: {cause}");
        }
    }

    Ok(())
}

/// format a socket addr so that it does not have to be escaped on the console
fn format_addr(addr: SocketAddr) -> String {
    if addr.is_ipv6() {
        format!("'{addr}'")
    } else {
        format!("{addr}")
    }
}

async fn accept(
    secret_key: SecretKey,
    config: TestConfig,
    relay_map: Option<RelayMap>,
    discovery: Option<Box<dyn Discovery>>,
) -> anyhow::Result<()> {
    let endpoint = make_endpoint(secret_key.clone(), relay_map, discovery).await?;
    let endpoints = endpoint
        .local_endpoints()
        .next()
        .await
        .context("no endpoints")?;
    let remote_addrs = endpoints
        .iter()
        .map(|endpoint| format!("--remote-endpoint {}", format_addr(endpoint.addr)))
        .collect::<Vec<_>>()
        .join(" ");
    println!("Connect to this node using one of the following commands:\n");
    println!(
        "\tUsing the relay url and direct connections:\niroh doctor connect {} {}\n",
        secret_key.public(),
        remote_addrs,
    );
    if let Some(relay_url) = endpoint.my_relay() {
        println!(
            "\tUsing just the relay url:\niroh doctor connect {} --relay-url {}\n",
            secret_key.public(),
            relay_url,
        );
    }
    if endpoint.discovery().is_some() {
        println!(
            "\tUsing just the node id:\niroh doctor connect {}\n",
            secret_key.public(),
        );
    }
    let connections = Arc::new(AtomicU64::default());
    while let Some(connecting) = endpoint.accept().await {
        let connections = connections.clone();
        let endpoint = endpoint.clone();
        tokio::task::spawn(async move {
            let n = connections.fetch_add(1, portable_atomic::Ordering::SeqCst);
            match connecting.await {
                Ok(connection) => {
                    if n == 0 {
                        let Ok(remote_peer_id) = endpoint::get_remote_node_id(&connection) else {
                            return;
                        };
                        println!("Accepted connection from {}", remote_peer_id);
                        let t0 = Instant::now();
                        let gui = Gui::new(endpoint.clone(), remote_peer_id);
                        if let Ok(stream) = endpoint.conn_type_stream(&remote_peer_id) {
                            log_connection_changes(gui.mp.clone(), remote_peer_id, stream);
                        }
                        let res = active_side(connection, &config, Some(&gui)).await;
                        gui.clear();
                        let dt = t0.elapsed().as_secs_f64();
                        if let Err(cause) = res {
                            eprintln!("Test finished after {dt}s: {cause}",);
                        } else {
                            eprintln!("Test finished after {dt}s",);
                        }
                    } else {
                        // silent
                        active_side(connection, &config, None).await.ok();
                    }
                }
                Err(cause) => {
                    eprintln!("error accepting connection {cause}");
                }
            };
            connections.sub(1, portable_atomic::Ordering::SeqCst);
        });
    }

    Ok(())
}

fn log_connection_changes(pb: MultiProgress, node_id: NodeId, mut stream: ConnectionTypeStream) {
    tokio::spawn(async move {
        let start = Instant::now();
        while let Some(conn_type) = stream.next().await {
            pb.println(format!(
                "Connection with {node_id:#} changed: {conn_type} (after {:?})",
                start.elapsed()
            ))
            .ok();
        }
    });
}

async fn port_map(protocol: &str, local_port: NonZeroU16, timeout: Duration) -> anyhow::Result<()> {
    // create the config that enables exclusively the required protocol
    let mut enable_upnp = false;
    let mut enable_pcp = false;
    let mut enable_nat_pmp = false;
    match protocol.to_ascii_lowercase().as_ref() {
        "upnp" => enable_upnp = true,
        "nat_pmp" => enable_nat_pmp = true,
        "pcp" => enable_pcp = true,
        other => anyhow::bail!("Unknown port mapping protocol {other}"),
    }
    let config = portmapper::Config {
        enable_upnp,
        enable_pcp,
        enable_nat_pmp,
    };
    let port_mapper = portmapper::Client::new(config);
    let mut watcher = port_mapper.watch_external_address();
    port_mapper.update_local_port(local_port);

    // wait for the mapping to be ready, or timeout waiting for a change.
    match tokio::time::timeout(timeout, watcher.changed()).await {
        Ok(Ok(_)) => match *watcher.borrow() {
            Some(address) => {
                println!("Port mapping ready: {address}");
                // Ensure the port mapper remains alive until the end.
                drop(port_mapper);
                Ok(())
            }
            None => anyhow::bail!("No port mapping found"),
        },
        Ok(Err(_recv_err)) => anyhow::bail!("Service dropped. This is a bug"),
        Err(_) => anyhow::bail!("Timed out waiting for a port mapping"),
    }
}

async fn port_map_probe(config: portmapper::Config) -> anyhow::Result<()> {
    println!("probing port mapping protocols with {config:?}");
    let port_mapper = portmapper::Client::new(config);
    let probe_rx = port_mapper.probe();
    let probe = probe_rx.await?.map_err(|e| anyhow::anyhow!(e))?;
    println!("{probe}");
    Ok(())
}

async fn relay_urls(count: usize, config: NodeConfig) -> anyhow::Result<()> {
    let key = SecretKey::generate();
    if config.relay_nodes.is_empty() {
        println!("No relay nodes specified in the config file.");
    }

    let dns_resolver = default_resolver();
    let mut clients = HashMap::new();
    for node in &config.relay_nodes {
        let secret_key = key.clone();
        let client = iroh::net::relay::http::ClientBuilder::new(node.url.clone())
            .build(secret_key, dns_resolver.clone());

        clients.insert(node.url.clone(), client);
    }

    let mut success = Vec::new();
    let mut fail = Vec::new();

    for i in 0..count {
        println!("Round {}/{count}", i + 1);
        let relay_nodes = config.relay_nodes.clone();
        for node in relay_nodes.into_iter() {
            let mut node_details = NodeDetails {
                connect: None,
                latency: None,
                error: None,
                host: node.url.clone(),
            };

            let client = clients.get(&node.url).map(|(c, _)| c.clone()).unwrap();

            if client.is_connected().await? {
                client.close_for_reconnect().await?;
            }
            assert!(!client.is_connected().await?);

            let start = std::time::Instant::now();
            match tokio::time::timeout(Duration::from_secs(2), client.connect()).await {
                Err(e) => {
                    tracing::warn!("connect timeout");
                    node_details.error = Some(e.to_string());
                }
                Ok(Err(e)) => {
                    tracing::warn!("connect error");
                    node_details.error = Some(e.to_string());
                }
                Ok(_) => {
                    assert!(client.is_connected().await?);
                    node_details.connect = Some(start.elapsed());

                    match client.ping().await {
                        Ok(latency) => {
                            node_details.latency = Some(latency);
                        }
                        Err(e) => {
                            tracing::warn!("ping error: {:?}", e);
                            node_details.error = Some(e.to_string());
                        }
                    }
                }
            }

            if node_details.error.is_none() {
                success.push(node_details);
            } else {
                fail.push(node_details);
            }
        }
    }

    // success.sort_by_key(|d| d.latency);
    if !success.is_empty() {
        println!("Relay Node Latencies:");
        println!();
    }
    for node in success {
        println!("{node}");
        println!();
    }
    if !fail.is_empty() {
        println!("Connection Failures:");
        println!();
    }
    for node in fail {
        println!("{node}");
        println!();
    }

    Ok(())
}

struct NodeDetails {
    connect: Option<Duration>,
    latency: Option<Duration>,
    host: RelayUrl,
    error: Option<String>,
}

impl std::fmt::Display for NodeDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.error {
            None => {
                write!(
                    f,
                    "Node {}\nConnect: {:?}\nLatency: {:?}",
                    self.host,
                    self.connect.unwrap_or_default(),
                    self.latency.unwrap_or_default(),
                )
            }
            Some(ref err) => {
                write!(f, "Node {}\nConnection Error: {:?}", self.host, err,)
            }
        }
    }
}

fn create_secret_key(secret_key: SecretKeyOption) -> anyhow::Result<SecretKey> {
    Ok(match secret_key {
        SecretKeyOption::Random => SecretKey::generate(),
        SecretKeyOption::Hex(hex) => {
            let bytes = hex::decode(hex)?;
            SecretKey::try_from(&bytes[..])?
        }
        SecretKeyOption::Local => {
            let path = IrohPaths::SecretKey.with_root(iroh_data_root()?);
            if path.exists() {
                let bytes = std::fs::read(&path)?;
                SecretKey::try_from_openssh(bytes)?
            } else {
                println!(
                    "Local key not found in {}. Using random key.",
                    path.display()
                );
                SecretKey::generate()
            }
        }
    })
}

fn create_discovery(disable_discovery: bool, secret_key: &SecretKey) -> Option<Box<dyn Discovery>> {
    if disable_discovery {
        None
    } else {
        Some(Box::new(ConcurrentDiscovery::from_services(vec![
            // Enable DNS discovery by default
            Box::new(DnsDiscovery::n0_dns()),
            // Enable pkarr publishing by default
            Box::new(PkarrPublisher::n0_dns(secret_key.clone())),
        ])))
    }
}

fn bold<T: Display>(x: T) -> String {
    style(x).bold().to_string()
}

fn to_z32(node_id: NodeId) -> String {
    pkarr::PublicKey::try_from(*node_id.as_bytes())
        .unwrap()
        .to_z32()
}

fn print_node_addr(prefix: &str, node_addr: &NodeAddr, zbase32: bool) {
    let node = if zbase32 {
        to_z32(node_addr.node_id)
    } else {
        node_addr.node_id.to_string()
    };
    println!("{}node-id: {}", prefix, bold(node));
    if let Some(relay_url) = node_addr.relay_url() {
        println!("{}relay-url: {}", prefix, bold(relay_url));
    }
    for addr in node_addr.direct_addresses() {
        println!("{}addr: {}", prefix, bold(addr.to_string()));
    }
}

fn inspect_ticket(ticket: &str, zbase32: bool) -> anyhow::Result<()> {
    if ticket.starts_with(BlobTicket::KIND) {
        let ticket = BlobTicket::from_str(ticket).context("failed parsing blob ticket")?;
        println!("BlobTicket");
        println!("  hash: {}", bold(ticket.hash()));
        println!("  format: {}", bold(ticket.format()));
        println!("  NodeInfo");
        print_node_addr("    ", ticket.node_addr(), zbase32);
    } else if ticket.starts_with(DocTicket::KIND) {
        let ticket = DocTicket::from_str(ticket).context("failed parsing doc ticket")?;
        println!("DocTicket:\n");
        match ticket.capability {
            Capability::Read(namespace) => {
                println!("  read: {}", bold(namespace));
            }
            Capability::Write(secret) => {
                println!("  write: {}", bold(secret));
            }
        }
        for node in &ticket.nodes {
            print_node_addr("    ", node, zbase32);
        }
    } else if ticket.starts_with(NodeTicket::KIND) {
        let ticket = NodeTicket::from_str(ticket).context("failed parsing node ticket")?;
        println!("NodeTicket");
        print_node_addr("  ", ticket.node_addr(), zbase32);
    }

    Ok(())
}

pub async fn run(command: Commands, config: &NodeConfig) -> anyhow::Result<()> {
    let data_dir = iroh_data_root()?;
    let _guard = crate::logging::init_terminal_and_file_logging(&config.file_logs, &data_dir)?;
    match command {
        Commands::Report {
            stun_host,
            stun_port,
        } => report(stun_host, stun_port, config).await,
        Commands::Connect {
            dial,
            secret_key,
            local_relay_server,
            relay_url,
            remote_endpoint,
            disable_discovery,
        } => {
            let (relay_map, relay_url) = if local_relay_server {
                let dm = configure_local_relay_map();
                let url = dm.urls().next().unwrap().clone();
                (Some(dm), Some(url))
            } else {
                (config.relay_map()?, relay_url)
            };
            let secret_key = create_secret_key(secret_key)?;

            let discovery = create_discovery(disable_discovery, &secret_key);
            connect(
                dial,
                secret_key,
                remote_endpoint,
                relay_url,
                relay_map,
                discovery,
            )
            .await
        }
        Commands::Accept {
            secret_key,
            local_relay_server,
            size,
            iterations,
            disable_discovery,
        } => {
            let relay_map = if local_relay_server {
                Some(configure_local_relay_map())
            } else {
                config.relay_map()?
            };
            let secret_key = create_secret_key(secret_key)?;
            let config = TestConfig { size, iterations };
            let discovery = create_discovery(disable_discovery, &secret_key);
            accept(secret_key, config, relay_map, discovery).await
        }
        Commands::PortMap {
            protocol,
            local_port,
            timeout_secs,
        } => port_map(&protocol, local_port, Duration::from_secs(timeout_secs)).await,
        Commands::PortMapProbe {
            enable_upnp,
            enable_pcp,
            enable_nat_pmp,
        } => {
            let config = portmapper::Config {
                enable_upnp,
                enable_pcp,
                enable_nat_pmp,
            };

            port_map_probe(config).await
        }
        Commands::RelayUrls { count } => {
            let config = NodeConfig::load(None).await?;
            relay_urls(count, config).await
        }
        Commands::TicketInspect { ticket, zbase32 } => inspect_ticket(&ticket, zbase32),
        Commands::BlobConsistencyCheck { path, repair } => {
            let blob_store = iroh::blobs::store::fs::Store::load(path).await?;
            let (send, recv) = flume::bounded(1);
            let task = tokio::spawn(async move {
                while let Ok(msg) = recv.recv_async().await {
                    println!("{:?}", msg);
                }
            });
            blob_store
                .consistency_check(repair, FlumeProgressSender::new(send).boxed())
                .await?;
            task.await?;
            Ok(())
        }
        Commands::BlobValidate { path, repair } => {
            let blob_store = iroh::blobs::store::fs::Store::load(path).await?;
            let (send, recv) = flume::bounded(1);
            let task = tokio::spawn(async move {
                while let Ok(msg) = recv.recv_async().await {
                    println!("{:?}", msg);
                }
            });
            blob_store
                .validate(repair, FlumeProgressSender::new(send).boxed())
                .await?;
            task.await?;
            Ok(())
        }
        Commands::Plot {
            interval,
            metrics,
            timeframe,
            scrape_url,
        } => {
            let metrics: Vec<String> = metrics.split(',').map(|s| s.to_string()).collect();
            let interval = Duration::from_millis(interval);

            enable_raw_mode()?;
            let mut stdout = io::stdout();
            execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
            let backend = CrosstermBackend::new(stdout);
            let mut terminal = Terminal::new(backend)?;

            let app = PlotterApp::new(metrics, timeframe, scrape_url);
            let res = run_plotter(&mut terminal, app, interval).await;
            disable_raw_mode()?;
            execute!(
                terminal.backend_mut(),
                LeaveAlternateScreen,
                DisableMouseCapture
            )?;
            terminal.show_cursor()?;

            if let Err(err) = res {
                println!("{err:?}");
            }

            Ok(())
        }
    }
}

async fn run_plotter<B: Backend>(
    terminal: &mut Terminal<B>,
    mut app: PlotterApp,
    tick_rate: Duration,
) -> anyhow::Result<()> {
    let mut last_tick = Instant::now();
    loop {
        terminal.draw(|f| plotter_draw(f, &mut app))?;

        if crossterm::event::poll(Duration::from_millis(100))? {
            if let Event::Key(key) = event::read()? {
                if key.kind == KeyEventKind::Press {
                    if let KeyCode::Char(c) = key.code {
                        app.on_key(c)
                    }
                }
            }
        }
        if last_tick.elapsed() >= tick_rate {
            app.on_tick().await;
            last_tick = Instant::now();
        }
        if app.should_quit {
            return Ok(());
        }
    }
}

fn area_into_chunks(area: Rect, n: usize, horizontal: bool) -> std::rc::Rc<[Rect]> {
    let mut constraints = vec![];
    for _ in 0..n {
        constraints.push(Constraint::Percentage(100 / n as u16));
    }
    let layout = match horizontal {
        true => Layout::horizontal(constraints),
        false => Layout::vertical(constraints),
    };
    layout.split(area)
}

fn generate_layout_chunks(area: Rect, n: usize) -> Vec<Rect> {
    if n < 4 {
        let chunks = area_into_chunks(area, n, false);
        return chunks.iter().copied().collect();
    }
    let main_chunks = area_into_chunks(area, 2, true);
    let left_chunks = area_into_chunks(main_chunks[0], n / 2 + n % 2, false);
    let right_chunks = area_into_chunks(main_chunks[1], n / 2, false);
    let mut chunks = vec![];
    chunks.extend(left_chunks.iter());
    chunks.extend(right_chunks.iter());
    chunks
}

fn plotter_draw(f: &mut Frame, app: &mut PlotterApp) {
    let area = f.size();

    let metrics_cnt = app.metrics.len();
    let areas = generate_layout_chunks(area, metrics_cnt);

    for (i, metric) in app.metrics.iter().enumerate() {
        plot_chart(f, areas[i], app, metric);
    }
}

fn plot_chart(frame: &mut Frame, area: Rect, app: &PlotterApp, metric: &str) {
    let elapsed = app.internal_ts.as_secs_f64();
    let data = app.data.get(metric).unwrap().clone();
    let data_y_range = app.data_y_range.get(metric).unwrap();

    let moved = (elapsed / 15.0).floor() * 15.0 - app.timeframe as f64;
    let moved = moved.max(0.0);
    let x_start = 0.0 + moved;
    let x_end = moved + app.timeframe as f64 + 25.0;

    let y_start = data_y_range.0;
    let y_end = data_y_range.1;

    let datasets = vec![Dataset::default()
        .name(metric)
        .marker(symbols::Marker::Dot)
        .graph_type(GraphType::Line)
        .style(Style::default().fg(Color::Cyan))
        .data(&data)];

    // TODO(arqu): labels are incorrectly spaced for > 3 labels https://github.com/ratatui-org/ratatui/issues/334
    let x_labels = vec![
        Span::styled(
            format!("{:.1}s", x_start),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.1}s", x_start + (x_end - x_start) / 2.0)),
        Span::styled(
            format!("{:.1}s", x_end),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    let mut y_labels = vec![Span::styled(
        format!("{:.1}", y_start),
        Style::default().add_modifier(Modifier::BOLD),
    )];

    for i in 1..=10 {
        y_labels.push(Span::raw(format!(
            "{:.1}",
            y_start + (y_end - y_start) / 10.0 * i as f64
        )));
    }

    y_labels.push(Span::styled(
        format!("{:.1}", y_end),
        Style::default().add_modifier(Modifier::BOLD),
    ));

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .title(format!("Chart: {}", metric)),
        )
        .x_axis(
            Axis::default()
                .title("X Axis")
                .style(Style::default().fg(Color::Gray))
                .labels(x_labels)
                .bounds([x_start, x_end]),
        )
        .y_axis(
            Axis::default()
                .title("Y Axis")
                .style(Style::default().fg(Color::Gray))
                .labels(y_labels)
                .bounds([y_start, y_end]),
        );

    frame.render_widget(chart, area);
}

struct PlotterApp {
    should_quit: bool,
    metrics: Vec<String>,
    start_ts: Instant,
    data: HashMap<String, Vec<(f64, f64)>>,
    data_y_range: HashMap<String, (f64, f64)>,
    timeframe: usize,
    rng: rand::rngs::ThreadRng,
    freeze: bool,
    internal_ts: Duration,
    scrape_url: String,
}

impl PlotterApp {
    fn new(metrics: Vec<String>, timeframe: usize, scrape_url: String) -> Self {
        let data = metrics.iter().map(|m| (m.clone(), vec![])).collect();
        let data_y_range = metrics.iter().map(|m| (m.clone(), (0.0, 0.0))).collect();
        Self {
            should_quit: false,
            metrics,
            start_ts: Instant::now(),
            data,
            data_y_range,
            timeframe: timeframe - 25,
            rng: rand::thread_rng(),
            freeze: false,
            internal_ts: Duration::default(),
            scrape_url,
        }
    }

    fn on_key(&mut self, c: char) {
        match c {
            'q' => {
                self.should_quit = true;
            }
            'f' => {
                self.freeze = !self.freeze;
            }
            _ => {}
        }
    }

    async fn on_tick(&mut self) {
        if self.freeze {
            return;
        }

        let req = reqwest::Client::new().get(&self.scrape_url).send().await;
        if req.is_err() {
            return;
        }
        let data = req.unwrap().text().await.unwrap();
        let metrics_response = parse_prometheus_metrics(&data);
        if metrics_response.is_err() {
            return;
        }
        let metrics_response = metrics_response.unwrap();
        self.internal_ts = self.start_ts.elapsed();
        for metric in &self.metrics {
            let val = if metric.eq("random") {
                self.rng.gen_range(0..101) as f64
            } else if let Some(v) = metrics_response.get(metric) {
                *v
            } else {
                0.0
            };
            let e = self.data.entry(metric.clone()).or_default();
            e.push((self.internal_ts.as_secs_f64(), val));
            let yr = self.data_y_range.get_mut(metric).unwrap();
            if val * 1.1 < yr.0 {
                yr.0 = val * 1.2;
            }
            if val * 1.1 > yr.1 {
                yr.1 = val * 1.2;
            }
        }
    }
}

fn parse_prometheus_metrics(data: &str) -> anyhow::Result<HashMap<String, f64>> {
    let mut metrics = HashMap::new();
    for line in data.lines() {
        if line.starts_with('#') {
            continue;
        }
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() < 2 {
            continue;
        }
        let metric = parts[0];
        let value = parts[1].parse::<f64>();
        if value.is_err() {
            continue;
        }
        metrics.insert(metric.to_string(), value.unwrap());
    }
    Ok(metrics)
}
