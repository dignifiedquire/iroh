use std::collections::HashMap;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use iroh_ctl::{
    gateway::{run_command as run_gateway_command, Gateway},
    metrics,
    p2p::{run_command as run_p2p_command, P2p},
    store::{run_command as run_store_command, Store},
};
use iroh_rpc_client::Client;
use iroh_util::{iroh_home_path, make_config};
use prometheus_client::registry::Registry;

use iroh_ctl::{
    config::{Config, CONFIG_FILE_NAME, ENV_PREFIX},
    status,
};

#[derive(Parser, Debug, Clone)]
#[clap(version, about, long_about = None, propagate_version = true)]
struct Cli {
    #[clap(long)]
    cfg: Option<PathBuf>,
    #[clap(long = "no-metrics")]
    no_metrics: bool,
    #[clap(subcommand)]
    command: Commands,
}

impl Cli {
    fn make_overrides_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("metrics.debug".to_string(), self.no_metrics.to_string());
        map
    }
}

#[derive(Subcommand, Debug, Clone)]
enum Commands {
    /// status checks the health of the different processes
    #[clap(about = "Check the health of the different iroh processes.")]
    Status {
        #[clap(short, long)]
        /// when true, updates the status table whenever a change in a process's status occurs
        watch: bool,
    },
    Version,
    P2p(P2p),
    Store(Store),
    Gateway(Gateway),
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let sources = vec![iroh_home_path(CONFIG_FILE_NAME), cli.cfg.clone()];
    let config = make_config(
        // default
        Config::default(),
        // potential config files
        sources,
        // env var prefix for this config
        ENV_PREFIX,
        // map of present command line arguments
        cli.make_overrides_map(),
    )
    .unwrap();

    let metrics_config = config.metrics.clone();

    // stubbing in metrics
    let prom_registry = Registry::default();
    // TODO: need to register prometheus metrics
    let metrics_handle = iroh_metrics::MetricsHandle::from_registry_with_tracer(
        metrics::metrics_config_with_compile_time_info(metrics_config),
        prom_registry,
    )
    .await
    .expect("failed to initialize metrics");

    let client = Client::new(config.rpc_client).await?;

    match cli.command {
        Commands::Status { watch } => {
            crate::status::status(client, watch).await?;
        }
        Commands::Version => {
            println!("v{}", env!("CARGO_PKG_VERSION"));
        }
        Commands::P2p(p2p) => run_p2p_command(client, p2p).await?,
        Commands::Store(store) => run_store_command(client, store).await?,
        Commands::Gateway(gateway) => run_gateway_command(client, gateway).await?,
    };

    metrics_handle.shutdown();
    Ok(())
}
