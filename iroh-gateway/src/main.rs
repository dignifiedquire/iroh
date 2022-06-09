use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use iroh_gateway::{
    config::{Config, CONFIG_FILE_NAME, ENV_PREFIX},
    core::Core,
    metrics,
};
use iroh_metrics::gateway::Metrics;
use iroh_util::{iroh_home_path, make_config};
use prometheus_client::registry::Registry;

#[derive(Parser, Debug, Clone)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long)]
    port: Option<u16>,
    #[clap(short, long)]
    writeable: Option<bool>,
    #[clap(short, long)]
    fetch: Option<bool>,
    #[clap(short, long)]
    cache: Option<bool>,
    #[clap(long = "no-metrics")]
    no_metrics: bool,
    #[clap(long)]
    cfg: Option<PathBuf>,
}

impl Args {
    fn make_overrides_map(&self) -> HashMap<&str, String> {
        let mut map: HashMap<&str, String> = HashMap::new();
        if let Some(port) = self.port {
            map.insert("port", port.to_string());
        }
        if let Some(writable) = self.writeable {
            map.insert("writable", writable.to_string());
        }
        if let Some(fetch) = self.fetch {
            map.insert("fetch", fetch.to_string());
        }
        if let Some(cache) = self.cache {
            map.insert("cache", cache.to_string());
        }
        map.insert("metrics.debug", self.no_metrics.to_string());
        map
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    let sources = vec![iroh_home_path(CONFIG_FILE_NAME), args.cfg.clone()];
    let mut config = make_config(
        // default
        Config::default(),
        // potential config files
        sources,
        // env var prefix for this config
        ENV_PREFIX,
        // map of present command line arguments
        args.make_overrides_map(),
    )
    .unwrap();
    config.metrics = metrics::metrics_config_with_compile_time_info(config.metrics);
    println!("{:#?}", config);

    let metrics_config = config.metrics.clone();
    let mut prom_registry = Registry::default();
    let gw_metrics = Metrics::new(&mut prom_registry);
    let handler = Core::new(config, gw_metrics, &mut prom_registry).await?;

    let metrics_handle = iroh_metrics::init_with_registry(metrics_config, prom_registry)
        .await
        .expect("failed to initialize metrics");
    let server = handler.server();
    println!("listening on {}", server.local_addr());
    let core_task = tokio::spawn(async move {
        server.await.unwrap();
    });

    iroh_util::block_until_sigint().await;
    core_task.abort();

    metrics_handle.shutdown();
    Ok(())
}
