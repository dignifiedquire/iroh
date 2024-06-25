//! Trait and utils for the node discovery mechanism.

use std::time::Duration;

use anyhow::{anyhow, ensure, Result};
use futures_lite::stream::{Boxed as BoxStream, StreamExt};
use iroh_base::node_addr::NodeAddr;
use tokio::{sync::oneshot, task::JoinHandle};
use tracing::{debug, error_span, warn, Instrument};

use crate::{AddrInfo, Endpoint, NodeId};

pub mod dns;

/// enable local node discovery
// TODO(ramfox): rename
#[cfg(feature = "local-node-discovery")]
pub mod local_node_discovery;
pub mod pkarr;

/// Name used for logging when new node addresses are added from discovery.
const SOURCE_NAME: &str = "discovery";

/// Node discovery for [`super::Endpoint`].
///
/// The purpose of this trait is to hook up a node discovery mechanism that
/// allows finding information such as the relay URL and direct addresses
/// of a node given its [`NodeId`].
///
/// To allow for discovery, the [`super::Endpoint`] will call `publish` whenever
/// discovery information changes. If a discovery mechanism requires a periodic
/// refresh, it should start its own task.
pub trait Discovery: std::fmt::Debug + Send + Sync {
    /// Publish the given [`AddrInfo`] to the discovery mechanisms.
    ///
    /// This is fire and forget, since the magicsock can not wait for successful
    /// publishing. If publishing is async, the implementation should start it's
    /// own task.
    ///
    /// This will be called from a tokio task, so it is safe to spawn new tasks.
    /// These tasks will be run on the runtime of the [`super::Endpoint`].
    fn publish(&self, _info: &AddrInfo) {}

    /// Resolve the [`AddrInfo`] for the given [`NodeId`].
    ///
    /// Once the returned [`BoxStream`] is dropped, the service should stop any pending
    /// work.
    fn resolve(
        &self,
        _endpoint: Endpoint,
        _node_id: NodeId,
    ) -> Option<BoxStream<Result<DiscoveryItem>>> {
        None
    }
}

/// The results returned from [`Discovery::resolve`].
#[derive(Debug, Clone)]
pub struct DiscoveryItem {
    /// A static string to identify the discovery source.
    ///
    /// Should be uniform per discovery service.
    pub provenance: &'static str,
    /// Optional timestamp when this node address info was last updated.
    ///
    /// Must be microseconds since the unix epoch.
    // TODO(ramfox): this is currently unused. As we develope more `DiscoveryService`s, we may discover that we do not need this. It is only truly relevant when comparing `relay_urls`, since we can attempt to dial any number of socket addresses, but expect each node to have one "home relay" that we will attempt to contact them on. This means we would need some way to determine which relay url to choose between, if more than one relay url is reported.
    pub last_updated: Option<u64>,
    /// The address info for the node being resolved.
    pub addr_info: AddrInfo,
}

/// A discovery service that combines multiple discovery sources.
///
/// The discovery services will resolve concurrently.
#[derive(Debug, Default)]
pub struct ConcurrentDiscovery {
    services: Vec<Box<dyn Discovery>>,
}

impl ConcurrentDiscovery {
    /// Create a empty [`ConcurrentDiscovery`].
    pub fn empty() -> Self {
        Self::default()
    }

    /// Create a new [`ConcurrentDiscovery`].
    pub fn from_services(services: Vec<Box<dyn Discovery>>) -> Self {
        Self { services }
    }

    /// Add a [`Discovery`] service.
    pub fn add(&mut self, service: impl Discovery + 'static) {
        self.services.push(Box::new(service));
    }
}

impl<T> From<T> for ConcurrentDiscovery
where
    T: IntoIterator<Item = Box<dyn Discovery>>,
{
    fn from(iter: T) -> Self {
        let services = iter.into_iter().collect::<Vec<_>>();
        Self { services }
    }
}

impl Discovery for ConcurrentDiscovery {
    fn publish(&self, info: &AddrInfo) {
        for service in &self.services {
            service.publish(info);
        }
    }

    fn resolve(
        &self,
        endpoint: Endpoint,
        node_id: NodeId,
    ) -> Option<BoxStream<Result<DiscoveryItem>>> {
        let streams = self
            .services
            .iter()
            .filter_map(|service| service.resolve(endpoint.clone(), node_id));

        let streams = futures_buffered::Merge::from_iter(streams);
        Some(Box::pin(streams))
    }
}

/// Maximum duration since the last control or data message received from an endpoint to make us
/// start a discovery task.
const MAX_AGE: Duration = Duration::from_secs(10);

/// A wrapper around a tokio task which runs a node discovery.
pub(super) struct DiscoveryTask {
    on_first_rx: oneshot::Receiver<Result<()>>,
    task: JoinHandle<()>,
}

impl DiscoveryTask {
    /// Start a discovery task.
    pub fn start(ep: Endpoint, node_id: NodeId) -> Result<Self> {
        ensure!(ep.discovery().is_some(), "No discovery services configured");
        let (on_first_tx, on_first_rx) = oneshot::channel();
        let me = ep.node_id();
        let task = tokio::task::spawn(
            async move { Self::run(ep, node_id, on_first_tx).await }.instrument(
                error_span!("discovery", me = %me.fmt_short(), node = %node_id.fmt_short()),
            ),
        );
        Ok(Self { task, on_first_rx })
    }

    /// Start a discovery task after a delay and only if no path to the node was recently active.
    ///
    /// This returns `None` if we received data or control messages from the remote endpoint
    /// recently enough. If not it returns a [`DiscoveryTask`].
    ///
    /// If `delay` is set, the [`DiscoveryTask`] will first wait for `delay` and then check again
    /// if we recently received messages from remote endpoint. If true, the task will abort.
    /// Otherwise, or if no `delay` is set, the discovery will be started.
    pub fn maybe_start_after_delay(
        ep: &Endpoint,
        node_id: NodeId,
        delay: Option<Duration>,
    ) -> Result<Option<Self>> {
        // If discovery is not needed, don't even spawn a task.
        if !Self::needs_discovery(ep, node_id) {
            return Ok(None);
        }
        ensure!(ep.discovery().is_some(), "No discovery services configured");
        let (on_first_tx, on_first_rx) = oneshot::channel();
        let ep = ep.clone();
        let me = ep.node_id();
        let task = tokio::task::spawn(
            async move {
                // If delay is set, wait and recheck if discovery is needed. If not, early-exit.
                if let Some(delay) = delay {
                    tokio::time::sleep(delay).await;
                    if !Self::needs_discovery(&ep, node_id) {
                        debug!("no discovery needed, abort");
                        on_first_tx.send(Ok(())).ok();
                        return;
                    }
                }
                Self::run(ep, node_id, on_first_tx).await
            }
            .instrument(
                error_span!("discovery", me = %me.fmt_short(), node = %node_id.fmt_short()),
            ),
        );
        Ok(Some(Self { task, on_first_rx }))
    }

    /// Wait until the discovery task produced at least one result.
    pub async fn first_arrived(&mut self) -> Result<()> {
        let fut = &mut self.on_first_rx;
        fut.await??;
        Ok(())
    }

    /// Cancel the discovery task.
    pub fn cancel(&self) {
        self.task.abort();
    }

    fn create_stream(ep: &Endpoint, node_id: NodeId) -> Result<BoxStream<Result<DiscoveryItem>>> {
        let discovery = ep
            .discovery()
            .ok_or_else(|| anyhow!("No discovery service configured"))?;
        let stream = discovery
            .resolve(ep.clone(), node_id)
            .ok_or_else(|| anyhow!("No discovery service can resolve node {node_id}",))?;
        Ok(stream)
    }

    /// We need discovery if we have no paths to the node, or if the paths we do have
    /// have timed out.
    fn needs_discovery(ep: &Endpoint, node_id: NodeId) -> bool {
        match ep.connection_info(node_id) {
            // No connection info means no path to node -> start discovery.
            None => true,
            Some(info) => {
                match (info.last_received(), info.last_alive_relay()) {
                    // No path to node -> start discovery.
                    (None, None) => true,
                    // If we haven't received on direct addresses or the relay for MAX_AGE,
                    // start discovery.
                    (Some(elapsed), Some(elapsed_relay)) => {
                        elapsed > MAX_AGE && elapsed_relay > MAX_AGE
                    }
                    (Some(elapsed), _) | (_, Some(elapsed)) => elapsed > MAX_AGE,
                }
            }
        }
    }

    async fn run(ep: Endpoint, node_id: NodeId, on_first_tx: oneshot::Sender<Result<()>>) {
        let mut stream = match Self::create_stream(&ep, node_id) {
            Ok(stream) => stream,
            Err(err) => {
                on_first_tx.send(Err(err)).ok();
                return;
            }
        };
        let mut on_first_tx = Some(on_first_tx);
        debug!("discovery: start");
        loop {
            let next = tokio::select! {
                _ = ep.cancelled() => break,
                next = stream.next() => next
            };
            match next {
                Some(Ok(r)) => {
                    if r.addr_info.is_empty() {
                        debug!(provenance = %r.provenance, addr = ?r.addr_info, "discovery: empty address found");
                        continue;
                    }
                    debug!(provenance = %r.provenance, addr = ?r.addr_info, "discovery: new address found");
                    let addr = NodeAddr {
                        info: r.addr_info,
                        node_id,
                    };
                    ep.add_node_addr_with_source(addr, SOURCE_NAME).ok();
                    if let Some(tx) = on_first_tx.take() {
                        tx.send(Ok(())).ok();
                    }
                }
                Some(Err(err)) => {
                    warn!(?err, "discovery service produced error");
                    break;
                }
                None => break,
            }
        }
        if let Some(tx) = on_first_tx.take() {
            let err = anyhow!("Discovery produced no results for {}", node_id.fmt_short());
            tx.send(Err(err)).ok();
        }
    }
}

impl Drop for DiscoveryTask {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashMap},
        net::SocketAddr,
        sync::Arc,
        time::SystemTime,
    };

    use parking_lot::Mutex;
    use rand::Rng;

    use crate::{key::SecretKey, relay::RelayMode};

    use super::*;

    #[derive(Debug, Clone, Default)]
    struct TestDiscoveryShared {
        nodes: Arc<Mutex<HashMap<NodeId, (AddrInfo, u64)>>>,
    }
    impl TestDiscoveryShared {
        pub fn create_discovery(&self, node_id: NodeId) -> TestDiscovery {
            TestDiscovery {
                node_id,
                shared: self.clone(),
                publish: true,
                resolve_wrong: false,
                delay: Duration::from_millis(200),
            }
        }

        pub fn create_lying_discovery(&self, node_id: NodeId) -> TestDiscovery {
            TestDiscovery {
                node_id,
                shared: self.clone(),
                publish: false,
                resolve_wrong: true,
                delay: Duration::from_millis(100),
            }
        }
    }
    #[derive(Debug)]
    struct TestDiscovery {
        node_id: NodeId,
        shared: TestDiscoveryShared,
        publish: bool,
        resolve_wrong: bool,
        delay: Duration,
    }

    impl Discovery for TestDiscovery {
        fn publish(&self, info: &AddrInfo) {
            if !self.publish {
                return;
            }
            let now = system_time_now();
            self.shared
                .nodes
                .lock()
                .insert(self.node_id, (info.clone(), now));
        }

        fn resolve(
            &self,
            endpoint: Endpoint,
            node_id: NodeId,
        ) -> Option<BoxStream<Result<DiscoveryItem>>> {
            let addr_info = match self.resolve_wrong {
                false => self.shared.nodes.lock().get(&node_id).cloned(),
                true => {
                    let ts = system_time_now() - 100_000;
                    let port: u16 = rand::thread_rng().gen_range(10_000..20_000);
                    // "240.0.0.0/4" is reserved and unreachable
                    let addr: SocketAddr = format!("240.0.0.1:{port}").parse().unwrap();
                    let addr_info = AddrInfo {
                        relay_url: None,
                        direct_addresses: BTreeSet::from([addr]),
                    };
                    Some((addr_info, ts))
                }
            };
            let stream = match addr_info {
                Some((addr_info, ts)) => {
                    let item = DiscoveryItem {
                        provenance: "test-disco",
                        last_updated: Some(ts),
                        addr_info,
                    };
                    let delay = self.delay;
                    let fut = async move {
                        tokio::time::sleep(delay).await;
                        tracing::debug!(
                            "resolve on {}: {} = {item:?}",
                            endpoint.node_id().fmt_short(),
                            node_id.fmt_short()
                        );
                        Ok(item)
                    };
                    futures_lite::stream::once_future(fut).boxed()
                }
                None => futures_lite::stream::empty().boxed(),
            };
            Some(stream)
        }
    }

    #[derive(Debug)]
    struct EmptyDiscovery;
    impl Discovery for EmptyDiscovery {
        fn publish(&self, _info: &AddrInfo) {}

        fn resolve(
            &self,
            _endpoint: Endpoint,
            _node_id: NodeId,
        ) -> Option<BoxStream<Result<DiscoveryItem>>> {
            Some(futures_lite::stream::empty().boxed())
        }
    }

    const TEST_ALPN: &[u8] = b"n0/iroh/test";

    /// This is a smoke test for our discovery mechanism.
    #[tokio::test]
    async fn endpoint_discovery_simple_shared() -> anyhow::Result<()> {
        let _guard = iroh_test::logging::setup();
        let disco_shared = TestDiscoveryShared::default();
        let ep1 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        let ep2 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        let ep1_addr = NodeAddr::new(ep1.node_id());
        // wait for out address to be updated and thus published at least once
        ep1.node_addr().await?;
        let _conn = ep2.connect(ep1_addr, TEST_ALPN).await?;
        Ok(())
    }

    /// This test adds an empty discovery which provides no addresses.
    #[tokio::test]
    async fn endpoint_discovery_combined_with_empty() -> anyhow::Result<()> {
        let _guard = iroh_test::logging::setup();
        let disco_shared = TestDiscoveryShared::default();
        let ep1 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        let ep2 = {
            let secret = SecretKey::generate();
            let disco1 = EmptyDiscovery;
            let disco2 = disco_shared.create_discovery(secret.public());
            let mut disco = ConcurrentDiscovery::empty();
            disco.add(disco1);
            disco.add(disco2);
            new_endpoint(secret, disco).await
        };
        let ep1_addr = NodeAddr::new(ep1.node_id());
        // wait for out address to be updated and thus published at least once
        ep1.node_addr().await?;
        let _conn = ep2.connect(ep1_addr, TEST_ALPN).await?;
        Ok(())
    }

    /// This test adds a "lying" discovery which provides a wrong address.
    /// This is to make sure that as long as one of the discoveries returns a working address, we
    /// will connect successfully.
    #[tokio::test]
    async fn endpoint_discovery_combined_with_empty_and_wrong() -> anyhow::Result<()> {
        let _guard = iroh_test::logging::setup();
        let disco_shared = TestDiscoveryShared::default();
        let ep1 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        let ep2 = {
            let secret = SecretKey::generate();
            let disco1 = EmptyDiscovery;
            let disco2 = disco_shared.create_lying_discovery(secret.public());
            let disco3 = disco_shared.create_discovery(secret.public());
            let mut disco = ConcurrentDiscovery::empty();
            disco.add(disco1);
            disco.add(disco2);
            disco.add(disco3);
            new_endpoint(secret, disco).await
        };
        let ep1_addr = NodeAddr::new(ep1.node_id());
        // wait for out address to be updated and thus published at least once
        ep1.node_addr().await?;
        let _conn = ep2.connect(ep1_addr, TEST_ALPN).await?;
        Ok(())
    }

    /// This test only has the "lying" discovery. It is here to make sure that this actually fails.
    #[tokio::test]
    async fn endpoint_discovery_combined_wrong_only() -> anyhow::Result<()> {
        let _guard = iroh_test::logging::setup();
        let disco_shared = TestDiscoveryShared::default();
        let ep1 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        let ep2 = {
            let secret = SecretKey::generate();
            let disco1 = disco_shared.create_lying_discovery(secret.public());
            let disco = ConcurrentDiscovery::from_services(vec![Box::new(disco1)]);
            new_endpoint(secret, disco).await
        };
        let ep1_addr = NodeAddr::new(ep1.node_id());
        // wait for out address to be updated and thus published at least once
        ep1.node_addr().await?;
        let res = ep2.connect(ep1_addr, TEST_ALPN).await;
        assert!(res.is_err());
        Ok(())
    }

    /// This test first adds a wrong address manually (e.g. from an outdated&node_id ticket).
    /// Connect should still succeed because the discovery service will be invoked (after a delay).
    #[tokio::test]
    async fn endpoint_discovery_with_wrong_existing_addr() -> anyhow::Result<()> {
        let _guard = iroh_test::logging::setup();
        let disco_shared = TestDiscoveryShared::default();
        let ep1 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        let ep2 = {
            let secret = SecretKey::generate();
            let disco = disco_shared.create_discovery(secret.public());
            new_endpoint(secret, disco).await
        };
        // wait for out address to be updated and thus published at least once
        ep1.node_addr().await?;
        let ep1_wrong_addr = NodeAddr {
            node_id: ep1.node_id(),
            info: AddrInfo {
                relay_url: None,
                direct_addresses: BTreeSet::from(["240.0.0.1:1000".parse().unwrap()]),
            },
        };
        let _conn = ep2.connect(ep1_wrong_addr, TEST_ALPN).await?;
        Ok(())
    }

    async fn new_endpoint(secret: SecretKey, disco: impl Discovery + 'static) -> Endpoint {
        Endpoint::builder()
            .secret_key(secret)
            .discovery(Box::new(disco))
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![TEST_ALPN.to_vec()])
            .bind(0)
            .await
            .unwrap()
    }

    fn system_time_now() -> u64 {
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .expect("time drift")
            .as_micros() as u64
    }
}

/// This module contains end-to-end tests for DNS node discovery.
///
/// The tests run a minimal test DNS server to resolve against, and a minimal pkarr relay to
/// publish to. The DNS and pkarr servers share their state.
#[cfg(test)]
mod test_dns_pkarr {
    use std::time::Duration;

    use anyhow::Result;
    use iroh_base::key::SecretKey;

    use crate::{
        discovery::pkarr::PkarrPublisher,
        dns::{node_info::NodeInfo, ResolverExt},
        relay::{RelayMap, RelayMode},
        test_utils::{
            dns_server::{create_dns_resolver, run_dns_server},
            pkarr_dns_state::State,
            run_relay_server, DnsPkarrServer,
        },
        AddrInfo, Endpoint, NodeAddr,
    };

    const PUBLISH_TIMEOUT: Duration = Duration::from_secs(10);

    #[tokio::test]
    async fn dns_resolve() -> Result<()> {
        let _logging_guard = iroh_test::logging::setup();

        let origin = "testdns.example".to_string();
        let state = State::new(origin.clone());
        let (nameserver, _dns_drop_guard) = run_dns_server(state.clone()).await?;

        let secret_key = SecretKey::generate();
        let node_info = NodeInfo::new(
            secret_key.public(),
            Some("https://relay.example".parse().unwrap()),
            Default::default(),
        );
        let signed_packet = node_info.to_pkarr_signed_packet(&secret_key, 30)?;
        state.upsert(signed_packet)?;

        let resolver = create_dns_resolver(nameserver)?;
        let resolved = resolver.lookup_by_id(&node_info.node_id, &origin).await?;

        assert_eq!(resolved, node_info.into());

        Ok(())
    }

    #[tokio::test]
    async fn pkarr_publish_dns_resolve() -> Result<()> {
        let _logging_guard = iroh_test::logging::setup();

        let origin = "testdns.example".to_string();

        let dns_pkarr_server = DnsPkarrServer::run_with_origin(origin.clone()).await?;

        let secret_key = SecretKey::generate();
        let node_id = secret_key.public();

        let addr_info = AddrInfo {
            relay_url: Some("https://relay.example".parse().unwrap()),
            ..Default::default()
        };

        let resolver = create_dns_resolver(dns_pkarr_server.nameserver)?;
        let publisher = PkarrPublisher::new(secret_key, dns_pkarr_server.pkarr_url.clone());
        // does not block, update happens in background task
        publisher.update_addr_info(&addr_info);
        // wait until our shared state received the update from pkarr publishing
        dns_pkarr_server.on_node(&node_id, PUBLISH_TIMEOUT).await?;
        let resolved = resolver.lookup_by_id(&node_id, &origin).await?;

        let expected = NodeAddr {
            info: addr_info,
            node_id,
        };

        assert_eq!(resolved, expected);
        Ok(())
    }

    const TEST_ALPN: &[u8] = b"TEST";

    #[tokio::test]
    async fn pkarr_publish_dns_discover() -> Result<()> {
        let _logging_guard = iroh_test::logging::setup();

        let dns_pkarr_server = DnsPkarrServer::run().await?;
        let (relay_map, _relay_url, _relay_guard) = run_relay_server().await?;

        let ep1 = ep_with_discovery(&relay_map, &dns_pkarr_server).await?;
        let ep2 = ep_with_discovery(&relay_map, &dns_pkarr_server).await?;

        // wait until our shared state received the update from pkarr publishing
        dns_pkarr_server
            .on_node(&ep1.node_id(), PUBLISH_TIMEOUT)
            .await?;

        // we connect only by node id!
        let res = ep2.connect(ep1.node_id().into(), TEST_ALPN).await;
        assert!(res.is_ok(), "connection established");
        Ok(())
    }

    #[tokio::test]
    async fn pkarr_publish_dns_discover_empty_node_addr() -> Result<()> {
        let _logging_guard = iroh_test::logging::setup();

        let dns_pkarr_server = DnsPkarrServer::run().await?;
        let (relay_map, _relay_url, _relay_guard) = run_relay_server().await?;

        let ep1 = ep_with_discovery(&relay_map, &dns_pkarr_server).await?;
        let ep2 = ep_with_discovery(&relay_map, &dns_pkarr_server).await?;

        // wait until our shared state received the update from pkarr publishing
        dns_pkarr_server
            .on_node(&ep1.node_id(), PUBLISH_TIMEOUT)
            .await?;

        // we connect only by node id!
        let res = ep2.connect(ep1.node_id().into(), TEST_ALPN).await;
        assert!(res.is_ok(), "connection established");
        Ok(())
    }

    async fn ep_with_discovery(
        relay_map: &RelayMap,
        dns_pkarr_server: &DnsPkarrServer,
    ) -> Result<Endpoint> {
        let secret_key = SecretKey::generate();
        let ep = Endpoint::builder()
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .insecure_skip_relay_cert_verify(true)
            .secret_key(secret_key.clone())
            .alpns(vec![TEST_ALPN.to_vec()])
            .dns_resolver(dns_pkarr_server.dns_resolver())
            .discovery(dns_pkarr_server.discovery(secret_key))
            .bind(0)
            .await?;
        Ok(ep)
    }
}
