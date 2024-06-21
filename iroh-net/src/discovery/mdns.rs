use std::{
    collections::{BTreeSet, HashMap},
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use anyhow::Result;
use derive_more::FromStr;
use futures_lite::{stream::Boxed as BoxStream, StreamExt};

use flume::Sender;
use iroh_base::key::PublicKey;
use swarm_discovery::{Discoverer, Peer};
use tokio::task::{JoinHandle, JoinSet};

use crate::{
    discovery::{Discovery, DiscoveryItem},
    AddrInfo, Endpoint, NodeId,
};

/// The n0 local node discovery name
// TODO(ramfox): bikeshed
const N0_MDNS_SWARM: &str = "iroh.local.node.discovery";

/// Provenance string
// TODO(ramfox): bikeshed
const PROVENANCE: &str = "local.node.discovery";

/// How long we will wait before we stop sending discovery items
const DISCOVERY_DURATION: Duration = Duration::from_secs(10);

/// Discovery using `swarm-discovery`, a variation on mdns
#[derive(Debug)]
pub struct LocalNodeDiscovery {
    handle: JoinHandle<()>,
    sender: Sender<Message>,
}

impl Drop for LocalNodeDiscovery {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

#[derive(Debug)]
enum Message {
    Discovery((String, Peer)),
    SendAddrs((NodeId, Sender<Result<DiscoveryItem>>)),
    ChangeLocalAddrs(AddrInfo),
    Timeout(NodeId),
}

/// The time after which a new peer should have discovered some parts of the swarm
const DEFAULT_CADENCE: Duration = Duration::from_secs(1);

impl LocalNodeDiscovery {
    /// Create a new LocalNodeDiscovery Service.
    ///
    /// This starts a `Discoverer` that broadcasts your addresses and receives addresses from other nodes in your local network.
    pub fn new(node_id: NodeId, info: Option<(u16, Vec<IpAddr>)>) -> Self {
        tracing::debug!("Creating new LocalNodeDiscovery service");
        let (send, recv) = flume::bounded(64);
        let task_sender = send.clone();
        let handle = tokio::spawn(async move {
            let mut guard =
                LocalNodeDiscovery::create_discoverer(node_id, task_sender.clone(), info)
                    .spawn(&tokio::runtime::Handle::current());
            tracing::debug!("Created LocalNodeDiscovery Service");
            let mut node_addrs: HashMap<PublicKey, Peer> = HashMap::default();
            let mut senders: HashMap<PublicKey, Sender<Result<DiscoveryItem>>> = HashMap::default();
            let mut timeouts = JoinSet::new();
            loop {
                tracing::debug!("LocalNodeDiscovery Service loop tick");
                let msg = match recv.recv() {
                    Err(err) => {
                        tracing::error!("LocalNodeDiscovery service error: {err:?}");
                        tracing::error!("closing LocalNodeDiscovery");
                        timeouts.abort_all();
                        return;
                    }
                    Ok(msg) => msg,
                };
                match msg {
                    Message::Discovery((discovered_node_id, peer_info)) => {
                        tracing::debug!(
                            ?discovered_node_id,
                            ?peer_info,
                            "LocalNodeDiscovery Message::Discovery"
                        );
                        let discovered_node_id = match PublicKey::from_str(&discovered_node_id) {
                            Ok(node_id) => node_id,
                            Err(e) => {
                                tracing::warn!(
                                    discovered_node_id,
                                    "couldn't parse node_id from mdns discovery service: {e:?}"
                                );
                                continue;
                            }
                        };

                        if discovered_node_id == node_id {
                            continue;
                        }

                        if peer_info.addrs.is_empty() {
                            tracing::debug!(
                                ?discovered_node_id,
                                "removing node from LocalNodeDiscovery address book"
                            );
                            node_addrs.remove(&discovered_node_id);
                            continue;
                        }

                        tracing::debug!(
                            ?discovered_node_id,
                            ?peer_info,
                            "adding node to LocalNodeDiscovery address book"
                        );
                        if let Some(sender) = senders.get(&node_id) {
                            let item = peer_info_to_discovery_time(&peer_info);
                            tracing::debug!(?item, "sending DiscoveryItem");
                            sender.send(Ok(item)).ok();
                        }
                        node_addrs.insert(node_id, peer_info);
                    }
                    Message::SendAddrs((node_id, sender)) => {
                        tracing::debug!(?node_id, "LocalNodeDiscovery Message::SendAddrs");
                        if let Some(peer_info) = node_addrs.get(&node_id) {
                            let item = peer_info_to_discovery_time(peer_info);
                            tracing::debug!(?item, "sending DiscoveryItem");
                            sender.send(Ok(item)).ok();
                        }
                        senders.insert(node_id, sender);
                        let timeout_sender = task_sender.clone();
                        timeouts.spawn(async move {
                            tokio::time::sleep(DISCOVERY_DURATION).await;
                            tracing::debug!(?node_id, "discovery timeout");
                            timeout_sender.send(Message::Timeout(node_id)).ok();
                        });
                    }
                    Message::Timeout(node_id) => {
                        tracing::debug!(?node_id, "LocalNodeDiscovery Message::Timeout");
                        senders.remove(&node_id);
                    }
                    Message::ChangeLocalAddrs(addrs) => {
                        tracing::debug!(?addrs, "LocalNodeDiscovery Message::ChangeLocalAddrs");
                        // TODO(ramfox): currently, filtering out any addrs that aren't ipv4 and private. If we add more information into our `AddrInfo`s to include the `EndpointType`, we can also add the private ipv6 addresses in.
                        // Either way, we will likely want to publish SocketAddrs rather than a single port to a vec of IpAddrs, in which case, we can just publish all direct addresses, rather than filtering any out.
                        let addrs = addrs
                            .direct_addresses
                            .into_iter()
                            .filter(|addr| {
                                if let IpAddr::V4(ipv4_addr) = addr.ip() {
                                    ipv4_addr.is_private()
                                } else {
                                    false
                                }
                            })
                            .collect::<Vec<_>>();
                        // if we have no addrs we should start a new discovery service that does not publish addresses, but only listens for addresses
                        let addrs = if addrs.is_empty() {
                            None
                        } else {
                            let port = addrs[0].port();
                            let addrs = addrs.into_iter().map(|addr| addr.ip()).collect();
                            tracing::debug!(
                                ?port,
                                ?addrs,
                                "LocalNodeDiscovery: publishing this node's address information"
                            );
                            Some((port, addrs))
                        };

                        let callback_send = task_sender.clone();
                        guard =
                            LocalNodeDiscovery::create_discoverer(node_id, callback_send, addrs)
                                .spawn(&tokio::runtime::Handle::current());
                    }
                }
                tracing::debug!("LocalNodeDiscovery end of loop");
            }
        });
        Self {
            handle,
            sender: send.clone(),
        }
    }

    fn create_discoverer(
        node_id: PublicKey,
        sender: Sender<Message>,
        addrs: Option<(u16, Vec<IpAddr>)>,
    ) -> Discoverer {
        let callback = move |node_id: &str, peer: &Peer| {
            tracing::debug!(
                node_id,
                ?peer,
                "Received peer information from LocalNodeDiscovery"
            );
            sender
                .send(Message::Discovery((node_id.to_string(), peer.clone())))
                .ok();
        };

        let mut discoverer = Discoverer::new(N0_MDNS_SWARM.to_string(), node_id.to_string())
            .with_callback(callback)
            .with_cadence(DEFAULT_CADENCE);
        if let Some(addrs) = addrs {
            discoverer = discoverer.with_addrs(addrs.0, addrs.1);
        }
        discoverer
    }
}

fn peer_info_to_discovery_time(peer_info: &Peer) -> DiscoveryItem {
    let port = peer_info.port;
    let direct_addresses: BTreeSet<SocketAddr> = peer_info
        .addrs
        .iter()
        .map(|ip| SocketAddr::new(*ip, port))
        .collect();
    DiscoveryItem {
        provenance: PROVENANCE,
        last_updated: None,
        addr_info: AddrInfo {
            relay_url: None,
            direct_addresses,
        },
    }
}

impl Discovery for LocalNodeDiscovery {
    fn resolve(&self, _ep: Endpoint, node_id: NodeId) -> Option<BoxStream<Result<DiscoveryItem>>> {
        let (send, recv) = flume::bounded(20);
        self.sender.send(Message::SendAddrs((node_id, send))).ok();
        Some(recv.into_stream().boxed())
    }

    fn publish(&self, info: &AddrInfo) {
        self.sender
            .send(Message::ChangeLocalAddrs(info.clone()))
            .ok();
    }
}
