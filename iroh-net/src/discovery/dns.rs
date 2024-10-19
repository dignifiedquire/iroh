//! DNS node discovery for iroh-net

use anyhow::Result;
use futures_lite::stream::Boxed as BoxStream;

use crate::{
    discovery::{Discovery, DiscoveryItem},
    dns::ResolverExt,
    Endpoint, NodeId,
};

/// The n0 testing DNS node origin, for production.
pub const N0_DNS_NODE_ORIGIN_PROD: &str = "dns.iroh.link";
/// The n0 testing DNS node origin, for testing.
pub const N0_DNS_NODE_ORIGIN_STAGING: &str = "staging-dns.iroh.link";
/// Testing DNS node origin, must run server from [`crate::test_utils::DnsPkarrServer`].
#[cfg(test)]
pub const TEST_DNS_NODE_ORIGIN: &str = "dns.iroh.test";

/// Environment variable to force the use of staging relays.
#[cfg(not(test))]
#[cfg_attr(iroh_docsrs, doc(cfg(not(any(test, feature = "test-utils")))))]
const ENV_FORCE_STAGING_RELAYS: &str = "IROH_FORCE_STAGING_RELAYS";

const DNS_STAGGERING_MS: &[u64] = &[200, 300];

/// DNS node discovery
///
/// When asked to resolve a [`NodeId`], this service performs a lookup in the Domain Name System (DNS).
///
/// It uses the [`Endpoint`]'s DNS resolver to query for `TXT` records under the domain
/// `_iroh.<z32-node-id>.<origin-domain>`:
///
/// * `_iroh`: is the record name
/// * `<z32-node-id>` is the [`NodeId`] encoded in [`z-base-32`] format
/// * `<origin-domain>` is the node origin domain as set in [`DnsDiscovery::new`].
///
/// Each TXT record returned from the query is expected to contain a string in the format `<name>=<value>`.
/// If a TXT record contains multiple character strings, they are concatenated first.
/// The supported attributes are:
/// * `relay=<url>`: The URL of the home relay server of the node
///
/// The DNS resolver defaults to using the nameservers configured on the host system, but can be changed
/// with [`crate::endpoint::Builder::dns_resolver`].
///
/// [z-base-32]: https://philzimmermann.com/docs/human-oriented-base-32-encoding.txt
#[derive(Debug)]
pub struct DnsDiscovery {
    origin_domain: String,
}

impl DnsDiscovery {
    /// Creates a new DNS discovery.
    pub fn new(origin_domain: String) -> Self {
        Self { origin_domain }
    }

    /// Creates a new DNS discovery using the `iroh.link` domain.
    ///
    /// This uses the [`N0_DNS_NODE_ORIGIN_PROD`] domain.
    ///
    /// # Usage during tests
    ///
    /// When `cfg(test)` is enabled or when using the `test-utils` cargo feature the
    /// [`TEST_DNS_NODE_ORIGIN`] is used.
    ///
    /// Note that the `iroh.test` domain is not integrated with the global DNS network and
    /// thus node discovery is effectively disabled.  To use node discovery in a test use
    /// the [`crate::test_utils::DnsPkarrServer`] in the test and configure it as a
    /// custom discovery mechanism.
    ///
    /// For testing it is also possible to use the [`N0_DNS_NODE_ORIGIN_STAGING`] domain
    /// with [`DnsDiscovery::new`].  This would then use a hosted discovery service again,
    /// but for testing purposes.
    pub fn n0_dns() -> Self {
        #[cfg(not(test))]
        {
            let force_staging_relays = match std::env::var(ENV_FORCE_STAGING_RELAYS) {
                Ok(value) => value == "1",
                Err(_) => false,
            };
            match force_staging_relays {
                true => Self::new(N0_DNS_NODE_ORIGIN_STAGING.to_string()),
                false => Self::new(N0_DNS_NODE_ORIGIN_PROD.to_string()),
            }
        }
        #[cfg(test)]
        Self::new(TEST_DNS_NODE_ORIGIN.to_string())
    }
}

impl Discovery for DnsDiscovery {
    fn resolve(&self, ep: Endpoint, node_id: NodeId) -> Option<BoxStream<Result<DiscoveryItem>>> {
        let resolver = ep.dns_resolver().clone();
        let origin_domain = self.origin_domain.clone();
        let fut = async move {
            let node_addr = resolver
                .lookup_by_id_staggered(&node_id, &origin_domain, DNS_STAGGERING_MS)
                .await?;
            Ok(DiscoveryItem {
                node_id,
                provenance: "dns",
                last_updated: None,
                addr_info: node_addr.info,
            })
        };
        let stream = futures_lite::stream::once_future(fut);
        Some(Box::pin(stream))
    }
}
