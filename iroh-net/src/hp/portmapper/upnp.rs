use std::{
    net::{Ipv4Addr, SocketAddrV4},
    num::NonZeroU16,
    time::Duration,
};

use anyhow::Result;
use igd::aio as aigd;

use iroh_metrics::{core::MRecorder, portmap::PortmapMetrics as Metrics};
use tracing::debug;

pub use aigd::Gateway;

/// Seconds we ask the router to maintain the port mapping. 0 means infinite.
const PORT_MAPPING_LEASE_DURATION_SECONDS: u32 = 0;

/// Maximum duration a UPnP search can take before timing out.
const SEARCH_TIMEOUT: Duration = Duration::from_secs(1);

/// Tailscale uses the recommended port mapping lifetime for PMP, which is 2 hours. So we assume a
/// half lifetime of 1h. See <https://datatracker.ietf.org/doc/html/rfc6886#section-3.3>
const HALF_LIFETIME: Duration = Duration::from_secs(60 * 60);

/// Name with which we register the mapping in the router.
const PORT_MAPPING_DESCRIPTION: &str = "iroh-portmap";

#[derive(derive_more::Debug, Clone)]
pub struct Mapping {
    /// The internet Gateway device (router) used to create this mapping.
    #[debug("{}", gateway)]
    gateway: aigd::Gateway,
    /// The external address obtained by this mapping.
    external_ip: Ipv4Addr,
    /// External port obtained by this mapping.
    external_port: NonZeroU16,
}

impl Mapping {
    pub(crate) async fn new(
        local_addr: Ipv4Addr,
        port: NonZeroU16,
        gateway: Option<aigd::Gateway>,
        preferred_port: Option<NonZeroU16>,
    ) -> Result<Self> {
        let local_addr = SocketAddrV4::new(local_addr, port.into());

        // search for a gateway if there is not one already
        let gateway = if let Some(known_gateway) = gateway {
            known_gateway
        } else {
            aigd::search_gateway(igd::SearchOptions {
                timeout: Some(SEARCH_TIMEOUT),
                ..Default::default()
            })
            .await?
        };

        let external_ip = gateway.get_external_ip().await?;

        // if we are trying to get a specific external port, try this first. If this fails, default
        // to try to get any port
        if let Some(external_port) = preferred_port {
            if gateway
                .add_port(
                    igd::PortMappingProtocol::UDP,
                    external_port.into(),
                    local_addr,
                    PORT_MAPPING_LEASE_DURATION_SECONDS,
                    PORT_MAPPING_DESCRIPTION,
                )
                .await
                .is_ok()
            {
                return Ok(Mapping {
                    gateway,
                    external_ip,
                    external_port,
                });
            }
        }

        let external_port = gateway
            .add_any_port(
                igd::PortMappingProtocol::UDP,
                local_addr,
                PORT_MAPPING_LEASE_DURATION_SECONDS,
                PORT_MAPPING_DESCRIPTION,
            )
            .await?
            .try_into()
            .map_err(|_| anyhow::anyhow!("upnp mapping got zero external port"))?;

        Ok(Mapping {
            gateway,
            external_ip,
            external_port,
        })
    }

    pub fn half_lifetime(&self) -> Duration {
        HALF_LIFETIME
    }

    /// Releases the mapping.
    pub(crate) async fn release(self) -> Result<()> {
        let Mapping {
            gateway,
            external_port,
            ..
        } = self;
        gateway
            .remove_port(igd::PortMappingProtocol::UDP, external_port.into())
            .await?;
        Ok(())
    }

    /// Returns the external gateway ip and port that can be used to contact this node.
    pub fn external(&self) -> (Ipv4Addr, NonZeroU16) {
        (self.external_ip, self.external_port)
    }
}

/// Searches for UPnP gateways.
pub async fn probe_available() -> Option<Gateway> {
    Metrics::UpnpProbes.inc();
    match aigd::search_gateway(igd::SearchOptions {
        timeout: Some(SEARCH_TIMEOUT),
        ..Default::default()
    })
    .await
    {
        Ok(gateway) => Some(gateway),
        Err(e) => {
            Metrics::UpnpProbesFailed.inc();
            debug!("upnp probe failed: {e}");
            None
        }
    }
}
