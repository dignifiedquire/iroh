//! Default values used in [`iroh-net`][`crate`]

use crate::derp::{DerpMap, DerpNode, DerpRegion, UseIpv4, UseIpv6};

/// Hostname of the default NA Derp.
pub const NA_DERP_HOSTNAME: &str = "use1-1.derp.iroh.network.";
/// IPv4 of the default NA Derp.
pub const NA_DERP_IPV4: std::net::Ipv4Addr = std::net::Ipv4Addr::new(34, 207, 161, 128);
/// NA region id
pub const NA_REGION_ID: u16 = 1;

/// Hostname of the default EU Derp.
pub const EU_DERP_HOSTNAME: &str = "euw1-1.derp.iroh.network.";
/// IPv4 of the default EU Derp.
pub const EU_DERP_IPV4: std::net::Ipv4Addr = std::net::Ipv4Addr::new(34, 253, 75, 5);
/// EU region id
pub const EU_REGION_ID: u16 = 2;

/// test region id
pub const TEST_REGION_ID: u16 = 65535;

/// STUN port as defined by [RFC 8489](<https://www.rfc-editor.org/rfc/rfc8489#section-18.6>)
pub const DEFAULT_DERP_STUN_PORT: u16 = 3478;

/// Get the default [`DerpMap`].
pub fn default_derp_map() -> DerpMap {
    DerpMap::from_regions([default_na_derp_region(), default_eu_derp_region()])
        .expect("default regions invalid")
}

/// Get the default [`DerpRegion`] for NA.
pub fn default_na_derp_region() -> DerpRegion {
    // The default NA derper run by number0.
    let default_n0_derp = DerpNode {
        name: "na-default-1".into(),
        region_id: NA_REGION_ID,
        url: format!("https://{NA_DERP_HOSTNAME}").parse().unwrap(),
        stun_only: false,
        stun_port: DEFAULT_DERP_STUN_PORT,
        ipv4: UseIpv4::Some(NA_DERP_IPV4),
        ipv6: UseIpv6::TryDns,
    };
    DerpRegion {
        region_id: NA_REGION_ID,
        nodes: vec![default_n0_derp.into()],
        avoid: false,
        region_code: "default-1".into(),
    }
}

/// Get the default [`DerpRegion`] for EU.
pub fn default_eu_derp_region() -> DerpRegion {
    // The default EU derper run by number0.
    let default_n0_derp = DerpNode {
        name: "eu-default-1".into(),
        region_id: EU_REGION_ID,
        url: format!("https://{EU_DERP_HOSTNAME}").parse().unwrap(),
        stun_only: false,
        stun_port: DEFAULT_DERP_STUN_PORT,
        ipv4: UseIpv4::Some(EU_DERP_IPV4),
        ipv6: UseIpv6::TryDns,
    };
    DerpRegion {
        region_id: EU_REGION_ID,
        nodes: vec![default_n0_derp.into()],
        avoid: false,
        region_code: "default-2".into(),
    }
}
