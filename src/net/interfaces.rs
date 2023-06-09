//! Contains helpers for looking up system network interfaces.

use std::{collections::HashMap, net::IpAddr};

#[cfg(any(
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "macos",
    target_os = "ios"
))]
mod bsd;
#[cfg(any(target_os = "linux", target_os = "android"))]
mod linux;
#[cfg(target_os = "windows")]
mod windows;

pub use default_net::ip::{Ipv4Net, Ipv6Net};

use crate::net::ip::{is_loopback, is_private_v6, is_up};

#[cfg(any(
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "macos",
    target_os = "ios"
))]
use self::bsd::default_route;
#[cfg(any(target_os = "linux", target_os = "android"))]
use self::linux::default_route;
#[cfg(target_os = "windows")]
use self::windows::default_route;

/// Represents a network interface.
#[derive(Debug)]
pub struct Interface {
    iface: default_net::interface::Interface,
}

impl PartialEq for Interface {
    fn eq(&self, other: &Self) -> bool {
        self.iface.index == other.iface.index
            && self.iface.name == other.iface.name
            && self.iface.flags == other.iface.flags
            && self.iface.mac_addr.as_ref().map(|a| a.octets())
                == other.iface.mac_addr.as_ref().map(|a| a.octets())
    }
}

impl Eq for Interface {}

impl Interface {
    /// Does this represent the loopback interface?
    pub fn is_loopback(&self) -> bool {
        is_loopback(&self.iface)
    }

    /// Is this interface up?
    pub fn is_up(&self) -> bool {
        is_up(&self.iface)
    }

    /// A list of all ip addresses of this interface.
    pub fn addrs(&self) -> impl Iterator<Item = IpNet> + '_ {
        self.iface
            .ipv4
            .iter()
            .cloned()
            .map(IpNet::V4)
            .chain(self.iface.ipv6.iter().cloned().map(IpNet::V6))
    }
}

/// Structure of an IP network, either IPv4 or IPv6.
#[derive(Clone, Debug)]
pub enum IpNet {
    /// Structure of IPv4 Network.
    V4(Ipv4Net),
    /// Structure of IPv6 Network.
    V6(Ipv6Net),
}

impl PartialEq for IpNet {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (IpNet::V4(a), IpNet::V4(b)) => {
                a.addr == b.addr && a.prefix_len == b.prefix_len && a.netmask == b.netmask
            }
            (IpNet::V6(a), IpNet::V6(b)) => {
                a.addr == b.addr && a.prefix_len == b.prefix_len && a.netmask == b.netmask
            }
            _ => false,
        }
    }
}
impl Eq for IpNet {}

impl IpNet {
    /// The IP address of this structure.
    pub fn addr(&self) -> IpAddr {
        match self {
            IpNet::V4(a) => IpAddr::V4(a.addr),
            IpNet::V6(a) => IpAddr::V6(a.addr),
        }
    }
}

/// Intended to store the state of the machine's network interfaces, routing table, and
/// other network configuration. For now it's pretty basic.
#[derive(Debug)]
pub struct State {
    /// Maps from an interface name to the IP addresses configured on that interface.
    pub interface_ips: HashMap<String, Vec<IpNet>>,
    /// List of available interfaces, identified by name.
    pub interface: HashMap<String, Interface>,

    /// Whether this machine has an IPv6 Global or Unique Local Address
    /// which might provide connectivity.
    pub have_v6: bool,

    /// Whether the machine has some non-localhost, non-link-local IPv4 address.
    pub have_v4: bool,

    //// Whether the current network interface is considered "expensive", which currently means LTE/etc
    /// instead of Wifi. This field is not populated by `get_state`.
    pub is_expensive: bool,

    /// The interface name for the machine's default route.
    ///
    /// It is not yet populated on all OSes.
    ///
    /// When set, its value is the map key into `interface` and `interface_ips`.
    pub default_route_interface: Option<String>,

    /// The HTTP proxy to use, if any.
    pub http_proxy: Option<String>,

    /// The URL to the Proxy Autoconfig URL, if applicable.
    pub pac: Option<String>,
}

impl State {
    /// Returns the state of all the current machine's network interfaces.
    ///
    /// It does not set the returned `State.is_expensive`. The caller can populate that.
    pub async fn new() -> Self {
        let mut interface_ips = HashMap::new();
        let mut interface = HashMap::new();
        let mut have_v6 = false;
        let mut have_v4 = false;

        let ifaces = default_net::interface::get_interfaces();
        for iface in ifaces {
            let ni = Interface { iface };
            let if_up = ni.is_up();
            let name = ni.iface.name.clone();
            let pfxs: Vec<_> = ni.addrs().collect();
            dbg!(&ni, if_up);
            if if_up {
                for pfx in &pfxs {
                    dbg!(pfx);
                    if dbg!(pfx.addr().is_loopback()) {
                        continue;
                    }
                    have_v6 |= is_usable_v6(&pfx.addr());
                    have_v4 |= is_usable_v4(&pfx.addr());
                }
            }

            interface.insert(name.clone(), ni);
            interface_ips.insert(name, pfxs);
        }

        let default_route_interface = default_route_interface().await;

        State {
            interface_ips,
            interface,
            have_v4,
            have_v6,
            is_expensive: false,
            default_route_interface,
            http_proxy: None,
            pac: None,
        }
    }

    /// Is a PAC set?
    pub fn has_pac(&self) -> bool {
        self.pac.is_some()
    }

    /// Reports whether this state and `s2` are equal, considering only interfaces in `self`
    /// for which `use_interface` returns `true`, and considering only IPs for those interfaces
    /// for which `use_ip` returns `true`.
    pub fn equal_filtered<F, G>(&self, s2: &Self, use_interface: F, use_ip: G) -> bool
    where
        F: Fn(&Interface, &[IpNet]) -> bool,
        G: Fn(IpAddr) -> bool,
    {
        if self.have_v6 != s2.have_v6
            || self.have_v4 != s2.have_v4
            || self.is_expensive != s2.is_expensive
            || self.default_route_interface != s2.default_route_interface
            || self.http_proxy != s2.http_proxy
            || self.pac != s2.pac
        {
            return false;
        }
        for (iname, i) in &self.interface {
            if let Some(ips) = self.interface_ips.get(iname) {
                if !use_interface(i, ips) {
                    continue;
                }
                let i2 = s2.interface.get(iname);
                if i2.is_none() {
                    return false;
                }
                let i2 = i2.unwrap();
                let ips2 = s2.interface_ips.get(iname);
                if ips2.is_some() {
                    return false;
                }
                let ips2 = ips2.unwrap();
                if i != i2 || !prefixes_equal_filtered(ips, ips2, &use_ip) {
                    return false;
                }
            }
        }
        true
    }

    /// Reports whether any interface has the provided IP address.
    pub fn has_ip(&self, ip: &IpAddr) -> bool {
        for pv in self.interface_ips.values() {
            for p in pv {
                match (p, ip) {
                    (IpNet::V4(a), IpAddr::V4(b)) => {
                        if &a.addr == b {
                            return true;
                        }
                    }
                    (IpNet::V6(a), IpAddr::V6(b)) => {
                        if &a.addr == b {
                            return true;
                        }
                    }
                    _ => {}
                }
            }
        }
        false
    }

    /// Reports whether any interface seems like it has internet access.
    pub fn any_interface_up(&self) -> bool {
        self.have_v4 || self.have_v6
    }
}

fn prefixes_equal_filtered<F>(a: &[IpNet], b: &[IpNet], use_ip: F) -> bool
where
    F: Fn(IpAddr) -> bool,
{
    if a.len() != b.len() {
        return false;
    }
    for (a, b) in a.iter().zip(b.iter()) {
        let use_a = use_ip(a.addr());
        let use_b = use_ip(b.addr());
        if use_a != use_b {
            return false;
        }
        if use_a && a.addr() != b.addr() {
            return false;
        }
    }

    true
}

/// Reports whether ip is a usable IPv4 address which could
/// conceivably be used to get Internet connectivity. Globally routable and
/// private IPv4 addresses are always Usable, and link local 169.254.x.x
/// addresses are in some environments.
fn is_usable_v4(ip: &IpAddr) -> bool {
    if !ip.is_ipv4() || ip.is_loopback() {
        return false;
    }

    true
}

/// Reports whether ip is a usable IPv6 address which could
/// conceivably be used to get Internet connectivity. Globally routable
/// IPv6 addresses are always Usable, and Unique Local Addresses
/// (fc00::/7) are in some environments used with address translation.
fn is_usable_v6(ip: &IpAddr) -> bool {
    match ip {
        IpAddr::V6(ip) => {
            // V6 Global1 2000::/3
            if matches!(ip.segments(), [0x2000, _, _, _, _, _, _, _]) {
                return true;
            }

            is_private_v6(ip)
        }
        IpAddr::V4(_) => false,
    }
}

/// The details about a default route.
#[derive(Debug, Clone)]
pub struct DefaultRouteDetails {
    /// The interface name.
    /// It's like "eth0" (Linux), "Ethernet 2" (Windows), "en0" (macOS).
    pub interface_name: String,

    /// Ppopulated on Windows at least. Longer description of the interface.
    pub interface_description: Option<String>,

    /// The index of the interface, `0` means not populated.
    pub interface_index: u32,
}

impl DefaultRouteDetails {
    /// Reads the default route from the current system and returns the details.
    pub async fn new() -> Option<Self> {
        default_route().await
    }
}

/// Like `DefaultRoutDetails::new` but only returns the interface name.
pub async fn default_route_interface() -> Option<String> {
    DefaultRouteDetails::new().await.map(|v| v.interface_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_default_route() {
        let default_route = DefaultRouteDetails::new()
            .await
            .expect("missing default route");
        println!("default_route: {:#?}", default_route);
    }
}
