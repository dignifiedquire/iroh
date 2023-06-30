//! Implements a socket that can change its communication path while in use, actively searching for the best way to communicate.
//!
//! Based on tailscale/wgengine/magicsock

mod conn;
mod derp_actor;
mod endpoint;
mod rebinding_conn;
mod timer;
mod udp_actor;

pub use self::conn::{Conn, Options};
pub use self::endpoint::EndpointInfo;
pub use self::timer::Timer;
