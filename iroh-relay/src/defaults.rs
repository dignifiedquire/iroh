//! Default values used in the relay.

/// The default STUN port used by the Relay server.
///
/// The STUN port as defined by [RFC
/// 8489](<https://www.rfc-editor.org/rfc/rfc8489#section-18.6>)
pub const DEFAULT_STUN_PORT: u16 = 3478;

/// The default QUIC port used by the Relay server to accept QUIC connections
/// for QUIC address discovery
///
/// The port is "QUIC" typed on a phone keypad.
pub const DEFAULT_QUIC_PORT: u16 = 7842;

/// The default HTTP port used by the Relay server.
pub const DEFAULT_HTTP_PORT: u16 = 80;

/// The default HTTPS port used by the Relay server.
pub const DEFAULT_HTTPS_PORT: u16 = 443;

/// The default metrics port used by the Relay server.
pub const DEFAULT_METRICS_PORT: u16 = 9090;

/// Contains all timeouts that we use in `iroh-net`.
pub(crate) mod timeouts {
    use std::time::Duration;

    /// Timeout used by the relay client while connecting to the relay server,
    /// using `TcpStream::connect`
    pub(crate) const DIAL_NODE_TIMEOUT: Duration = Duration::from_millis(1500);
    /// Timeout for expecting a pong from the relay server
    pub(crate) const PING_TIMEOUT: Duration = Duration::from_secs(5);
    /// Timeout for the entire relay connection, which includes dns, dialing
    /// the server, upgrading the connection, and completing the handshake
    pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
    /// Timeout for our async dns resolver
    pub(crate) const DNS_TIMEOUT: Duration = Duration::from_secs(1);

    /// Maximum time the client will wait to receive on the connection, since
    /// the last message. Longer than this time and the client will consider
    /// the connection dead.
    pub(crate) const CLIENT_RECV_TIMEOUT: Duration = Duration::from_secs(120);

    /// Maximum time the server will attempt to get a successful write to the connection.
    #[cfg(feature = "server")]
    #[cfg_attr(iroh_docsrs, doc(cfg(feature = "server")))]
    pub(crate) const SERVER_WRITE_TIMEOUT: Duration = Duration::from_secs(2);
}
