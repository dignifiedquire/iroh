//! Defaults used by netcheck.
// TODO(@divma): move these to they respective files?

/// The default STUN port used by the Relay server.
///
/// The STUN port as defined by [RFC
/// 8489](<https://www.rfc-editor.org/rfc/rfc8489#section-18.6>)
pub const DEFAULT_STUN_PORT: u16 = 3478;

#[cfg(test)]
pub(crate) mod staging {
    /// Hostname of the default EU relay.
    pub const EU_RELAY_HOSTNAME: &str = "staging-euw1-1.relay.iroh.network.";
}

pub(crate) mod timeouts {
    use std::time::Duration;

    // Timeouts for netcheck

    /// Maximum duration to wait for a netcheck report.
    pub(crate) const NETCHECK_REPORT_TIMEOUT: Duration = Duration::from_secs(10);

    /// The maximum amount of time netcheck will spend gathering a single report.
    pub(crate) const OVERALL_REPORT_TIMEOUT: Duration = Duration::from_secs(5);

    /// The total time we wait for all the probes.
    ///
    /// This includes the STUN, ICMP and HTTPS probes, which will all
    /// start at different times based on the ProbePlan.
    pub(crate) const PROBES_TIMEOUT: Duration = Duration::from_secs(3);

    /// How long to await for a captive-portal result.
    ///
    /// This delay is chosen so it starts after good-working STUN probes
    /// would have finished, but not too long so the delay is bearable if
    /// STUN is blocked.
    pub(crate) const CAPTIVE_PORTAL_DELAY: Duration = Duration::from_millis(200);

    /// Timeout for captive portal checks
    ///
    /// Must be lower than [`OVERALL_REPORT_TIMEOUT`] minus
    /// [`CAPTIVE_PORTAL_DELAY`].
    pub(crate) const CAPTIVE_PORTAL_TIMEOUT: Duration = Duration::from_secs(2);

    pub(crate) const DNS_TIMEOUT: Duration = Duration::from_secs(3);

    /// The amount of time we wait for a hairpinned packet to come back.
    pub(crate) const HAIRPIN_CHECK_TIMEOUT: Duration = Duration::from_millis(100);

    /// Default Pinger timeout
    pub(crate) const DEFAULT_PINGER_TIMEOUT: Duration = Duration::from_secs(5);
}
