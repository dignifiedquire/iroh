//! Port mapping client and service.

use std::{
    net::SocketAddrV4,
    num::NonZeroU16,
    time::{Duration, Instant},
};

use anyhow::{anyhow, Result};
use futures::StreamExt;
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, trace};

use iroh_metrics::{inc, portmap::PortmapMetrics as Metrics};

use crate::util;

use mapping::CurrentMapping;

mod mapping;
mod upnp;

/// If a port mapping service has been seen within the last [`AVAILABILITY_TRUST_DURATION`] it will
/// not be probed again.
const AVAILABILITY_TRUST_DURATION: Duration = Duration::from_secs(60 * 10); // 10 minutes

#[derive(Debug, Clone)]
pub struct ProbeOutput {
    /// If UPnP can be considered available.
    pub upnp: bool,
    /// If PCP can be considered available.
    pub pcp: bool,
    /// If PMP can be considered available.
    pub pmp: bool,
}

#[derive(derive_more::Debug)]
enum Message {
    /// Request to update the local port.
    ///
    /// The resulting external address can be obtained subscribing using
    /// [`Client::watch_external_address`].
    /// A value of `None` will deactivate port mapping.
    UpdateLocalPort { local_port: Option<NonZeroU16> },
    /// Request to probe the port mapping protocols.
    ///
    /// The requester should wait for the result at the [`oneshot::Receiver`] counterpart of the
    /// [`oneshot::Sender`].
    Probe {
        /// Sender side to communicate the result of the probe.
        #[debug("_")]
        result_tx: oneshot::Sender<Result<ProbeOutput, String>>,
    },
}

/// Port mapping client.
#[derive(Debug, Clone)]
pub struct Client {
    /// A watcher over the most recent external address obtained from port mapping.
    ///
    /// See [`watch::Receiver`].
    port_mapping: watch::Receiver<Option<SocketAddrV4>>,
    /// Channel used to communicate with the port mapping service.
    service_tx: mpsc::Sender<Message>,
    /// A handle to the service that will cancel the spawned task once the client is dropped.
    _service_handle: std::sync::Arc<util::CancelOnDrop>,
}

impl Client {
    /// Create a new port mapping client.
    pub async fn new() -> Self {
        let (service_tx, service_rx) = mpsc::channel(4);

        let (service, watcher) = Service::new(service_rx);

        let handle = util::CancelOnDrop::new(
            "portmap_service",
            tokio::spawn(async move { service.run().await }).abort_handle(),
        );

        Client {
            port_mapping: watcher,
            service_tx,
            _service_handle: std::sync::Arc::new(handle),
        }
    }

    /// Request a probe to the port mapping protocols.
    ///
    /// Return the [`oneshot::Receiver`] used to obtain the result of the probe.
    pub fn probe(&self) -> oneshot::Receiver<Result<ProbeOutput, String>> {
        let (result_tx, result_rx) = oneshot::channel();

        use mpsc::error::TrySendError::*;
        if let Err(e) = self.service_tx.try_send(Message::Probe { result_tx }) {
            // recover the sender and return the error there
            let (result_tx, e) = match e {
                Full(Message::Probe { result_tx }) => (result_tx, "Port mapping channel full"),
                Closed(Message::Probe { result_tx }) => (result_tx, "Port mapping channel closed"),
                Full(_) | Closed(_) => unreachable!("Sent value is a probe."),
            };

            result_tx
                .send(Err(e.into()))
                .expect("receiver counterpart has not been dropped or closed.");
        }
        result_rx
    }

    /// Update the local port.
    ///
    /// A value of `None` will invalidate any active mapping and deactivate port mapping
    /// maintenance.
    /// This can fail if communicating with the port-mapping service fails.
    // TODO(@divma): there is nothing that can be done when receiving this error. Maybe it's best
    // to log it and move on.
    pub fn update_local_port(&self, local_port: Option<NonZeroU16>) -> Result<()> {
        self.service_tx
            .try_send(Message::UpdateLocalPort { local_port })
            .map_err(Into::into)
    }

    /// Watch the external address for changes in the mappings.
    pub fn watch_external_address(&self) -> watch::Receiver<Option<SocketAddrV4>> {
        self.port_mapping.clone()
    }
}

/// Port mapping protocol information obtained during a probe.
///
/// This can be updated with [`Probe::new_from_valid_probe`].
#[derive(Debug, Default)]
struct Probe {
    /// The last [`igd::aio::Gateway`] and when was it last seen.
    last_upnp_gateway_addr: Option<(upnp::Gateway, Instant)>,
    // TODO(@divma): pcp placeholder.
    last_pcp: Option<Instant>,
    // TODO(@divma): pmp placeholder.
    last_pmp: Option<Instant>,
}

impl Probe {
    /// Creates a new probe that is considered valid.
    ///
    /// For any protocol, if the passed result is `None` it will be probed.
    async fn new_from_valid_probe(mut valid_probe: Probe) -> Probe {
        let mut upnp_probing_task = util::MaybeFuture {
            inner: valid_probe.last_upnp_gateway_addr.is_none().then(|| {
                Box::pin(async {
                    upnp::probe_available()
                        .await
                        .map(|addr| (addr, Instant::now()))
                })
            }),
        };

        // placeholder tasks
        let pcp_probing_task = async { None };
        let pmp_probing_task = async { None };

        let mut upnp_done = upnp_probing_task.inner.is_none();
        let mut pcp_done = true;
        let mut pmp_done = true;

        tokio::pin!(pmp_probing_task);
        tokio::pin!(pcp_probing_task);

        while !upnp_done || !pcp_done || !pmp_done {
            tokio::select! {
                last_upnp_gateway_addr = &mut upnp_probing_task, if !upnp_done => {
                    trace!("tick: upnp probe ready");
                    valid_probe.last_upnp_gateway_addr = last_upnp_gateway_addr;
                    upnp_done = true;
                },
                last_pmp = &mut pmp_probing_task, if !pmp_done => {
                    trace!("tick: pmp probe ready");
                    valid_probe.last_pmp = last_pmp;
                    pmp_done = true;
                },
                last_pcp = &mut pcp_probing_task, if !pcp_done => {
                    trace!("tick: pcp probe ready");
                    valid_probe.last_pcp = last_pcp;
                    pcp_done = true;
                },
            }
        }

        valid_probe
    }

    /// Returns a positive probe result if all services have been seen recently enough.
    ///
    /// If at least one protocol needs to be probed, returns a [`Probe`] with any invalid value
    /// removed.
    // TODO(@divma): function name is lame
    fn result(&self) -> Result<ProbeOutput, Probe> {
        let now = Instant::now();

        // get the last upnp gateway if it's valid
        let last_valid_upnp_gateway =
            self.last_upnp_gateway_addr
                .as_ref()
                .filter(|(_gateway_addr, last_probed)| {
                    *last_probed + AVAILABILITY_TRUST_DURATION > now
                });

        // not probing for now
        let last_valid_pcp = Some(now);

        // not probing for now
        let last_valid_pmp = Some(now);

        // decide if a new probe is necessary
        if last_valid_upnp_gateway.is_none() || last_valid_pmp.is_none() || last_valid_pcp.is_none()
        {
            Err(Probe {
                last_upnp_gateway_addr: last_valid_upnp_gateway.cloned(),
                last_pcp: last_valid_pcp,
                last_pmp: last_valid_pmp,
            })
        } else {
            // TODO(@divma): note that if we are here then all services are ready (should all be
            // `true`). But since pcp and pmp are not being probed, they get hardcoded to `false`
            Ok(ProbeOutput {
                upnp: true,
                pcp: false,
                pmp: false,
            })
        }
    }

    /// Produces a [`ProbeOutput`] without checking if the services can stll be considered valid.
    fn output(&self) -> ProbeOutput {
        ProbeOutput {
            upnp: self.last_upnp_gateway_addr.is_some(),
            pcp: false,
            pmp: false,
        }
    }
}

// mainly to make clippy happy
type ProbeResult = Result<ProbeOutput, String>;

/// A port mapping client.
#[derive(Debug)]
pub struct Service {
    /// Local port to map.
    local_port: Option<NonZeroU16>,
    /// Channel over which the service is informed of messages.
    ///
    /// The service will stop when all senders are gone.
    rx: mpsc::Receiver<Message>,
    /// Currently active mapping.
    current_mapping: CurrentMapping,
    /// Last updated probe.
    full_probe: Probe,
    /// Task attempting to get a port mapping.
    ///
    /// This task will be cancelled if a request to set the local port arrives before it's
    /// finished.
    mapping_task: Option<util::AbortingJoinHandle<Result<upnp::Mapping>>>,
    /// Task probing the necessary protocols.
    ///
    /// Requests for a probe that arrive while this task is still in progress will receive the same
    /// result.
    probing_task: Option<(
        util::AbortingJoinHandle<Probe>,
        Vec<oneshot::Sender<ProbeResult>>,
    )>,
}

#[derive(PartialEq, Eq, Debug)]
enum ReleaseMapping {
    Yes,
    No,
}

impl Service {
    fn new(rx: mpsc::Receiver<Message>) -> (Self, watch::Receiver<Option<SocketAddrV4>>) {
        let (current_mapping, watcher) = CurrentMapping::new();
        let service = Service {
            local_port: None,
            rx,
            current_mapping,
            full_probe: Default::default(),
            mapping_task: None,
            probing_task: None,
        };

        (service, watcher)
    }
    /// Clears the current mapping and releases it if necessary.
    async fn invalidate_mapping(&mut self, release: ReleaseMapping) {
        if let Some(old_mapping) = self.current_mapping.update(None) {
            if release == ReleaseMapping::Yes {
                if let Err(e) = old_mapping.release().await {
                    debug!("failed to release mapping {e}");
                }
            }
        }
    }

    async fn run(mut self) -> Result<()> {
        debug!("portmap starting");
        loop {
            tokio::select! {
                msg = self.rx.recv() => {
                    trace!("tick: msg {msg:?}");
                    match msg {
                        Some(msg) => {
                            self.handle_msg(msg).await;
                        },
                        None => {
                            debug!("portmap service channel dropped. Likely shutting down.");
                            break;
                        }
                    }
                }
                mapping_result = util::MaybeFuture{inner: self.mapping_task.as_mut()} => {
                    trace!("tick: mapping ready");
                    // regardless of outcome, the task is finished, clear it
                    self.mapping_task = None;
                    // there isn't really a way to react to a join error here. Flatten it to make
                    // it easier to work with
                    let result = match mapping_result {
                        Ok(result) => result,
                        Err(join_err) => Err(anyhow!("Failed to obtain a result {join_err}"))
                    };
                    self.on_mapping_result(result).await;
                }
                probe_result = util::MaybeFuture{inner: self.probing_task.as_mut().map(|(fut, _rec)| fut)} => {
                    trace!("tick: probe ready");
                    // retrieve the receivers and clear the task.
                    let receivers = self.probing_task.take().expect("is some").1;
                    let probe_result = probe_result.map_err(|join_err|anyhow!("Failed to obtain a result {join_err}"));
                    self.on_probe_result(probe_result, receivers).await;
                }
                Some(event) = self.current_mapping.next() => {
                    trace!("tick: mapping event {event:?}");
                    match event {
                        mapping::Event::Renew { external_port } => todo!(),
                        mapping::Event::Expired { external_port } => todo!(),
                    }

                }
            }
        }
        Ok(())
    }

    async fn on_probe_result(
        &mut self,
        result: Result<Probe>,
        receivers: Vec<oneshot::Sender<ProbeResult>>,
    ) {
        let result = match result {
            Err(e) => Err(e.to_string()),
            Ok(probe) => {
                self.full_probe = probe;
                Ok(self.full_probe.output())
            }
        };
        for tx in receivers {
            // ignore the error. If the receiver is no longer there we don't really care
            let _ = tx.send(result.clone());
        }
    }

    async fn on_mapping_result(&mut self, result: Result<upnp::Mapping>) {
        match result {
            Ok(mapping) => {
                let old_mapping = self.current_mapping.update(Some(mapping));
            }
            Err(e) => debug!("failed to get a port mapping {e}"),
        }
    }

    async fn handle_msg(&mut self, msg: Message) {
        match msg {
            Message::UpdateLocalPort { local_port } => self.update_local_port(local_port).await,
            Message::Probe { result_tx } => self.probe_request(result_tx),
        }
    }

    /// Updates the local port of the port mapping service.
    ///
    /// If the port changed, any port mapping task is cancelled. If the new port is some, it will
    /// start a new port mapping task.
    async fn update_local_port(&mut self, local_port: Option<NonZeroU16>) {
        // Ignore requests to update the local port in a way that does not produce a change.
        if local_port != self.local_port {
            let old_port = std::mem::replace(&mut self.local_port, local_port);

            // Clear the current mapping task if any.

            let dropped_task = self.mapping_task.take();
            // Check if the dropped task had finished to reduce log noise.
            let did_cancel = dropped_task
                .map(|task| !task.is_finished())
                .unwrap_or_default();

            if did_cancel {
                debug!(
                    "canceled mapping task due to local port update. Old: {:?} New: {:?}",
                    old_port, self.local_port
                )
            }

            // Since the port has changed, the current mapping is no longer valid and should be
            // release.

            self.invalidate_mapping(ReleaseMapping::Yes).await;

            // Start a new mapping task to account for the new port if necessary.

            if let Some(local_port) = self.local_port {
                debug!("getting a port mapping for port {local_port}");
                let handle = tokio::spawn(upnp::Mapping::new(
                    std::net::Ipv4Addr::LOCALHOST,
                    local_port,
                ));
                self.mapping_task = Some(handle.into());
            }
        }
    }

    /// Handles a probe request.
    ///
    /// If there is a task getting a probe, the receiver will be added with any other waiting for a
    /// result. If no probe is underway, a result can be returned immediately if it's still
    /// considered valid. Otherwise, a new probe task will be started.
    fn probe_request(&mut self, result_tx: oneshot::Sender<Result<ProbeOutput, String>>) {
        inc!(Metrics::ProbeRequests);
        match self.probing_task.as_mut() {
            Some((_task_handle, receivers)) => receivers.push(result_tx),
            None => {
                match self.full_probe.result() {
                    Ok(probe_result) => {
                        // We don't care if the requester is no longer there.
                        let _ = result_tx.send(Ok(probe_result));
                    }
                    Err(base_probe) => {
                        let receivers = vec![result_tx];
                        let handle =
                            tokio::spawn(
                                async move { Probe::new_from_valid_probe(base_probe).await },
                            );
                        self.probing_task = Some((handle.into(), receivers));
                    }
                }
            }
        }
    }
}
