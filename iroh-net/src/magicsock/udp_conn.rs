use std::{
    fmt::Debug,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::{ready, Context, Poll},
};

use anyhow::{bail, Context as _};
use netwatch::UdpSocket;
use quinn::AsyncUdpSocket;
use quinn_udp::{Transmit, UdpSockRef};
use tokio::io::Interest;
use tracing::{debug, trace};

/// A UDP socket implementing Quinn's [`AsyncUdpSocket`].
#[derive(Clone, Debug)]
pub struct UdpConn {
    io: Arc<UdpSocket>,
    inner: Arc<quinn_udp::UdpSocketState>,
}

impl UdpConn {
    pub(super) fn as_socket(&self) -> Arc<UdpSocket> {
        self.io.clone()
    }

    pub(super) fn bind(addr: SocketAddr) -> anyhow::Result<Self> {
        let sock = bind(addr)?;
        let state = quinn_udp::UdpSocketState::new(quinn_udp::UdpSockRef::from(&sock))?;
        Ok(Self {
            io: Arc::new(sock),
            inner: Arc::new(state),
        })
    }

    pub fn port(&self) -> u16 {
        self.local_addr().map(|p| p.port()).unwrap_or_default()
    }

    #[allow(clippy::unused_async)]
    pub async fn close(&self) -> Result<(), io::Error> {
        // Nothing to do atm
        Ok(())
    }
}

impl AsyncUdpSocket for UdpConn {
    fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn quinn::UdpPoller>> {
        let sock = self.io.clone();
        Box::pin(IoPoller {
            next_waiter: move || {
                let sock = sock.clone();
                async move { sock.writable().await }
            },
            waiter: None,
        })
    }

    fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        self.io.try_io(Interest::WRITABLE, || {
            let sock_ref = UdpSockRef::from(&self.io);
            self.inner.send(sock_ref, transmit)
        })
    }

    fn poll_recv(
        &self,
        cx: &mut Context,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [quinn_udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.io.poll_recv_ready(cx))?;
            if let Ok(res) = self.io.try_io(Interest::READABLE, || {
                self.inner.recv(Arc::as_ref(&self.io).into(), bufs, meta)
            }) {
                for meta in meta.iter().take(res) {
                    trace!(
                        src = %meta.addr,
                        len = meta.len,
                        count = meta.len / meta.stride,
                        dst = %meta.dst_ip.map(|x| x.to_string()).unwrap_or_default(),
                        "UDP recv"
                    );
                }

                return Poll::Ready(Ok(res));
            }
        }
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.io.local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }

    fn max_transmit_segments(&self) -> usize {
        self.inner.max_gso_segments()
    }

    fn max_receive_segments(&self) -> usize {
        self.inner.gro_segments()
    }
}

fn bind(mut addr: SocketAddr) -> anyhow::Result<UdpSocket> {
    debug!(%addr, "binding");

    // Build a list of preferred ports.
    // - Best is the port that the user requested.
    // - Second best is the port that is currently in use.
    // - If those fail, fall back to 0.

    let mut ports = Vec::new();
    if addr.port() != 0 {
        ports.push(addr.port());
    }
    // Backup port
    ports.push(0);
    // Remove duplicates. (All duplicates are consecutive.)
    ports.dedup();
    debug!(?ports, "candidate ports");

    for port in &ports {
        addr.set_port(*port);
        match UdpSocket::bind_full(addr) {
            Ok(pconn) => {
                let local_addr = pconn.local_addr().context("UDP socket not bound")?;
                debug!(%addr, %local_addr, "successfully bound");
                return Ok(pconn);
            }
            Err(err) => {
                debug!(%addr, "failed to bind: {err:#}");
                continue;
            }
        }
    }

    // Failed to bind, including on port 0 (!).
    bail!("failed to bind any ports on {:?} (tried {:?})", addr, ports);
}

/// Poller for when the socket is writable.
///
/// The tricky part is that we only have `tokio::net::UdpSocket::writable()` to create the
/// waiter we need, which does not return a named future type.  In order to be able to store
/// this waiter in a struct without boxing we need to specify the future itself as a type
/// parameter, which we can only do if we introduce a second type parameter which returns
/// the future.  So we end up with a function which we do not need, but it makes the types
/// work.
#[derive(derive_more::Debug)]
#[pin_project::pin_project]
struct IoPoller<F, Fut>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + Sync + 'static,
{
    /// Function which can create a new waiter if there is none.
    #[debug("next_waiter")]
    next_waiter: F,
    /// The waiter which tells us when the socket is writable.
    #[debug("waiter")]
    #[pin]
    waiter: Option<Fut>,
}

impl<F, Fut> quinn::UdpPoller for IoPoller<F, Fut>
where
    F: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = io::Result<()>> + Send + Sync + 'static,
{
    fn poll_writable(self: Pin<&mut Self>, cx: &mut Context) -> Poll<io::Result<()>> {
        let mut this = self.project();
        if this.waiter.is_none() {
            this.waiter.set(Some((this.next_waiter)()));
        }
        let result = this
            .waiter
            .as_mut()
            .as_pin_mut()
            .expect("just set")
            .poll(cx);
        if result.is_ready() {
            this.waiter.set(None);
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use netwatch::IpFamily;
    use tokio::sync::mpsc;
    use tracing::{info_span, Instrument};

    use super::*;
    use crate::{key, tls};

    const ALPN: &[u8] = b"n0/test/1";

    fn wrap_socket(conn: impl AsyncUdpSocket) -> Result<(quinn::Endpoint, key::SecretKey)> {
        let key = key::SecretKey::generate();
        let quic_server_config = tls::make_server_config(&key, vec![ALPN.to_vec()], false)?;
        let server_config = quinn::ServerConfig::with_crypto(Arc::new(quic_server_config));
        let mut quic_ep = quinn::Endpoint::new_with_abstract_socket(
            quinn::EndpointConfig::default(),
            Some(server_config),
            Arc::new(conn),
            Arc::new(quinn::TokioRuntime),
        )?;

        let quic_client_config = tls::make_client_config(&key, None, vec![ALPN.to_vec()], false)?;
        let client_config = quinn::ClientConfig::new(Arc::new(quic_client_config));
        quic_ep.set_default_client_config(client_config);
        Ok((quic_ep, key))
    }

    #[tokio::test]
    async fn test_rebinding_conn_send_recv_ipv4() -> Result<()> {
        let _guard = iroh_test::logging::setup();
        rebinding_conn_send_recv(IpFamily::V4).await
    }

    #[tokio::test]
    async fn test_rebinding_conn_send_recv_ipv6() -> Result<()> {
        let _guard = iroh_test::logging::setup();
        if !net_report::os_has_ipv6() {
            return Ok(());
        }
        rebinding_conn_send_recv(IpFamily::V6).await
    }

    async fn rebinding_conn_send_recv(network: IpFamily) -> Result<()> {
        let m1 = UdpConn::bind(SocketAddr::new(network.unspecified_addr(), 0))?;
        let (m1, _m1_key) = wrap_socket(m1)?;

        let m2 = UdpConn::bind(SocketAddr::new(network.unspecified_addr(), 0))?;
        let (m2, _m2_key) = wrap_socket(m2)?;

        let m1_addr = SocketAddr::new(network.local_addr(), m1.local_addr()?.port());
        let (m1_send, mut m1_recv) = mpsc::channel(8);

        let m1_task = tokio::task::spawn(
            async move {
                // we skip accept() errors, they can be caused by retransmits
                if let Some(conn) = m1.accept().await.and_then(|inc| inc.accept().ok()) {
                    let conn = conn.await?;
                    let (mut send_bi, mut recv_bi) = conn.accept_bi().await?;

                    let val = recv_bi.read_to_end(usize::MAX).await?;
                    m1_send.send(val).await?;
                    send_bi.finish()?;
                    send_bi.stopped().await?;
                }

                Ok::<_, anyhow::Error>(())
            }
            .instrument(info_span!("m1_task")),
        );

        let conn = m2.connect(m1_addr, "localhost")?.await?;

        let (mut send_bi, mut recv_bi) = conn.open_bi().await?;
        send_bi.write_all(b"hello").await?;
        send_bi.finish()?;

        let _ = recv_bi.read_to_end(usize::MAX).await?;
        conn.close(0u32.into(), b"done");
        m2.wait_idle().await;

        drop(send_bi);

        // make sure the right values arrived
        let val = m1_recv.recv().await.unwrap();
        assert_eq!(val, b"hello");

        m1_task.await??;

        Ok(())
    }
}
