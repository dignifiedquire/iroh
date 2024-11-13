//! The main event loop for the relay server.
//!
//! based on tailscale/derp/derp_server.go

use std::{collections::HashMap, time::Duration};

use anyhow::{bail, Result};
use bytes::Bytes;
use iroh_base::key::PublicKey;
use iroh_metrics::{core::UsageStatsReport, inc, inc_by, report_usage_stats};
use time::{Date, OffsetDateTime};
use tokio::sync::mpsc;
use tokio_util::{sync::CancellationToken, task::AbortOnDropHandle};
use tracing::{info, info_span, trace, warn, Instrument};

use crate::{
    defaults::timeouts::SERVER_WRITE_TIMEOUT as WRITE_TIMEOUT,
    protos::relay::SERVER_CHANNEL_SIZE,
    server::{client_conn::ClientConnConfig, clients::Clients, metrics::Metrics},
};

// TODO: skipping `verboseDropKeys` for now

#[derive(Debug)]
pub(super) enum Message {
    SendPacket {
        dst: PublicKey,
        data: Bytes,
        src: PublicKey,
    },
    SendDiscoPacket {
        dst: PublicKey,
        data: Bytes,
        src: PublicKey,
    },
    CreateClient(ClientConnConfig),
    RemoveClient {
        key: PublicKey,
        conn_num: usize,
    },
}

/// A request to write a dataframe to a Client
#[derive(Debug, Clone)]
pub(super) struct Packet {
    /// The sender of the packet
    pub(super) src: PublicKey,
    /// The data packet bytes.
    pub(super) data: Bytes,
}

/// The task for a running server actor.
///
/// Will forcefully abort the server actor loop when dropped.
/// For stopping gracefully, use [`ServerActorTask::close`].
///
/// Responsible for managing connections to relay [`Conn`](crate::RelayConn)s, sending packets from one client to another.
#[derive(Debug)]
pub(super) struct ServerActorTask {
    /// Specifies how long to wait before failing when writing to a client.
    pub(super) write_timeout: Duration,
    /// Channel on which to communicate to the [`ServerActor`]
    pub(super) server_channel: mpsc::Sender<Message>,
    /// Server loop handler
    loop_handler: AbortOnDropHandle<Result<()>>,
    /// Token to shutdown the actor loop.
    cancel: CancellationToken,
}

impl ServerActorTask {
    /// Creates a new `ServerActorTask` and start the actor.
    pub(super) fn spawn() -> Self {
        let (server_channel_s, server_channel_r) = mpsc::channel(SERVER_CHANNEL_SIZE);
        let server_actor = Actor::new(server_channel_r);
        let cancel_token = CancellationToken::new();
        let done = cancel_token.clone();
        let server_task = AbortOnDropHandle::new(tokio::spawn(
            async move { server_actor.run(done).await }.instrument(info_span!("relay.server")),
        ));

        Self {
            write_timeout: WRITE_TIMEOUT,
            server_channel: server_channel_s,
            loop_handler: server_task,
            cancel: cancel_token,
        }
    }

    /// Closes the server and waits for the connections to disconnect.
    pub(super) async fn close(self) {
        self.cancel.cancel();
        match self.loop_handler.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => warn!("error shutting down server: {e:?}"),
            Err(e) => warn!("error waiting for the server process to close: {e:?}"),
        }
    }
}

struct Actor {
    /// Channel to receive control messages
    receiver: mpsc::Receiver<Message>,
    /// All clients connected to this server
    clients: Clients,
    /// Statistics about the connected clients
    client_counter: ClientCounter,
}

impl Actor {
    fn new(receiver: mpsc::Receiver<Message>) -> Self {
        Self {
            receiver,
            clients: Clients::default(),
            client_counter: ClientCounter::default(),
        }
    }

    async fn run(mut self, done: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = done.cancelled() => {
                    info!("server actor loop cancelled, closing loop");
                    // TODO: stats: drain channel & count dropped packets etc
                    // close all client connections and client read/write loops
                    self.clients.shutdown().await;
                    return Ok(());
                }
                msg = self.receiver.recv() => match msg {
                    Some(msg) => {
                        self.handle_message(msg).await;
                    }
                    None => {
                        warn!("unexpected actor error: receiver gone, shutting down actor loop");
                        self.clients.shutdown().await;
                        bail!("unexpected actor error, closed client connections, and shutting down actor loop");
                    }
                }
            }
        }
    }

    async fn handle_message(&mut self, msg: Message) {
        match msg {
            Message::SendPacket { dst, data, src } => {
                trace!(
                    "send packet from: {:?} to: {:?} ({}b)",
                    src,
                    dst,
                    data.len()
                );
                if self.clients.contains_key(&dst) {
                    match self.clients.send_packet(&dst, Packet { data, src }).await {
                        Ok(()) => {
                            self.clients.record_send(&src, dst);
                            inc!(Metrics, send_packets_sent);
                        }
                        Err(err) => {
                            trace!("failed to send packet to {dst:?}: {err:?}");
                            inc!(Metrics, send_packets_dropped);
                        }
                    }
                } else {
                    warn!("no way to reach client {dst:?}, dropped packet");
                    inc!(Metrics, send_packets_dropped);
                }
            }
            Message::SendDiscoPacket { dst, data, src } => {
                trace!(
                    "send disco packet from: {:?} to: {:?} ({}b)",
                    src,
                    dst,
                    data.len()
                );
                if self.clients.contains_key(&dst) {
                    match self
                        .clients
                        .send_disco_packet(&dst, Packet { data, src })
                        .await
                    {
                        Ok(()) => {
                            self.clients.record_send(&src, dst);
                            inc!(Metrics, disco_packets_sent);
                        }
                        Err(err) => {
                            trace!("failed to send disco packet to {dst:?}: {err:?}");
                            inc!(Metrics, disco_packets_dropped);
                        }
                    }
                } else {
                    warn!("disco: no way to reach client {dst:?}, dropped packet");
                    inc!(Metrics, disco_packets_dropped);
                }
            }
            Message::CreateClient(client_builder) => {
                inc!(Metrics, accepts);

                trace!("create client: {:?}", client_builder.key);
                let key = client_builder.key;

                // build and register client, starting up read & write loops for the client connection
                self.clients.register(client_builder).await;
                tokio::task::spawn(async move {
                    report_usage_stats(&UsageStatsReport::new(
                        "relay_accepts".to_string(),
                        "relay_server".to_string(), // TODO: other id?
                        1,
                        None, // TODO(arqu): attribute to user id; possibly with the re-introduction of request tokens or other auth
                        Some(key.to_string()),
                    ))
                    .await;
                });
                let nc = self.client_counter.update(key);
                inc_by!(Metrics, unique_client_keys, nc);
            }
            Message::RemoveClient { key, conn_num } => {
                inc!(Metrics, disconnects);
                trace!("remove client: {:?}", key);
                // ensure we still have the client in question
                if self.clients.has_client(&key, conn_num) {
                    // remove the client from the map of clients, & notify any peers that it
                    // has sent messages that it has left the network
                    self.clients.unregister(&key).await;
                }
            }
        }
    }
}

/// Counts how many `PublicKey`s seen, how many times.
/// Gets reset every day.
struct ClientCounter {
    clients: HashMap<PublicKey, usize>,
    last_clear_date: Date,
}

impl Default for ClientCounter {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
            last_clear_date: OffsetDateTime::now_utc().date(),
        }
    }
}

impl ClientCounter {
    fn check_and_clear(&mut self) {
        let today = OffsetDateTime::now_utc().date();
        if today != self.last_clear_date {
            self.clients.clear();
            self.last_clear_date = today;
        }
    }

    /// Updates the client counter.
    fn update(&mut self, client: PublicKey) -> u64 {
        self.check_and_clear();
        let new_conn = !self.clients.contains_key(&client);
        let counter = self.clients.entry(client).or_insert(0);
        *counter += 1;
        new_conn as u64
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use iroh_base::key::SecretKey;
    use tokio::io::DuplexStream;
    use tokio_util::codec::Framed;

    use super::*;
    use crate::{
        protos::relay::{recv_frame, DerpCodec, Frame, FrameType},
        server::{
            client_conn::ClientConnConfig,
            streams::{MaybeTlsStream, RelayedStream},
        },
    };

    fn test_client_builder(
        key: PublicKey,
        server_channel: mpsc::Sender<Message>,
    ) -> (ClientConnConfig, Framed<DuplexStream, DerpCodec>) {
        let (test_io, io) = tokio::io::duplex(1024);
        (
            ClientConnConfig {
                key,
                stream: RelayedStream::Derp(Framed::new(MaybeTlsStream::Test(io), DerpCodec)),
                write_timeout: Duration::from_secs(1),
                channel_capacity: 10,
                server_channel,
            },
            Framed::new(test_io, DerpCodec),
        )
    }

    #[tokio::test]
    async fn test_server_actor() -> Result<()> {
        // make server actor
        let (server_channel, server_channel_r) = mpsc::channel(20);
        let server_actor: Actor = Actor::new(server_channel_r);
        let done = CancellationToken::new();
        let server_done = done.clone();

        // run server actor
        let server_task = tokio::spawn(
            async move { server_actor.run(server_done).await }
                .instrument(info_span!("relay.server")),
        );

        let key_a = SecretKey::generate().public();
        let (client_a, mut a_io) = test_client_builder(key_a, server_channel.clone());

        // create client a
        server_channel
            .send(Message::CreateClient(client_a))
            .await
            .map_err(|_| anyhow::anyhow!("server gone"))?;

        // server message: create client b
        let key_b = SecretKey::generate().public();
        let (client_b, mut b_io) = test_client_builder(key_b, server_channel.clone());
        server_channel
            .send(Message::CreateClient(client_b))
            .await
            .map_err(|_| anyhow::anyhow!("server gone"))?;

        // write message from b to a
        let msg = b"hello world!";
        crate::client::conn::send_packet(&mut b_io, &None, key_a, Bytes::from_static(msg)).await?;

        // get message on a's reader
        let frame = recv_frame(FrameType::RecvPacket, &mut a_io).await?;
        assert_eq!(
            frame,
            Frame::RecvPacket {
                src_key: key_b,
                content: msg.to_vec().into()
            }
        );

        // remove b
        server_channel
            .send(Message::RemoveClient {
                key: key_b,
                conn_num: 1,
            })
            .await
            .map_err(|_| anyhow::anyhow!("server gone"))?;

        // get peer gone message on a about b leaving the network
        // (we get this message because b has sent us a packet before)
        let frame = recv_frame(FrameType::PeerGone, &mut a_io).await?;
        assert_eq!(Frame::PeerGone { peer: key_b }, frame);

        // close gracefully
        done.cancel();
        server_task.await??;
        Ok(())
    }
}
