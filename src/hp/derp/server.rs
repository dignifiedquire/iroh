//! based on tailscale/derp/derp_server.go
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::BytesMut;
use hyper::HeaderMap;
use postcard::experimental::max_size::MaxSize;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::{instrument, trace};

use crate::hp::key::node::{PublicKey, SecretKey};

use super::client_conn::ClientConnBuilder;
use super::{
    clients::Clients,
    types::{PacketForwarder, PeerConnState, ServerMessage},
    MeshKey,
};
use super::{
    recv_client_key, types::ServerInfo, write_frame, write_frame_timeout, FrameType, MAGIC,
    PER_CLIENT_SEND_QUEUE_DEPTH, PROTOCOL_VERSION, SERVER_CHANNEL_SIZE,
};
// TODO: skiping `verboseDropKeys` for now

static CONN_NUM: AtomicUsize = AtomicUsize::new(1);
fn new_conn_num() -> usize {
    CONN_NUM.fetch_add(1, Ordering::Relaxed)
}

pub(crate) const WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// A DERP server.
///
#[derive(Debug)]
pub struct Server<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    /// Optionally specifies how long to wait before failing when writing
    /// to a client
    write_timeout: Option<Duration>,
    /// secret_key of the client
    secret_key: SecretKey,
    // TODO: this is a string in the go impl, I made it a standard length array
    // of bytes in this impl for ease of serializing. (Postcard cannot estimate
    // the size of the serialized struct if this field is a `String`). This should
    // be discussed and worked out.
    // from go impl: log.Fatalf("key in %s must contain 64+ hex digits", *meshPSKFile)
    mesh_key: Option<MeshKey>,
    /// The DER encoded x509 cert to send after `LetsEncrypt` cert+intermediate.
    meta_cert: Vec<u8>,
    /// Channel on which to communicate to the `ServerActor`
    server_channel: mpsc::Sender<ServerMessage<R, W, P>>,
    /// When true, the server has been shutdown.
    closed: bool,
    /// The information we send to the client about the [`Server`]'s protocol version
    /// and required rate limiting (if any)
    server_info: ServerInfo,
    /// Server loop handler
    loop_handler: JoinHandle<Result<()>>,
    /// Done token, forces a hard shutdown. To gracefully shutdown, use `Server::close`
    cancel: CancellationToken,
    // TODO: stats collection
    // Counters:
    // 	packetsSent, bytesSent       expvar.Int
    // packetsRecv, bytesRecv       expvar.Int
    // packetsRecvByKind            metrics.LabelMap
    // packetsRecvDisco             *expvar.Int
    // packetsRecvOther             *expvar.Int
    // _                            align64
    // packetsDropped               expvar.Int
    // packetsDroppedReason         metrics.LabelMap
    // packetsDroppedReasonCounters []*expvar.Int // indexed by dropReason
    // packetsDroppedType           metrics.LabelMap
    // packetsDroppedTypeDisco      *expvar.Int
    // packetsDroppedTypeOther      *expvar.Int
    // _                            align64
    // packetsForwardedOut          expvar.Int
    // packetsForwardedIn           expvar.Int
    // peerGoneFrames               expvar.Int // number of peer gone frames sent
    // gotPing                      expvar.Int // number of ping frames from client
    // sentPong                     expvar.Int // number of pong frames enqueued to client
    // accepts                      expvar.Int
    // curClients                   expvar.Int
    // curHomeClients               expvar.Int // ones with preferred
    // dupClientKeys                expvar.Int // current number of public keys we have 2+ connections for
    // dupClientConns               expvar.Int // current number of connections sharing a public key
    // dupClientConnTotal           expvar.Int // total number of accepted connections when a dup key existed
    // unknownFrames                expvar.Int
    // homeMovesIn                  expvar.Int // established clients announce home server moves in
    // homeMovesOut                 expvar.Int // established clients announce home server moves out
    // multiForwarderCreated        expvar.Int
    // multiForwarderDeleted        expvar.Int
    // removePktForwardOther        expvar.Int
    // avgQueueDuration             *uint64          // In milliseconds; accessed atomically
    // tcpRtt                       metrics.LabelMap // histogram
}

impl<R, W, P> Server<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    /// TODO: replace with builder
    pub fn new(key: SecretKey, mesh_key: Option<MeshKey>) -> Self {
        let (server_channel_s, server_channel_r) = mpsc::channel(SERVER_CHANNEL_SIZE);
        let server_actor = ServerActor::new(key.public_key(), server_channel_r);
        let cancel_token = CancellationToken::new();
        let done = cancel_token.clone();
        let server_task = tokio::spawn(async move { server_actor.run(done).await });
        let meta_cert = init_meta_cert(&key.public_key());
        Self {
            write_timeout: Some(WRITE_TIMEOUT),
            secret_key: key,
            mesh_key,
            meta_cert,
            server_channel: server_channel_s,
            closed: false,
            // TODO: come up with good default
            server_info: ServerInfo::no_rate_limit(),
            loop_handler: server_task,
            cancel: cancel_token,
        }
    }

    /// Reports whether the server is configured with a mesh key.
    pub fn has_mesh_key(&self) -> bool {
        self.mesh_key.is_some()
    }

    /// Returns the configured mesh key, may be empty.
    pub fn mesh_key(&self) -> Option<MeshKey> {
        self.mesh_key
    }

    /// Returns the server's private key.
    pub fn private_key(&self) -> SecretKey {
        self.secret_key.clone()
    }

    /// Returns the server's public key.
    pub fn public_key(&self) -> PublicKey {
        self.secret_key.public_key()
    }

    /// Closes the server and waits for the connections to disconnect.
    pub async fn close(mut self) {
        if !self.closed {
            if let Err(err) = self.server_channel.send(ServerMessage::Shutdown).await {
                tracing::warn!(
                    "could not shutdown the server gracefully, doing a forced shutdown: {:?}",
                    err
                );
                self.cancel.cancel();
            }
            match self.loop_handler.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => tracing::warn!("error shutting down server: {e:?}"),
                Err(e) => tracing::warn!("error waiting for the server process to close: {e:?}"),
            }
            self.closed = true;
        }
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Create a [`PacketForwarderHandler`], which can add or remove [`PacketForwarder`]s from the
    /// [`Server`].
    pub fn packet_forwarder_handler(&self) -> PacketForwarderHandler<R, W, P> {
        PacketForwarderHandler {
            server_channel: self.server_channel.clone(),
        }
    }

    /// Create a [`ClientConnHandler`], which can verify connections and add them to the
    /// [`Server`].
    pub fn client_conn_handler(&self, default_headers: HeaderMap) -> ClientConnHandler<R, W, P> {
        ClientConnHandler {
            mesh_key: self.mesh_key,
            server_channel: self.server_channel.clone(),
            secret_key: self.secret_key.clone(),
            write_timeout: self.write_timeout,
            server_info: self.server_info.clone(),
            default_headers: Arc::new(default_headers),
        }
    }

    /// Returns the server metadata cert that can be sent by the TLS server to
    /// let the client skip a round trip during start-up.
    pub fn meta_cert(&self) -> &[u8] {
        &self.meta_cert
    }
}

/// Call `PacketForwarderHandler::add_packet_forwarder` to associate a given [`PublicKey` ] to
/// a [`PacketForwarder`] in the [`Server`].
/// Call `PacketForwarderHandler::remove_packet_forwarder` to remove any associated [`PublicKey` ] with a [`PacketForwarder`].
///
/// Created by the [`Server`] by calling [`Server::packet_forwarder_handler`].
///
/// Can be cheaply cloned.
#[derive(Debug, Clone)]
pub struct PacketForwarderHandler<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    server_channel: mpsc::Sender<ServerMessage<R, W, P>>,
}

impl<R, W, P> PacketForwarderHandler<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    pub fn add_packet_forwarder(&self, client_key: PublicKey, fwd: P) -> Result<()> {
        self.server_channel
            .try_send(ServerMessage::AddPacketForwarder((client_key, fwd)))?;
        Ok(())
    }

    pub fn remove_packet_forwarder(&self, client_key: PublicKey) -> Result<()> {
        self.server_channel
            .try_send(ServerMessage::RemovePacketForwarder(client_key))?;
        Ok(())
    }
}

/// Handle incoming connections to the Server.
///
/// Created by the [`Server`] by calling [`Server::client_conn_handler`].
///
/// Can be cheaply cloned.
#[derive(Debug)]
pub struct ClientConnHandler<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    mesh_key: Option<MeshKey>,
    server_channel: mpsc::Sender<ServerMessage<R, W, P>>,
    secret_key: SecretKey,
    write_timeout: Option<Duration>,
    server_info: ServerInfo,
    pub(super) default_headers: Arc<HeaderMap>,
}

impl<R, W, P> Clone for ClientConnHandler<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    fn clone(&self) -> Self {
        Self {
            mesh_key: self.mesh_key,
            server_channel: self.server_channel.clone(),
            secret_key: self.secret_key.clone(),
            write_timeout: self.write_timeout,
            server_info: self.server_info.clone(),
            default_headers: Arc::clone(&self.default_headers),
        }
    }
}

impl<R, W, P> ClientConnHandler<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    /// Adds a new connection to the server and serves it.
    ///
    /// Will error if it takes too long (10 sec) to write or read to the connection, if there is
    /// some read or write error to the connection,  if the server is meant to verify clients,
    /// and is unable to verify this one, or if there is some issue communicating with the server.
    ///
    /// The provided [`AsyncRead`] and [`AsyncWrite`] must be already connected to the connection.
    pub async fn accept(&self, mut reader: R, mut writer: W) -> Result<()> {
        trace!("accept: start");
        self.send_server_key(&mut writer)
            .await
            .context("unable to send server key to client")?;
        trace!("accept: recv client key");
        let (client_key, client_info) = recv_client_key(self.secret_key.clone(), &mut reader)
            .await
            .context("unable to receive client information")?;
        trace!("accept: send server info");
        self.send_server_info(&mut writer, &client_key)
            .await
            .context("unable to sent server info to client {client_key}")?;
        trace!("accept: build client conn");
        let client_conn_builder = ClientConnBuilder {
            key: client_key,
            conn_num: new_conn_num(),
            reader,
            writer,
            can_mesh: self.can_mesh(client_info.mesh_key),
            write_timeout: self.write_timeout,
            channel_capacity: PER_CLIENT_SEND_QUEUE_DEPTH,
            server_channel: self.server_channel.clone(),
        };
        trace!("accept: create client");
        self.server_channel
            .send(ServerMessage::CreateClient(client_conn_builder))
            .await
            .context("server channel closed, the server is probably shutdown")?;
        Ok(())
    }

    async fn send_server_key(&self, mut writer: &mut W) -> Result<()> {
        let mut buf = Vec::new();
        buf.extend_from_slice(MAGIC.as_bytes());
        buf.extend_from_slice(self.secret_key.public_key().as_bytes());
        let content = &[buf.as_slice()];
        write_frame_timeout(
            &mut writer,
            FrameType::ServerKey,
            content,
            Some(Duration::from_secs(10)),
        )
        .await?;
        writer.flush().await?;
        Ok(())
    }

    async fn send_server_info(&self, mut writer: &mut W, client_key: &PublicKey) -> Result<()> {
        let mut buf = BytesMut::zeroed(ServerInfo::POSTCARD_MAX_SIZE);
        let msg = postcard::to_slice(&self.server_info, &mut buf)?;
        let msg = self.secret_key.seal_to(client_key, msg);
        let msg = &[msg.as_slice()];
        write_frame(&mut writer, FrameType::ServerInfo, msg).await?;
        writer.flush().await?;
        Ok(())
    }

    /// Determines if the server and client can mesh, and, if so, are apart of the same mesh.
    fn can_mesh(&self, client_mesh_key: Option<MeshKey>) -> bool {
        if let (Some(a), Some(b)) = (self.mesh_key, client_mesh_key) {
            return a == b;
        }
        false
    }
}

pub(crate) struct ServerActor<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    key: PublicKey,
    receiver: mpsc::Receiver<ServerMessage<R, W, P>>,
    /// All clients connected to this server
    clients: Clients,
    /// Representation of the mesh network. Keys that are associated with `None` are strictly local
    /// clients.
    client_mesh: HashMap<PublicKey, Option<P>>,
    /// Mesh clients that need to be appraised on the state of the network
    watchers: HashSet<PublicKey>,
    name: String,
}

impl<R, W, P> ServerActor<R, W, P>
where
    R: AsyncRead + Unpin + Send + Sync + 'static,
    W: AsyncWrite + Unpin + Send + Sync + 'static,
    P: PacketForwarder,
{
    pub(crate) fn new(key: PublicKey, receiver: mpsc::Receiver<ServerMessage<R, W, P>>) -> Self {
        let name = format!("derp-{}", hex::encode(&key.as_ref()[..8]));
        Self {
            key,
            receiver,
            clients: Clients::new(),
            client_mesh: HashMap::default(),
            watchers: HashSet::default(),
            name,
        }
    }

    #[instrument(skip_all, fields(self.name = %self.name))]
    pub(crate) async fn run(mut self, done: CancellationToken) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = done.cancelled() => {
                    tracing::warn!("server actor loop cancelled, closing loop");
                    // TODO: stats: drain channel & count dropped packets etc
                    // close all client connections and client read/write loops
                    self.clients.shutdown().await;
                    return Ok(());
                }
                msg = self.receiver.recv() => {
                    let msg = match msg {
                        Some(m) => m,
                        None => {
                            tracing::warn!("server channel sender closed unexpectedly, shutting down server loop");
                            self.clients.shutdown().await;
                            anyhow::bail!("server channel sender closed unexpectedly, closed client connections, and shutting down server loop");
                        }
                    };
                   match msg {
                       ServerMessage::AddWatcher(key) => {
                           tracing::trace!("add watcher: {:?} (is_self: {})", key, key == self.key);
                           // connecting to ourselves, ignore
                           if key == self.key {
                               continue;
                           }
                           // list of all connected clients
                           let updates = self.clients.all_clients().map(|k| PeerConnState{ peer: k.clone(), present: true }).collect();
                           // send list of connected clients to the client
                           self.clients.send_mesh_updates(&key, updates);

                           // add to the list of watchers
                           self.watchers.insert(key.clone());
                       },
                       ServerMessage::ClosePeer(key) => {
                           tracing::trace!("close peer: {:?}", key);
                           // close the actual underlying connection to the client, but don't remove it from
                           // the list of clients
                           self.clients.close_conn(&key);
                       },
                        ServerMessage::SendPacket((key, packet)) => {
                           tracing::trace!("send disco packet from: {:?} to: {:?} ({}b)", packet.src, key, packet.bytes.len());
                            let src = packet.src.clone();
                            if self.clients.contains_key(&key) {
                                // if this client is in our local network, just try to send the
                                // packet
                                if self.clients.send_packet(&key, packet).is_ok() {
                                    self.clients.record_send(&src, key);
                                }
                            } else if let Some(Some(fwd)) = self.client_mesh.get_mut(&key) {
                                // if this client is in our mesh network & has a packet
                                // forwarder
                                fwd.forward_packet(packet.src, key, packet.bytes);
                            } else {
                                tracing::warn!("send packet: no way to reach client {key:?}, dropped packet");
                            }
                        }
                       ServerMessage::SendDiscoPacket((key, packet)) => {
                           tracing::trace!("send disco packet from: {:?} to: {:?} ({}b)", packet.src, key, packet.bytes.len());
                            let src = packet.src.clone();
                            if self.clients.contains_key(&key) {
                                // if this client is in our local network, just try to send the
                                // packet
                                if self.clients.send_disco_packet(&key, packet).is_ok() {
                                    self.clients.record_send(&src, key);
                                }
                            } else if let Some(Some(fwd)) = self.client_mesh.get_mut(&key) {
                                // if this client is in our mesh network & has a packet
                                // forwarder
                                fwd.forward_packet(packet.src, key, packet.bytes);
                            } else {
                                tracing::warn!("send disco packet: no way to reach client {key:?}, dropped packet");
                            }
                       }
                       ServerMessage::CreateClient(client_builder) => {
                           tracing::trace!("create client: {:?}", client_builder.key);
                           let key = client_builder.key.clone();
                           // add client to mesh
                            if !self.client_mesh.contains_key(&key) {
                                // `None` means its a local client (so it doesn't need a packet
                                // forwarder)
                                self.client_mesh.insert(key.clone(), None);
                            }
                            // build and register client, starting up read & write loops for the
                            // client connection
                            self.clients.register(client_builder);
                            // broadcast to watchers that a new peer has joined the network
                            self.broadcast_peer_state_change(key, true);

                        }
                       ServerMessage::RemoveClient(key) => {
                           tracing::trace!("remove client: {:?}", key);
                           // remove the client from the map of clients, & notify any peers that it
                           // has sent messages that it has left the network
                           self.clients.unregister(&key);
                           // remove from mesh
                           self.client_mesh.remove(&key);
                           // broadcast to watchers that this peer has left the network
                           self.broadcast_peer_state_change(key, false);
                       }
                       ServerMessage::AddPacketForwarder((key, packet_forwarder)) => {
                           tracing::trace!("add packet forwarder: {:?}", key);
                           // Only one packet forward allowed at a time right now
                           self.client_mesh.insert(key, Some(packet_forwarder));
                       },

                       ServerMessage::RemovePacketForwarder(key) => {
                           tracing::trace!("remove packet forwarder: {:?}", key);
                           // check if we have a local connection to the client at `key`
                           if self.clients.contains_key(&key) {
                               // remove any current packet forwarder associated with key
                               // and not that we have a local connection to the client at the
                               // given key
                               self.client_mesh.insert(key, None);
                           } else {
                               self.client_mesh.remove(&key);
                           }
                       },
                       ServerMessage::Shutdown => {
                        tracing::info!("server gracefully shutting down...");
                        // close all client connections and client read/write loops
                        self.clients.shutdown().await;
                        return Ok(());
                       }
                   }
                }
            }
        }
    }

    pub(crate) fn broadcast_peer_state_change(&mut self, peer: PublicKey, present: bool) {
        let keys = self.watchers.iter();
        self.clients
            .broadcast_peer_state_change(keys, vec![PeerConnState { peer, present }]);
    }
}

/// Initializes `the Server` with a self-signed x509 cert
/// encoding this server's public key and protocol version. "cmd/derp_server
/// then sends this after the Let's Encrypt leaf + intermediate certs after
/// the ServerHello (encrypted in TLS 1.3, not that is matters much).
///
/// Then the client can save a round trime getting that and can start speaking
/// DERP right away. (we don't use ALPN because that's sent in the clear and
/// we're being paranoid to not look too weird to any middleboxes, given that
/// DERP is an ultimate fallback path). But since the post-ServerHello certs
/// are encrypted we can have the client also use them as a signal to be able
/// to start speaking DERP right away, starting with its identity proof,
/// encrypted to the server's public key.
///
/// This RTT optimization fails where there's a corp-mandated TLS proxy with
/// corp-mandated root certs on employee machines and TLS proxy cleans up
/// unnecessary certs. In that case we jsut fall back to the extra RTT.
fn init_meta_cert(server_key: &PublicKey) -> Vec<u8> {
    let mut params =
        rcgen::CertificateParams::new([format!("derpkey{}", hex::encode(server_key.as_bytes()))]);
    params.serial_number = Some(PROTOCOL_VERSION as u64);
    // Windows requires not_after and not_before set:
    params.not_after = time::OffsetDateTime::now_utc()
        .checked_add(30 * time::Duration::DAY)
        .unwrap();
    params.not_before = time::OffsetDateTime::now_utc()
        .checked_sub(30 * time::Duration::DAY)
        .unwrap();

    rcgen::Certificate::from_params(params)
        .expect("fixed inputs")
        .serialize_der()
        .expect("fixed allocations")
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::hp::{
        derp::{
            client::ClientBuilder, client_conn::ClientConnBuilder, types::ClientInfo,
            ReceivedMessage, MAX_FRAME_SIZE,
        },
        key::node::PUBLIC_KEY_LENGTH,
    };

    use anyhow::Result;
    use bytes::{Bytes, BytesMut};
    use tokio::io::DuplexStream;

    struct MockPacketForwarder {
        packets: mpsc::Sender<(PublicKey, PublicKey, bytes::Bytes)>,
    }

    impl MockPacketForwarder {
        fn new() -> (mpsc::Receiver<(PublicKey, PublicKey, bytes::Bytes)>, Self) {
            let (send, recv) = mpsc::channel(10);
            (recv, Self { packets: send })
        }
    }

    impl PacketForwarder for MockPacketForwarder {
        fn forward_packet(&mut self, srckey: PublicKey, dstkey: PublicKey, packet: bytes::Bytes) {
            let _ = self.packets.try_send((srckey, dstkey, packet));
        }
    }

    fn test_client_builder(
        key: PublicKey,
        conn_num: usize,
        server_channel: mpsc::Sender<
            ServerMessage<DuplexStream, DuplexStream, MockPacketForwarder>,
        >,
    ) -> (
        ClientConnBuilder<DuplexStream, DuplexStream, MockPacketForwarder>,
        DuplexStream,
        DuplexStream,
    ) {
        let (test_reader, writer) = tokio::io::duplex(1024);
        let (reader, test_writer) = tokio::io::duplex(1024);
        (
            ClientConnBuilder {
                key,
                conn_num,
                reader,
                writer,
                can_mesh: true,
                write_timeout: None,
                channel_capacity: 10,
                server_channel,
            },
            test_reader,
            test_writer,
        )
    }

    #[tokio::test]
    async fn test_server_actor() -> Result<()> {
        let server_key = PublicKey::from([1u8; PUBLIC_KEY_LENGTH]);

        // make server actor
        let (server_channel, server_channel_r) = mpsc::channel(20);
        let server_actor: ServerActor<DuplexStream, DuplexStream, MockPacketForwarder> =
            ServerActor::new(server_key, server_channel_r);
        let done = CancellationToken::new();
        let server_done = done.clone();

        // run server actor
        let server_task = tokio::spawn(async move { server_actor.run(server_done).await });

        let key_a = PublicKey::from([3u8; PUBLIC_KEY_LENGTH]);
        let (client_a, mut a_reader, _a_writer) =
            test_client_builder(key_a.clone(), 1, server_channel.clone());

        // create client a
        server_channel
            .send(ServerMessage::CreateClient(client_a))
            .await?;

        // add a to watcher list
        server_channel
            .send(ServerMessage::AddWatcher(key_a.clone()))
            .await?;

        // a expects mesh peer update about itself, aka the only peer in the network currently
        let mut buf = BytesMut::new();
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut a_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerPresent);
        assert_eq!(key_a.as_bytes()[..], buf[..]);

        let key_b = PublicKey::from([9u8; PUBLIC_KEY_LENGTH]);

        // server message: create client b
        let (client_b, _b_reader, mut b_writer) =
            test_client_builder(key_b.clone(), 2, server_channel.clone());
        server_channel
            .send(ServerMessage::CreateClient(client_b))
            .await?;

        // expect mesh update message on client a about client b joining the network
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut a_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerPresent);
        assert_eq!(key_b.as_bytes()[..], buf[..]);

        // server message: create client c
        let key_c = PublicKey::from([10u8; PUBLIC_KEY_LENGTH]);
        let (client_c, mut c_reader, _c_writer) =
            test_client_builder(key_c.clone(), 3, server_channel.clone());
        server_channel
            .send(ServerMessage::CreateClient(client_c))
            .await?;

        // expect mesh update message on client_a about client_c joining the network
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut a_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerPresent);
        assert_eq!(key_c.as_bytes()[..], buf[..]);

        // server message: add client c as watcher
        server_channel
            .send(ServerMessage::AddWatcher(key_c.clone()))
            .await?;

        // expect mesh update message on client c about all peers in the network (a, b, & c)
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut c_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerPresent);
        let mut peers = vec![buf[..PUBLIC_KEY_LENGTH].to_vec()];
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut c_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerPresent);
        peers.push(buf[..PUBLIC_KEY_LENGTH].to_vec());
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut c_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerPresent);
        peers.push(buf[..PUBLIC_KEY_LENGTH].to_vec());
        assert!(peers.contains(&key_a.as_bytes().to_vec()));
        assert!(peers.contains(&key_b.as_bytes().to_vec()));
        assert!(peers.contains(&key_c.as_bytes().to_vec()));

        // add packet forwarder for client d
        let key_d = PublicKey::from([11u8; PUBLIC_KEY_LENGTH]);
        let (packet_s, mut packet_r) = mpsc::channel(10);
        let fwd_d = MockPacketForwarder { packets: packet_s };
        server_channel
            .send(ServerMessage::AddPacketForwarder((key_d.clone(), fwd_d)))
            .await?;

        // write message from b to a
        let msg = b"hello world!";
        crate::hp::derp::client::send_packets(&mut b_writer, &None, key_a.clone(), &[msg]).await?;

        // get message on a's reader
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut a_reader, MAX_FRAME_SIZE, &mut buf).await?;
        let (key, frame) = crate::hp::derp::client::parse_recv_frame(buf.clone())?;
        assert_eq!(FrameType::RecvPacket, frame_type);
        assert_eq!(key_b, key);
        assert_eq!(msg, &frame[..]);

        // write disco message from b to d
        let mut disco_msg = crate::hp::disco::MAGIC.as_bytes().to_vec();
        disco_msg.extend_from_slice(key_b.as_bytes());
        disco_msg.extend_from_slice(msg);
        crate::hp::derp::client::send_packets(&mut b_writer, &None, key_d.clone(), &[&disco_msg])
            .await?;

        // get message on d's reader
        let (got_src, got_dst, got_packet) = packet_r.recv().await.unwrap();
        assert_eq!(got_src, key_b);
        assert_eq!(got_dst, key_d);
        assert_eq!(disco_msg, got_packet.to_vec());

        // remove b
        server_channel
            .send(ServerMessage::RemoveClient(key_b.clone()))
            .await?;

        // get peer gone message on a about b leaving the network
        // (we get this message because b has sent us a packet before)
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut a_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerGone);
        assert_eq!(&key_b.as_bytes()[..], &buf[..]);

        // get mesh update on a & c about b leaving the network
        // (we get this message on a & c because they are "watchers")
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut a_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerGone);
        assert_eq!(&key_b.as_bytes()[..], &buf[..]);

        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut c_reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(frame_type, FrameType::PeerGone);
        assert_eq!(&key_b.as_bytes()[..], &buf[..]);

        // close gracefully
        server_channel.send(ServerMessage::Shutdown).await?;
        server_task.await??;
        Ok(())
    }

    #[tokio::test]
    async fn test_client_conn_handler() -> Result<()> {
        // create client connection handler
        let (server_channel_s, mut server_channel_r) = mpsc::channel(10);
        let handler = ClientConnHandler::<DuplexStream, DuplexStream, MockPacketForwarder> {
            mesh_key: Some([1u8; 32]),
            secret_key: SecretKey::generate(),
            write_timeout: None,
            server_info: ServerInfo::no_rate_limit(),
            server_channel: server_channel_s,
            default_headers: Default::default(),
        };

        // create the parts needed for a client
        let (mut client_reader, server_writer) = tokio::io::duplex(10);
        let (server_reader, mut client_writer) = tokio::io::duplex(10);
        let client_key = SecretKey::generate();

        // start a task as if a client is doing the "accept" handshake
        let pub_client_key = client_key.public_key();
        let expect_server_key = handler.secret_key.public_key();
        let client_task: JoinHandle<Result<()>> = tokio::spawn(async move {
            // get the server key
            let got_server_key =
                crate::hp::derp::client::recv_server_key(&mut client_reader).await?;
            assert_eq!(expect_server_key, got_server_key);

            // send the client info
            let client_info = ClientInfo {
                version: PROTOCOL_VERSION,
                mesh_key: Some([1u8; 32]),
                can_ack_pings: true,
                is_prober: true,
            };
            crate::hp::derp::send_client_key(
                &mut client_writer,
                &client_key,
                &got_server_key,
                &client_info,
            )
            .await?;

            // get the server info
            let mut buf = BytesMut::new();
            let (frame_type, _) =
                crate::hp::derp::read_frame(&mut client_reader, MAX_FRAME_SIZE, &mut buf).await?;
            assert_eq!(FrameType::ServerInfo, frame_type);
            let msg = client_key.open_from(&got_server_key, &buf)?;
            let _info: ServerInfo = postcard::from_bytes(&msg)?;
            Ok(())
        });

        // attempt to add the connection to the server
        handler.accept(server_reader, server_writer).await?;
        client_task.await??;

        // ensure we inform the server to create the client from the connection!
        match server_channel_r.recv().await.unwrap() {
            ServerMessage::CreateClient(builder) => {
                assert_eq!(pub_client_key, builder.key);
            }
            _ => anyhow::bail!("unexpected server message"),
        }
        Ok(())
    }

    fn make_test_client(
        secret_key: SecretKey,
    ) -> (
        tokio::io::DuplexStream,
        tokio::io::DuplexStream,
        ClientBuilder<tokio::io::DuplexStream, tokio::io::DuplexStream>,
    ) {
        let (client_reader, server_writer) = tokio::io::duplex(10);
        let (server_reader, client_writer) = tokio::io::duplex(10);
        (
            server_reader,
            server_writer,
            ClientBuilder::new(
                secret_key,
                "127.0.0.1:0".parse().unwrap(),
                client_reader,
                client_writer,
            ),
        )
    }

    #[tokio::test]
    async fn test_server_basic() -> Result<()> {
        // create the server!
        let server_key = SecretKey::generate();
        let mesh_key = Some([1u8; 32]);
        let server: Server<DuplexStream, DuplexStream, MockPacketForwarder> =
            Server::new(server_key, mesh_key);

        // create client a and connect it to the server
        let key_a = SecretKey::generate();
        let public_key_a = key_a.public_key();
        let (reader_a, writer_a, client_a_builder) = make_test_client(key_a);
        let handler = server.client_conn_handler(Default::default());
        let handler_task = tokio::spawn(async move { handler.accept(reader_a, writer_a).await });
        let client_a = client_a_builder.build(None).await?;
        handler_task.await??;

        // create client b and connect it to the server
        let key_b = SecretKey::generate();
        let public_key_b = key_b.public_key();
        let (reader_b, writer_b, client_b_builder) = make_test_client(key_b);
        let handler = server.client_conn_handler(Default::default());
        let handler_task = tokio::spawn(async move { handler.accept(reader_b, writer_b).await });
        let client_b = client_b_builder.build(None).await?;
        handler_task.await??;

        // create a packet forwarder for client c and add it to the server
        let key_c = PublicKey::from([1u8; 32]);
        let (mut fwd_recv, packet_fwd) = MockPacketForwarder::new();
        let handler = server.packet_forwarder_handler();
        handler.add_packet_forwarder(key_c.clone(), packet_fwd)?;

        // send message from a to b!
        let msg = Bytes::from_static(b"hello client b!!");
        client_a
            .send(public_key_b.clone(), vec![msg.clone()])
            .await?;
        match client_b.recv().await? {
            ReceivedMessage::ReceivedPacket { source, data } => {
                assert_eq!(public_key_a, source);
                assert_eq!(&msg[..], data);
            }
            msg => {
                anyhow::bail!("expected ReceivedPacket msg, got {msg:?}");
            }
        }

        // send message from b to a!
        let msg = Bytes::from_static(b"nice to meet you client a!!");
        client_b
            .send(public_key_a.clone(), vec![msg.clone()])
            .await?;
        match client_a.recv().await? {
            ReceivedMessage::ReceivedPacket { source, data } => {
                assert_eq!(public_key_b, source);
                assert_eq!(&msg[..], data);
            }
            msg => {
                anyhow::bail!("expected ReceivedPacket msg, got {msg:?}");
            }
        }

        // send message from a to c
        let msg = Bytes::from_static(b"can you pass this to client d?");
        client_a.send(key_c.clone(), vec![msg.clone()]).await?;
        let (got_src, got_dst, got_packet) = fwd_recv.recv().await.unwrap();
        assert_eq!(public_key_a, got_src);
        assert_eq!(key_c, got_dst);
        assert_eq!(&msg[..], &got_packet);

        // remove the packet forwarder for c
        handler.remove_packet_forwarder(key_c.clone())?;
        // try to send c a message
        let msg = Bytes::from_static(b"can you pass this to client d?");
        client_a.send(key_c, vec![msg.clone()]).await?;
        // packet forwarder has been removed
        assert!(fwd_recv.recv().await.is_none());

        // close the server and clients
        server.close().await;

        // client connections have been shutdown
        client_a
            .send(public_key_b, vec![Bytes::from_static(b"try to send")])
            .await?;
        assert!(client_b.recv().await.is_err());
        Ok(())
    }
}
