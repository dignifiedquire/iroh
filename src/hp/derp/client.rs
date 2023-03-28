//! based on tailscale/derp/derp_client.go
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, ensure, Context, Result};
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

use super::PER_CLIENT_SEND_QUEUE_DEPTH;
use super::{
    read_frame,
    types::{ClientInfo, RateLimiter, ServerInfo},
    write_frame, FrameType, FRAME_CLOSE_PEER, FRAME_FORWARD_PACKET, FRAME_HEALTH, FRAME_KEEP_ALIVE,
    FRAME_NOTE_PREFERRED, FRAME_PEER_GONE, FRAME_PEER_PRESENT, FRAME_PING, FRAME_PONG,
    FRAME_RECV_PACKET, FRAME_RESTARTING, FRAME_SEND_PACKET, FRAME_SERVER_INFO, FRAME_SERVER_KEY,
    FRAME_WATCH_CONNS, MAGIC, MAX_FRAME_SIZE, MAX_PACKET_SIZE, NOT_PREFERRED, PREFERRED,
    PROTOCOL_VERSION,
};

use crate::hp::key::node::{PublicKey, SecretKey, PUBLIC_KEY_LENGTH};

impl<R: AsyncRead + Unpin> PartialEq for Client<R> {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<R: AsyncRead + Unpin> Eq for Client<R> {}

/// A DERP Client.
/// Cheaply clonable.
/// Call `close` to shutdown the write loop and read functionality.
#[derive(Debug)]
pub struct Client<R>
where
    R: AsyncRead + Unpin,
{
    inner: Arc<InnerClient<R>>,
}

impl<R: AsyncRead + Unpin> Clone for Client<R> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

#[derive(Debug)]
pub struct InnerClient<R>
where
    R: AsyncRead + Unpin,
{
    /// Server key of the DERP server, not a machine or node key
    server_key: PublicKey,
    /// The public/private keypair
    secret_key: SecretKey,
    // our local addrs
    local_addr: SocketAddr,
    /// TODO: This is a string in the go impl, need to make these back into Strings
    mesh_key: Option<[u8; 32]>,
    can_ack_pings: bool,
    is_prober: bool,

    /// Channel on which to communicate to the server. The associated [`mpsc::Receiver`] will close
    /// if there is ever an error writing to the server.
    writer_channel: mpsc::Sender<ClientWriterMessage>,
    /// JoinHandle for the [`ClientWriter`] task
    writer_task: Mutex<Option<JoinHandle<Result<()>>>>,
    /// The reader connected to the server
    reader: Mutex<R>,
}

/// A channel on which we recieve messages from the server
type ReceivedMessages = mpsc::Receiver<ReceivedMessage>;

// TODO: I believe that any of these that error should actually trigger a shut down of the client
impl<R: AsyncRead + Unpin> Client<R> {
    /// Returns a reference to the server's public key.
    pub fn server_public_key(&self) -> PublicKey {
        self.inner.server_key.clone()
    }

    /// Sends a packet to the node identified by `dstkey`
    ///
    /// Errors if the packet is larger than [`MAX_PACKET_SIZE`]
    // TODO: the rate limiter is only on this method, is it because it's the only method that
    // theoretically sends a bunch of data, or is it an oversight? For example, the `forward_packet` method does not have a rate limiter, but _does_ have a timeout.
    pub async fn send(&self, dstkey: PublicKey, packet: Vec<u8>) -> Result<()> {
        let mut buf = BytesMut::new();
        self.inner
            .writer_channel
            .send(ClientWriterMessage::Packet((dstkey, packet)))
            .await?;
        Ok(())
    }

    /// Used by mesh peers to forward packets.
    ///
    // TODO: this is the only method with a timeout, why? Why does it have a timeout and no rate
    // limiter?
    pub async fn forward_packet(
        &self,
        srckey: PublicKey,
        dstkey: PublicKey,
        packet: Bytes,
    ) -> Result<()> {
        self.inner
            .writer_channel
            .send(ClientWriterMessage::FwdPacket((srckey, dstkey, packet)))
            .await?;
        Ok(())
    }

    pub async fn send_ping(&self, data: [u8; 8]) -> Result<()> {
        self.inner
            .writer_channel
            .send(ClientWriterMessage::Ping(data))
            .await?;
        Ok(())
    }

    pub async fn send_pong(&self, data: [u8; 8]) -> Result<()> {
        self.inner
            .writer_channel
            .send(ClientWriterMessage::Pong(data))
            .await?;
        Ok(())
    }

    /// Sends a packet that tells the server whether this
    /// client is the user's preferred server. This is only
    /// used in the server for stats.
    pub async fn note_preferred(&self, preferred: bool) -> Result<()> {
        self.inner
            .writer_channel
            .send(ClientWriterMessage::NotePreferred(preferred))
            .await?;
        Ok(())
    }

    /// Sends a request to subscribe to the peer's connection list.
    /// It's a fatal error if the client wasn't created using [`MeshKey`].
    pub async fn watch_connection_changes(&self) -> Result<()> {
        self.inner
            .writer_channel
            .send(ClientWriterMessage::WatchConnectionChanges)
            .await?;
        Ok(())
    }

    /// Asks the server to close the target's TCP connection.
    /// It's a fatal error if the client wasn't created using [`MeshKey`]
    pub async fn close_peer(&self, target: PublicKey) -> Result<()> {
        self.inner
            .writer_channel
            .send(ClientWriterMessage::ClosePeer(target))
            .await?;
        Ok(())
    }

    pub async fn local_addr(&self) -> Result<SocketAddr> {
        Ok(self.inner.local_addr)
    }

    pub async fn is_closed(&self) -> bool {
        self.inner.writer_task.lock().await.is_none()
    }

    /// Reads a messages from a DERP server.
    ///
    /// The returned message may alias memory owned by the [`Client`]; if
    /// should only be accessed until the next call to [`Client`].
    ///
    /// Once [`recv`] returns an error, the [`Client`] is dead forever.
    pub async fn recv(&self) -> Result<ReceivedMessage> {
        if self.is_closed().await {
            bail!(ClientError::Closed);
        }

        // in practice, quic packets (and thus DERP frames) are under 1.5 KiB
        let mut frame_payload = BytesMut::with_capacity(1024 + 512);
        loop {
            let mut reader = self.inner.reader.lock().await;
            let (frame_type, frame_len) =
                match read_frame(&mut *reader, MAX_FRAME_SIZE, &mut frame_payload).await {
                    Ok((t, l)) => (t, l),
                    Err(e) => {
                        self.close().await;
                        bail!(e);
                    }
                };

            match frame_type {
                FRAME_KEEP_ALIVE => {
                    // A one-way keep-alive message that doesn't require an ack.
                    // This predated FRAME_PING/FRAME_PONG.
                    return Ok(ReceivedMessage::KeepAlive);
                }
                FRAME_PEER_GONE => {
                    if (frame_len) < PUBLIC_KEY_LENGTH {
                        tracing::warn!(
                            "unexpected: dropping short PEER_GONE frame from DERP server"
                        );
                        continue;
                    }
                    return Ok(ReceivedMessage::PeerGone(PublicKey::try_from(
                        &frame_payload[..PUBLIC_KEY_LENGTH],
                    )?));
                }
                FRAME_PEER_PRESENT => {
                    if (frame_len) < PUBLIC_KEY_LENGTH {
                        tracing::warn!(
                            "unexpected: dropping short PEER_PRESENT frame from DERP server"
                        );
                        continue;
                    }
                    return Ok(ReceivedMessage::PeerPresent(PublicKey::try_from(
                        &frame_payload[..PUBLIC_KEY_LENGTH],
                    )?));
                }
                FRAME_RECV_PACKET => {
                    if (frame_len) < PUBLIC_KEY_LENGTH {
                        tracing::warn!("unexpected: dropping short packet from DERP server");
                        continue;
                    }
                    let (source, data) = parse_recv_frame(&frame_payload)?;
                    let packet = ReceivedMessage::ReceivedPacket {
                        source,
                        data: data.to_vec(),
                    };
                    return Ok(packet);
                }
                FRAME_PING => {
                    if frame_len < 8 {
                        tracing::warn!("unexpected: dropping short PING frame");
                        continue;
                    }
                    let ping = <[u8; 8]>::try_from(&frame_payload[..8])?;
                    return Ok(ReceivedMessage::Ping(ping));
                }
                FRAME_PONG => {
                    if frame_len < 8 {
                        tracing::warn!("unexpected: dropping short PONG frame");
                        continue;
                    }
                    let pong = <[u8; 8]>::try_from(&frame_payload[..8])?;
                    return Ok(ReceivedMessage::Pong(pong));
                }
                FRAME_HEALTH => {
                    let problem = Some(String::from_utf8_lossy(&frame_payload).into());
                    return Ok(ReceivedMessage::Health { problem });
                }
                FRAME_RESTARTING => {
                    if frame_len < 8 {
                        tracing::warn!("unexpected: dropping short server restarting frame");
                        continue;
                    }
                    let reconnect_in = <[u8; 4]>::try_from(&frame_payload[..4])?;
                    let try_for = <[u8; 4]>::try_from(&frame_payload[4..8])?;
                    let reconnect_in =
                        Duration::from_millis(u32::from_be_bytes(reconnect_in) as u64);
                    let try_for = Duration::from_millis(u32::from_be_bytes(try_for) as u64);
                    return Ok(ReceivedMessage::ServerRestarting {
                        reconnect_in,
                        try_for,
                    });
                }
                _ => {
                    frame_payload.clear();
                }
            }
        }
    }

    /// Close the client
    ///
    /// Shuts down the write loop directly and marks the client as closed. The `ClientReader` will
    /// check if the client is closed before attempting to read from it.
    pub async fn close(&self) {
        let mut writer_task = self.inner.writer_task.lock().await;
        let task = writer_task.take();
        match task {
            None => {}
            Some(task) => {
                // only error would be that the writer_channel receiver is closed
                let _ = self
                    .inner
                    .writer_channel
                    .send(ClientWriterMessage::Shutdown)
                    .await;
                match task.await {
                    Ok(Err(e)) => {
                        tracing::warn!("error closing down the client: {e:?}");
                    }
                    Err(e) => {
                        tracing::warn!("error closing down the client: {e:?}");
                    }
                    _ => {}
                }
            }
        }
    }
}

/// The kinds of messages we can send to the Server
#[derive(Debug)]
enum ClientWriterMessage {
    /// Send a packet (addressed to the [`PublicKey`]) to the server
    Packet((PublicKey, Vec<u8>)),
    /// Forward a packet from the src [`PublicKey`] to the dst [`PublicKey`] to the server
    /// Should only be used for mesh clients.
    FwdPacket((PublicKey, PublicKey, Bytes)),
    /// Send a pong to the server
    Pong([u8; 8]),
    /// Send a ping to the server
    Ping([u8; 8]),
    /// Tell the server whether or not this client is the user's preferred client
    NotePreferred(bool),
    /// Subscribe to the server's connection list.
    /// Should only be used for mesh clients.
    WatchConnectionChanges,
    /// Asks the server to close the target's connection.
    /// Should only be used for mesh clients.
    ClosePeer(PublicKey),
    /// Shutdown the writer
    Shutdown,
}

/// Call [`ClientWriter::run`] to listen for messages to send to the client.
/// Should be used by the [`Client`]
///
/// Shutsdown when you send a `ClientWriterMessage::Shutdown`, or if there is an error writing to
/// the server.
struct ClientWriter<W: AsyncWrite + Unpin + Send + 'static> {
    recv_msgs: mpsc::Receiver<ClientWriterMessage>,
    writer: W,
    rate_limiter: Option<RateLimiter>,
}

impl<W: AsyncWrite + Unpin + Send + 'static> ClientWriter<W> {
    async fn run(mut self) -> Result<()> {
        loop {
            match self.recv_msgs.recv().await {
                None => {
                    bail!("channel unexpectedly closed");
                }
                Some(ClientWriterMessage::Packet((key, bytes))) => {
                    send_packet(&mut self.writer, &self.rate_limiter, key, &bytes).await?;
                }
                Some(ClientWriterMessage::FwdPacket((srckey, dstkey, bytes))) => {
                    tokio::time::timeout(
                        Duration::from_secs(5),
                        forward_packet(&mut self.writer, srckey, dstkey, &bytes),
                    )
                    .await??;
                }
                Some(ClientWriterMessage::Pong(msg)) => {
                    send_pong(&mut self.writer, &msg).await?;
                }
                Some(ClientWriterMessage::Ping(msg)) => {
                    send_ping(&mut self.writer, &msg).await?;
                }
                Some(ClientWriterMessage::NotePreferred(preferred)) => {
                    send_note_preferred(&mut self.writer, preferred).await?;
                }
                Some(ClientWriterMessage::WatchConnectionChanges) => {
                    watch_connection_changes(&mut self.writer).await?;
                }
                Some(ClientWriterMessage::ClosePeer(target)) => {
                    close_peer(&mut self.writer, target).await?;
                }
                Some(ClientWriterMessage::Shutdown) => {
                    return Ok(());
                }
            }
        }
    }
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ClientError {
    #[error("todo")]
    Todo,
    #[error("closed")]
    Closed,
    // Read error?
    // BadKey error?
}

/// builder returns a client & and a client reader, runs a client writer loop

pub struct ClientBuilder<W, R>
where
    W: AsyncWrite + Send + Unpin + 'static,
    R: AsyncRead + Unpin,
{
    secret_key: SecretKey,
    reader: R,
    writer: W,
    local_addr: SocketAddr,
    mesh_key: Option<[u8; 32]>,
    is_prober: bool,
    server_public_key: Option<PublicKey>,
    can_ack_pings: bool,
    writer_queue_depth: usize,
}

impl<W, R> ClientBuilder<W, R>
where
    W: AsyncWrite + Send + Unpin + 'static,
    R: AsyncRead + Send + Unpin + 'static,
{
    pub fn new(secret_key: SecretKey, local_addr: SocketAddr, reader: R, writer: W) -> Self {
        Self {
            secret_key,
            reader,
            writer,
            local_addr,
            mesh_key: None,
            is_prober: false,
            server_public_key: None,
            can_ack_pings: false,
            writer_queue_depth: PER_CLIENT_SEND_QUEUE_DEPTH,
        }
    }

    pub fn mesh_key(mut self, mesh_key: [u8; 32]) -> Self {
        self.mesh_key = Some(mesh_key);
        self
    }

    pub fn is_prober(mut self) -> Self {
        self.is_prober = true;
        self
    }

    // Set the expected server_public_key. If this is not what is sent by the server, it is an
    // error.
    pub fn server_public_key(mut self, key: PublicKey) -> Self {
        self.server_public_key = Some(key);
        self
    }

    pub fn can_ack_pings(mut self) -> Self {
        self.can_ack_pings = true;
        self
    }

    /// Default is [`derp::PER_CLIENT_SEND_QUEUE_DEPTH`]
    ///
    /// Will error if the queue depth is set to zero
    pub fn writer_queue_depth(mut self, depth: usize) -> Result<Self> {
        if depth == 0 {
            bail!("cannot set queue to 0, no unbounded channels allowed");
        }
        self.writer_queue_depth = depth;
        Ok(self)
    }

    async fn server_handshake(&mut self) -> Result<(PublicKey, Option<RateLimiter>)> {
        let server_key = recv_server_key(&mut self.reader)
            .await
            .context("failed to receive server key")?;
        if let Some(expected_key) = &self.server_public_key {
            if *expected_key != server_key {
                bail!("unexpected server key, expected {expected_key:?} got {server_key:?}");
            }
        }
        let client_info = ClientInfo {
            version: PROTOCOL_VERSION,
            mesh_key: self.mesh_key,
            can_ack_pings: self.can_ack_pings,
            is_prober: self.is_prober,
        };
        crate::hp::derp::send_client_key(
            &mut self.writer,
            &self.secret_key,
            &server_key,
            &client_info,
        )
        .await?;
        let mut buf = BytesMut::new();
        let (frame_type, _) =
            crate::hp::derp::read_frame(&mut self.reader, MAX_FRAME_SIZE, &mut buf).await?;
        assert_eq!(FRAME_SERVER_INFO, frame_type);
        let msg = self.secret_key.open_from(&server_key, &buf)?;
        let info: ServerInfo = postcard::from_bytes(&msg)?;
        if info.version != PROTOCOL_VERSION {
            bail!(
                "incompatiable protocol version, expected {PROTOCOL_VERSION}, got {}",
                info.version
            );
        }
        let rate_limiter = RateLimiter::new(
            info.token_bucket_bytes_per_second,
            info.token_bucket_bytes_burst,
        )?;
        Ok((server_key, rate_limiter))
    }

    pub async fn build(mut self) -> Result<Client<R>>
    where
        R: AsyncRead + Unpin,
    {
        // exchange information with the server
        let (server_key, rate_limiter) = self.server_handshake().await?;

        // create task to handle writing to the server
        let (writer_sender, writer_recv) = mpsc::channel(self.writer_queue_depth);
        let writer_task = tokio::spawn(async move {
            let client_writer = ClientWriter {
                rate_limiter,
                writer: self.writer,
                recv_msgs: writer_recv,
            };
            client_writer.run().await?;
            Ok(())
        });

        let client = Client {
            inner: Arc::new(InnerClient {
                server_key,
                secret_key: self.secret_key,
                local_addr: self.local_addr,
                mesh_key: self.mesh_key,
                can_ack_pings: self.can_ack_pings,
                is_prober: self.is_prober,
                writer_channel: writer_sender,
                writer_task: Mutex::new(Some(writer_task)),
                reader: Mutex::new(self.reader),
            }),
        };

        Ok(client)
    }
}

pub(crate) async fn recv_server_key<R: AsyncRead + Unpin>(mut reader: R) -> Result<PublicKey> {
    // expecting MAGIC followed by 32 bytes that contain the server key
    let magic_len = MAGIC.len();
    let expected_frame_len = magic_len + 32;
    let mut buf = BytesMut::with_capacity(expected_frame_len);

    let (frame_type, frame_len) = read_frame(&mut reader, MAX_FRAME_SIZE, &mut buf).await?;

    if expected_frame_len != frame_len
        || frame_type != FRAME_SERVER_KEY
        || buf[..magic_len] != *MAGIC.as_bytes()
    {
        bail!("invalid server greeting");
    }

    Ok(get_key_from_slice(&buf[magic_len..expected_frame_len])?)
}

// errors if `frame_len` is less than the expected key size
fn get_key_from_slice(payload: &[u8]) -> Result<PublicKey> {
    Ok(<[u8; PUBLIC_KEY_LENGTH]>::try_from(payload)?.into())
}

#[derive(Debug, Clone)]
pub enum ReceivedMessage {
    /// Represents an incoming packet.
    ReceivedPacket {
        source: PublicKey,
        /// The received packet bytes. It aliases the memory passed to Client.Recv.
        data: Vec<u8>, // TODO: ref
    },
    /// Indicates that the client identified by the underlying public key had previously sent you a
    /// packet but has now disconnected from the server.
    PeerGone(PublicKey),
    /// Indicates that the client is connected to the server. (Only used by trusted mesh clients)
    PeerPresent(PublicKey),
    /// Sent by the server upon first connect.
    ServerInfo {
        /// How many bytes per second the server says it will accept, including all framing bytes.
        ///
        /// Zero means unspecified. There might be a limit, but the client need not try to respect it.
        token_bucket_bytes_per_second: usize,
        /// TokenBucketBytesBurst is how many bytes the server will
        /// allow to burst, temporarily violating
        /// TokenBucketBytesPerSecond.
        ///
        /// Zero means unspecified. There might be a limit, but the client need not try to respect it.
        token_bucket_bytes_burst: usize,
    },
    /// Request from a client or server to reply to the
    /// other side with a PongMessage with the given payload.
    Ping([u8; 8]),
    /// Reply to a Ping from a client or server
    /// with the payload sent previously in a Ping.
    Pong([u8; 8]),
    /// A one-way empty message from server to client, just to
    /// keep the connection alive. It's like a Ping, but doesn't solicit
    /// a reply from the client.
    KeepAlive,
    /// A one-way message from server to client, declaring the connection health state.
    Health {
        /// If set, is a description of why the connection is unhealthy.
        ///
        /// If `None` means the connection is healthy again.
        ///
        /// The default condition is healthy, so the server doesn't broadcast a HealthMessage
        /// until a problem exists.
        problem: Option<String>,
    },
    /// A one-way message from server to client, advertising that the server is restarting.
    ServerRestarting {
        /// An advisory duration that the client should wait before attempting to reconnect.
        /// It might be zero. It exists for the server to smear out the reconnects.
        reconnect_in: Duration,
        /// An advisory duration for how long the client should attempt to reconnect
        /// before giving up and proceeding with its normal connection failure logic. The interval
        /// between retries is undefined for now. A server should not send a TryFor duration more
        /// than a few seconds.
        try_for: Duration,
    },
}

pub(crate) async fn send_packet<W: AsyncWrite + Unpin>(
    mut writer: W,
    rate_limiter: &Option<RateLimiter>,
    dstkey: PublicKey,
    packet: &[u8],
) -> Result<()> {
    ensure!(
        packet.len() <= MAX_PACKET_SIZE,
        "packet too big: {}",
        packet.len()
    );
    let frame_len = PUBLIC_KEY_LENGTH + packet.len();
    if let Some(rate_limiter) = rate_limiter {
        if rate_limiter.check_n(frame_len).is_err() {
            tracing::warn!("dropping send: rate limit reached");
            return Ok(());
        }
    }
    write_frame(&mut writer, FRAME_SEND_PACKET, &[dstkey.as_bytes(), packet]).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn forward_packet<W: AsyncWrite + Unpin>(
    mut writer: W,
    srckey: PublicKey,
    dstkey: PublicKey,
    packet: &[u8],
) -> Result<()> {
    ensure!(
        packet.len() <= MAX_PACKET_SIZE,
        "packet too big: {}",
        packet.len()
    );

    write_frame(
        &mut writer,
        FRAME_FORWARD_PACKET,
        &[srckey.as_bytes(), dstkey.as_bytes(), packet],
    )
    .await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn send_ping<W: AsyncWrite + Unpin>(mut writer: W, data: &[u8; 8]) -> Result<()> {
    send_ping_or_pong(&mut writer, FRAME_PING, data).await
}

async fn send_pong<W: AsyncWrite + Unpin>(mut writer: W, data: &[u8; 8]) -> Result<()> {
    send_ping_or_pong(&mut writer, FRAME_PONG, data).await
}

async fn send_ping_or_pong<W: AsyncWrite + Unpin>(
    mut writer: W,
    frame_type: FrameType,
    data: &[u8; 8],
) -> Result<()> {
    write_frame(&mut writer, frame_type, &[data]).await?;
    writer.flush().await?;
    Ok(())
}

pub async fn send_note_preferred<W: AsyncWrite + Unpin>(
    mut writer: W,
    preferred: bool,
) -> Result<()> {
    let byte = {
        if preferred {
            [PREFERRED]
        } else {
            [NOT_PREFERRED]
        }
    };
    write_frame(&mut writer, FRAME_NOTE_PREFERRED, &[&byte]).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn watch_connection_changes<W: AsyncWrite + Unpin>(mut writer: W) -> Result<()> {
    write_frame(&mut writer, FRAME_WATCH_CONNS, &[]).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn close_peer<W: AsyncWrite + Unpin>(
    mut writer: W,
    target: PublicKey,
) -> Result<()> {
    write_frame(&mut writer, FRAME_CLOSE_PEER, &[target.as_bytes()]).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) fn parse_recv_frame(frame: &[u8]) -> Result<(PublicKey, &[u8])> {
    ensure!(
        frame.len() >= PUBLIC_KEY_LENGTH,
        "frame is shorter than expected"
    );
    Ok((
        PublicKey::try_from(&frame[..PUBLIC_KEY_LENGTH])?,
        &frame[PUBLIC_KEY_LENGTH..],
    ))
}
