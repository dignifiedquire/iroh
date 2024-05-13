use anyhow::ensure;
use futures::TryFutureExt;
use futures_concurrency::future::TryJoin;
use iroh_base::{hash::Hash, key::NodeId};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    task::JoinSet,
};
use tracing::{debug, error_span, instrument, trace, warn, Instrument};

use crate::{
    actor::WillowHandle,
    proto::wgps::{
        AccessChallenge, ChallengeHash, LogicalChannel, Message, CHALLENGE_HASH_LENGTH,
        MAX_PAYLOAD_SIZE_POWER,
    },
    session::{
        channels::{Channels, LogicalChannelReceivers, LogicalChannelSenders},
        Role, SessionInit,
    },
    util::channel::{
        inbound_channel, outbound_channel, Guarantees, Reader, Receiver, Sender, Writer,
    },
};

pub const CHANNEL_CAP: usize = 1024 * 64;

#[instrument(skip_all, name = "willow_net", fields(me=%me.fmt_short(), peer=%peer.fmt_short()))]
pub async fn run(
    me: NodeId,
    store: WillowHandle,
    conn: quinn::Connection,
    peer: NodeId,
    our_role: Role,
    init: SessionInit,
) -> anyhow::Result<()> {
    debug!(?our_role, "connected");
    let mut join_set = JoinSet::new();

    let (mut control_send_stream, mut control_recv_stream) = match our_role {
        Role::Alfie => conn.open_bi().await?,
        Role::Betty => conn.accept_bi().await?,
    };
    control_send_stream.set_priority(i32::MAX)?;

    let initial_transmission =
        exchange_commitments(&mut control_send_stream, &mut control_recv_stream).await?;
    debug!("commitments exchanged");

    let (control_send, control_recv) = spawn_channel(
        &mut join_set,
        LogicalChannel::Control,
        CHANNEL_CAP,
        CHANNEL_CAP,
        Guarantees::Unlimited,
        control_send_stream,
        control_recv_stream,
    );

    let (logical_send, logical_recv) = open_logical_channels(&mut join_set, conn, our_role).await?;
    debug!("channels opened");
    let channels = Channels {
        control_send,
        control_recv,
        logical_send,
        logical_recv,
    };
    let handle = store
        .init_session(peer, our_role, initial_transmission, channels, init)
        .await?;

    join_set.spawn(async move {
        handle.on_finish().await?;
        Ok(())
    });

    join_all(join_set).await?;
    debug!("all tasks finished");
    Ok(())
}

#[derive(Debug, thiserror::Error)]
#[error("missing channel: {0:?}")]
struct MissingChannel(LogicalChannel);

async fn open_logical_channels(
    join_set: &mut JoinSet<anyhow::Result<()>>,
    conn: quinn::Connection,
    our_role: Role,
) -> anyhow::Result<(LogicalChannelSenders, LogicalChannelReceivers)> {
    let cap = CHANNEL_CAP;
    let channels = [LogicalChannel::Reconciliation, LogicalChannel::StaticToken];
    let mut channels = match our_role {
        // Alfie opens a quic stream for each logical channel, and sends a single byte with the
        // channel id.
        Role::Alfie => {
            channels
                .map(|ch| {
                    let conn = conn.clone();
                    async move {
                        let ch_id = ch as u8;
                        let (mut send, recv) = conn.open_bi().await?;
                        send.write_u8(ch_id).await?;
                        Result::<_, anyhow::Error>::Ok((ch, Some((send, recv))))
                    }
                })
                .try_join()
                .await
        }
        // Alfie accepts as many quick streams as there are logical channels, and reads a single
        // byte on each, which is expected to contain a channel id.
        Role::Betty => {
            channels
                .map(|_| async {
                    let (send, mut recv) = conn.accept_bi().await?;
                    let channel_id = recv.read_u8().await?;
                    let channel = LogicalChannel::try_from(channel_id)?;
                    Result::<_, anyhow::Error>::Ok((channel, Some((send, recv))))
                })
                .try_join()
                .await
        }
    }?;

    let mut take_and_spawn_channel = |ch| {
        channels
            .iter_mut()
            .find(|(c, _)| *c == ch)
            .map(|(_, streams)| streams.take())
            .flatten()
            .ok_or(MissingChannel(ch))
            .map(|(send_stream, recv_stream)| {
                spawn_channel(
                    join_set,
                    ch,
                    cap,
                    cap,
                    Guarantees::Limited(0),
                    send_stream,
                    recv_stream,
                )
            })
    };

    let rec = take_and_spawn_channel(LogicalChannel::Reconciliation)?;
    let stt = take_and_spawn_channel(LogicalChannel::StaticToken)?;
    Ok((
        LogicalChannelSenders {
            reconciliation: rec.0,
            static_tokens: stt.0,
        },
        LogicalChannelReceivers {
            reconciliation: rec.1,
            static_tokens: stt.1,
        },
    ))
}

fn spawn_channel(
    join_set: &mut JoinSet<anyhow::Result<()>>,
    ch: LogicalChannel,
    send_cap: usize,
    recv_cap: usize,
    guarantees: Guarantees,
    send_stream: quinn::SendStream,
    recv_stream: quinn::RecvStream,
) -> (Sender<Message>, Receiver<Message>) {
    let (sender, outbound_reader) = outbound_channel(send_cap, guarantees);
    let (inbound_writer, recveiver) = inbound_channel(recv_cap);

    let recv_fut = recv_loop(recv_stream, inbound_writer)
        .map_err(move |e| e.context(format!("receive loop for {ch:?} failed")))
        .instrument(error_span!("recv", ch=%ch.fmt_short()));

    join_set.spawn(recv_fut);

    let send_fut = send_loop(send_stream, outbound_reader)
        .map_err(move |e| e.context(format!("send loop for {ch:?} failed")))
        .instrument(error_span!("send", ch=%ch.fmt_short()));

    join_set.spawn(send_fut);

    (sender, recveiver)
}

async fn recv_loop(
    mut recv_stream: quinn::RecvStream,
    mut channel_writer: Writer,
) -> anyhow::Result<()> {
    let max_buffer_size = channel_writer.max_buffer_size();
    while let Some(buf) = recv_stream.read_chunk(max_buffer_size, true).await? {
        channel_writer.write_all(&buf.bytes[..]).await?;
        trace!(len = buf.bytes.len(), "recv");
    }
    channel_writer.close();
    Ok(())
}

async fn send_loop(
    mut send_stream: quinn::SendStream,
    channel_reader: Reader,
) -> anyhow::Result<()> {
    while let Some(data) = channel_reader.read_bytes().await {
        let len = data.len();
        send_stream.write_chunk(data).await?;
        trace!(len, "sent");
    }
    send_stream.finish().await?;
    Ok(())
}

async fn exchange_commitments(
    send_stream: &mut quinn::SendStream,
    recv_stream: &mut quinn::RecvStream,
) -> anyhow::Result<InitialTransmission> {
    let our_nonce: AccessChallenge = rand::random();
    let challenge_hash = Hash::new(&our_nonce);
    send_stream.write_u8(MAX_PAYLOAD_SIZE_POWER).await?;
    send_stream.write_all(challenge_hash.as_bytes()).await?;

    let their_max_payload_size = {
        let power = recv_stream.read_u8().await?;
        ensure!(power <= 64, "max payload size too large");
        2u64.pow(power as u32)
    };

    let mut received_commitment = [0u8; CHALLENGE_HASH_LENGTH];
    recv_stream.read_exact(&mut received_commitment).await?;
    Ok(InitialTransmission {
        our_nonce,
        received_commitment,
        their_max_payload_size,
    })
}

#[derive(Debug)]
pub struct InitialTransmission {
    pub our_nonce: AccessChallenge,
    pub received_commitment: ChallengeHash,
    pub their_max_payload_size: u64,
}

async fn join_all(mut join_set: JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
    let mut final_result = Ok(());
    while let Some(res) = join_set.join_next().await {
        let res = match res {
            Ok(Ok(())) => Ok(()),
            Ok(Err(err)) => Err(err),
            Err(err) => Err(err.into()),
        };
        if res.is_err() && final_result.is_ok() {
            final_result = res;
        } else if res.is_err() {
            warn!("join error after initial error: {res:?}");
        }
    }
    final_result
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        time::{Instant},
    };

    use futures_lite::StreamExt;
    use iroh_base::{hash::Hash, key::SecretKey};
    use iroh_net::MagicEndpoint;
    use rand::SeedableRng;
    use rand_core::CryptoRngCore;
    use tracing::{debug, info};

    use crate::{
        actor::WillowHandle,
        net::run,
        proto::{
            grouping::{AreaOfInterest, ThreeDRange},
            keys::{NamespaceId, NamespaceKind, NamespaceSecretKey, UserPublicKey, UserSecretKey},
            meadowcap::{AccessMode, McCapability, OwnedCapability},
            wgps::ReadCapability,
            willow::{Entry, InvalidPath, Path, WriteCapability},
        },
        session::{Role, SessionInit},
        store::MemoryStore,
    };

    const ALPN: &[u8] = b"iroh-willow/0";

    #[tokio::test(flavor = "multi_thread")]
    async fn smoke() -> anyhow::Result<()> {
        iroh_test::logging::setup_multithreaded();
        let mut rng = rand_chacha::ChaCha12Rng::seed_from_u64(1);
        let n_betty: usize = std::env::var("N_BETTY")
            .as_deref()
            .unwrap_or("1000")
            .parse()
            .unwrap();
        let n_alfie: usize = std::env::var("N_ALFIE")
            .as_deref()
            .unwrap_or("1000")
            .parse()
            .unwrap();

        let ep_alfie = MagicEndpoint::builder()
            .secret_key(SecretKey::generate_with_rng(&mut rng))
            .alpns(vec![ALPN.to_vec()])
            .bind(0)
            .await?;
        let ep_betty = MagicEndpoint::builder()
            .secret_key(SecretKey::generate_with_rng(&mut rng))
            .alpns(vec![ALPN.to_vec()])
            .bind(0)
            .await?;

        let addr_betty = ep_betty.my_addr().await?;
        let node_id_betty = ep_betty.node_id();
        let node_id_alfie = ep_alfie.node_id();

        debug!("start connect");
        let (conn_alfie, conn_betty) = tokio::join!(
            async move { ep_alfie.connect(addr_betty, ALPN).await },
            async move {
                let connecting = ep_betty.accept().await.unwrap();
                connecting.await
            }
        );
        let conn_alfie = conn_alfie.unwrap();
        let conn_betty = conn_betty.unwrap();
        info!("connected! now start reconciliation");

        let namespace_secret = NamespaceSecretKey::generate(&mut rng, NamespaceKind::Owned);
        let namespace_id: NamespaceId = namespace_secret.public_key().into();

        let start = Instant::now();
        let mut expected_entries = HashSet::new();

        let store_alfie = MemoryStore::default();
        let handle_alfie = WillowHandle::spawn(store_alfie, node_id_alfie);

        let store_betty = MemoryStore::default();
        let handle_betty = WillowHandle::spawn(store_betty, node_id_betty);

        let init_alfie = setup_and_insert(
            &mut rng,
            &handle_alfie,
            &namespace_secret,
            n_alfie,
            &mut expected_entries,
            |n| Path::new(&[b"alfie", n.to_string().as_bytes()]),
        )
        .await?;
        let init_betty = setup_and_insert(
            &mut rng,
            &handle_betty,
            &namespace_secret,
            n_betty,
            &mut expected_entries,
            |n| Path::new(&[b"betty", n.to_string().as_bytes()]),
        )
        .await?;

        debug!("init constructed");
        println!("init took {:?}", start.elapsed());
        let start = Instant::now();

        // tokio::task::spawn({
        //     let handle_alfie = handle_alfie.clone();
        //     let handle_betty = handle_betty.clone();
        //     async move {
        //         loop {
        //             info!(
        //                 "alfie count: {}",
        //                 handle_alfie
        //                     .get_entries(namespace_id, ThreeDRange::full())
        //                     .await
        //                     .unwrap()
        //                     .count()
        //                     .await
        //             );
        //             info!(
        //                 "betty count: {}",
        //                 handle_betty
        //                     .get_entries(namespace_id, ThreeDRange::full())
        //                     .await
        //                     .unwrap()
        //                     .count()
        //                     .await
        //             );
        //             tokio::time::sleep(Duration::from_secs(1)).await;
        //         }
        //     }
        // });

        let (res_alfie, res_betty) = tokio::join!(
            run(
                node_id_alfie,
                handle_alfie.clone(),
                conn_alfie,
                node_id_betty,
                Role::Alfie,
                init_alfie
            ),
            run(
                node_id_betty,
                handle_betty.clone(),
                conn_betty,
                node_id_alfie,
                Role::Betty,
                init_betty
            ),
        );
        info!(time=?start.elapsed(), "reconciliation finished!");
        println!("reconciliation took {:?}", start.elapsed());

        info!("alfie res {:?}", res_alfie);
        info!("betty res {:?}", res_betty);
        // info!(
        //     "alfie store {:?}",
        //     get_entries_debug(&handle_alfie, namespace_id).await?
        // );
        // info!(
        //     "betty store {:?}",
        //     get_entries_debug(&handle_betty, namespace_id).await?
        // );

        assert!(res_alfie.is_ok());
        assert!(res_betty.is_ok());
        assert_eq!(
            get_entries(&handle_alfie, namespace_id).await?,
            expected_entries,
            "alfie expected entries"
        );
        assert_eq!(
            get_entries(&handle_betty, namespace_id).await?,
            expected_entries,
            "bettyexpected entries"
        );

        Ok(())
    }
    async fn get_entries(
        store: &WillowHandle,
        namespace: NamespaceId,
    ) -> anyhow::Result<HashSet<Entry>> {
        let entries: HashSet<_> = store
            .get_entries(namespace, ThreeDRange::full())
            .await?
            .collect::<HashSet<_>>()
            .await;
        Ok(entries)
    }

    async fn setup_and_insert(
        rng: &mut impl CryptoRngCore,
        store: &WillowHandle,
        namespace_secret: &NamespaceSecretKey,
        count: usize,
        track_entries: &mut impl Extend<Entry>,
        path_fn: impl Fn(usize) -> Result<Path, InvalidPath>,
    ) -> anyhow::Result<SessionInit> {
        let user_secret = UserSecretKey::generate(rng);
        let (read_cap, write_cap) = create_capabilities(namespace_secret, user_secret.public_key());
        for i in 0..count {
            let path = path_fn(i).expect("invalid path");
            let entry = Entry::new_current(
                namespace_secret.id(),
                user_secret.id(),
                path,
                Hash::new("hello"),
                5,
            );
            track_entries.extend([entry.clone()]);
            let entry = entry.attach_authorisation(write_cap.clone(), &user_secret)?;
            store.ingest_entry(entry).await?;
        }
        let init = SessionInit::with_interest(user_secret, read_cap, AreaOfInterest::full());
        Ok(init)
    }

    fn create_capabilities(
        namespace_secret: &NamespaceSecretKey,
        user_public_key: UserPublicKey,
    ) -> (ReadCapability, WriteCapability) {
        let read_capability = McCapability::Owned(OwnedCapability::new(
            &namespace_secret,
            user_public_key,
            AccessMode::Read,
        ));
        let write_capability = McCapability::Owned(OwnedCapability::new(
            &namespace_secret,
            user_public_key,
            AccessMode::Write,
        ));
        (read_capability, write_capability)
    }

    // async fn get_entries_debug(
    //     store: &StoreHandle,
    //     namespace: NamespaceId,
    // ) -> anyhow::Result<Vec<(SubspaceId, Path)>> {
    //     let entries = get_entries(store, namespace).await?;
    //     let mut entries: Vec<_> = entries
    //         .into_iter()
    //         .map(|e| (e.subspace_id, e.path))
    //         .collect();
    //     entries.sort();
    //     Ok(entries)
    // }
}
