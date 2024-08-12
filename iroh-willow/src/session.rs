use std::sync::Arc;

use channels::ChannelSenders;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    interest::Interests,
    session::{error::ChannelReceiverDropped, intents::Intent},
};

mod aoi_finder;
mod capabilities;
mod challenge;
pub mod channels;
mod data;
pub mod error;
pub mod intents;
mod pai_finder;
mod payload;
mod reconciler;
mod resource;
mod run;
mod static_tokens;

pub(crate) use self::challenge::InitialTransmission;
pub(crate) use self::channels::Channels;
pub(crate) use self::error::Error;
pub(crate) use self::run::run_session;

pub type SessionId = u64;

/// To break symmetry, we refer to the peer that initiated the synchronisation session as Alfie,
/// and the other peer as Betty.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Role {
    /// The peer that initiated the synchronisation session.
    Alfie,
    /// The peer that accepted the synchronisation session.
    Betty,
}

impl Role {
    /// Returns `true` if we initiated the session.
    pub fn is_alfie(&self) -> bool {
        matches!(self, Role::Alfie)
    }
    /// Returns `true` if we accepted the session.
    pub fn is_betty(&self) -> bool {
        matches!(self, Role::Betty)
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum SessionMode {
    /// Run a single, full reconciliation, and then quit.
    ReconcileOnce,
    /// Run reconciliations and data mode, until intentionally closed.
    Live,
}

impl SessionMode {
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Live)
    }
}

/// Options to initialize a session with.
#[derive(Debug)]
pub struct SessionInit {
    /// List of interests we wish to synchronize, together with our capabilities to read them.
    pub interests: Interests,
    pub mode: SessionMode,
}

impl SessionInit {
    pub fn new(interests: impl Into<Interests>, mode: SessionMode) -> Self {
        let interests = interests.into();
        Self { interests, mode }
    }

    pub fn continuous(interests: impl Into<Interests>) -> Self {
        Self::new(interests, SessionMode::Live)
    }

    pub fn reconcile_once(interests: impl Into<Interests>) -> Self {
        Self::new(interests, SessionMode::ReconcileOnce)
    }
}

/// The bind scope for resources.
///
/// Resources are bound by either peer
#[derive(Copy, Clone, Debug)]
pub enum Scope {
    /// Resources bound by ourselves.
    Ours,
    /// Resources bound by the other peer.
    Theirs,
}

#[derive(Debug, Clone)]
pub struct EventSender(pub mpsc::Sender<SessionEvent>);

impl EventSender {
    pub async fn send(&self, event: SessionEvent) -> Result<(), ChannelReceiverDropped> {
        self.0.send(event).await.map_err(|_| ChannelReceiverDropped)
    }
}

#[derive(derive_more::Debug)]
pub enum SessionEvent {
    Established,
    Complete {
        result: Result<(), Arc<Error>>,
        // who_cancelled: WhoCancelled,
        we_cancelled: bool,
        #[debug("ChannelSenders")]
        senders: ChannelSenders,
        remaining_intents: Vec<Intent>,
        #[debug("Receiver<SessionUpdate>")]
        update_receiver: mpsc::Receiver<SessionUpdate>,
    },
}

#[derive(Debug)]
pub enum SessionUpdate {
    SubmitIntent(Intent),
}

#[derive(Debug)]
pub struct SessionHandle {
    pub cancel_token: CancellationToken,
    pub update_tx: mpsc::Sender<SessionUpdate>,
    pub event_rx: mpsc::Receiver<SessionEvent>,
}

impl SessionHandle {
    /// Wait for the session to finish.
    ///
    /// Returns the channel senders and a boolean indicating if we cancelled the session.
    /// Returns an error if the session failed to complete.
    pub async fn complete(&mut self) -> Result<(ChannelSenders, bool), Arc<Error>> {
        while let Some(event) = self.event_rx.recv().await {
            if let SessionEvent::Complete {
                result,
                senders,
                we_cancelled,
                ..
            } = event
            {
                return result.map(|()| (senders, we_cancelled));
            }
        }
        Err(Arc::new(Error::ActorFailed))
    }

    /// Submit a new synchronisation intent.
    pub async fn submit_intent(&self, intent: Intent) -> anyhow::Result<()> {
        self.update_tx
            .send(SessionUpdate::SubmitIntent(intent))
            .await?;
        Ok(())
    }

    /// Finish the session gracefully.
    ///
    /// After calling this, no further protocol messages will be sent from this node.
    /// Previously queued messages will still be sent out. The session will only be closed
    /// once the other peer closes their senders as well.
    pub fn close(&self) {
        tracing::debug!("close session (session handle close called)");
        self.cancel_token.cancel();
    }
}
