use std::collections::HashMap;

use futures::channel::{mpsc, oneshot};
use libp2p::core::connection::ListenerId;
use libp2p::request_response::RequestId;
use libp2p::Multiaddr;
use libp2p::PeerId;

use crate::behaviour::rpc::RpcResponseChannel;
use crate::error::RpcError;
use crate::stream::{Header, Packet, StreamType};

// Commands are commands from the Client going out to the server or network
// They should include a sender on which the server will send the response from
// the network to the client
#[derive(Debug)]
pub enum Command {
    // Commands handled by RpcProtocol
    StartListening {
        addr: Multiaddr,
        sender: OneshotSender,
    },
    Dial {
        peer_id: PeerId,
        peer_addr: Multiaddr,
        sender: OneshotSender,
    },
    PeerId {
        sender: OneshotSender,
    },
    SendRequest {
        namespace: String,
        method: String,
        peer_id: PeerId,
        params: Vec<u8>,
        sender: OneshotSender,
    },
    SendResponse {
        payload: Vec<u8>,
        channel: RpcResponseChannel,
    },
    ErrorResponse {
        error: RpcError,
        channel: RpcResponseChannel,
    },

    StreamRequest {
        id: u64,
        namespace: String,
        method: String,
        peer_id: PeerId,
        params: Vec<u8>,
        sender: OneshotSender,
    },
    HeaderResponse {
        header: Header,
        channel: RpcResponseChannel,
    },
    SendPacket {
        peer_id: PeerId,
        packet: Packet,
        sender: OneshotSender,
    },

    CloseStream {
        id: u64,
    },
    ShutDown,
}

pub type OneshotSender = oneshot::Sender<SenderType>;

#[derive(Debug)]
pub enum SenderType {
    Ack,
    File(Vec<u8>),
    PeerId(PeerId),
    Multiaddr(Multiaddr),
    Stream {
        header: Header,
        stream: mpsc::Receiver<StreamType>,
    },
    Error(RpcError),
    Res(Vec<u8>),
}

impl std::fmt::Display for SenderType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            SenderType::Ack => write!(f, "SenderType::Ack"),
            SenderType::Error(e) => write!(f, "SenderType::Error: {}", e),
            SenderType::File(_) => write!(f, "SenderType::File"),
            SenderType::Multiaddr(m) => write!(f, "SenderType::Multiaddr: {}", m),
            SenderType::PeerId(p) => write!(f, "SenderType::PeerId: {}", p),
            SenderType::Res(_) => write!(f, "SenderType::Res"),
            SenderType::Stream { header, .. } => write!(f, "SenderType::Stream: {}", header.id),
        }
    }
}

pub type PendingMap = HashMap<PendingId, OneshotSender>;

#[derive(PartialEq, Eq, Hash)]
pub enum PendingId {
    Peer(PeerId),
    Request(RequestId),
    Listener(ListenerId),
}
