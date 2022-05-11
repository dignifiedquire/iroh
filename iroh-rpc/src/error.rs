use bytecheck::CheckBytes;
use rkyv::{Archive, Deserialize, Serialize};
use thiserror::Error;

#[derive(Archive, Serialize, Deserialize, Error, Debug, PartialEq, Clone, Eq)]
#[archive(compare(PartialEq))]
#[archive_attr(derive(Debug, CheckBytes))]
pub enum RpcError {
    #[error("TODO: Implement error")]
    TODO,
    #[error("Method `{0}` not found")]
    MethodNotFound(String),
    #[error("Namespace `{0}` not found")]
    NamespaceNotFound(String),
    #[error("Bad Request")]
    BadRequest,
    #[error("Bad Response")]
    BadResponse,
    #[error("Stream Closed")]
    StreamClosed,
    #[error("Bad config: `{0}`")]
    BadConfig(String),
    #[error("Unexpected response type")]
    UnexpectedResponseType,
    #[error("DialError: `{0}`")]
    DialError(String),
    #[error("OutboundFailure: `{0}`")]
    OutboundFailure(String),
    #[error("TransportError: `{0}`")]
    TransportError(String),
    #[error("JoinError: `{0}`")]
    JoinError(String),
}
