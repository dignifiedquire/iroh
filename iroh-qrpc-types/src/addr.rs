use anyhow::{anyhow, bail};
use quic_rpc::RpcMessage;
use serde_with::{DeserializeFromStr, SerializeDisplay};
use std::{
    fmt::{Debug, Display},
    net::SocketAddr,
    str::FromStr,
};

/// An address. This can be either a memory address, already containing the channel, or a network
/// address which will have to be opened.
#[derive(SerializeDisplay, DeserializeFromStr)]
pub enum Addr<In: RpcMessage, Out: RpcMessage> {
    Http2(SocketAddr),
    Http2Lookup(String),
    MemClient(quic_rpc::mem::ClientChannel<In, Out>),
    MemServer(quic_rpc::mem::ServerChannel<In, Out>),
}

impl<Req: RpcMessage, Res: RpcMessage> PartialEq for Addr<Req, Res> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Http2(addr1), Self::Http2(addr2)) => addr1.eq(addr2),
            (Self::Http2Lookup(addr1), Self::Http2Lookup(addr2)) => addr1.eq(addr2),
            _ => false,
        }
    }
}

impl<Req: RpcMessage, Res: RpcMessage> Addr<Req, Res> {
    pub fn new_mem() -> (Addr<Req, Res>, Addr<Res, Req>) {
        let (server, client) = quic_rpc::mem::connection(1);

        (Addr::MemServer(server), Addr::MemClient(client))
    }

    pub fn flip(&self) -> anyhow::Result<Addr<Res, Req>> {
        match self {
            Self::Http2(addr) => Ok(Addr::Http2(*addr)),
            Self::Http2Lookup(addr) => Ok(Addr::Http2Lookup(addr.clone())),
            Self::MemServer(_) => bail!("Cannot flip mem channel"),
            Self::MemClient(_) => bail!("Cannot flip mem channel"),
        }
    }
}

impl<Req: RpcMessage, Res: RpcMessage> Addr<Req, Res> {
    pub fn try_as_socket_addr(&self) -> Option<SocketAddr> {
        if let Addr::Http2(addr) = self {
            return Some(*addr);
        }
        None
    }
}

impl<Req: RpcMessage, Res: RpcMessage> Display for Addr<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Addr::Http2(addr) => write!(f, "http://{}", addr),
            Addr::Http2Lookup(addr) => write!(f, "http://{}", addr),
            Addr::MemClient(_) => write!(f, "mem"),
            Addr::MemServer(_) => write!(f, "mem"),
            #[allow(unreachable_patterns)]
            _ => unreachable!(),
        }
    }
}

impl<Req: RpcMessage, Res: RpcMessage> Debug for Addr<Req, Res> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(self, f)
    }
}

impl<Req: RpcMessage, Res: RpcMessage> Clone for Addr<Req, Res> {
    fn clone(&self) -> Self {
        match self {
            Addr::Http2(addr) => Addr::Http2(*addr),
            Addr::Http2Lookup(addr) => Addr::Http2Lookup(addr.clone()),
            Addr::MemClient(mem) => Addr::MemClient(mem.clone()),
            Addr::MemServer(mem) => Addr::MemServer(mem.clone()),
        }
    }
}

impl<Req: RpcMessage, Res: RpcMessage> FromStr for Addr<Req, Res> {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "mem" {
            bail!("memory addresses can not be serialized or deserialized");
        }

        let mut parts = s.split("://");
        if let Some(prefix) = parts.next() {
            if prefix == "http" {
                if let Some(part) = parts.next() {
                    return Ok(if let Ok(addr) = part.parse() {
                        Addr::Http2(addr)
                    } else {
                        Addr::Http2Lookup(part.to_string())
                    });
                }
            }
        }

        Err(anyhow!("invalid addr: {}", s))
    }
}

#[cfg(test)]
mod tests {

    #[cfg(feature = "grpc")]
    #[test]
    fn test_addr_roundtrip_grpc_http2() {
        use crate::gateway::GatewayClientAddr;

        let socket: SocketAddr = "198.168.2.1:1234".parse().unwrap();
        let addr = Addr::Qrpc(socket);

        assert_eq!(addr.to_string().parse::<GatewayClientAddr>().unwrap(), addr);
        assert_eq!(addr.to_string(), "qrpc://198.168.2.1:1234");
    }
}
