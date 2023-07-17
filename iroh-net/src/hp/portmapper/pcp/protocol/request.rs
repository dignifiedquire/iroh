use std::net::Ipv6Addr;

use super::{opcode_data::OpcodeData, Version};

/// A PCP Request.
///
/// See [RFC 6887 Request Header](https://datatracker.ietf.org/doc/html/rfc6887#section-7.1)
///
// NOTE: PCP Options are optional, and currently not used in this code, thus not implemented
#[derive(Debug, PartialEq, Eq)]
pub struct Request {
    /// [`Version`] to use in this request.
    pub(super) version: Version,
    /// Requested lifetime in seconds.
    pub(super) lifetime_seconds: u32,
    /// IP Address of the client.
    ///
    /// If the IP is an IpV4 address, is represented as a IpV4-mapped IpV6 address.
    pub(super) client_addr: Ipv6Addr,
    /// Data associated to the [`super::Opcode`] in this request.
    pub(super) opcode_data: OpcodeData,
}

impl Request {
    /// Size of a [`Request`] sent by this client, in bytes.
    pub const MIN_SIZE: usize = // parts:
        1 + // version
        1 + // opcode
        2 + // reserved
        4 + // lifetime
        16; // local ip

    /// Encode this [`Request`].
    pub fn encode(&self) -> Vec<u8> {
        let Request {
            version,
            lifetime_seconds,
            client_addr,
            opcode_data,
        } = self;
        let mut buf = Vec::with_capacity(Self::MIN_SIZE + opcode_data.encoded_size());
        // buf[0]
        buf.push((*version).into());
        // buf[1]
        buf.push(opcode_data.opcode().into());
        // buf[2] reserved
        buf.push(0);
        // buf[3] reserved
        buf.push(0);
        // buf[4..8]
        buf.extend_from_slice(&lifetime_seconds.to_be_bytes());
        // buf[8..12]
        buf.extend_from_slice(&client_addr.octets());
        // buf[12..]
        opcode_data.encode_into(&mut buf);

        buf
    }

    /// Create an announce request.
    pub fn annouce(client_addr: Ipv6Addr) -> Request {
        Request {
            version: Version::Pcp,
            // opcode announce requires a lifetime of 0 and to ignore the lifetime on response
            lifetime_seconds: 0,
            client_addr,
            // the pcp announce opcode requests and responses have no opcode-specific payload
            opcode_data: OpcodeData::Announce,
        }
    }

    #[cfg(test)]
    fn random(opcode: super::Opcode) -> Self {
        let opcode_data = OpcodeData::random(opcode);
        // initiallize in zero using the UNSPECIFIED addr
        let addr_octects: [u8; 16] = rand::random();
        Request {
            version: Version::Pcp,
            lifetime_seconds: rand::random(),
            client_addr: Ipv6Addr::from(addr_octects),
            opcode_data,
        }
    }

    #[cfg(test)]
    #[track_caller]
    fn decode(buf: &[u8]) -> Self {
        let version: Version = buf[0].try_into().unwrap();
        let opcode: super::Opcode = buf[1].try_into().unwrap();
        // buf[2] reserved
        // buf[3] reserved
        let lifetime_bytes: [u8; 4] = buf[4..8].try_into().unwrap();
        let lifetime_seconds = u32::from_be_bytes(lifetime_bytes);

        let local_ip_bytes: [u8; 16] = buf[8..24].try_into().unwrap();
        let client_addr: Ipv6Addr = local_ip_bytes.into();

        let opcode_data = OpcodeData::decode(opcode, buf).unwrap();
        Self {
            version,
            lifetime_seconds,
            client_addr,
            opcode_data,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_addr_request() {
        let request = Request::random(super::super::Opcode::Announce);
        let encoded = request.encode();
        assert_eq!(request, Request::decode(&encoded));
    }
}
