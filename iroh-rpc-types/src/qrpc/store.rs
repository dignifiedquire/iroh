use bytes::Bytes;
use cid::Cid;
use derive_more::{From, TryInto};
use quic_rpc::{message::RpcMsg, Service};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionRequest;

#[derive(Serialize, Deserialize, Debug)]
pub struct VersionResponse {
    pub version: String,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PutRequest {
    pub cid: Cid,
    pub blob: Bytes,
    pub links: Vec<Cid>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PutManyRequest {
    pub blocks: Vec<PutRequest>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetRequest {
    pub cid: Cid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetResponse {
    pub data: Option<Bytes>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HasRequest {
    pub cid: Cid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HasResponse {
    pub has: bool,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetLinksRequest {
    pub cid: Cid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetLinksResponse {
    pub links: Option<Vec<Cid>>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetSizeRequest {
    pub cid: Cid,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GetSizeResponse {
    pub size: Option<u64>,
}

#[derive(Serialize, Deserialize, Debug, From, TryInto)]
pub enum StoreRequest {
    Version(VersionRequest),
    Put(PutRequest),
    PutMany(PutManyRequest),
    Get(GetRequest),
    Has(HasRequest),
    GetLinks(GetLinksRequest),
    GetSize(GetSizeRequest),
}

#[derive(Serialize, Deserialize, Debug, From, TryInto)]
pub enum StoreResponse {
    Version(VersionResponse),
    Get(GetResponse),
    Has(HasResponse),
    GetLinks(GetLinksResponse),
    GetSize(GetSizeResponse),
    Unit(()),
}

#[derive(Debug, Clone, Copy)]
pub struct StoreService;

impl Service for StoreService {
    type Req = StoreRequest;

    type Res = StoreResponse;
}

impl RpcMsg<StoreService> for VersionRequest {
    type Response = VersionResponse;
}

impl RpcMsg<StoreService> for GetRequest {
    type Response = GetResponse;
}

impl RpcMsg<StoreService> for PutRequest {
    type Response = ();
}

impl RpcMsg<StoreService> for HasRequest {
    type Response = HasResponse;
}

impl RpcMsg<StoreService> for PutManyRequest {
    type Response = ();
}

impl RpcMsg<StoreService> for GetLinksRequest {
    type Response = GetLinksResponse;
}

impl RpcMsg<StoreService> for GetSizeRequest {
    type Response = GetSizeResponse;
}
