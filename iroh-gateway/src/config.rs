use crate::constants::*;
use axum::http::{header::*, Method};
use config::{ConfigError, Map, Source, Value};
use headers::{
    AccessControlAllowHeaders, AccessControlAllowMethods, AccessControlAllowOrigin, HeaderMapExt,
};
use iroh_metrics::config::Config as MetricsConfig;
use iroh_rpc_client::Config as RpcClientConfig;
use iroh_rpc_types::gateway::GatewayServerAddr;
use iroh_util::insert_into_config_map;
use serde::{Deserialize, Serialize};

/// CONFIG_FILE_NAME is the name of the optional config file located in the iroh home directory
pub const CONFIG_FILE_NAME: &str = "gateway.config.toml";
/// ENV_PREFIX should be used along side the config field name to set a config field using
/// environment variables
/// For example, `IROH_GATEWAY_PORT=1000` would set the value of the `Config.port` field
pub const ENV_PREFIX: &str = "IROH_GATEWAY";
pub const DEFAULT_PORT: u16 = 9050;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct Config {
    /// flag to toggle whether the gateway allows writing/pushing data
    pub writeable: bool,
    /// flag to toggle whether the gateway allows fetching data from other nodes or is local only
    pub fetch: bool,
    /// flag to toggle whether the gateway enables/utilizes caching
    pub cache: bool,
    /// default port to listen on
    pub port: u16,
    /// rpc listening addr
    pub rpc_addr: GatewayServerAddr,
    // NOTE: for toml to serialize properly, the "table" values must be serialized at the end, and
    // so much come at the end of the `Config` struct
    /// set of user provided headers to attach to all responses
    #[serde(with = "http_serde::header_map")]
    pub headers: HeaderMap,
    /// rpc addresses for the gateway & addresses for the rpc client to dial
    pub rpc_client: RpcClientConfig,
    /// metrics configuration
    pub metrics: MetricsConfig,
}

impl Config {
    pub fn new(
        writeable: bool,
        fetch: bool,
        cache: bool,
        port: u16,
        rpc_addr: GatewayServerAddr,
        rpc_client: RpcClientConfig,
    ) -> Self {
        Self {
            writeable,
            fetch,
            cache,
            headers: HeaderMap::new(),
            port,
            rpc_addr,
            rpc_client,
            metrics: MetricsConfig::default(),
        }
    }

    pub fn set_default_headers(&mut self) {
        self.headers = default_headers();
    }
}

fn default_headers() -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.typed_insert(AccessControlAllowOrigin::ANY);
    headers.typed_insert(
        [
            Method::GET,
            Method::PUT,
            Method::POST,
            Method::DELETE,
            Method::HEAD,
            Method::OPTIONS,
        ]
        .into_iter()
        .collect::<AccessControlAllowMethods>(),
    );
    headers.typed_insert(
        [
            CONTENT_TYPE,
            CONTENT_DISPOSITION,
            LAST_MODIFIED,
            CACHE_CONTROL,
            ACCEPT_RANGES,
            ETAG,
            HEADER_SERVICE_WORKER.clone(),
            HEADER_X_IPFS_GATEWAY_PREFIX.clone(),
            HEADER_X_TRACE_ID.clone(),
            HEADER_X_CONTENT_TYPE_OPTIONS.clone(),
            HEADER_X_IPFS_PATH.clone(),
            HEADER_X_IPFS_ROOTS.clone(),
        ]
        .into_iter()
        .collect::<AccessControlAllowHeaders>(),
    );
    // todo(arqu): remove these once propperly implmented
    headers.insert(CACHE_CONTROL, VALUE_NO_CACHE_NO_TRANSFORM.clone());
    headers.insert(ACCEPT_RANGES, VALUE_NONE.clone());
    headers
}

impl Default for Config {
    fn default() -> Self {
        let rpc_client = RpcClientConfig::default_grpc();
        let mut t = Self {
            writeable: false,
            fetch: false,
            cache: false,
            headers: HeaderMap::new(),
            port: DEFAULT_PORT,
            rpc_addr: rpc_client
                .gateway_addr
                .as_ref()
                .unwrap()
                .to_string()
                .parse()
                .unwrap(),
            rpc_client,
            metrics: MetricsConfig::default(),
        };
        t.set_default_headers();
        t
    }
}

impl Source for Config {
    fn clone_into_box(&self) -> Box<dyn Source + Send + Sync> {
        Box::new(self.clone())
    }

    fn collect(&self) -> Result<Map<String, Value>, ConfigError> {
        let rpc_client = self.rpc_client.collect()?;
        let mut map: Map<String, Value> = Map::new();
        insert_into_config_map(&mut map, "writeable", self.writeable);
        insert_into_config_map(&mut map, "fetch", self.fetch);
        insert_into_config_map(&mut map, "cache", self.cache);
        // Some issue between deserializing u64 & u16, converting this to
        // an signed int fixes the issue
        insert_into_config_map(&mut map, "port", self.port as i32);
        insert_into_config_map(&mut map, "rpc_addr", self.rpc_addr.to_string());
        insert_into_config_map(&mut map, "headers", collect_headers(&self.headers)?);
        insert_into_config_map(&mut map, "rpc_client", rpc_client);
        let metrics = self.metrics.collect()?;
        insert_into_config_map(&mut map, "metrics", metrics);
        Ok(map)
    }
}

fn collect_headers(headers: &HeaderMap) -> Result<Map<String, Value>, ConfigError> {
    let mut map = Map::new();
    for (key, value) in headers.iter() {
        insert_into_config_map(
            &mut map,
            key.as_str(),
            value.to_str().map_err(|e| ConfigError::Foreign(e.into()))?,
        );
    }
    Ok(map)
}

#[cfg(test)]
mod tests {
    use super::*;
    use config::Config as ConfigBuilder;

    #[test]
    fn test_default_headers() {
        let headers = default_headers();
        assert_eq!(headers.len(), 5);
        let h = headers.get(&ACCESS_CONTROL_ALLOW_ORIGIN).unwrap();
        assert_eq!(h, "*");
    }

    #[test]
    fn default_config() {
        let config = Config::default();
        assert!(!config.writeable);
        assert!(!config.fetch);
        assert!(!config.cache);
        assert_eq!(config.port, DEFAULT_PORT);
    }

    #[test]
    fn test_collect() {
        let default = Config::default();
        let mut expect: Map<String, Value> = Map::new();
        expect.insert("writeable".to_string(), Value::new(None, default.writeable));
        expect.insert("fetch".to_string(), Value::new(None, default.fetch));
        expect.insert("cache".to_string(), Value::new(None, default.cache));
        expect.insert("port".to_string(), Value::new(None, default.port as i64));
        expect.insert(
            "headers".to_string(),
            Value::new(None, collect_headers(&default.headers).unwrap()),
        );
        expect.insert(
            "rpc_addr".to_string(),
            Value::new(None, default.rpc_addr.to_string()),
        );
        expect.insert(
            "rpc_client".to_string(),
            Value::new(None, default.rpc_client.collect().unwrap()),
        );
        expect.insert(
            "metrics".to_string(),
            Value::new(None, default.metrics.collect().unwrap()),
        );

        let got = default.collect().unwrap();
        for key in got.keys() {
            let left = expect.get(key).unwrap_or_else(|| panic!("{}", key));
            let right = got.get(key).unwrap();
            assert_eq!(left, right);
        }
    }

    #[test]
    fn test_collect_headers() {
        let mut expect = Map::new();
        expect.insert(
            "access-control-allow-origin".to_string(),
            Value::new(None, "*"),
        );
        expect.insert(
            "access-control-allow-methods".to_string(),
            Value::new(None, "GET, PUT, POST, DELETE, HEAD, OPTIONS"),
        );
        expect.insert("access-control-allow-headers".to_string(), Value::new(None, "content-type, content-disposition, last-modified, cache-control, accept-ranges, etag, service-worker, x-ipfs-gateway-prefix, x-trace-id, x-content-type-options, x-ipfs-path, x-ipfs-roots"));
        expect.insert(
            "cache-control".to_string(),
            Value::new(None, "no-cache, no-transform"),
        );
        expect.insert("accept-ranges".to_string(), Value::new(None, "none"));
        let got = collect_headers(&default_headers()).unwrap();
        assert_eq!(expect, got);
    }

    #[test]
    fn test_build_config_from_struct() {
        let mut expect = Config::default();
        expect.set_default_headers();
        let source = expect.clone();
        let got: Config = ConfigBuilder::builder()
            .add_source(source)
            .build()
            .unwrap()
            .try_deserialize()
            .unwrap();

        assert_eq!(expect, got);
    }
}
