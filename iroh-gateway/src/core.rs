use axum::{
    body::{self, BoxBody},
    error_handling::HandleErrorLayer,
    extract::{Extension, Path, Query},
    http::{header::*, StatusCode},
    response::{IntoResponse, Redirect},
    routing::get,
    BoxError, Router,
};
use cid::Cid;
use metrics::increment_counter;
use serde::{Deserialize, Serialize};
use serde_qs;
use std::{
    borrow::Cow,
    collections::HashMap,
    sync::Arc,
    time::{self, Duration},
};
use tower::ServiceBuilder;

use crate::{
    client::{Client, Request},
    config::Config,
    constants::*,
    error::GatewayError,
    headers::*,
    metrics::{get_current_trace_id, METRICS_CNT_REQUESTS_TOTAL},
    response::{get_response_format, GatewayResponse, ResponseFormat},
};

#[derive(Debug)]
pub struct Core {
    pub config: Arc<Config>,
    client: Arc<Client>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetParams {
    // todo(arqu): swap this for ResponseFormat
    /// specifies the expected format of the response
    format: Option<String>,
    /// specifies the desired filename of the response
    filename: Option<String>,
    /// specifies whether the response should be of disposition inline or attachment
    download: Option<bool>,
}

impl GetParams {
    pub fn to_query_string(&self) -> String {
        let q = serde_qs::to_string(self).unwrap();
        if q.is_empty() {
            q
        } else {
            format!("?{}", q)
        }
    }
}

impl Core {
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(config),
            client: Arc::new(Client::new()),
        }
    }

    pub async fn serve(self) {
        // todo(arqu): ?uri=... https://github.com/ipfs/go-ipfs/pull/7802
        let app = Router::new()
            .route("/ipfs/:cid", get(get_ipfs))
            .route("/ipfs/:cid/*cpath", get(get_ipfs))
            .route("/ipfs/ipfs/:cid", get(redundant_ipfs))
            .route("/ipfs/ipfs/:cid/*cpath", get(redundant_ipfs))
            .layer(Extension(Arc::clone(&self.config)))
            .layer(Extension(Arc::clone(&self.client)))
            .layer(
                ServiceBuilder::new()
                    // Handle errors from middleware
                    .layer(HandleErrorLayer::new(middleware_error_handler))
                    .load_shed()
                    .concurrency_limit(1024)
                    .timeout(Duration::from_secs(10))
                    .into_inner(),
            );
        // todo(arqu): make configurable
        let addr = format!("0.0.0.0:{}", self.config.port);
        axum::Server::bind(&addr.parse().unwrap())
            .http1_preserve_header_case(true)
            .http1_title_case_headers(true)
            .serve(app.into_make_service())
            .await
            .unwrap();
    }
}

#[tracing::instrument()]
async fn redundant_ipfs(
    Path(params): Path<HashMap<String, String>>,
    Query(query_params): Query<GetParams>,
) -> impl IntoResponse {
    let cid = params.get("cid").unwrap();
    let cpath = "".to_string();
    let cpath = params.get("cpath").unwrap_or(&cpath);
    let redirect_path: String = if cpath.is_empty() {
        format!("/ipfs/{}", cid)
    } else {
        format!("/ipfs/{}/{}", cid, cpath)
    };
    let redirect_path = format!("{}{}", redirect_path, query_params.to_query_string());
    Redirect::to(&redirect_path).into_response()
}

#[tracing::instrument()]
async fn get_ipfs(
    Extension(config): Extension<Arc<Config>>,
    Extension(client): Extension<Arc<Client>>,
    Path(params): Path<HashMap<String, String>>,
    Query(query_params): Query<GetParams>,
    request_headers: HeaderMap,
) -> Result<GatewayResponse, GatewayError> {
    increment_counter!(METRICS_CNT_REQUESTS_TOTAL);
    let start_time = time::Instant::now();
    // parse path params
    let cid_param = params.get("cid").unwrap();
    let cid = Cid::try_from(cid_param.clone());
    let cpath = "".to_string();
    let cpath = params.get("cpath").unwrap_or(&cpath);

    if request_headers.contains_key(&HEADER_SERVICE_WORKER) {
        let sw = request_headers.get(&HEADER_SERVICE_WORKER).unwrap();
        if sw.to_str().unwrap() == "script" && cpath.is_empty() {
            return error(StatusCode::BAD_REQUEST, "Service Worker not supported");
        }
    }
    if request_headers.contains_key(&HEADER_X_IPFS_GATEWAY_PREFIX) {
        return error(StatusCode::BAD_REQUEST, "Unsupported HTTP header");
    }

    let cid = match cid {
        Ok(cid) => cid,
        Err(_) => {
            // todo (arqu): improve error handling if possible https://github.com/dignifiedquire/iroh/pull/4#pullrequestreview-953147597
            return error(StatusCode::BAD_REQUEST, "invalid cid");
        }
    };
    let full_content_path = format!("/ipfs/{}{}", cid, cpath);

    // todo(arqu): actually plug in a resolver
    let resolved_cid = resolve_cid(&cid).await.unwrap();

    // parse query params
    let format = match get_response_format(&request_headers, query_params.format) {
        Ok(format) => format,
        Err(err) => {
            return error(StatusCode::BAD_REQUEST, &err);
        }
    };

    let query_file_name = query_params.filename.unwrap_or_default();
    let download = query_params.download.unwrap_or_default();

    let mut headers = HeaderMap::new();

    if request_headers.contains_key("If-None-Match") {
        // todo(arqu): handle dir etags
        let cid_etag = get_etag(&resolved_cid, Some(format.clone()));
        let inm = request_headers
            .get("If-None-Match")
            .unwrap()
            .to_str()
            .unwrap();
        if etag_matches(inm, &cid_etag) {
            return response(StatusCode::NOT_MODIFIED, body::BoxBody::default(), headers);
        }
    }

    // init headers
    format.write_headers(&mut headers);
    add_user_headers(&mut headers, config.headers.clone());
    headers.insert(
        &HEADER_X_IPFS_PATH,
        HeaderValue::from_str(&full_content_path).unwrap(),
    );
    // todo(arqu): add X-Ipfs-Roots

    // handle request and fetch data
    let req = Request {
        format,
        cid,
        full_content_path,
        query_file_name,
        content_path: cpath.to_string(),
        download,
    };
    match req.format {
        ResponseFormat::Raw => serve_raw(&req, *client, headers, start_time).await,
        ResponseFormat::Car => serve_car(&req, *client, headers, start_time).await,
        ResponseFormat::Fs(_) => serve_fs(&req, *client, headers, start_time).await,
    }
}

// todo(arqu): flesh out resolving
#[tracing::instrument()]
async fn resolve_cid(cid: &Cid) -> Result<Cid, String> {
    Ok(*cid)
}

#[tracing::instrument()]
async fn serve_raw(
    req: &Request,
    client: Client,
    mut headers: HeaderMap,
    start_time: std::time::Instant,
) -> Result<GatewayResponse, GatewayError> {
    let body = client
        .get_file_simulated(req.full_content_path.as_str(), start_time)
        .await;
    let body = match body {
        Ok(b) => b,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, &e);
        }
    };
    set_content_disposition_headers(
        &mut headers,
        format!("{}.bin", req.cid).as_str(),
        DISPOSITION_ATTACHMENT,
    );
    set_etag_headers(&mut headers, get_etag(&req.cid, Some(req.format.clone())));
    add_cache_control_headers(&mut headers, req.full_content_path.to_string());
    response(StatusCode::OK, body::boxed(body), headers.clone())
}

#[tracing::instrument()]
async fn serve_car(
    req: &Request,
    client: Client,
    mut headers: HeaderMap,
    start_time: std::time::Instant,
) -> Result<GatewayResponse, GatewayError> {
    let body = client
        .get_file_simulated(req.full_content_path.as_str(), start_time)
        .await;
    let body = match body {
        Ok(b) => b,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, &e);
        }
    };
    set_content_disposition_headers(
        &mut headers,
        format!("{}.car", req.cid).as_str(),
        DISPOSITION_ATTACHMENT,
    );
    // todo(arqu): this should be root cid
    let etag = format!("W/{}", get_etag(&req.cid, Some(req.format.clone())));
    set_etag_headers(&mut headers, etag);
    // todo(arqu): check if etag matches for root cid
    response(StatusCode::OK, body::boxed(body), headers.clone())
}

#[tracing::instrument()]
async fn serve_fs(
    req: &Request,
    client: Client,
    mut headers: HeaderMap,
    start_time: std::time::Instant,
) -> Result<GatewayResponse, GatewayError> {
    let body = client
        .get_file_simulated(req.full_content_path.as_str(), start_time)
        .await;
    let body = match body {
        Ok(b) => b,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, &e);
        }
    };
    let name = add_content_disposition_headers(
        &mut headers,
        &req.query_file_name,
        &req.content_path,
        req.download,
    );
    set_etag_headers(&mut headers, get_etag(&req.cid, Some(req.format.clone())));
    add_cache_control_headers(&mut headers, req.full_content_path.to_string());
    add_content_type_headers(&mut headers, &name);
    response(StatusCode::OK, body::boxed(body), headers.clone())
}

#[tracing::instrument()]
fn response(
    status_code: StatusCode,
    body: BoxBody,
    headers: HeaderMap,
) -> Result<GatewayResponse, GatewayError> {
    Ok(GatewayResponse {
        status_code,
        body,
        headers,
        trace_id: get_current_trace_id().to_string(),
    })
}

#[tracing::instrument()]
fn error(status_code: StatusCode, message: &str) -> Result<GatewayResponse, GatewayError> {
    Err(GatewayError {
        status_code,
        message: message.to_string(),
        trace_id: get_current_trace_id().to_string(),
    })
}

async fn middleware_error_handler(error: BoxError) -> impl IntoResponse {
    if error.is::<tower::timeout::error::Elapsed>() {
        return (StatusCode::REQUEST_TIMEOUT, Cow::from("request timed out"));
    }

    if error.is::<tower::load_shed::error::Overloaded>() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Cow::from("service is overloaded, try again later"),
        );
    }

    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Cow::from(format!("unhandled internal error: {}", error)),
    )
}
