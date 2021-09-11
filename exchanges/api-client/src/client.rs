use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use reqwest::{header::HeaderMap, StatusCode};
use tokio::sync::RwLock;

use crate::{
    sync::ValueSemaphore,
    errors::{Error, ErrorKind},
};

type Cache = HashMap<String, Arc<Bytes>>;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Clone)]
pub struct QueryParams(Vec<(&'static str, String, bool)>);

impl QueryParams {
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn add<T: ToString>(&mut self, name: &'static str, value: T, cacheable: bool) {
        self.0.push((name, value.to_string(), cacheable));
    }

    pub fn extend(&mut self, params: QueryParams) {
        self.0.extend(params.0);
    }

    pub fn to_string(&self) -> String {
        self.0
            .iter()
            .map(|(k, v, _)| format!("{}={}", k, v))
            .collect::<Vec<String>>()
            .join("&")
    }

    pub fn as_cacheable_string(&self) -> String {
        self.0
            .iter()
            .filter_map(|(k, v, c)| match *c {
                true => Some(v.to_string() + *k),
                false => None,
            })
            .collect::<Vec<String>>()
            .join("")
    }
}

pub struct ApiClient {
    endpoint_sem: ValueSemaphore<String>,
    endpoint_params_sem: ValueSemaphore<String>,
    cache: Arc<RwLock<Cache>>,
}

impl ApiClient {
    pub fn new(endpoint_concurrency: usize) -> Self {
        Self {
            endpoint_sem: ValueSemaphore::with_capacity(endpoint_concurrency),
            endpoint_params_sem: ValueSemaphore::new(),
            cache: Arc::new(RwLock::new(Cache::new())),
        }
    }

    pub async fn make_request(
        &self,
        endpoint: &str,
        query_params: Option<QueryParams>,
        headers: Option<HeaderMap>,
        cache_response: bool,
    ) -> Result<Arc<Bytes>> {
        let client = reqwest::Client::new();

        let _endpoint_params_perm;
        let cache_key = match cache_response {
            true => {
                // form a cache key = endpoint + cacheable query params
                let key = match query_params.as_ref() {
                    Some(q) => format!("{}{}", endpoint, q.as_cacheable_string()),
                    None => endpoint.to_string(),
                };
                // Block this endpoint + cacheable params until the response is added to the cache
                _endpoint_params_perm = self
                    .endpoint_params_sem
                    .acquire_for(key.clone())
                    .await?;
                match Arc::clone(&self.cache).read().await.get(&key) {
                    Some(v) => {
                        return Ok(Arc::clone(v));
                    }
                    None => (),
                }
                Some(key)
            }
            false => None
        };

        // Restrict concurrency for each API endpoint, this allows `endpoint_concurrency`
        // requests to be awaiting on the same endpoint at the same time.
        let _endpoint_perm = self.endpoint_sem.acquire_for(endpoint.to_string()).await?;

        let full_url = match query_params {
            Some(q) => format!("{}?{}", endpoint, q.to_string()),
            None => endpoint.to_string(),
        };

        let mut r = client.get(&full_url);
        if let Some(h) = headers {
            r = r.headers(h);
        }
        let resp = r.send().await;

        match self.validate_response(resp?).await {
            Ok(resp) => {
                let resp_bytes = Arc::new(resp.bytes().await?);
                if let Some(cache_key) = cache_key {
                    let cache = Arc::clone(&self.cache);
                    cache.write().await.insert(cache_key, Arc::clone(&resp_bytes));
                } 
                Ok(resp_bytes)
            }
            Err(err) => Err(err.into()),
        }
    }

    async fn validate_response(&self, resp: reqwest::Response) -> Result<reqwest::Response> {
        match resp.status() {
            StatusCode::OK => Ok(resp),
            StatusCode::INTERNAL_SERVER_ERROR => Err(Error::new(resp.text().await?, ErrorKind::Internal)),
            StatusCode::SERVICE_UNAVAILABLE => Err(Error::new(resp.text().await?, ErrorKind::ServiceUnavailable)),
            StatusCode::UNAUTHORIZED => Err(Error::new(resp.text().await?, ErrorKind::Unauthorized)),
            StatusCode::BAD_REQUEST => Err(Error::new(resp.text().await?, ErrorKind::BadRequest)),
            status => Err(Error::new(format!("text={} status={}", resp.text().await?, status), ErrorKind::Other)),
        }
    }
}
