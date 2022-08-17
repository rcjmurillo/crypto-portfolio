use std::collections::HashMap;
use std::task::Poll;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, RwLock as SyncRwLock},
};

use anyhow::{anyhow, Result};
use bytes::Bytes;
use reqwest::{header::HeaderMap, StatusCode};
use reqwest::{header::HeaderValue, Url};
use tokio::sync::RwLock;
use tower::Service;

use crate::errors::Error as ApiError;

type Cache = HashMap<String, Arc<Bytes>>;

enum ValueState {
    Closure(Box<dyn Fn() -> String + Send + Sync>),
    Materialized(String),
}

/// Stores the result of the query materialization
pub struct QueryString {
    pub full_query: String,
    pub cacheable_query: String,
}

/// Allows to manipulate a query string. It's also possible to add lazy paramaters which delay 
/// the evaluation of the parameter value until just before the request is sent.
#[derive(Clone)]
pub struct Query {
    params: Arc<SyncRwLock<Vec<(&'static str, bool)>>>,
    values: Arc<SyncRwLock<HashMap<&'static str, Arc<ValueState>>>>,
    on_materialize_cb: Option<Arc<Box<dyn 'static + Fn(QueryString) -> QueryString + Send + Sync>>>,
}

impl Query {
    pub fn new() -> Self {
        Query {
            params: Arc::new(SyncRwLock::new(vec![])),
            values: Arc::new(SyncRwLock::new(HashMap::new())),
            on_materialize_cb: None,
        }
    }

    fn update_param_name(&self, name: &'static str, cacheable: bool) {
        if self.params.read().unwrap().contains(&(name, cacheable)) {
            let mut params = self.params.write().unwrap();
            let i = params.iter().position(|(f, _)| *f == name).unwrap();
            params.splice(i..i + 1, vec![(name, cacheable)]);
        } else {
            self.params.write().unwrap().push((name, cacheable));
        }
    }

    fn add_param(&self, name: &'static str, value_state: ValueState, cacheable: bool) {
        self.update_param_name(name, cacheable);
        self.values
            .write()
            .unwrap()
            .insert(name, Arc::new(value_state));
    }

    fn merge_param(&self, name: &'static str, value_state: Arc<ValueState>, cacheable: bool) {
        self.update_param_name(name, cacheable);
        self.values.write().unwrap().insert(name, value_state);
    }

    fn materialize_query(&self, params: &[&str]) -> String {
        params
            .iter()
            .filter_map(
                |name| match self.values.read().unwrap().get(name).unwrap().as_ref() {
                    ValueState::Materialized(value) => Some(format!("{}={}", name, value)),
                    ValueState::Closure(f) => Some(format!("{}={}", name, f().to_string())),
                    _ => unreachable!(),
                },
            )
            .collect::<Vec<String>>()
            .join("&")
    }

    pub fn on_materialize<F: 'static + Fn(QueryString) -> QueryString + Send + Sync>(
        &mut self,
        f: F,
    ) {
        self.on_materialize_cb = Some(Arc::new(Box::new(f)));
    }

    // Materialize the query string, it'll run any lazy parameter clousures.
    pub fn materialize(&self) -> QueryString {
        let all_params: Vec<&str> = self
            .params
            .read()
            .unwrap()
            .iter()
            .map(|(name, _)| *name)
            .collect();
        let cacheable_params: Vec<&str> = self
            .params
            .read()
            .unwrap()
            .iter()
            .filter_map(|(name, c)| if *c { Some(*name) } else { None })
            .collect();

        let qs = QueryString {
            full_query: self.materialize_query(&all_params),
            cacheable_query: self.materialize_query(&cacheable_params),
        };
        match self.on_materialize_cb.as_ref() {
            Some(cb) => (*cb)(qs),
            None => qs,
        }
    }

    pub fn param<T: ToString>(&mut self, name: &'static str, value: T) -> &mut Self {
        self.add_param(name, ValueState::Materialized(value.to_string()), false);
        self
    }

    pub fn cached_param<T: ToString>(&mut self, name: &'static str, value: T) -> &mut Self {
        self.add_param(name, ValueState::Materialized(value.to_string()), true);
        self
    }

    pub fn lazy_param<F: 'static + Fn() -> String + Send + Sync>(
        &mut self,
        name: &'static str,
        f: F,
    ) -> &mut Self {
        self.add_param(name, ValueState::Closure(Box::new(f)), false);
        self
    }

    pub fn merge(&mut self, other: Query) {
        for (name, cacheable) in other.params.read().unwrap().iter() {
            self.merge_param(
                name,
                other.values.read().unwrap().get(name).unwrap().clone(),
                *cacheable,
            );
        }
    }
}

async fn validate_response(resp: reqwest::Response) -> Result<reqwest::Response> {
    let status = resp.status();
    match status {
        StatusCode::OK => Ok(resp),
        StatusCode::INTERNAL_SERVER_ERROR => {
            Err(anyhow!(resp.text().await?).context(ApiError::Internal))
        }
        StatusCode::SERVICE_UNAVAILABLE => {
            Err(anyhow!(resp.text().await?).context(ApiError::ServiceUnavailable))
        }
        StatusCode::UNAUTHORIZED => {
            Err(anyhow!(resp.text().await?).context(ApiError::Unauthorized))
        }
        StatusCode::BAD_REQUEST => Err(anyhow!("bad request for {}", resp.url()).context(
            ApiError::BadRequest {
                body: resp.text().await?,
            },
        )),
        StatusCode::NOT_FOUND => Err(anyhow!(resp.text().await?).context(ApiError::NotFound)),
        StatusCode::TOO_MANY_REQUESTS => {
            let default = HeaderValue::from(0isize);
            let h = resp.headers().get("retry-after").unwrap_or(&default);
            Err(anyhow!(ApiError::TooManyRequests {
                retry_after: h.to_str()?.parse::<usize>()?,
            }))
        }
        status => Err(anyhow!("{}", resp.text().await?).context(ApiError::Other {
            status: status.into(),
        })),
    }
}

async fn make_request(local_cache: Arc<RwLock<Cache>>, req: Request) -> Result<Arc<Bytes>> {
    let client = reqwest::Client::new();

    let cache = local_cache.read().await;
    let query_str = req.query_params.map(|q| q.materialize());
    let cache_key = match req.cache_response {
        true => {
            // form a cache key = endpoint + cacheable query params
            let key = match query_str.as_ref() {
                Some(q) => format!("{}{}", req.endpoint, q.cacheable_query),
                None => req.endpoint.to_string(),
            };
            match &cache.get(&key) {
                Some(v) => {
                    return Ok(Arc::clone(v));
                }
                None => (),
            }
            Some(key)
        }
        false => None,
    };
    drop(cache);

    let full_url = match query_str {
        Some(q) => {
            format!("{}{}?{}", req.domain, req.endpoint, q.full_query)
        }
        None => format!("{}{}", req.domain, req.endpoint),
    };

    log::debug!("full url: {}", full_url);

    let mut r = client.get(&full_url);
    if let Some(h) = req.headers {
        r = r.headers(h);
    }
    let resp = r.send().await;

    match validate_response(resp?).await {
        Ok(resp) => {
            let resp_bytes = Arc::new(resp.bytes().await?);
            if let Some(cache_key) = cache_key {
                let cache = Arc::clone(&local_cache);
                cache
                    .write()
                    .await
                    .insert(cache_key, Arc::clone(&resp_bytes));
            }
            log::debug!("successful response");
            Ok(resp_bytes)
        }
        Err(err) => {
            log::debug!("response error: {:?}", err);
            Err(err)
        }
    }
}

// #[derive(Clone)]
pub struct ApiClient {
    cache: Arc<RwLock<Cache>>,
}

impl<'a> ApiClient {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(RwLock::new(Cache::new())),
        }
    }

    pub async fn make_request(
        &self,
        endpoint: &str,
        query_params: Option<Query>,
        headers: Option<HeaderMap>,
        cache_response: bool,
    ) -> Result<Arc<Bytes>> {
        let url = Url::parse(endpoint)?;
        let domain = format!(
            "{}://{}",
            url.scheme(),
            url.domain()
                .ok_or_else(|| anyhow!("missing domain in URL: {}", url.to_string()))?
        );
        let endpoint = url.path();

        log::debug!(
            "making request to {} {:?}",
            url.to_string(),
            query_params.as_ref().map(|q| q.materialize().full_query)
        );

        // fixme: accept Request once all usages have been replaced
        let mut req_builder = RequestBuilder::default();
        let mut req_builder = req_builder
            .domain(domain.to_string())
            .endpoint(endpoint.to_string());
        if query_params.is_some() {
            req_builder = req_builder.query_params(query_params.unwrap());
        }
        if headers.is_some() {
            req_builder = req_builder.headers(headers.unwrap());
        }
        let req = req_builder.cache_response(cache_response).build()?;
        make_request(self.cache.clone(), req).await
    }
}

impl Service<Request> for Arc<ApiClient> {
    type Response = Arc<Bytes>;

    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // always ready to make more requests!
    }

    fn call(&mut self, req: Request) -> Self::Future {
        Box::pin(make_request(self.cache.clone(), req))
    }
}

impl AsRef<ApiClient> for ApiClient {
    fn as_ref(&self) -> &ApiClient {
        &self
    }
}

#[derive(Builder, Clone)]
pub struct Request {
    pub domain: String,
    pub endpoint: String,
    #[builder(setter(strip_option), default)]
    pub query_params: Option<Query>,
    #[builder(setter(strip_option), default)]
    pub headers: Option<HeaderMap>,
    pub cache_response: bool,
}
