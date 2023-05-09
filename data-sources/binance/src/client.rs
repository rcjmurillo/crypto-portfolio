use std::{collections::HashMap, env, marker::PhantomData, sync::Arc};

use anyhow::{anyhow, Context, Result};

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Duration, DurationRound, Utc};
use futures::prelude::*;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    Url,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::Mutex;
use tower::{
    layer::Layer,
    limit::{rate::Rate, RateLimit},
    retry::{Policy, Retry, RetryLayer},
    Service, ServiceExt,
};

use api_client::{
    errors::ClientError, ApiClient, Cache, Query, RedisCache, Request, RequestBuilder, Response,
};
use exchange::{
    operations::storage::{Record, Redis as RedisStorage},
    Candle,
};
use market::{Market, MarketData};

use crate::{api_model::*, errors::Error as ApiError};

const API_DOMAIN_GLOBAL: &str = "https://api.binance.com";
const API_DOMAIN_US: &str = "https://api.binance.us";

#[derive(Copy, Clone)]
pub enum Region {
    Global,
    Us,
}

pub struct RegionUs;
pub struct RegionGlobal;

pub struct Credentials<Region> {
    api_key: String,
    secret_key: String,
    region: PhantomData<Region>,
}

impl<Region> Credentials<Region> {
    fn from_config(config: &Config) -> Self {
        Self {
            api_key: config.api_key.clone(),
            secret_key: config.secret_key.clone(),
            region: PhantomData,
        }
    }
}

impl Credentials<RegionGlobal> {
    fn new() -> Self {
        Self {
            api_key: env::var("BINANCE_API_KEY").expect("missing BINANCE_API_KEY env var"),
            secret_key: env::var("BINANCE_API_SECRET").expect("missing BINANCE_API_SECRET env var"),
            region: PhantomData,
        }
    }
}

impl Credentials<RegionUs> {
    fn new() -> Self {
        Self {
            api_key: env::var("BINANCE_API_KEY_US").unwrap(),
            secret_key: env::var("BINANCE_API_SECRET_US").unwrap(),
            region: PhantomData,
        }
    }
}

pub enum ApiUs {
    Trades,
    Klines,
    Prices,
    ExchangeInfo,
    Deposits,
    Withdraws,
    FiatDeposits,
    FiatWithdraws,
}

impl ApiUs {
    fn to_rate(&self) -> Rate {
        self.into()
    }
}

impl AsRef<str> for ApiUs {
    fn as_ref(&self) -> &str {
        match self {
            Self::Trades => "/api/v3/myTrades",
            Self::Klines => "/api/v3/klines",
            Self::Prices => "/api/v3/ticker/price",
            Self::ExchangeInfo => "/api/v3/exchangeInfo",
            Self::Deposits => "/sapi/v1/capital/deposit/hisrec",
            Self::Withdraws => "/sapi/v1/capital/withdraw/history",
            Self::FiatDeposits => "/sapi/v1/fiatpayment/query/deposit/history",
            Self::FiatWithdraws => "/sapi/v1/fiatpayment/query/withdraw/history",
        }
    }
}

impl ToString for ApiUs {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

impl From<&ApiUs> for Rate {
    fn from(v: &ApiUs) -> Self {
        match v {
            ApiUs::Deposits => Rate::new(1, Duration::seconds(10).to_std().unwrap()),
            ApiUs::Withdraws => Rate::new(1, Duration::seconds(10).to_std().unwrap()),
            ApiUs::FiatDeposits => Rate::new(1, Duration::seconds(10).to_std().unwrap()),
            ApiUs::FiatWithdraws => Rate::new(1, Duration::seconds(10).to_std().unwrap()),
            _ => Rate::new(1000, Duration::seconds(1).to_std().unwrap()),
        }
    }
}

pub enum ApiGlobal {
    Trades,
    Klines,
    Prices,
    ExchangeInfo,
    Deposits,
    Withdraws,
    FiatOrders,
    MarginTrades,
    MarginLoans,
    MarginRepays,
    CrossedMarginPairs,
    IsolatedMarginPairs,
    AllMarginAssets,
}

impl ApiGlobal {
    fn to_rate(&self) -> Rate {
        self.into()
    }
}

impl AsRef<str> for ApiGlobal {
    fn as_ref(&self) -> &str {
        match self {
            Self::Trades => "/api/v3/myTrades",
            Self::Klines => "/api/v3/klines",
            Self::Prices => "/api/v3/ticker/price",
            Self::ExchangeInfo => "/api/v3/exchangeInfo",
            Self::Deposits => "/sapi/v1/capital/deposit/hisrec",
            Self::Withdraws => "/sapi/v1/capital/withdraw/history",
            Self::FiatOrders => "/sapi/v1/fiat/orders",
            Self::MarginTrades => "/sapi/v1/margin/myTrades",
            Self::MarginLoans => "/sapi/v1/margin/loan",
            Self::MarginRepays => "/sapi/v1/margin/repay",
            Self::CrossedMarginPairs => "/sapi/v1/margin/allPairs",
            Self::IsolatedMarginPairs => "/sapi/v1/margin/isolated/allPairs",
            Self::AllMarginAssets => "/sapi/v1/margin/allAssets",
        }
    }
}

impl ToString for ApiGlobal {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

impl From<&ApiGlobal> for Rate {
    fn from(v: &ApiGlobal) -> Self {
        match v {
            ApiGlobal::FiatOrders => Rate::new(1, Duration::seconds(30).to_std().unwrap()),
            _ => Rate::new(1000, Duration::seconds(1).to_std().unwrap()),
        }
    }
}

macro_rules! endpoint_services {
    ($client:expr, $cache_storage:expr, $($endpoint:expr),+) => {{
        let retry_layer = RetryLayer::new(RetryErrorResponse(5));
        let mut services = HashMap::new();
        $(
            services.insert(
                $endpoint.to_string(),
                Mutex::new(
                    Cache::new(
                        RateLimit::new(retry_layer.layer($client), $endpoint.to_rate()),
                        $cache_storage
                    )
                )
            );
        )+
        services
    }};
}

struct EndpointServices<Region> {
    client: ApiClient,
    services:
        HashMap<String, Mutex<Cache<RateLimit<Retry<RetryErrorResponse, ApiClient>>, RedisCache>>>,
    region: PhantomData<Region>,
}

impl<Region> EndpointServices<Region> {
    pub async fn route(&self, request: Request) -> Result<Bytes> {
        log::trace!(
            "routing url: {}?{}",
            request.url,
            request
                .query_params
                .as_ref()
                .map(|qp| qp.materialize().full_query)
                .unwrap_or_default()
        );
        let mut svc = self
            .services
            .get(request.url.path())
            .ok_or(anyhow!("no service found for endpoint {}", request.url))?
            .lock()
            .await;

        svc.ready()
            .await?
            .call(request)
            .await
            .map_err(|e| anyhow!(e))
    }
}

impl EndpointServices<RegionUs> {
    pub fn new() -> Self {
        let client = ApiClient::new();

        let mut s = Self {
            client,
            services: HashMap::new(),
            region: PhantomData,
        };

        s.services = endpoint_services![
            s.client.clone(),
            RedisCache::new("0.0.0.0".to_string(), 6379).expect("couldn't create redis client"),
            ApiUs::Trades,
            ApiUs::Klines,
            ApiUs::Prices,
            ApiUs::ExchangeInfo,
            ApiUs::Deposits,
            ApiUs::Withdraws,
            ApiUs::FiatDeposits,
            ApiUs::FiatWithdraws
        ];
        s
    }
}

impl EndpointServices<RegionGlobal> {
    pub fn new() -> Self {
        let client = ApiClient::new();

        let mut s = Self {
            client,
            services: HashMap::new(),
            region: PhantomData,
        };

        s.services = endpoint_services![
            s.client.clone(),
            RedisCache::new("0.0.0.0".to_string(), 6379).expect("couldn't create redis client"),
            ApiUs::Trades,
            ApiUs::Klines,
            ApiUs::Prices,
            ApiUs::ExchangeInfo,
            ApiGlobal::Deposits,
            ApiGlobal::Withdraws,
            ApiGlobal::FiatOrders,
            ApiGlobal::MarginTrades,
            ApiGlobal::MarginLoans,
            ApiGlobal::MarginRepays,
            ApiGlobal::CrossedMarginPairs,
            ApiGlobal::IsolatedMarginPairs,
            ApiGlobal::AllMarginAssets
        ];
        s
    }
}

#[derive(Clone)]
pub struct Config {
    pub api_key: String,
    pub secret_key: String,
    pub start_date: DateTime<Utc>,
    pub symbols: Vec<Market>,
}

impl Config {
    pub fn empty() -> Self {
        Self {
            start_date: Utc::now(),
            symbols: Vec::new(),
            api_key: "".to_string(),
            secret_key: "".to_string(),
        }
    }
}

/// Implements a policy for errors responses that if retried have to eventually
/// succeed, but also don't retry indefinitely.
#[derive(Clone)]
struct RetryErrorResponse(u8);

impl Policy<Request, Response, ClientError> for RetryErrorResponse {
    type Future = future::Ready<Self>;

    fn retry(
        &self,
        req: &Request,
        result: Result<&Response, &ClientError>,
    ) -> Option<Self::Future> {
        match result {
            Ok(_) => {
                // Treat all `Response`s as success,
                // so don't retry...
                None
            }
            Err(err) => match err.into() {
                ApiError::InvalidTimestamp => {
                    log::debug!(
                        "it took too long to send the request for {} to, retrying",
                        req.url
                    );
                    if self.0 > 0 {
                        // Try again!
                        Some(future::ready(RetryErrorResponse(self.0 - 1)))
                    } else {
                        // Used all our attempts, no retry...
                        None
                    }
                }
                _ => None,
            },
        }
    }

    fn clone_request(&self, req: &Request) -> Option<Request> {
        Some(req.clone())
    }
}

pub struct BinanceFetcher<Region> {
    endpoint_services: EndpointServices<Region>,
    pub config: Option<Config>,
    pub api_client: ApiClient,
    pub credentials: Credentials<Region>,
    pub domain: &'static str,
    storage: RedisStorage,
}

impl<'a, Region> BinanceFetcher<Region> {
    fn data_start_date(&self) -> &DateTime<Utc> {
        &self
            .config
            .as_ref()
            .expect("missing config in BinanceFetcher")
            .start_date
    }

    pub fn symbols(&self) -> &Vec<Market> {
        &self
            .config
            .as_ref()
            .expect("missing config in BinanceFetcher")
            .symbols
    }

    async fn from_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(&resp_bytes.clone()) {
            Ok(val) => Ok(val),
            Err(err) => Err(anyhow!(err.to_string()).context(format!(
                "couldn't parse binance API response: {:?} {}",
                resp_bytes.clone(),
                std::any::type_name::<T>()
            ))),
        }
    }

    fn sign_request(&self, query: &mut Query) {
        let secret_key = self.credentials.secret_key.to_owned();
        query.on_materialize(move |mut query_str| {
            let mut signed_key = Hmac::<Sha256>::new_from_slice(secret_key.as_bytes()).unwrap();
            signed_key.update(query_str.full_query.as_bytes());
            let hexed_signature = hex_encode(signed_key.finalize().into_bytes());
            query_str.full_query =
                format!("{}&signature={}", query_str.full_query, hexed_signature);
            query_str
        });
    }

    fn default_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-mbx-apikey"),
            HeaderValue::from_str(self.credentials.api_key.as_str()).unwrap(),
        );
        headers
    }

    pub async fn fetch_exchange_symbols(&self, endpoint: &str) -> Result<Vec<Symbol>> {
        #[derive(Deserialize, Clone)]
        struct EndpointResponse {
            symbols: Vec<Symbol>,
        }

        let req = RequestBuilder::default()
            .url(Url::parse(self.domain)?.join(endpoint)?)
            .headers(self.default_headers())
            .cache_response(true)
            .build()?;
        let resp = self.endpoint_services.route(req).await?;

        let EndpointResponse { symbols } = self.from_json::<EndpointResponse>(&resp).await?;
        Ok(symbols)
    }

    pub async fn fetch_all_prices(&self, endpoint: &str) -> Result<Vec<SymbolPrice>> {
        let req = RequestBuilder::default()
            .url(Url::parse(self.domain)?.join(endpoint)?)
            .cache_response(true)
            .build()?;
        let resp = self.endpoint_services.route(req).await?;
        self.from_json(&resp).await
    }

    pub async fn fetch_price_at(
        &self,
        endpoint: &str,
        symbol: &str,
        datetime: &DateTime<Utc>,
    ) -> Result<f64> {
        let time = datetime.timestamp_millis();
        let start_time = time - 30 * 60 * 1000;
        let end_time = time + 30 * 60 * 1000;

        let mut query = Query::new();
        query
            .cached_param("symbol", symbol)
            .cached_param("interval", "1h")
            .cached_param("startTime", start_time)
            .cached_param("endTime", end_time);

        let req = RequestBuilder::default()
            .url(Url::parse(self.domain)?.join(endpoint)?)
            .query_params(query)
            .headers(self.default_headers())
            .cache_response(true)
            .build()?;

        let resp = self.endpoint_services.route(req).await?;

        let klines: Vec<Vec<Value>> = self.from_json(&resp).await?;

        if klines.is_empty() {
            return Err(anyhow!("couldn't find price for {symbol} at {datetime}"));
        }

        let s = &klines[0];
        let high = s[2].as_str().unwrap().parse::<f64>().unwrap();
        let low = s[3].as_str().unwrap().parse::<f64>().unwrap();
        Ok((high + low) / 2.0) // avg
    }

    pub async fn fetch_prices_in_range(
        &self,
        endpoint: &str,
        market: &Market,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<Candle>> {
        // Fetch the prices' 30m-candles from `start_ts` to `end_ts`.
        // The API only returns at max 1000 entries per request, thus the full
        // range needs to be split into buckets of 1000 30m-candles.

        // Shift the start and end times a bit to include both in the first and last buckets.
        let start_time = start_ts - 30 * 60 * 1000 * 2;
        let end_time = end_ts + 30 * 60 * 1000 * 2;

        let limit = 1000; // API response size limit

        let divmod = |a: u64, b: u64| (a / b, a % b);

        let candle_size_millis: u64 = 30 * 60 * 1000;
        let num_candles = match divmod(end_time - start_time, candle_size_millis) {
            (r, 0) => r,
            (r, _) => r + 1,
        };
        let num_batches = match divmod(num_candles, limit) {
            (r, 0) => r,
            (r, _) => r + 1,
        };
        let millis_per_batch = candle_size_millis * limit;
        // Generate the set of timestamp ranges to fetch from the API
        let ranges: Vec<(u64, u64)> = (0..num_batches)
            .scan(start_time, |current_ts, _| {
                let ts = *current_ts;
                *current_ts += millis_per_batch;
                Some((ts, *current_ts))
            })
            .collect();

        let endpoint = Arc::new(format!("{}{}", self.domain, endpoint));

        let mut handles = Vec::new();
        let mut all_prices = Vec::new();

        for (start, end) in ranges {
            let mut query = Query::new();
            query
                .cached_param("symbol", market.join(""))
                .cached_param("interval", "30m")
                .cached_param("startTime", start)
                .cached_param("endTime", end)
                .cached_param("limit", limit);
            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint.as_str())?)
                .query_params(query)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;
            handles.push(self.endpoint_services.route(req));
        }

        for resp in stream::iter(handles)
            .buffer_unordered(500)
            .collect::<Vec<_>>()
            .await
        {
            match resp {
                Ok(resp) => {
                    let klines = self.from_json::<Vec<Vec<Value>>>(&resp).await?;
                    all_prices.extend(klines.iter().map(|x| Candle {
                        open_time: x[0].as_u64().unwrap(),
                        close_time: x[6].as_u64().unwrap(),
                        open_price: x[1].as_str().unwrap().parse::<f64>().unwrap(),
                        close_price: x[4].as_str().unwrap().parse::<f64>().unwrap(),
                    }));
                }
                Err(err) => match err.downcast::<ClientError>() {
                    Ok(client_error) => {
                        return Err(anyhow!(format!(
                            "couldn't fetch prices for symbol: {}",
                            market.join("")
                        ))
                        .context(client_error))
                    }
                    Err(err) => return Err(err),
                },
            }
        }
        all_prices.sort_by_key(|c| c.close_time);
        Ok(all_prices)
    }

    async fn fetch_trades_from_endpoint(
        &self,
        symbol: &Market,
        endpoint: &str,
        extra_params: Option<Query>,
    ) -> Result<Vec<Trade>> {
        let storage_key = format!(
            "binance.trade[d={},e={},ep={}]",
            self.domain,
            endpoint,
            extra_params
                .as_ref()
                .map_or_else(|| "none".to_string(), |q| q.materialize().cacheable_query)
        );

        let trades: Vec<Trade> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::new());
        let mut new_trades = Vec::new();
        let mut last_id = trades.last().map(|t| t.id + 1).unwrap_or(0);

        let mut query = Query::new();
        query
            .cached_param("symbol", symbol.join(""))
            .cached_param("fromId", last_id)
            .cached_param("limit", 1000)
            .cached_param("recvWindow", 60000)
            .lazy_param("timestamp", || Utc::now().timestamp_millis().to_string());
        self.sign_request(&mut query);
        if let Some(extra_params) = extra_params {
            query.merge(extra_params);
        }

        loop {
            let mut q = query.clone();
            q.cached_param("fromId", last_id);
            let _qstr = q.materialize().full_query;

            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint)?)
                .query_params(q)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;
            let resp = self.endpoint_services.route(req).await?;

            // match resp {
            // Ok(resp) => {
            let mut binance_trades = self
                .from_json::<Vec<Trade>>(&resp)
                .await
                .map_err(|err| anyhow!("response error").context(err))?;
            binance_trades.sort_by_key(|k| k.time);
            let fetch_more = binance_trades.len() >= 1000;
            log::debug!(
                "getting more trades for {} got {}",
                symbol.join(""),
                binance_trades.len()
            );
            if fetch_more {
                // the API will return id >= fromId, thus add one to not include
                // the last processed id.
                last_id = binance_trades.iter().last().unwrap().id + 1;
                log::debug!("last_id updated to {}", last_id);
            };
            for mut t in binance_trades.into_iter() {
                t.base_asset = Some(symbol.base.clone());
                t.quote_asset = Some(symbol.quote.clone());
                new_trades.push(t);
            }
            if !fetch_more {
                break;
            }
            // }
            // Err(err) => {
            //     println!("error when fetching margin trades: {err:?}");
            // let client_error = err.downcast::<ClientError>()?;
            // log::error!("error when fetching trades from {endpoint}: {client_error}");
            // return Err(anyhow!(client_error).context(format!(
            //     "couldn't fetch trades from {}{:?} for symbol: {}",
            //     endpoint,
            //     qstr,
            //     symbol.join("")
            // )));
            //     return Err(err);
            // }
        }
        // }

        self.storage
            .insert_records(&storage_key, &new_trades)
            .await?;
        let mut trades = trades;
        trades.extend(new_trades);
        Ok(trades)
    }

    pub async fn fetch_trades(&self, endpoint: &str, symbol: &Market) -> Result<Vec<Trade>> {
        self.fetch_trades_from_endpoint(symbol, endpoint, None)
            .await
    }
}

impl BinanceFetcher<RegionGlobal> {
    pub fn new() -> Self {
        Self {
            endpoint_services: EndpointServices::<RegionGlobal>::new(),
            config: None,
            credentials: Credentials::<RegionGlobal>::new(),
            domain: API_DOMAIN_GLOBAL,
            api_client: ApiClient::new(),
            storage: RedisStorage::new("0.0.0.0".to_string(), 6379).unwrap(),
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(),
            credentials: Credentials::from_config(&config),
            config: Some(config),
            domain: API_DOMAIN_GLOBAL,
            endpoint_services: EndpointServices::<RegionGlobal>::new(),
            storage: RedisStorage::new("0.0.0.0".to_string(), 6379).unwrap(),
        }
    }

    pub async fn fetch_fiat_orders(&self, tx_type: &str) -> Result<Vec<FiatOrder>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            data: Vec<FiatOrder>,
        }

        let storage_key = format!("binance.fiat_orders[d={},t={}]", self.domain, tx_type);

        let orders: Vec<FiatOrder> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::new());
        let mut new_orders = Vec::new();
        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = orders
            .last()
            .map(|x| x.create_time)
            .unwrap_or(*self.data_start_date());

        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            let mut current_page = 1usize;

            let mut query = Query::new();
            query
                .cached_param("transactionType", tx_type)
                .cached_param("beginTime", curr_start.timestamp_millis())
                .cached_param("endTime", end.timestamp_millis())
                .lazy_param("timestamp", || Utc::now().timestamp_millis().to_string())
                .cached_param("recvWindow", 60000)
                .cached_param("rows", 500);
            self.sign_request(&mut query);

            loop {
                let mut q = query.clone();
                q.cached_param("page", current_page);

                let endpoint = ApiGlobal::FiatOrders.to_string();
                let req = RequestBuilder::default()
                    .url(Url::parse(self.domain)?.join(endpoint.as_str())?)
                    .query_params(q)
                    .headers(self.default_headers())
                    .cache_response(true)
                    .build()?;

                let resp = self.endpoint_services.route(req).await;

                match resp {
                    Ok(resp) => {
                        let Response { data } = self.from_json(&resp).await.context("query")?;
                        if data.len() > 0 {
                            new_orders
                                .extend(data.into_iter().filter(|x| x.status == "Successful"));
                            current_page += 1;
                        } else {
                            break;
                        }
                    }
                    Err(e) => return Err(e),
                }
            }

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        self.storage
            .insert_records(&storage_key, &new_orders)
            .await?;

        let mut orders = orders;
        orders.extend(new_orders);

        Ok(orders)
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders("0").await
    }

    pub async fn fetch_fiat_withdrawals(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders("1").await
    }

    /// Fetch from endpoint with no params and deserialize to the specified type
    async fn fetch_from_endpoint<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let mut query = Query::new();
        query
            .lazy_param("timestamp", || {
                Utc::now().naive_utc().timestamp_millis().to_string()
            })
            .cached_param("recvWindow", 60000);
        self.sign_request(&mut query);

        let req = RequestBuilder::default()
            .url(Url::parse(self.domain)?.join(endpoint)?)
            .query_params(query)
            .headers(self.default_headers())
            .cache_response(true)
            .build()?;

        let resp = self.endpoint_services.route(req).await?;

        self.from_json::<T>(&resp).await
    }

    async fn fetch_margin_pairs(&self, endpoint: &str) -> Result<Vec<Market>> {
        #[derive(Deserialize)]
        struct Pair {
            base: String,
            quote: String,
        }

        let resp = self.fetch_from_endpoint::<Vec<Pair>>(endpoint).await?;
        Ok(resp
            .into_iter()
            .map(|p| Market::new(p.base, p.quote))
            .collect())
    }

    pub async fn fetch_margin_trades(&self, symbol: &Market) -> Result<Vec<Trade>> {
        let crossed_margin_pairs = self
            .fetch_margin_pairs(&ApiGlobal::CrossedMarginPairs.to_string())
            .await?;
        let isolated_margin_pairs = self
            .fetch_margin_pairs(&ApiGlobal::IsolatedMarginPairs.to_string())
            .await?;

        let endpoint = ApiGlobal::MarginTrades.to_string();
        let mut trades = Vec::new();

        for is_isolated in &[true, false] {
            let mut extra_params = Query::new();
            extra_params.cached_param("isIsolated", if *is_isolated { "TRUE" } else { "FALSE" });
            if *is_isolated {
                if !isolated_margin_pairs.contains(symbol) {
                    break;
                }
            } else if !crossed_margin_pairs.contains(symbol) {
                break;
            }
            let result = self
                .fetch_trades_from_endpoint(symbol, &endpoint, Some(extra_params))
                .await;

            match result {
                Ok(result_trades) => trades.extend(result_trades),
                Err(err) => {
                    match err.downcast::<ClientError>() {
                        Ok(client_error) => {
                            match client_error.into() {
                                // ApiErrorKind::UnavailableSymbol means the symbol is not
                                // available in the exchange, so we can just ignore it.
                                // Even though the margin pairs are verified above, sometimes
                                // the data returned by the exchange is not accurate.
                                ApiError::UnavailableSymbol => {
                                    log::warn!(
                                        "ignoring symbol {} for margin trades",
                                        symbol.join(""),
                                    );
                                    break;
                                }
                                err => return Err(anyhow!(err)),
                            }
                        }
                        Err(err) => return Err(anyhow!(err)),
                    }
                }
            }

            // futures.push(fut);
        }

        // let results = futures::stream::iter(futures)
        //     .buffer_unordered(1000)
        //     .collect::<Vec<Result<Vec<Trade>>>>()
        //     .await;
        // for result in results {
        //     match result {
        //         Ok(result_trades) => trades.extend(result_trades),
        //         Err(err) => match err.downcast::<ClientError>() {
        //             Ok(client_error) => {
        //                 println!("request error: {client_error:?}");
        //                 match client_error.into() {
        //                     // ApiErrorKind::UnavailableSymbol means the symbol is not
        //                     // available in the exchange, so we can just ignore it.
        //                     // Even though the margin pairs are verified above, sometimes
        //                     // the data returned by the exchange is not accurate.
        //                     ApiError::Api(ApiErrorKind::UnavailableSymbol) => {
        //                         log::debug!(
        //                             "ignoring symbol {} for margin trades",
        //                             symbol.join(""),
        //                         );
        //                         break;
        //                     }
        //                     err => {
        //                         println!("downcasted another error: {err:?}");
        //                         return Err(anyhow!(err));
        //                     },
        //                 }
        //             }
        //             Err(err) => {
        //                 println!("downcast error: {err:?}");
        //                 return Err(err);
        //             },
        //         },
        //     }
        // }

        Ok(trades)
    }

    pub async fn fetch_margin_transactions<T>(
        &self,
        asset: &String,
        isolated_symbol: Option<&Market>,
        endpoint: &str,
    ) -> Result<Vec<T>>
    where
        T: Serialize + DeserializeOwned + Record<Id = u64>,
    {
        #[derive(Deserialize)]
        struct Response<U> {
            rows: Vec<U>,
            total: u16,
        }

        let storage_key = format!(
            "binance.margin_transactions[d={},e={endpoint},s={isolated_symbol:?}]",
            self.domain
        );
        let txns: Vec<T> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::<T>::new());
        let mut new_txns = Vec::new();

        let now = Utc::now();
        let start = txns
            .last()
            .map(|tx| tx.datetime() + Duration::milliseconds(1))
            .unwrap_or_else(|| *self.data_start_date());
        let archived_cutoff = now - Duration::days(30 * 6);
        // round archived_cutoff to the last millisecond of the day
        let archived_cutoff =
            archived_cutoff.duration_trunc(Duration::days(1)).unwrap() - Duration::milliseconds(1);

        // ranges to query for archived/recent trades
        let ranges = if start < archived_cutoff {
            vec![
                (start, archived_cutoff, true),
                (archived_cutoff + Duration::milliseconds(1), now, false),
            ]
        } else {
            vec![(start, now, false)]
        };

        for (rstart, rend, archived) in ranges {
            let mut curr_start = rstart;

            loop {
                // the API only allows 90 days between start and end
                let curr_end = std::cmp::min(curr_start + Duration::days(89), rend); // inclusive
                let mut current_page: usize = 1;

                loop {
                    let mut query = Query::new();
                    query.cached_param("asset", &asset);
                    if let Some(s) = isolated_symbol {
                        query.cached_param("isolatedSymbol", s.join(""));
                    }
                    query
                        .cached_param("startTime", curr_start.timestamp_millis())
                        .cached_param("endTime", curr_end.timestamp_millis())
                        .cached_param("size", 100)
                        .cached_param("archived", archived)
                        .cached_param("current", current_page)
                        .lazy_param("timestamp", || Utc::now().timestamp_millis().to_string())
                        .cached_param("recvWindow", 60000);
                    self.sign_request(&mut query);

                    let req = RequestBuilder::default()
                        .url(Url::parse(self.domain)?.join(endpoint)?)
                        .query_params(query)
                        .headers(self.default_headers())
                        .cache_response(true)
                        .build()?;

                    let resp = self.endpoint_services.route(req).await?;

                    let txns_resp = self.from_json::<Response<T>>(&resp).await;
                    match txns_resp {
                        Ok(result_txns) => {
                            new_txns.extend(result_txns.rows);
                            if result_txns.total >= 100 {
                                current_page += 1;
                            } else {
                                break;
                            }
                        }
                        Err(err) => {
                            match err.downcast::<ClientError>() {
                                Ok(client_error) => {
                                    match client_error.into() {
                                        // ApiErrorKind::UnavailableSymbol means the symbol is not
                                        // available in the exchange, so we can just ignore it.
                                        // Even though the margin pairs are verified above, sometimes
                                        // the data returned by the exchange is not accurate.
                                        ApiError::UnavailableSymbol => {
                                            log::warn!(
                                                "ignoring asset {} for margin trades isolated_symbol={:?}",
                                                asset,
                                                isolated_symbol.and_then(|a| Some(a.join(""))).unwrap_or_else(|| "".to_string())
                                            );
                                            break;
                                        }
                                        ApiError::InvalidTimestamp => {
                                            log::error!(
                                                "it took to long to send the request for asset={} isolated_symbol={}, retrying",
                                                asset,
                                                isolated_symbol.and_then(|a| Some(a.join(""))).unwrap_or_else(|| "".to_string())
                                            );
                                            continue;
                                        }
                                        err => {
                                            log::error!("err: {:?}", err);
                                            return Err(anyhow!(err));
                                        }
                                    }
                                }
                                Err(err) => {
                                    log::error!("err: {:?}", err);
                                    return Err(err);
                                }
                            }
                        }
                    }
                }

                // move to the next date
                curr_start = curr_end + Duration::milliseconds(1);
                if curr_end == rend {
                    break;
                }
            }
        }

        self.storage.insert_records(&storage_key, &txns).await?;
        let mut txns = txns;
        txns.extend(new_txns);

        Ok(txns)
    }

    pub async fn fetch_margin_loans(
        &self,
        asset: &String,
        isolated_symbol: Option<&Market>,
    ) -> Result<Vec<MarginLoan>> {
        self.fetch_margin_transactions(asset, isolated_symbol, &ApiGlobal::MarginLoans.to_string())
            .await
    }

    pub async fn fetch_margin_repays(
        &self,
        asset: &String,
        isolated_symbol: Option<&Market>,
    ) -> Result<Vec<MarginRepay>> {
        self.fetch_margin_transactions(asset, isolated_symbol, &ApiGlobal::MarginRepays.to_string())
            .await
    }

    pub async fn fetch_deposits(&self) -> Result<Vec<Deposit>> {
        let endpoint = ApiGlobal::Deposits.to_string();
        let storage_key = format!("binance.crypto_deposits[d={},e={}]", self.domain, endpoint);
        let mut deposits: Vec<Deposit> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::<Deposit>::new());
        let mut new_deposits = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = deposits
            .last()
            .map(|d| (d.insert_time + Duration::milliseconds(1)))
            .unwrap_or_else(|| {
                self.data_start_date()
                    .duration_trunc(Duration::days(1))
                    .unwrap()
            });
        loop {
            let now = Utc::now();
            // the API only allows max 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            let mut query = Query::new();
            query
                .lazy_param("timestamp", move || now.timestamp_millis().to_string())
                .cached_param("recvWindow", 60000)
                .cached_param("startTime", curr_start.timestamp_millis())
                .cached_param("endTime", end.timestamp_millis());
            self.sign_request(&mut query);

            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint.as_str())?)
                .query_params(query)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;

            let resp = self.endpoint_services.route(req).await?;

            let deposit_list: Vec<Deposit> = self.from_json(&resp).await?;
            new_deposits.extend(deposit_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        self.storage
            .insert_records(&storage_key, &new_deposits)
            .await?;
        deposits.extend(new_deposits);

        Ok(deposits)
    }

    pub async fn fetch_withdrawals(&self) -> Result<Vec<Withdraw>> {
        let endpoint = ApiGlobal::Withdraws.to_string();
        let storage_key = format!(
            "binance.crypto_withdrawals[d={},e={}]",
            self.domain, endpoint
        );
        let mut withdrawals: Vec<Withdraw> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::<Withdraw>::new());
        let mut new_withdrawals = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = withdrawals
            .last()
            .map(|w| (w.apply_time + Duration::milliseconds(1)))
            .unwrap_or_else(|| {
                self.data_start_date()
                    .duration_trunc(Duration::days(1))
                    .unwrap()
            });

        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(90), now);

            let mut query = Query::new();
            query
                .lazy_param("timestamp", move || now.timestamp_millis().to_string())
                .cached_param("recvWindow", 60000)
                .cached_param("startTime", curr_start.timestamp_millis())
                .cached_param("endTime", end.timestamp_millis());
            self.sign_request(&mut query);

            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint.as_str())?)
                .query_params(query)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;

            let resp = self.endpoint_services.route(req).await?;

            let withdraw_list: Vec<Withdraw> = self.from_json(&resp).await?;
            new_withdrawals.extend(withdraw_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        self.storage
            .insert_records(&storage_key, &new_withdrawals)
            .await?;
        withdrawals.extend(new_withdrawals);

        Ok(withdrawals)
    }
}

impl BinanceFetcher<RegionUs> {
    pub fn new() -> Self {
        Self {
            api_client: ApiClient::new(),
            config: None,
            credentials: Credentials::<RegionUs>::new(),
            domain: API_DOMAIN_US,
            endpoint_services: EndpointServices::<RegionUs>::new(),
            storage: RedisStorage::new("0.0.0.0".to_string(), 6379).unwrap(),
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(),
            credentials: Credentials::from_config(&config),
            config: Some(config),
            domain: API_DOMAIN_US,
            endpoint_services: EndpointServices::<RegionUs>::new(),
            storage: RedisStorage::new("0.0.0.0".to_string(), 6379).unwrap(),
        }
    }

    pub async fn fetch_fiat_orders(&self, endpoint: &str) -> Result<Vec<FiatOrder>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            asset_log_record_list: Vec<FiatOrder>,
        }

        let storage_key = format!("binance.fiat_orders[d={},e={}]", self.domain, endpoint);
        let mut orders: Vec<FiatOrder> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::<FiatOrder>::new());
        let mut new_orders = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = orders
            .last()
            .map(|o| (o.create_time + Duration::milliseconds(1)))
            .unwrap_or_else(|| {
                self.data_start_date()
                    .duration_trunc(Duration::days(1))
                    .unwrap()
            });

        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            let mut query = Query::new();
            query
                // .cached_param("fiatCurrency", "USD")
                .cached_param("endTime", end.timestamp_millis())
                .cached_param("startTime", curr_start.timestamp_millis())
                .lazy_param("timestamp", move || {
                    Utc::now().timestamp_millis().to_string()
                })
                .cached_param("recvWindow", 60000);
            self.sign_request(&mut query);

            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint)?)
                .query_params(query)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;

            let resp = self.endpoint_services.route(req).await?;

            let Response {
                asset_log_record_list,
            } = self.from_json(&&resp).await?;
            new_orders.extend(
                asset_log_record_list
                    .into_iter()
                    .filter(|x| x.status == "Successful"),
            );
            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        self.storage
            .insert_records(&storage_key, &new_orders)
            .await?;
        orders.extend(new_orders);

        Ok(orders)
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders(&ApiUs::FiatDeposits.to_string())
            .await
    }

    pub async fn fetch_fiat_withdrawals(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders(&ApiUs::FiatWithdraws.to_string())
            .await
    }

    pub async fn fetch_deposits(&self) -> Result<Vec<Deposit>> {
        let endpoint = ApiUs::Deposits.to_string();
        let storage_key = format!("binance.crypto_deposits[d={},e={}]", self.domain, endpoint);
        let mut deposits: Vec<Deposit> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::<Deposit>::new());
        let mut new_deposits = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = deposits
            .last()
            .map(|d| (d.insert_time + Duration::milliseconds(1)))
            .unwrap_or_else(|| {
                self.data_start_date()
                    .duration_trunc(Duration::days(1))
                    .unwrap()
            });
        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(90), now);

            let mut query = Query::new();
            query
                .lazy_param("timestamp", move || now.timestamp_millis().to_string())
                .cached_param("recvWindow", 60000)
                .cached_param("startTime", curr_start.timestamp_millis())
                .cached_param("endTime", end.timestamp_millis())
                .cached_param("status", 1);
            self.sign_request(&mut query);

            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint.as_str())?)
                .query_params(query)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;

            let resp = self.endpoint_services.route(req).await?;

            let deposit_list: Vec<Deposit> = self.from_json(&resp).await?;
            new_deposits.extend(deposit_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        self.storage
            .insert_records(&storage_key, &new_deposits)
            .await?;
        deposits.extend(new_deposits);

        Ok(deposits)
    }

    pub async fn fetch_withdrawals(&self) -> Result<Vec<Withdraw>> {
        let endpoint = ApiUs::Withdraws.to_string();
        let storage_key = format!(
            "binance.crypto_withdrawals[d={},e={}]",
            self.domain, endpoint
        );
        let mut withdrawals: Vec<Withdraw> = self
            .storage
            .get_records(&storage_key)
            .await?
            .unwrap_or_else(|| Vec::<Withdraw>::new());
        let mut new_withdrawals = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = withdrawals
            .last()
            .map(|w| (w.apply_time + Duration::milliseconds(1)))
            .unwrap_or_else(|| {
                self.data_start_date()
                    .duration_trunc(Duration::days(1))
                    .unwrap()
            });
        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(90), now);

            let mut query = Query::new();
            query
                .cached_param("recvWindow", 60000)
                .cached_param("startTime", curr_start.timestamp_millis())
                .cached_param("endTime", end.timestamp_millis())
                .lazy_param("timestamp", move || now.timestamp_millis().to_string());
            self.sign_request(&mut query);

            let req = RequestBuilder::default()
                .url(Url::parse(self.domain)?.join(endpoint.as_str())?)
                .query_params(query)
                .headers(self.default_headers())
                .cache_response(true)
                .build()?;

            let resp = self.endpoint_services.route(req).await?;

            let withdraw_list: Vec<Withdraw> = self.from_json(&resp).await?;
            new_withdrawals.extend(withdraw_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        self.storage
            .insert_records(&storage_key, &new_withdrawals)
            .await?;
        withdrawals.extend(new_withdrawals);

        Ok(withdrawals)
    }
}

#[async_trait]
impl MarketData for BinanceFetcher<RegionGlobal> {
    async fn has_market(&self, market: &Market) -> Result<bool> {
        let symbols = self
            .fetch_exchange_symbols(ApiGlobal::ExchangeInfo.as_ref())
            .await?;
        Ok(symbols
            .iter()
            .find(|s| s.base_asset == market.base && s.quote_asset == market.quote)
            .is_some())
    }

    fn normalize(&self, market: &Market) -> Result<Market> {
        Ok(Market::new(
            market.base.to_ascii_uppercase(),
            market.quote.to_ascii_uppercase(),
        ))
    }

    async fn markets(&self) -> Result<Vec<Market>> {
        let symbols = self
            .fetch_exchange_symbols(ApiGlobal::ExchangeInfo.as_ref())
            .await?;
        Ok(symbols
            .into_iter()
            .map(|s| Market::new(s.base_asset, s.quote_asset))
            .collect())
    }

    async fn price_at(&self, market: &Market, time: &DateTime<Utc>) -> Result<f64> {
        self.fetch_price_at(
            &ApiGlobal::Klines.as_ref(),
            &format!("{}{}", market.base, market.quote),
            time,
        )
        .await
    }
}
