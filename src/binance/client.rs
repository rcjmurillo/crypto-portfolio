use std::{collections::HashMap, env, sync::Arc};

use async_trait::async_trait;
use binance::{
    api::Binance,
    config::Config,
    market::Market,
    model::{Prices, SymbolPrice},
};
use bytes::Bytes;
use chrono::{prelude::*, Duration};
use futures::future::join_all;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use reqwest::{
    header::{HeaderMap, HeaderName, HeaderValue},
    StatusCode,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::RwLock;

use crate::{
    binance::response_model::*, data_fetch::ExchangeClient, errors::Error, result::Result,
    sync::ValueLock,
};

const ENDPOINT_CONCURRENCY: usize = 10;

#[derive(Copy, Clone)]
pub enum Region {
    Global,
    Us,
}

struct Credentials {
    api_key: String,
    secret_key: String,
}

impl From<Region> for Credentials {
    fn from(region: Region) -> Self {
        match region {
            Region::Global => Credentials {
                api_key: env::var("API_KEY").unwrap(),
                secret_key: env::var("API_SECRET").unwrap(),
            },
            Region::Us => Credentials {
                api_key: env::var("API_KEY_US").unwrap(),
                secret_key: env::var("API_SECRET_US").unwrap(),
            },
        }
    }
}

struct Endpoints {
    fiat_deposits: Option<&'static str>,
    deposits: &'static str,
    withdraws: Option<&'static str>,
    trades: &'static str,
    margin_trades: Option<&'static str>,
    margin_borrows: Option<&'static str>,
    margin_repays: Option<&'static str>,
    klines: &'static str,
    exchange_info: &'static str,
}

impl From<Region> for Endpoints {
    fn from(region: Region) -> Self {
        match region {
            Region::Global => Endpoints {
                deposits: "/wapi/v3/depositHistory.html",
                fiat_deposits: None,
                trades: "/api/v3/myTrades",
                klines: "/api/v3/klines",
                exchange_info: "/api/v3/exchangeInfo".into(),
                withdraws: Some("/wapi/v3/withdrawHistory.html"),
                margin_trades: Some("/sapi/v1/margin/myTrades"),
                margin_borrows: Some("/sapi/v1/margin/loan"),
                margin_repays: Some("/sapi/v1/margin/repay"),
            },
            Region::Us => Endpoints {
                deposits: "/wapi/v3/depositHistory.html",
                fiat_deposits: Some("/sapi/v1/fiatpayment/query/deposit/history"),
                trades: "/api/v3/myTrades",
                klines: "/api/v3/klines",
                exchange_info: "/api/v3/exchangeInfo".into(),
                withdraws: None,
                margin_trades: None,
                margin_borrows: None,
                margin_repays: None,
            },
        }
    }
}

type Domain = String;

impl From<Region> for Domain {
    fn from(region: Region) -> Self {
        match region {
            Region::Global => String::from("https://api3.binance.com"),
            Region::Us => String::from("https://api.binance.us"),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Response<T> {
    Success(T),
    Error { code: i16, msg: String },
}

type Cache = HashMap<String, Bytes>;

#[derive(Clone)]
struct QueryParams(Vec<(&'static str, String)>);

impl QueryParams {
    fn new() -> Self {
        Self(Vec::new())
    }

    fn add<T: ToString>(&mut self, name: &'static str, value: T) {
        self.0.push((name, value.to_string()));
    }

    fn extend(&mut self, params: QueryParams) {
        self.0.extend(params.0);
    }

    fn to_string(&self) -> String {
        self.0
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect::<Vec<String>>()
            .join("&")
    }
}

pub struct BinanceFetcher {
    market_client: Market,
    credentials: Credentials,
    region: Region,
    endpoints: Endpoints,
    endpoint_params_lock: ValueLock<String>,
    endpoint_lock: ValueLock<String>,
    domain: Domain,
    cache: Arc<RwLock<Cache>>,
}

impl BinanceFetcher {
    pub fn new(region: Region) -> Self {
        let credentials: Credentials = region.into();
        let config = if let Region::Global = region {
            Config::default()
        } else {
            Config {
                rest_api_endpoint: "https://api.binance.us".into(),
                ws_endpoint: "wss://stream.binance.us.9443/ws".into(),

                futures_rest_api_endpoint: "https://fapi.binance.com".into(),
                futures_ws_endpoint: "wss://fstream.binance.com".into(),

                recv_window: 6000,
            }
        };
        Self {
            market_client: Binance::new_with_config(
                Some(credentials.api_key.clone()),
                Some(credentials.secret_key.clone()),
                &config,
            ),
            credentials,
            region,
            endpoints: region.into(),
            endpoint_params_lock: ValueLock::new(),
            endpoint_lock: ValueLock::with_capacity(ENDPOINT_CONCURRENCY),
            domain: region.into(),
            cache: Arc::new(RwLock::new(Cache::new())),
        }
    }

    async fn make_request<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        query: Option<QueryParams>,
        signed: bool,
        add_timestamp: bool,
        cache: bool,
    ) -> Result<T> {
        let client = reqwest::Client::new();

        let (query, cache_key) = match query {
            Some(mut query) => {
                let query_str = query.to_string();
                if add_timestamp {
                    query.add("recvWindow", 60000);
                    query.add("timestamp", Utc::now().timestamp_millis());
                }
                if signed {
                    query = self.sign_request(query);
                }
                (
                    query,
                    // form a cache key = endpoint + query params
                    format!("{}{}", endpoint, query_str),
                )
            }
            None => (QueryParams::new(), endpoint.to_string()),
        };

        let _endpoint_params_lock;
        if cache {
            // Lock this endpoint with the cacheable params until the response is added to the cache
            _endpoint_params_lock = self
                .endpoint_params_lock
                .lock_for(cache_key.clone())
                .await?;
            match Arc::clone(&self.cache).read().await.get(&cache_key) {
                Some(v) => {
                    return self.as_json(&v).await;
                }
                None => (),
            }
        }

        // Restrict concurrency for each API endpoint, this allows `ENDPOINT_CONCURRENCY` requests to
        // be awaiting on the same endpoint at the same time.
        let _endpoint_lock = self.endpoint_lock.lock_for(endpoint.to_string()).await?;

        let full_url = format!("{}{}?{}", self.domain, endpoint, query.to_string());

        let r = client
            .get(&full_url)
            .headers(self.default_headers())
            .timeout(std::time::Duration::from_secs(60))
            .send()
            .await;

        let x = match r {
            Ok(mut resp) => {
                resp = self.validate_response(resp).await?;
                let resp_bytes = resp.bytes().await?;

                if cache {
                    Arc::clone(&self.cache)
                        .write()
                        .await
                        .insert(cache_key, resp_bytes.clone());
                }
                // parse the json response into the provided struct
                match self.as_json::<T>(&resp_bytes).await {
                    Ok(v) => Ok(v),
                    Err(err) => {
                        println!("could not parse response: {:?}", resp_bytes);
                        Err(err)
                    }
                }
            }
            Err(err) => Err(err.into()),
        };
        x
    }

    async fn validate_response(&self, resp: reqwest::Response) -> Result<reqwest::Response> {
        match resp.status() {
            StatusCode::OK => Ok(resp),
            StatusCode::INTERNAL_SERVER_ERROR => Err(Error::new("Internal Server Error".into())),
            StatusCode::SERVICE_UNAVAILABLE => Err(Error::new("Service Unavailable".into())),
            StatusCode::UNAUTHORIZED => Err(Error::new("Unauthorized".into())),
            // this type of response is in json format with an error message
            // the caller should be able to parse it.
            StatusCode::BAD_REQUEST => Ok(resp),
            s => Err(Error::new(format!(
                "Received response status={:?} text={:?}",
                s,
                resp.text().await?
            ))),
        }
    }

    async fn as_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(resp_bytes) {
            Ok(val) => Ok(val),
            Err(err) => Err(err.into()),
        }
    }

    fn sign_request(&self, mut query_params: QueryParams) -> QueryParams {
        let mut signed_key =
            Hmac::<Sha256>::new_from_slice(self.credentials.secret_key.as_bytes()).unwrap();
        signed_key.update(query_params.to_string().as_ref());
        let signature = hex_encode(signed_key.finalize().into_bytes());
        query_params.add("signature", signature);
        query_params
    }

    fn default_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            HeaderName::from_static("x-mbx-apikey"),
            HeaderValue::from_str(self.credentials.api_key.as_str()).unwrap(),
        );
        headers
    }

    pub async fn exchange_symbols(&self) -> Result<Vec<Symbol>> {
        #[derive(Deserialize, Clone)]
        struct EndpointResponse {
            symbols: Vec<Symbol>,
        }

        let resp = self
            .make_request(self.endpoints.exchange_info, None, false, false, true)
            .await;
        match resp {
            Ok(Response::Success(EndpointResponse { symbols })) => Ok(symbols),
            Ok(Response::Error { code, msg }) => Err(Error::new(format!(
                "error from API: {:?} code: {}",
                msg, code
            ))),
            Err(err) => {
                println!("could not parse exchange info response: {:?}", err);
                Err(err)
            }
        }
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatDeposit>> {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        #[serde(rename_all = "camelCase")]
        struct DepositResponse {
            asset_log_record_list: Vec<FiatDeposit>,
        }

        if let None = self.endpoints.fiat_deposits {
            return Ok(vec![]);
        }

        let now = Utc::now();

        let mut deposits: Vec<FiatDeposit> = Vec::new();
        // TODO: devise a better way to fetch all deposits
        for i in 0..5 {
            let start = now - Duration::days(90 * (i + 1));
            let end = now - Duration::days(90 * i);

            let mut query = QueryParams::new();
            query.add("startTime", start.timestamp_millis());
            query.add("endTime", end.timestamp_millis());

            let result_deposits = self
                .make_request::<DepositResponse>(
                    self.endpoints.fiat_deposits.as_ref().unwrap(),
                    Some(query),
                    true,
                    true,
                    true,
                )
                .await?;

            deposits.extend(
                result_deposits
                    .asset_log_record_list
                    .into_iter()
                    .filter_map(|x| {
                        if x.order_status == "Successful" {
                            Some(x)
                        } else {
                            None
                        }
                    }),
            );
        }

        Ok(deposits)
    }

    pub async fn fetch_withdraws(&self) -> Result<Vec<Withdraw>> {
        #[derive(Debug, Serialize, Deserialize, Clone)]
        #[serde(rename_all = "camelCase")]
        struct WithdrawResponse {
            withdraw_list: Vec<Withdraw>,
        }

        if let None = self.endpoints.withdraws {
            return Ok(Vec::new());
        }

        let now = Utc::now();
        let mut withdraws = Vec::<Withdraw>::new();
        // TODO: devise a better way to fetch all withdraws
        for i in 0..6 {
            let start = now - Duration::days(90 * (i + 1));
            let end = now - Duration::days(90 * i);

            let mut query = QueryParams::new();
            query.add("startTime", start.timestamp_millis().to_string());
            query.add("endTime", end.timestamp_millis().to_string());

            let resp = self
                .make_request::<WithdrawResponse>(
                    self.endpoints.withdraws.as_ref().unwrap(),
                    Some(query),
                    true,
                    true,
                    true,
                )
                .await?;
            withdraws.extend(resp.withdraw_list.into_iter());
        }
        Ok(withdraws)
    }

    pub fn all_prices(&self) -> Result<Vec<SymbolPrice>> {
        match self.market_client.get_all_prices() {
            Ok(Prices::AllPrices(prices)) => Ok(prices),
            Err(err) => {
                panic!("could not get price for: {}", err);
            }
        }
    }

    pub async fn price_at(&self, symbol: &str, time: u64) -> Result<f64> {
        let start_time = time - 30 * 60 * 1000;
        let end_time = time + 30 * 60 * 1000;

        let mut query = QueryParams::new();
        query.add("symbol", symbol.to_string());
        query.add("interval", "30m".to_string());
        query.add("startTime", start_time.to_string());
        query.add("endTime", end_time.to_string());

        let resp = self
            .make_request::<Response<Vec<Vec<Value>>>>(
                self.endpoints.klines,
                Some(query),
                false,
                false,
                true,
            )
            .await;

        match resp {
            Ok(Response::Success(klines)) => {
                let s = &klines[0];
                let high = s[2].as_str().unwrap().parse::<f64>().unwrap();
                let low = s[3].as_str().unwrap().parse::<f64>().unwrap();
                Ok((high + low) / 2.0) // avg
            }
            Ok(Response::Error { code: _, msg }) => {
                return Err(Error::new(msg));
            }
            Err(err) => Err(err.into()),
        }
    }

    pub async fn prices_in_range(
        &self,
        symbol: &str,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, f64)>> {
        // Fetch the prices' 30m-candles from `start_ts` to `end_ts`.
        // The API only returns at max 1000 entries per request, thus the full 
        // range needs to be split into buckets of 1000 30m-candles.

        // Shift the start and end times a bit so both in the first and last buckets.
        let start_time = start_ts - 30 * 60 * 1000 * 2;
        let end_time = end_ts + 30 * 60 * 1000 * 2;

        let limit = 1000;  // API response size limit

        let divmod = |a: u64, b: u64| (a / b, a % b);

        let candle_size_milis: u64 = 30 * 60 * 1000;
        let num_candles = match divmod(end_time - start_time, candle_size_milis) {
            (r, 0) => r,
            (r, _) => r + 1
        };
        let num_batches = match divmod(num_candles, limit) {
            (r, 0) => r,
            (r, _) => r + 1,
        };
        let milis_per_batch = candle_size_milis * limit;
        // Generate the set of timestamp ranges to fetch from the API
        let ranges: Vec<(u64, u64)> = (0..num_batches)
            .scan(start_time, |current_ts, _| {
                let ts = *current_ts;
                *current_ts += milis_per_batch;
                Some((ts, *current_ts))
            })
            .collect();

        let mut all_prices = Vec::new();

        let mut handles = Vec::new();

        for (start, end) in ranges {
            let mut query = QueryParams::new();
            query.add("symbol", symbol);
            query.add("interval", "30m");
            query.add("startTime", start);
            query.add("endTime", end);
            query.add("limit", limit);

            handles.push(self.make_request::<Response<Vec<Vec<Value>>>>(
                self.endpoints.klines,
                Some(query),
                false,
                false,
                true,
            ));
        }

        for resp in join_all(handles).await {
            match resp {
                Ok(Response::Success(klines)) => {
                    all_prices.extend(klines.iter().map(|x| {
                        let high = x[2].as_str().unwrap().parse::<f64>().unwrap();
                        let low = x[3].as_str().unwrap().parse::<f64>().unwrap();
                        (
                            x[6].as_u64().unwrap(), // close time
                            (high + low) / 2.0,     // avg
                        )
                    }))
                }
                Ok(Response::Error { code, msg }) => {
                    // -11001 means the trade pair is not available in the exchange,
                    // so we can just ignore it.
                    if code != -11001 {
                        return Err(Error::new(msg));
                    }
                }
                Err(err) => {
                    println!("could not parse klines response {:?}: {:?}", symbol, err);
                    return Err(err.into());
                }
            }
        }
        all_prices.sort_by_key(|x| x.0);
        Ok(all_prices)
    }

    async fn usd_prices_in_range(
        &self,
        asset: &str,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, f64)>> {
        let (first, second) = match self.region {
            Region::Global => ("USDT", "USD"),
            Region::Us => ("USD", "USDT"),
        };
        match self
            .prices_in_range(&format!("{}{}", asset, first), start_ts, end_ts)
            .await
        {
            Ok(p) => Ok(p),
            Err(_) => {
                // retry with the other one
                self.prices_in_range(&format!("{}{}", asset, second), start_ts, end_ts)
                    .await
            }
        }
    }

    async fn fetch_trades_from_endpoint(
        &self,
        symbol: &str,
        endpoint: &str,
        extra_params: Option<QueryParams>,
    ) -> Result<Vec<Trade>> {
        let mut trades = Vec::<Trade>::new();
        let mut last_id = 0;

        let exchange_symbols = self.exchange_symbols().await?;

        loop {
            let mut query = QueryParams::new();
            query.add("symbol", symbol);
            query.add("fromId", last_id);
            query.add("limit", 1000);

            if let Some(extra_params) = extra_params.as_ref() {
                query.extend(extra_params.clone());
            }

            let resp = self
                .make_request::<Response<Vec<Trade>>>(endpoint, Some(query), true, true, true)
                .await;

            match resp {
                Ok(parsed) => match parsed {
                    Response::Success(mut binance_trades) => {
                        binance_trades.sort_by_key(|k| k.time);
                        let min = match binance_trades.first() {
                            Some(t) => Some(t.time),
                            _ => None,
                        };
                        let max = match binance_trades.last() {
                            Some(t) => Some(t.time),
                            _ => None,
                        };

                        let fetch_more = binance_trades.len() >= 1000;
                        if fetch_more {
                            // the API will return id >= fromId, thus add one to not include
                            // the last processed id.
                            last_id = binance_trades.iter().last().unwrap().id + 1;
                        };
                        match (min, max) {
                            (Some(min), Some(max)) => {
                                let (base_asset, _) = symbol_into_assets(symbol, &exchange_symbols);
                                let prices =
                                    self.usd_prices_in_range(&base_asset, min, max).await?;
                                for mut t in binance_trades.into_iter() {
                                    let (b, q) = symbol_into_assets(&t.symbol, &exchange_symbols);
                                    t.base_asset = b;
                                    t.quote_asset = q;
                                    let price = match &t.base_asset {
                                        q if q.starts_with("USD") => 1.0,
                                        _ => find_price_at(&prices, t.time),
                                    };
                                    t.cost = t.qty * price;
                                    trades.push(t);
                                }
                            }
                            (_, _) => (),
                        }
                        if !fetch_more {
                            break;
                        }
                    }
                    Response::Error { code, msg } => {
                        // -11001 means the trade pair is not available in the exchange,
                        // so we can just ignore it.
                        if code != -11001 {
                            return Err(Error::new(msg));
                        } else {
                            break;
                        }
                    }
                },
                Err(err) => return Err(err.into()),
            }
        }
        Ok(trades)
    }

    pub async fn fetch_trades(&self, symbol: String) -> Result<Vec<Trade>> {
        let endpoints: Endpoints = self.region.into();
        self.fetch_trades_from_endpoint(&symbol, &endpoints.trades, None)
            .await
    }

    async fn fetch_margin_trades(&self, symbol: String) -> Result<Vec<Trade>> {
        if let None = self.endpoints.margin_trades {
            return Ok(Vec::new());
        }

        let mut trades = Vec::<Trade>::new();

        for is_isolated in vec!["TRUE", "FALSE"] {
            let mut extra_params = QueryParams::new();
            extra_params.add("isIsolated", is_isolated);
            let result_trades = self
                .fetch_trades_from_endpoint(
                    &symbol,
                    self.endpoints.margin_trades.as_ref().unwrap(),
                    Some(extra_params),
                )
                .await?;
            trades.extend(result_trades);
        }

        Ok(trades)
    }

    pub async fn margin_borrows(&self, asset: String, symbol: String) -> Result<Vec<MarginBorrow>> {
        if let None = self.endpoints.margin_borrows {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct EndpointResponse {
            rows: Vec<MarginBorrow>,
            total: u16,
        }

        let mut borrows = Vec::<MarginBorrow>::new();

        let ts = NaiveDate::from_ymd(2018, 1, 1)
            .and_hms(0, 0, 0)
            .timestamp_millis() as u64;

        for isolated_symbol in &[Some(&symbol), None] {
            for archived in &["true", "false"] {
                let mut current_page: usize = 1;
                loop {
                    let mut query = QueryParams::new();
                    query.add("asset", &asset);
                    query.add("startTime", ts);
                    query.add("size", 100);
                    query.add("archived", archived);
                    query.add("current", current_page);
                    if let Some(s) = isolated_symbol {
                        query.add("isolatedSymbol", s);
                    }

                    let resp = self
                        .make_request::<EndpointResponse>(
                            self.endpoints.margin_borrows.as_ref().unwrap(),
                            Some(query),
                            true,
                            true,
                            true,
                        )
                        .await;
                    match resp {
                        Ok(borrows_resp) => {
                            borrows.extend(borrows_resp.rows);
                            if borrows_resp.total >= 100 {
                                current_page += 1;
                            } else {
                                break;
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
        }

        Ok(borrows)
    }

    pub async fn margin_repays(&self, asset: String, symbol: String) -> Result<Vec<MarginRepay>> {
        if let None = self.endpoints.margin_repays {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct EndpointResponse {
            rows: Vec<MarginRepay>,
            total: u16,
        }

        let mut repays = Vec::<MarginRepay>::new();

        let ts = NaiveDate::from_ymd(2018, 1, 1)
            .and_hms(0, 0, 0)
            .timestamp_millis() as u64;

        for isolated_symbol in &[Some(&symbol), None] {
            for archived in &["true", "false"] {
                let mut current_page: usize = 1;
                loop {
                    let mut query = QueryParams::new();
                    query.add("asset", &asset);
                    query.add("startTime", ts);
                    query.add("size", 100);
                    query.add("archived", archived);
                    query.add("current", current_page);
                    if let Some(s) = isolated_symbol {
                        query.add("isolatedSymbol", s);
                    }
                    let resp = self
                        .make_request::<EndpointResponse>(
                            self.endpoints.margin_repays.as_ref().unwrap(),
                            Some(query),
                            true,
                            true,
                            true,
                        )
                        .await;
                    match resp {
                        Ok(repays_resp) => {
                            repays.extend(repays_resp.rows);
                            if repays_resp.total >= 100 {
                                current_page += 1;
                            } else {
                                break;
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
        }

        Ok(repays)
    }
}

#[async_trait]
impl ExchangeClient for BinanceFetcher {
    type Trade = Trade;
    type Loan = MarginBorrow;
    type Repay = MarginRepay;
    type Deposit = FiatDeposit;
    type Withdraw = Withdraw;

    async fn trades(&self, symbols: &[String]) -> Result<Vec<Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in symbols.iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_trades(symbol.clone()));
            }
        }

        let all_trades: Vec<Trade> = join_all(handles)
            .await
            .into_iter()
            .filter_map(|r| match r {
                Ok(trades) => Some(trades),
                // fixme: propagate the error
                Err(err) => panic!("could not fetch trades for {:?}: {:?}", symbols, err),
            })
            .flatten()
            .collect();

        Ok(all_trades)
    }

    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<Trade>> {
        // Processing binance margin trades
        let all_symbols: Vec<String> = self
            .exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in symbols.iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_margin_trades(symbol.clone()));
            }
        }

        Ok(join_all(handles)
            .await
            .into_iter()
            .filter_map(|trades_result| match trades_result {
                Ok(trades) => Some(trades),
                // fixme: propagate the error
                Err(err) => panic!("could not get trades: {:?}", err),
            })
            .flatten()
            .collect())
    }

    async fn loans(&self, symbols: &[String]) -> Result<Vec<MarginBorrow>> {
        let mut handles = Vec::new();
        let exchange_symbols = self.exchange_symbols().await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        for symbol in symbols.into_iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = symbol_into_assets(&symbol, &exchange_symbols);
                handles.push(self.margin_borrows(asset, symbol.clone()));
            }
        }
        Ok(join_all(handles)
            .await
            .into_iter()
            .filter_map(|result| match result {
                Ok(borrows) => Some(borrows),
                Err(err) => panic!("could not get borrows: {:?}", err),
            })
            .flatten()
            .collect())
    }

    async fn repays(&self, symbols: &[String]) -> Result<Vec<MarginRepay>> {
        let mut handles = Vec::new();
        let exchange_symbols = self.exchange_symbols().await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        for symbol in symbols.into_iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = symbol_into_assets(&symbol, &exchange_symbols);
                handles.push(self.margin_repays(asset, symbol.clone()));
            }
        }
        Ok(join_all(handles)
            .await
            .into_iter()
            .filter_map(|result| match result {
                Ok(repays) => Some(repays),
                // fixme: propagate the error
                Err(err) => panic!("could not get repays: {:?}", err),
            })
            .flatten()
            .collect())
    }

    async fn deposits(&self, _: &[String]) -> Result<Vec<FiatDeposit>> {
        Ok(Vec::new())
    }

    async fn withdraws(&self, _: &[String]) -> Result<Vec<Withdraw>> {
        self.fetch_withdraws().await
    }
}

fn find_price_at(prices: &Vec<(u64, f64)>, time: u64) -> f64 {
    prices
        .iter()
        .find_map(|p| match p.0 > time {
            true => Some(p.1),
            false => None,
        })
        .unwrap_or(0.0)
}

fn symbol_into_assets(symbol: &str, exchange_symbols: &Vec<Symbol>) -> (String, String) {
    let mut iter = exchange_symbols.iter();
    loop {
        if let Some(s) = iter.next() {
            if symbol.starts_with(&s.base_asset) {
                let (base, quote) = symbol.split_at(s.base_asset.len());
                break (base.to_string(), quote.to_string());
            } else if symbol.starts_with(&s.quote_asset) {
                let (base, quote) = symbol.split_at(s.quote_asset.len());
                break (base.to_string(), quote.to_string());
            }
        } else {
            panic!("could not find a asset for symbol {:?}", symbol);
        }
    }
}
