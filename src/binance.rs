use std::collections::HashMap;
use std::env;
use std::sync::Arc;

use async_trait::async_trait;
use binance::api::Binance;
use binance::config::Config;
use binance::market::Market;
use binance::model::{Prices, SymbolPrice};
use bytes::Bytes;
use chrono::prelude::*;
use chrono::Duration;
use futures::future::join_all;
use futures::future::Future;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;
use tokio::sync::Mutex;

use crate::data_fetch::{self, ExchangeClient, OperationStatus, TradeSide};
use crate::errors::Error;
use crate::result::Result;
use crate::sync::ValueLock;

const AWAIT_CHUNK_SIZE: usize = 10;

#[derive(Copy, Clone)]
pub enum BinanceRegion {
    Global,
    Us,
}

struct BinanceCredentials {
    api_key: String,
    secret_key: String,
}

impl From<BinanceRegion> for BinanceCredentials {
    fn from(region: BinanceRegion) -> Self {
        match region {
            BinanceRegion::Global => BinanceCredentials {
                api_key: env::var("API_KEY").unwrap(),
                secret_key: env::var("API_SECRET").unwrap(),
            },
            BinanceRegion::Us => BinanceCredentials {
                api_key: env::var("API_KEY_US").unwrap(),
                secret_key: env::var("API_SECRET_US").unwrap(),
            },
        }
    }
}

struct BinanceEndpoints {
    fiat_deposits: Option<String>,
    deposits: String,
    withdraws: Option<String>,
    trades: String,
    margin_trades: Option<String>,
    margin_borrows: Option<String>,
    margin_repays: Option<String>,
    klines: String,
    exchange_info: String,
}

impl From<BinanceRegion> for BinanceEndpoints {
    fn from(region: BinanceRegion) -> Self {
        match region {
            BinanceRegion::Global => BinanceEndpoints {
                deposits: String::from("/wapi/v3/depositHistory.html"),
                fiat_deposits: None,
                trades: String::from("/api/v3/myTrades"),
                klines: String::from("/api/v3/klines"),
                exchange_info: "/api/v3/exchangeInfo".into(),
                withdraws: Some(String::from("/wapi/v3/withdrawHistory.html")),
                margin_trades: Some(String::from("/sapi/v1/margin/myTrades")),
                margin_borrows: Some(String::from("/sapi/v1/margin/loan")),
                margin_repays: Some(String::from("/sapi/v1/margin/repay")),
            },
            BinanceRegion::Us => BinanceEndpoints {
                deposits: String::from("/wapi/v3/depositHistory.html"),
                fiat_deposits: Some(String::from("/sapi/v1/fiatpayment/query/deposit/history")),
                trades: String::from("/api/v3/myTrades"),
                klines: String::from("/api/v3/klines"),
                exchange_info: "/api/v3/exchangeInfo".into(),
                withdraws: None,
                margin_trades: None,
                margin_borrows: None,
                margin_repays: None,
            },
        }
    }
}

type BinanceDomain = String;

impl From<BinanceRegion> for BinanceDomain {
    fn from(region: BinanceRegion) -> Self {
        match region {
            BinanceRegion::Global => String::from("https://api3.binance.com"),
            BinanceRegion::Us => String::from("https://api.binance.us"),
        }
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Response<T> {
    Success(T),
    Error { code: i16, msg: String },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BinanceFiatDeposit {
    order_id: String,
    offset: Option<u64>,
    payment_channel: String,
    payment_method: String,
    order_status: String,
    #[serde(with = "string_or_float")]
    pub amount: f64,
    #[serde(with = "string_or_float")]
    transaction_fee: f64,
    #[serde(with = "string_or_float")]
    platform_fee: f64,
}

impl From<BinanceFiatDeposit> for data_fetch::Deposit {
    fn from(d: BinanceFiatDeposit) -> Self {
        Self {
            asset: "USD".to_string(), // fixme: grab the actual asset from the API
            amount: d.amount,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BinanceDepositResponse {
    asset_log_record_list: Vec<BinanceFiatDeposit>,
    //success: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BinanceWithdraw {
    id: String,
    withdraw_order_id: Option<String>,
    amount: f64,
    transaction_fee: f64,
    address: String,
    asset: String,
    tx_id: String,
    apply_time: u64,
    status: u16,
}

impl From<BinanceWithdraw> for data_fetch::Withdraw {
    fn from(w: BinanceWithdraw) -> Self {
        Self {
            asset: w.asset,
            amount: w.amount,
            time: w.apply_time,
            fee: w.transaction_fee,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
struct BinanceWithdrawResponse {
    withdraw_list: Vec<BinanceWithdraw>,
    success: bool,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct BinanceTrade {
    pub symbol: String,
    id: u64,
    order_id: i64,
    #[serde(with = "string_or_float")]
    price: f64,
    #[serde(with = "string_or_float")]
    qty: f64,
    // #[serde(with = "string_or_float")]
    // quote_qty: f64,
    #[serde(with = "string_or_float")]
    commission: f64,
    commission_asset: String,
    pub time: u64,
    is_buyer: bool,
    // is_maker: bool,
    // is_best_match: bool,

    // computed
    #[serde(skip)]
    cost: f64,
    #[serde(skip)]
    base_asset: String,
    #[serde(skip)]
    quote_asset: String,
}

impl From<BinanceTrade> for data_fetch::Trade {
    fn from(t: BinanceTrade) -> Self {
        Self {
            symbol: t.symbol,
            base_asset: t.base_asset,
            quote_asset: t.quote_asset,
            price: t.price,
            cost: t.cost,
            amount: t.qty,
            fee: t.commission,
            fee_asset: t.commission_asset,
            time: t.time,
            side: if t.is_buyer {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginBorrow {
    tx_id: u64,
    asset: String,
    #[serde(with = "string_or_float")]
    principal: f64,
    timestamp: u64,
    status: String,
}

impl From<MarginBorrow> for data_fetch::Loan {
    fn from(m: MarginBorrow) -> Self {
        Self {
            asset: m.asset,
            amount: m.principal,
            timestamp: m.timestamp,
            status: match m.status.as_str() {
                "CONFIRMED" => OperationStatus::Success,
                _ => OperationStatus::Failed,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginRepay {
    //isolated_symbol: String,
    #[serde(with = "string_or_float")]
    amount: f64,
    asset: String,
    #[serde(with = "string_or_float")]
    interest: f64,
    #[serde(with = "string_or_float")]
    principal: f64,
    status: String,
    timestamp: u64,
    tx_id: u64,
}

impl From<MarginRepay> for data_fetch::Repay {
    fn from(r: MarginRepay) -> Self {
        Self {
            asset: r.asset,
            amount: r.amount,
            interest: r.interest,
            timestamp: r.timestamp,
            status: match r.status.as_str() {
                "CONFIRMED" => OperationStatus::Success,
                _ => OperationStatus::Failed,
            },
        }
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
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
    credentials: BinanceCredentials,
    region: BinanceRegion,
    endpoints: BinanceEndpoints,
    api_lock: ValueLock<String>,
    domain: BinanceDomain,
    cache: Arc<Mutex<Cache>>,
}

impl BinanceFetcher {
    pub fn new(region: BinanceRegion) -> Self {
        let credentials: BinanceCredentials = region.into();
        let config = if let BinanceRegion::Global = region {
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
            api_lock: ValueLock::new(),
            domain: region.into(),
            cache: Arc::new(Mutex::new(Cache::new())),
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
                (query, format!("{}{}", endpoint, query_str))
            }
            None => (QueryParams::new(), endpoint.to_string()),
        };

        let _endpoint_lock;
        if cache {
            _endpoint_lock = self.api_lock.lock_for(cache_key.clone()).await.unwrap();
            match Arc::clone(&self.cache).lock().await.get(&cache_key) {
                Some(v) => return self.as_json(&v).await,
                None => (),
            }
        }

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
                        .lock()
                        .await
                        .insert(cache_key, resp_bytes.clone());
                }

                match self.as_json::<T>(&resp_bytes).await {
                    Ok(v) => Ok(v),
                    Err(err) => {
                        println!("could not parse response: {:?}", resp_bytes);
                        Err(err)
                    }
                }
            }
            Err(err) => {
                println!("err could not process response");
                Err(err.into())
            },
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
            .make_request(&self.endpoints.exchange_info, None, false, false, true)
            .await;
        match resp {
            Ok(Response::Success(EndpointResponse { symbols })) => Ok(symbols),
            Ok(Response::Error { code, msg }) => {
                println!();
                Err(Error::new(format!(
                    "error from API: {:?} code: {}",
                    msg, code
                )))
            }
            Err(err) => {
                println!("could not parse exchange info response: {:?}", err);
                Err(err)
            }
        }
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<BinanceFiatDeposit>> {
        if let None = self.endpoints.fiat_deposits {
            return Ok(vec![]);
        }

        let now = Utc::now();

        let mut deposits: Vec<BinanceFiatDeposit> = Vec::new();
        // TODO: devise a better way to fetch all deposits
        for i in 0..5 {
            let start = now - Duration::days(90 * (i + 1));
            let end = now - Duration::days(90 * i);

            let mut query = QueryParams::new();
            query.add("startTime", start.timestamp_millis());
            query.add("endTime", end.timestamp_millis());

            let result_deposits = self
                .make_request::<BinanceDepositResponse>(
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

    pub async fn fetch_withdraws(&self) -> Result<Vec<BinanceWithdraw>> {
        if let None = self.endpoints.withdraws {
            return Ok(Vec::new());
        }

        let now = Utc::now();
        let mut withdraws = Vec::<BinanceWithdraw>::new();
        // TODO: devise a better way to fetch all withdraws
        for i in 0..6 {
            let start = now - Duration::days(90 * (i + 1));
            let end = now - Duration::days(90 * i);

            let mut query = QueryParams::new();
            query.add("startTime", start.timestamp_millis().to_string());
            query.add("endTime", end.timestamp_millis().to_string());

            let resp = self
                .make_request::<BinanceWithdrawResponse>(
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

    pub fn price(&self, symbol: String) -> Result<SymbolPrice> {
        match self.market_client.get_price(symbol.clone()) {
            Ok(symbol_price) => Ok(symbol_price),
            Err(err) => {
                panic!("could not get price for {}: {}", symbol, err);
            }
        }
    }

    pub fn all_prices(&self) -> Result<Vec<SymbolPrice>> {
        match self.market_client.get_all_prices() {
            Ok(Prices::AllPrices(prices)) => Ok(prices),
            Err(err) => {
                panic!("could not get price for: {}", err);
            }
        }
    }

    async fn asset_usd_price_at_time(&self, asset: &String, time: u64) -> Result<f64> {
        let (first, second) = match self.region {
            BinanceRegion::Global => ("USDT", "USD"),
            BinanceRegion::Us => ("USD", "USDT"),
        };
        match self.price_at(&format!("{}{}", asset, first), time).await {
            Ok(p) => Ok(p),
            Err(_) => {
                // retry with the other one
                self.price_at(&format!("{}{}", asset, second), time).await
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
            .make_request::<Vec<Vec<Value>>>(
                &self.endpoints.klines,
                Some(query),
                false,
                false,
                true,
            )
            .await;

        match resp {
            Ok(klines) => {
                let s = &klines[0];
                let high = s[2].as_str().unwrap().parse::<f64>().unwrap();
                let low = s[3].as_str().unwrap().parse::<f64>().unwrap();
                Ok((high + low) / 2.0) // avg
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
        let start_time = start_ts - 30 * 60 * 1000 * 2;
        let end_time = end_ts + 30 * 60 * 1000 * 2;

        let limit = 1000;

        // create buckets to fetch the results
        let candle_size_milis: u64 = 30 * 60 * 1000;
        let milis_per_batch = candle_size_milis * limit;
        let num_candles = (end_time - start_time) / candle_size_milis
            + (if (end_time - start_time) % candle_size_milis > 0 {
                1
            } else {
                0
            });
        let num_batches = num_candles / limit + (if num_candles % limit > 0 { 1 } else { 0 });
        let ranges: Vec<(u64, u64)> = (0..num_batches)
            .scan(start_time, |current_ts, _| {
                let ts = *current_ts;
                *current_ts += milis_per_batch;
                Some((ts, *current_ts))
            })
            .collect();

        let mut all_prices = Vec::new();

        for (start, end) in ranges {
            let mut query = QueryParams::new();
            query.add("symbol", symbol);
            query.add("interval", "30m");
            query.add("startTime", start);
            query.add("endTime", end);
            query.add("limit", limit);

            let resp = self
                .make_request::<Vec<Vec<Value>>>(
                    &self.endpoints.klines,
                    Some(query),
                    false,
                    false,
                    true,
                )
                .await;

            match resp {
                Ok(klines) => {
                    all_prices.extend(klines.iter().map(|x| {
                        let high = x[2].as_str().unwrap().parse::<f64>().unwrap();
                        let low = x[3].as_str().unwrap().parse::<f64>().unwrap();
                        (
                            x[6].as_u64().unwrap(), // close time
                            (high + low) / 2.0,     // avg
                        )
                    }))
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
            BinanceRegion::Global => ("USDT", "USD"),
            BinanceRegion::Us => ("USD", "USDT"),
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
    ) -> Result<Vec<BinanceTrade>> {
        let mut trades = Vec::<BinanceTrade>::new();
        let mut last_id = 0;
        loop {
            let mut query = QueryParams::new();
            query.add("symbol", symbol);
            query.add("fromId", last_id);
            query.add("limit", 1000);

            if let Some(extra_params) = extra_params.as_ref() {
                query.extend(extra_params.clone());
            }

            let resp = self
                .make_request::<Response<Vec<BinanceTrade>>>(
                    endpoint,
                    Some(query),
                    true,
                    true,
                    true,
                )
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
                                let (base_asset, _) = self.symbol_into_assets(symbol).await;
                                let prices =
                                    self.usd_prices_in_range(&base_asset, min, max).await?;
                                for mut t in binance_trades.into_iter() {
                                    let (b, q) = self.symbol_into_assets(&t.symbol).await;
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

    pub async fn fetch_trades(&self, symbol: String) -> Result<Vec<BinanceTrade>> {
        let endpoints: BinanceEndpoints = self.region.into();
        self.fetch_trades_from_endpoint(&symbol, &endpoints.trades, None)
            .await
    }

    async fn fetch_margin_trades(&self, symbol: String) -> Result<Vec<BinanceTrade>> {
        if let None = self.endpoints.margin_trades {
            return Ok(Vec::new());
        }

        let mut trades = Vec::<BinanceTrade>::new();

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

    async fn symbol_into_assets(&self, symbol: &str) -> (String, String) {
        match self.exchange_symbols().await {
            Ok(symbols) => {
                let mut iter = symbols.iter();
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
            Err(err) => panic!("couldn't get binance symbols: {:?}", err),
        }
    }
}

#[async_trait]
impl ExchangeClient for BinanceFetcher {
    type Trade = BinanceTrade;
    type Loan = MarginBorrow;
    type Repay = MarginRepay;
    type Deposit = BinanceFiatDeposit;
    type Withdraw = BinanceWithdraw;

    async fn trades(&self, symbols: &[String]) -> Result<Vec<BinanceTrade>> {
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

        let all_trades: Vec<BinanceTrade> = await_chunks(handles, AWAIT_CHUNK_SIZE)
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

    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<BinanceTrade>> {
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

        Ok(await_chunks(handles, AWAIT_CHUNK_SIZE)
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
        let all_symbols: Vec<String> = self
            .exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();
        for symbol in symbols.iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = self.symbol_into_assets(&symbol).await;
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
        let all_symbols: Vec<String> = self
            .exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();
        for symbol in symbols.iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = self.symbol_into_assets(&symbol).await;
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

    async fn deposits(&self, _: &[String]) -> Result<Vec<BinanceFiatDeposit>> {
        Ok(Vec::new())
    }

    async fn withdraws(&self, _: &[String]) -> Result<Vec<BinanceWithdraw>> {
        self.fetch_withdraws().await
    }
}

async fn await_chunks<T>(handles: Vec<impl Future<Output = T>>, chunk_size: usize) -> Vec<T> {
    let mut all_results = Vec::new();

    let mut chunk = Vec::new();
    let handles_size = handles.len();
    for (i, handle) in handles.into_iter().enumerate() {
        chunk.push(handle);

        if chunk.len() == chunk_size || i == handles_size - 1 {
            all_results.extend(join_all(chunk).await);
            chunk = Vec::new();
        }
    }
    all_results
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

pub(crate) mod string_or_float {
    use std::fmt;

    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: fmt::Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrFloat {
            String(String),
            Float(f64),
        }

        match StringOrFloat::deserialize(deserializer)? {
            StringOrFloat::String(s) => s.parse().map_err(de::Error::custom),
            StringOrFloat::Float(i) => Ok(i),
        }
    }
}
