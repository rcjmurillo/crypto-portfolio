use std::collections::HashMap;
use std::env;
use std::sync::{Arc, Mutex};

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
use tokio;
use tokio::sync::{Mutex as AsyncMutex, OwnedSemaphorePermit, Semaphore};

use crate::data_fetch::{self, ExchangeClient, TradeSide};
use crate::errors::Error;
use crate::result::Result;
use crate::tracker::{IntoOperations, Operation};

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

#[async_trait]
impl IntoOperations for BinanceWithdraw {
    fn into_ops(&self) -> Vec<Operation> {
        vec![Operation {
            asset: self.asset.clone(),
            amount: -self.transaction_fee,
            cost: 0.0,
        }]
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

#[async_trait]
impl IntoOperations for MarginBorrow {
    fn into_ops(&self) -> Vec<Operation> {
        let mut ops = Vec::new();
        if self.status == "CONFIRMED" {
            ops.push(Operation {
                asset: self.asset.to_string(),
                amount: self.principal,
                cost: 0.0,
            });
        }
        ops
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

#[async_trait]
impl IntoOperations for MarginRepay {
    fn into_ops(&self) -> Vec<Operation> {
        let mut ops = Vec::new();
        if self.status == "CONFIRMED" {
            ops.push(Operation {
                asset: self.asset.to_string(),
                amount: -self.amount,
                cost: 0.0,
            });
        }
        ops
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

struct ApiLock {
    entries: AsyncMutex<HashMap<(String, String), Arc<Semaphore>>>,
}

impl ApiLock {
    fn new() -> Self {
        Self {
            entries: AsyncMutex::new(HashMap::new()),
        }
    }

    async fn lock_for(&self, endpoint: &str, query: String) -> Result<OwnedSemaphorePermit> {
        let key = (endpoint.to_string(), query.clone());
        let mut entries = self.entries.lock().await;
        if !entries.contains_key(&key) {
            entries.insert(key, Arc::new(Semaphore::new(1)));
        }
        let key = (endpoint.to_string(), query);
        let sem = entries.get(&key).unwrap();
        Ok(sem.clone().acquire_owned().await.unwrap())
    }
}

pub struct BinanceFetcher {
    market_client: Market,
    credentials: BinanceCredentials,
    region: BinanceRegion,
    endpoints: BinanceEndpoints,
    api_lock: ApiLock,
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
            api_lock: ApiLock::new(),
            domain: region.into(),
            cache: Arc::new(Mutex::new(Cache::new())),
        }
    }

    async fn make_request<T: DeserializeOwned>(
        &self,
        endpoint: &str,
        query: Option<String>,
        signed: bool,
        add_timestamp: bool,
        cache: bool,
    ) -> Result<T> {
        let client = reqwest::Client::new();

        let _endpoint_lock;
        if cache {
            _endpoint_lock = self
                .api_lock
                .lock_for(endpoint, query.clone().unwrap_or("".to_string()))
                .await
                .unwrap();
        }
        let cache_key = format!("{} {}", endpoint, query.as_ref().unwrap_or(&"".to_string()));
        let b;
        {
            // the lock grabbed here must not live enough to reach the await
            // below, otherwise the future generated might use it.
            let c = Arc::clone(&self.cache);
            b = match c.lock().unwrap().get(&cache_key) {
                Some(v) => Some(v.clone()),
                None => None,
            };
        }
        if let Some(val) = b {
            return self.as_json(&val.clone()).await;
        }

        let full_url = if let Some(ref q) = query {
            let mut full_query = q.clone();
            if add_timestamp {
                full_query = format!(
                    "{}&timestamp={}&recvWindow=60000",
                    q,
                    Utc::now().timestamp_millis()
                );
            }
            format!(
                "{}{}?{}",
                self.domain,
                endpoint,
                if signed {
                    self.sign_request(&full_query)
                } else {
                    full_query.clone()
                }
            )
        } else {
            format!("{}{}", self.domain, endpoint)
        };

        // println!("request -> {:?}", full_url);

        let r = client
            .get(full_url)
            .headers(self.default_headers())
            .timeout(std::time::Duration::from_secs(20))
            .send()
            .await;

        let x = match r {
            Ok(mut resp) => {
                resp = self.validate_response(resp).await?;
                let resp_bytes = resp.bytes().await?;

                if cache {
                    let c = Arc::clone(&self.cache);
                    c.lock().unwrap().insert(cache_key, resp_bytes.clone());
                }

                match self.as_json::<T>(&resp_bytes).await {
                    Ok(v) => Ok(v),
                    Err(err) => {
                        println!("could not parse = {:?}", resp_bytes);
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

    fn sign_request(&self, params: &String) -> String {
        let mut signed_key =
            Hmac::<Sha256>::new_from_slice(self.credentials.secret_key.as_bytes()).unwrap();
        signed_key.update(params.as_bytes());
        let signature = hex_encode(signed_key.finalize().into_bytes());
        format!("{}&signature={}", params, signature)
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
        struct Response {
            symbols: Vec<Symbol>,
        }

        let resp = self
            .make_request(&self.endpoints.exchange_info, None, false, false, true)
            .await;
        match resp {
            Ok(Response { symbols }) => Ok(symbols),
            Err(err) => {
                println!("could not parse exchange info response: {:?}", err);
                Err(err)
            }
        }
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<BinanceFiatDeposit>> {
        if let None = self.endpoints.fiat_deposits {
            return Ok(vec![])
        }

        let now = Utc::now();

        let mut deposits: Vec<BinanceFiatDeposit> = Vec::new();
        // TODO: devise a better way to fetch all deposits
        for i in 0..5 {
            let start = now - Duration::days(90 * (i + 1));
            let end = now - Duration::days(90 * i);

            let query = format!(
                "startTime={}&endTime={}",
                start.timestamp_millis(),
                end.timestamp_millis(),
            );

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

            let query = format!(
                "startTime={}&endTime={}",
                start.timestamp_millis(),
                end.timestamp_millis(),
            );

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
        let query = format!(
            "symbol={}&interval=30m&startTime={}&endTime={}",
            symbol, start_time, end_time,
        );

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
            Err(err) => {
                println!("could not parse klines response {:?}: {:?}", symbol, err);
                Err(err.into())
            }
        }
    }

    async fn fetch_trades_from_endpoint(
        &self,
        symbol: &str,
        endpoint: &str,
        extra_params: Option<&str>,
    ) -> Result<Vec<BinanceTrade>> {
        let mut trades = Vec::<BinanceTrade>::new();

        let query = format!(
            "symbol={}&fromId={}&limit=1000{}",
            symbol,
            0,
            if let Some(params) = extra_params {
                format!("&{}", params)
            } else {
                "".to_string()
            }
        );

        let resp = self
            .make_request::<Response<Vec<BinanceTrade>>>(endpoint, Some(query), true, true, true)
            .await;

        match resp {
            Ok(parsed) => match parsed {
                Response::Success(binance_trades) => {
                    if binance_trades.len() >= 500 {
                        println!("still have to query for more trades for {:?}", symbol);
                    }
                    let mut handles = Vec::new();
                    for mut t in binance_trades.into_iter() {
                        handles.push(async {
                            let (b, q) = self.symbol_into_assets(&t.symbol).await;
                            t.base_asset = b;
                            t.quote_asset = q;

                            if t.base_asset == "" || t.quote_asset == "" {
                                panic!("empty base or quote asset for symbol: {:?}", t.symbol);
                            }

                            let price = match &t.base_asset {
                                q if q.starts_with("USD") => 1.0,
                                _ => self
                                    .asset_usd_price_at_time(&t.base_asset, t.time)
                                    .await
                                    .unwrap(),
                            };
                            t.cost = t.qty * price;
                            t
                        });
                    }
                    let binance_trades = await_chunks(handles, 15).await;
                    trades.extend(binance_trades);
                }
                Response::Error { code, msg } => {
                    if code != -11001 {
                        return Err(Error::new(msg));
                    }
                }
            },
            Err(err) => return Err(err.into()),
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
            // println!("margin trades not supported in exchange");
            return Ok(Vec::new());
        }

        let mut trades = Vec::<BinanceTrade>::new();

        for is_isolated in vec!["TRUE", "FALSE"] {
            let result_trades = self
                .fetch_trades_from_endpoint(
                    &symbol,
                    self.endpoints.margin_trades.as_ref().unwrap(),
                    Some(&format!("isIsolated={}", is_isolated)),
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
        struct Response {
            rows: Vec<MarginBorrow>,
            total: u16,
        }

        let mut borrows = Vec::<MarginBorrow>::new();

        let ts = NaiveDate::from_ymd(2018, 1, 1)
            .and_hms(0, 0, 0)
            .timestamp_millis() as u64;

        for isolated_symbol in &[Some(&symbol), None] {
            for archived in &["true", "false"] {
                let mut current_page = 1;
                loop {
                    let query = format!(
                        "asset={}&startTime={}&size=100&archived={}&current={}{}",
                        asset,
                        ts,
                        archived,
                        current_page,
                        if let Some(s) = isolated_symbol {
                            format!("&isolatedSymbol={}", s)
                        } else {
                            "".to_string()
                        }
                    );

                    let resp = self
                        .make_request::<Response>(
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
            // println!("margin repays not supported in exchange");
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct Response {
            rows: Vec<MarginRepay>,
            total: u16,
        }

        let mut repays = Vec::<MarginRepay>::new();

        let ts = NaiveDate::from_ymd(2018, 1, 1)
            .and_hms(0, 0, 0)
            .timestamp_millis() as u64;

        for isolated_symbol in &[Some(&symbol), None] {
            for archived in &["true", "false"] {
                let query = format!(
                    "asset={}&startTime={}&size=100&archived={}{}",
                    asset,
                    ts,
                    archived,
                    if let Some(s) = isolated_symbol {
                        format!("&isolatedSymbol={}", s)
                    } else {
                        "".to_string()
                    }
                );
                let resp = self
                    .make_request::<Response>(
                        self.endpoints.margin_repays.as_ref().unwrap(),
                        Some(query),
                        true,
                        true,
                        true,
                    )
                    .await;
                match resp {
                    Ok(repays_resp) => {
                        if repays_resp.total >= 100 {
                            println!("still have to query for more repays for {:?}", symbol);
                        }
                        repays.extend(repays_resp.rows);
                    }
                    Err(err) => {
                        println!("couldn't get margin repays: {:?}", err);
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
                        if s.base_asset == "" || s.quote_asset == "" {
                            panic!("found no base or quote asset for binance symbol");
                        }
                        if symbol.starts_with(&s.base_asset) {
                            let (base, quote) = symbol.split_at(s.base_asset.len());
                            if base == "" || quote == "" {
                                panic!(
                                    "found no base or quote asset for binance symbol {} {} {}",
                                    base, quote, symbol
                                );
                            }
                            break (base.to_string(), quote.to_string());
                        } else if symbol.starts_with(&s.quote_asset) {
                            let (base, quote) = symbol.split_at(s.quote_asset.len());
                            if base == "" || quote == "" {
                                panic!(
                                    "found no base or quote asset for binance symbol {} {} {}",
                                    base, quote, symbol
                                );
                            }
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
    async fn trades(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>> {
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
                // println!("getting trades for {:?}", symbol);
                handles.push(self.fetch_trades(symbol.clone()));
            }
        }

        let all_trades: Vec<BinanceTrade> = await_chunks(handles, 15)
            .await
            .into_iter()
            .filter_map(|r| match r {
                Ok(trades) => Some(trades),
                Err(err) => panic!("could not fetch trades for {:?}: {:?}", symbols, err),
            })
            .flatten()
            .collect();

        //println!("fetching trades for {} pairs from binance", handles.len());
        Ok(all_trades
            .into_iter()
            .map(|x| Box::new(data_fetch::Trade::from(x)) as Box<dyn IntoOperations>)
            .collect())
    }

    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>> {
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
                // println!("getting trades for {:?}", symbol);
                handles.push(self.fetch_margin_trades(symbol.clone()));
            }
        }

        let all_trades: Vec<BinanceTrade> = await_chunks(handles, 15)
            .await
            .into_iter()
            .filter_map(|trades_result| match trades_result {
                Ok(trades) => Some(trades),
                Err(err) => panic!("could not get trades: {:?}", err),
            })
            .flatten()
            .collect();

        Ok(all_trades
            .into_iter()
            .map(|x| Box::new(data_fetch::Trade::from(x)) as Box<dyn IntoOperations>)
            .collect())
    }

    async fn loans(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>> {
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
                // println!("fetching borrows for asset {:?}", asset);
                handles.push(self.margin_borrows(asset, symbol.clone()));
            }
        }
        println!("fetching borrows for {} pairs from binance", handles.len());
        Ok(join_all(handles)
            .await
            .into_iter()
            .filter_map(|result| match result {
                Ok(borrows) => Some(borrows),
                Err(err) => panic!("could not get borrows: {:?}", err),
            })
            .flatten()
            .map(|x| Box::new(x) as Box<dyn IntoOperations>)
            .collect())
    }

    async fn repays(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>> {
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
                // println!("fetching repays for symbol {:?}", symbol);
                handles.push(self.margin_repays(asset, symbol.clone()));
            }
        }
        println!("fetching repays for {} pairs from binance", handles.len());
        Ok(join_all(handles)
            .await
            .into_iter()
            .filter_map(|result| match result {
                Ok(repays) => Some(repays),
                Err(err) => panic!("could not get repays: {:?}", err),
            })
            .flatten()
            .map(|x| Box::new(x) as Box<dyn IntoOperations>)
            .collect())
    }

    async fn deposits(&self, _: &[String]) -> Result<Vec<Box<dyn IntoOperations>>> {
        // Ok(self.fetch_fiat_deposits()
        //     .await?
        //     .into_iter()
        //     .map(|x| Box::new(x) as Box<dyn IntoOperations>)
        //     .collect())
        Ok(Vec::new())
    }

    async fn withdraws(&self, _: &[String]) -> Result<Vec<Box<dyn IntoOperations>>> {
        Ok(self
            .fetch_withdraws()
            .await?
            .into_iter()
            .map(|x| Box::new(x) as Box<dyn IntoOperations>)
            .collect())
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
