use std::{env, sync::Arc, fmt};

use bytes::Bytes;
use chrono::{prelude::*, Duration};
use futures::future::join_all;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac, NewMac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use sha2::Sha256;

use api_client::{ApiClient, QueryParams};

use crate::{
    errors::{ApiErrorKind, Error, ErrorKind},
    response_model::*,
};

pub type Result<T> = std::result::Result<T, Error>;

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

impl Credentials {
    fn for_region(region: &Region) -> Self {
        match region {
            Region::Global => Credentials {
                api_key: env::var("BINANCE_API_KEY").unwrap(),
                secret_key: env::var("BINANCE_API_SECRET").unwrap(),
            },
            Region::Us => Credentials {
                api_key: env::var("BINANCE_API_KEY_US").unwrap(),
                secret_key: env::var("BINANCE_API_SECRET_US").unwrap(),
            },
        }
    }
}

struct Endpoints {
    fiat_deposits: Option<&'static str>,
    withdraws: Option<&'static str>,
    trades: &'static str,
    margin_trades: Option<&'static str>,
    margin_loans: Option<&'static str>,
    margin_repays: Option<&'static str>,
    klines: &'static str,
    prices: &'static str,
    exchange_info: &'static str,
}

impl Endpoints {
    fn for_region(region: &Region) -> Self {
        match region {
            Region::Global => Endpoints {
                fiat_deposits: None,
                trades: "/api/v3/myTrades",
                klines: "/api/v3/klines",
                prices: "/api/v3/ticker/price",
                exchange_info: "/api/v3/exchangeInfo",
                withdraws: Some("/sapi/v1/capital/withdraw/history"),
                margin_trades: Some("/sapi/v1/margin/myTrades"),
                margin_loans: Some("/sapi/v1/margin/loan"),
                margin_repays: Some("/sapi/v1/margin/repay"),
            },
            Region::Us => Endpoints {
                fiat_deposits: Some("/sapi/v1/fiatpayment/query/deposit/history"),
                trades: "/api/v3/myTrades",
                klines: "/api/v3/klines",
                prices: "/api/v3/ticker/price",
                exchange_info: "/api/v3/exchangeInfo",
                withdraws: None,
                margin_trades: None,
                margin_loans: None,
                margin_repays: None,
            },
        }
    }
}

enum Domain {
    Global(&'static str),
    Us(&'static str)
}

impl Domain {
    fn for_region(region: &Region) -> Self {
        match region {
            Region::Global => Domain::Global("https://api3.binance.com"),
            Region::Us => Domain::Us("https://api.binance.us"),
        }
    }
}

impl fmt::Display for Domain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Global(s) | Self::Us(s) => write!(f, "{}", s)
        }
    }
}

pub struct Config {
    pub start_datetime: DateTime<Utc>,
}

pub struct BinanceFetcher<'a> {
    config: &'a Option<Config>,
    api_client: ApiClient,
    credentials: Credentials,
    region: Region,
    domain: Domain,
    endpoints: Endpoints,
}

impl<'a> BinanceFetcher<'a> {
    pub fn new(region: Region, config: &'a Option<Config>) -> Self {
        let credentials = Credentials::for_region(&region);
        Self {
            api_client: ApiClient::new(ENDPOINT_CONCURRENCY),
            config,
            credentials,
            region,
            endpoints: Endpoints::for_region(&region),
            domain: Domain::for_region(&region),
        }
    }

    fn data_start_date(&self) -> Result<DateTime<Utc>> {
        match self.config {
            Some(config) => Ok(config.start_datetime),
            // fetch last 6 months
            None => Ok(Utc::now() - Duration::weeks(24)),
        }
    }

    async fn as_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(resp_bytes) {
            Ok(val) => Ok(val),
            Err(err) => Err(Error::new(err.to_string(), ErrorKind::Parse)),
        }
    }

    fn sign_request(&self, mut query_params: QueryParams) -> QueryParams {
        let mut signed_key =
            Hmac::<Sha256>::new_from_slice(self.credentials.secret_key.as_bytes()).unwrap();
        signed_key.update(query_params.to_string().as_ref());
        let signature = hex_encode(signed_key.finalize().into_bytes());
        query_params.add("signature", signature, false);
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

    pub async fn fetch_exchange_symbols(&self) -> Result<Vec<Symbol>> {
        #[derive(Deserialize, Clone)]
        struct EndpointResponse {
            symbols: Vec<Symbol>,
        }

        let resp = self
            .api_client
            .make_request(
                &format!("{}{}", self.domain, self.endpoints.exchange_info),
                None,
                Some(self.default_headers()),
                true,
            )
            .await?;

        let EndpointResponse { symbols } = self.as_json::<EndpointResponse>(resp.as_ref()).await?;
        Ok(symbols)
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatDeposit>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DepositResponse {
            asset_log_record_list: Vec<FiatDeposit>,
        }

        if let None = self.endpoints.fiat_deposits {
            return Ok(vec![]);
        }

        let mut deposits: Vec<FiatDeposit> = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date()?;
        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            let mut query = QueryParams::new();
            query.add("startTime", curr_start.timestamp_millis(), true);
            query.add("endTime", end.timestamp_millis(), true);
            query.add("timestamp", now.timestamp_millis(), false);
            query.add("recvWindow", 60000, true);
            query = self.sign_request(query);

            let resp = self
                .api_client
                .make_request(
                    &format!(
                        "{}{}",
                        self.domain,
                        self.endpoints.fiat_deposits.as_ref().unwrap()
                    ),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let DepositResponse {
                asset_log_record_list,
            } = self.as_json::<DepositResponse>(&resp.as_ref()).await?;
            deposits.extend(asset_log_record_list.into_iter().filter_map(|x| {
                if x.order_status == "Successful" {
                    Some(x)
                } else {
                    None
                }
            }));
            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        Ok(deposits)
    }

    pub async fn fetch_withdraws(&self) -> Result<Vec<Withdraw>> {
        if let None = self.endpoints.withdraws {
            return Ok(Vec::new());
        }

        let mut withdraws = Vec::<Withdraw>::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date()?;
        loop {
            let now = Utc::now();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(90), now);

            let mut query = QueryParams::new();
            query.add("timestamp", now.timestamp_millis(), false);
            query.add("recvWindow", 60000, true);
            query.add("startTime", curr_start.timestamp_millis(), true);
            query.add("endTime", end.timestamp_millis(), true);
            query = self.sign_request(query);

            let resp = self
                .api_client
                .make_request(
                    &format!(
                        "{}{}",
                        self.domain,
                        self.endpoints.withdraws.as_ref().unwrap()
                    ),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let withdraw_list: Vec<Withdraw> = self.as_json(resp.as_ref()).await?;
            withdraws.extend(withdraw_list.into_iter());

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }
        Ok(withdraws)
    }

    pub async fn fetch_all_prices(&self) -> Result<Vec<SymbolPrice>> {
        let resp = self.api_client.make_request(
            &format!("{}{}", self.domain, self.endpoints.prices),
            None, 
            None, 
            true
         )
         .await?; 
         
         self.as_json(resp.as_ref()).await
    }

    pub async fn fetch_price_at(&self, symbol: &str, time: u64) -> Result<f64> {
        let start_time = time - 30 * 60 * 1000;
        let end_time = time + 30 * 60 * 1000;

        let mut query = QueryParams::new();
        query.add("symbol", symbol, true);
        query.add("interval", "30m", true);
        query.add("startTime", start_time, true);
        query.add("endTime", end_time, true);

        let resp = self
            .api_client
            .make_request(
                &format!("{}{}", self.domain, self.endpoints.klines),
                Some(query),
                Some(self.default_headers()),
                true,
            )
            .await?;
        let klines: Vec<Vec<Value>> = self.as_json(resp.as_ref()).await?;
        let s = &klines[0];
        let high = s[2].as_str().unwrap().parse::<f64>().unwrap();
        let low = s[3].as_str().unwrap().parse::<f64>().unwrap();
        Ok((high + low) / 2.0) // avg
    }

    async fn fetch_prices_in_range(
        &self,
        symbol: &str,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<(u64, f64)>> {
        // Fetch the prices' 30m-candles from `start_ts` to `end_ts`.
        // The API only returns at max 1000 entries per request, thus the full
        // range needs to be split into buckets of 1000 30m-candles.

        // Shift the start and end times a bit to include both in the first and last buckets.
        let start_time = start_ts - 30 * 60 * 1000 * 2;
        let end_time = end_ts + 30 * 60 * 1000 * 2;

        let limit = 1000; // API response size limit

        let divmod = |a: u64, b: u64| (a / b, a % b);

        let candle_size_milis: u64 = 30 * 60 * 1000;
        let num_candles = match divmod(end_time - start_time, candle_size_milis) {
            (r, 0) => r,
            (r, _) => r + 1,
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

        let endpoint = Arc::new(format!("{}{}", self.domain, self.endpoints.klines));
        let mut handles = Vec::new();
        for (start, end) in ranges {
            let mut query = QueryParams::new();
            query.add("symbol", symbol, true);
            query.add("interval", "30m", true);
            query.add("startTime", start, true);
            query.add("endTime", end, true);
            query.add("limit", limit, true);

            handles.push(self.api_client.make_request(
                &endpoint,
                Some(query),
                Some(self.default_headers()),
                true,
            ));
        }

        for resp in join_all(handles).await {
            match resp {
                Ok(resp) => {
                    let klines = self.as_json::<Vec<Vec<Value>>>(resp.as_ref()).await?;
                    all_prices.extend(klines.iter().map(|x| {
                        let high = x[2].as_str().unwrap().parse::<f64>().unwrap();
                        let low = x[3].as_str().unwrap().parse::<f64>().unwrap();
                        (
                            x[6].as_u64().unwrap(), // close time
                            (high + low) / 2.0,     // avg
                        )
                    }));
                }
                Err(err) => {
                    let err: Error = err.into();
                    match err.kind {
                        // if ApiErrorType::UnavailableSymbol means the symbol is not
                        // available in the exchange, so we can just ignore it.
                        ErrorKind::Api(ApiErrorKind::UnavailableSymbol) => continue,
                        _ => return Err(err),
                    }
                }
            }
        }
        all_prices.sort_by_key(|x| x.0);
        Ok(all_prices)
    }

    async fn fetch_usd_prices_in_range(
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
            .fetch_prices_in_range(&format!("{}{}", asset, first), start_ts, end_ts)
            .await
        {
            Ok(p) => Ok(p),
            Err(_) => {
                // retry with the other one
                self.fetch_prices_in_range(&format!("{}{}", asset, second), start_ts, end_ts)
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
        let mut last_id: u64 = 0;

        let exchange_symbols = self.fetch_exchange_symbols().await?;

        loop {
            let mut query = QueryParams::new();
            query.add("symbol", symbol, true);
            query.add("fromId", last_id, true);
            query.add("limit", 1000, true);
            query.add("recvWindow", 60000, true);
            query.add("timestamp", Utc::now().timestamp_millis(), false);
            
            if let Some(extra_params) = extra_params.as_ref() {
                query.extend(extra_params.clone());
            }
            query = self.sign_request(query);
            let resp = self
                .api_client
                .make_request(
                    &format!("{}{}", self.domain, endpoint),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await;

            match resp {
                Ok(resp) => {
                    let mut binance_trades = self.as_json::<Vec<Trade>>(resp.as_ref()).await?;
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
                            let prices = self.fetch_usd_prices_in_range(&base_asset, min, max).await?;
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
                Err(err) => {
                    let err: Error = err.into();
                    match err.kind {
                        // if ApiErrorType::UnavailableSymbol means the symbol is not
                        // available in the exchange, so we can just ignore it.
                        ErrorKind::Api(ApiErrorKind::UnavailableSymbol) => break,
                        _ => return Err(err),
                    }
                }
            }
        }
        Ok(trades)
    }

    pub async fn fetch_trades(&self, symbol: String) -> Result<Vec<Trade>> {
        let endpoints = Endpoints::for_region(&self.region);
        self.fetch_trades_from_endpoint(&symbol, &endpoints.trades, None)
            .await
    }

    pub async fn fetch_margin_trades(&self, symbol: String) -> Result<Vec<Trade>> {
        if let None = self.endpoints.margin_trades {
            return Ok(Vec::new());
        }

        let mut trades = Vec::<Trade>::new();

        for is_isolated in vec!["TRUE", "FALSE"] {
            let mut extra_params = QueryParams::new();
            extra_params.add("isIsolated", is_isolated, true);
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

    pub async fn fetch_margin_loans(&self, asset: String, symbol: String) -> Result<Vec<MarginLoan>> {
        if let None = self.endpoints.margin_loans {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct EndpointResponse {
            rows: Vec<MarginLoan>,
            total: u16,
        }

        let mut loans = Vec::<MarginLoan>::new();

        let ts = self.data_start_date()?.timestamp_millis() as u64;

        for isolated_symbol in &[Some(&symbol), None] {
            for archived in &["true", "false"] {
                let mut current_page: usize = 1;
                loop {
                    let mut query = QueryParams::new();
                    query.add("asset", &asset, true);
                    query.add("startTime", ts, true);
                    query.add("size", 100, true);
                    query.add("archived", archived, true);
                    query.add("current", current_page, true);
                    query.add("recvWindow", 60000, true);
                    query.add("timestamp", Utc::now().timestamp_millis(), false);
                    if let Some(s) = isolated_symbol {
                        query.add("isolatedSymbol", s, true);
                    }
                    query = self.sign_request(query);

                    let resp = self
                        .api_client
                        .make_request(
                            &format!(
                                "{}{}",
                                self.domain,
                                self.endpoints.margin_loans.as_ref().unwrap()
                            ),
                            Some(query),
                            Some(self.default_headers()),
                            true,
                        )
                        .await?;

                    let loans_resp = self.as_json::<EndpointResponse>(resp.as_ref()).await?;

                    loans.extend(loans_resp.rows);
                    if loans_resp.total >= 100 {
                        current_page += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(loans)
    }

    pub async fn fetch_margin_repays(&self, asset: String, symbol: String) -> Result<Vec<MarginRepay>> {
        if let None = self.endpoints.margin_repays {
            return Ok(Vec::new());
        }

        #[derive(Deserialize)]
        struct EndpointResponse {
            rows: Vec<MarginRepay>,
            total: u16,
        }

        let mut repays = Vec::<MarginRepay>::new();

        let ts = self.data_start_date()?.timestamp_millis() as u64;

        for isolated_symbol in &[Some(&symbol), None] {
            for archived in &["true", "false"] {
                let mut current_page: usize = 1;
                loop {
                    let mut query = QueryParams::new();
                    query.add("asset", &asset, true);
                    query.add("startTime", ts, true);
                    query.add("size", 100, true);
                    query.add("archived", archived, true);
                    query.add("current", current_page, true);
                    query.add("recvWindow", 60000, true);
                    query.add("timestamp", Utc::now().timestamp_millis(), false);
                    if let Some(s) = isolated_symbol {
                        query.add("isolatedSymbol", s, true);
                    }
                    query = self.sign_request(query);
                    let resp = self
                        .api_client
                        .make_request(
                            &format!(
                                "{}{}",
                                self.domain,
                                self.endpoints.margin_repays.as_ref().unwrap()
                            ),
                            Some(query),
                            Some(self.default_headers()),
                            true,
                        )
                        .await?;
                    let repays_resp = self.as_json::<EndpointResponse>(resp.as_ref()).await?;
                    repays.extend(repays_resp.rows);
                    if repays_resp.total >= 100 {
                        current_page += 1;
                    } else {
                        break;
                    }
                }
            }
        }

        Ok(repays)
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

pub fn symbol_into_assets(symbol: &str, exchange_symbols: &Vec<Symbol>) -> (String, String) {
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
            println!("could not find a asset for symbol {:?}", symbol);
        }
    }
}
