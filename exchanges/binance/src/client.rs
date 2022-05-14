use std::{env, marker::PhantomData, sync::Arc};

use anyhow::{anyhow, Result};

use bytes::Bytes;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::future::join_all;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use sha2::Sha256;

use api_client::{errors::Error as ClientError, ApiClient, QueryParams};
use exchange::{Candle, AssetPair};

use crate::{
    api_model::*,
    errors::{ApiErrorKind, Error as ApiError},
};

const ENDPOINT_CONCURRENCY: usize = 10;
const API_DOMAIN_GLOBAL: &str = "https://api3.binance.com";
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

impl Credentials<RegionGlobal> {
    fn new() -> Self {
        Self {
            api_key: env::var("BINANCE_API_KEY").unwrap(),
            secret_key: env::var("BINANCE_API_SECRET").unwrap(),
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

pub enum EndpointsUs {
    Trades,
    Klines,
    Prices,
    ExchangeInfo,
    Deposits,
    Withdraws,
    FiatDeposits,
    FiatWithdraws,
}

impl ToString for EndpointsUs {
    fn to_string(&self) -> String {
        match self {
            Self::Trades => "/api/v3/myTrades",
            Self::Klines => "/api/v3/klines",
            Self::Prices => "/api/v3/ticker/price",
            Self::ExchangeInfo => "/api/v3/exchangeInfo",
            Self::Deposits => "/wapi/v3/depositHistory.html",
            Self::Withdraws => "/wapi/v3/withdrawHistory.html",
            Self::FiatDeposits => "/sapi/v1/fiatpayment/query/deposit/history",
            Self::FiatWithdraws => "/sapi/v1/fiatpayment/query/withdraw/history",
        }
        .to_string()
    }
}

pub enum EndpointsGlobal {
    Trades,
    Klines,
    Prices,
    ExchangeInfo,
    Deposits,
    Withdraws,
    FiatDeposits,
    FiatWithdraws,
    FiatOrders,
    MarginTrades,
    MarginLoans,
    MarginRepays,
}

impl ToString for EndpointsGlobal {
    fn to_string(&self) -> String {
        match self {
            Self::Trades => "/api/v3/myTrades",
            Self::Klines => "/api/v3/klines",
            Self::Prices => "/api/v3/ticker/price",
            Self::ExchangeInfo => "/api/v3/exchangeInfo",
            Self::Deposits => "/sapi/v1/capital/deposit/hisrec",
            Self::Withdraws => "/sapi/v1/capital/withdraw/history",
            Self::FiatOrders => "/sapi/v1/fiat/orders",
            Self::FiatDeposits => "",
            Self::FiatWithdraws => "",
            Self::MarginTrades => "/sapi/v1/margin/myTrades",
            Self::MarginLoans => "/sapi/v1/margin/loan",
            Self::MarginRepays => "/sapi/v1/margin/repay",
        }
        .to_string()
    }
}

pub struct Config {
    pub start_date: NaiveDate,
    pub symbols: Vec<AssetPair>,
}

impl Config {
    pub fn empty() -> Self {
        Self {
            start_date: Utc::now().naive_utc().date(),
            symbols: Vec::new(),
        }
    }
}

impl Clone for Config {
    fn clone(&self) -> Self {
        Self {
            start_date: self.start_date.clone(),
            symbols: self.symbols.clone(),
        }
    }
}

pub struct BinanceFetcher<Region> {
    pub config: Option<Config>,
    pub api_client: ApiClient,
    pub credentials: Credentials<Region>,
    pub domain: &'static str,
}

impl<Region> BinanceFetcher<Region> {
    fn data_start_date(&self) -> &NaiveDate {
        &self
            .config
            .as_ref()
            .expect("missing config in BinanceFetcher")
            .start_date
    }

    pub fn symbols(&self) -> &Vec<AssetPair> {
        &self
            .config
            .as_ref()
            .expect("missing config in BinanceFetcher")
            .symbols
    }

    async fn from_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(&resp_bytes.clone()) {
            Ok(val) => Ok(val),
            Err(err) => Err(anyhow!(err.to_string())
                .context(format!("couldn't parse: {:?}", resp_bytes.clone()))),
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

    pub async fn fetch_exchange_symbols(&self, endpoint: &str) -> Result<Vec<Symbol>> {
        #[derive(Deserialize, Clone)]
        struct EndpointResponse {
            symbols: Vec<Symbol>,
        }

        let resp = self
            .api_client
            .make_request(
                &format!("{}{}", self.domain, endpoint),
                None,
                Some(self.default_headers()),
                true,
            )
            .await?;
        let EndpointResponse { symbols } =
            self.from_json::<EndpointResponse>(resp.as_ref()).await?;
        Ok(symbols)
    }

    pub async fn fetch_all_prices(&self, endpoint: &str) -> Result<Vec<SymbolPrice>> {
        let resp = self
            .api_client
            .make_request(&format!("{}{}", self.domain, endpoint), None, None, true)
            .await?;

        self.from_json(resp.as_ref()).await
    }

    pub async fn fetch_price_at(
        &self,
        endpoint: &str,
        symbol: &str,
        datetime: &DateTime<Utc>,
    ) -> Result<f64> {
        let time = datetime.timestamp_millis();
        let start_time = time - 30 * 60;
        let end_time = time + 30 * 60;

        let mut query = QueryParams::new();
        query.add("symbol", symbol, true);
        query.add("interval", "30m", true);
        query.add("startTime", start_time, true);
        query.add("endTime", end_time, true);

        let resp = self
            .api_client
            .make_request(
                &format!("{}{}", self.domain, endpoint),
                Some(query),
                Some(self.default_headers()),
                true,
            )
            .await?;
        let klines: Vec<Vec<Value>> = self.from_json(resp.as_ref()).await?;
        let s = &klines[0];
        let high = s[2].as_str().unwrap().parse::<f64>().unwrap();
        let low = s[3].as_str().unwrap().parse::<f64>().unwrap();
        Ok((high + low) / 2.0) // avg
    }

    pub async fn fetch_prices_in_range(
        &self,
        endpoint: &str,
        symbol: &str,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<Candle>> {
        // Fetch the prices' 30m-candles from `start_ts` to `end_ts`.
        // The API only returns at max 1000 entries per request, thus the full
        // range needs to be split into buckets of 1000 30m-candles.

        // Shift the start and end times a bit to include both in the first and last buckets.
        let start_time = start_ts - 30 * 60 * 2;
        let end_time = end_ts + 30 * 60 * 2;

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

        let mut all_prices = Vec::new();

        let endpoint = Arc::new(format!("{}{}", self.domain, endpoint));
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
                    let klines = self.from_json::<Vec<Vec<Value>>>(resp.as_ref()).await?;
                    all_prices.extend(klines.iter().map(|x| Candle {
                        open_time: x[0].as_u64().unwrap(),
                        close_time: x[6].as_u64().unwrap(),
                        open_price: x[1].as_str().unwrap().parse::<f64>().unwrap(),
                        close_price: x[4].as_str().unwrap().parse::<f64>().unwrap(),
                    }));
                }
                Err(err) => {
                    match err.downcast::<ClientError>() {
                        Ok(client_error) => {
                            match client_error.into() {
                                // ApiErrorType::UnavailableSymbol means the symbol is not
                                // available in the exchange, so we can just ignore it.
                                ApiError::Api(ApiErrorKind::UnavailableSymbol) => continue,
                                err => {
                                    return Err(anyhow!(err).context(format!(
                                        "couldn't fetch prices for symbol: {}",
                                        symbol
                                    )))
                                }
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
        }
        all_prices.sort_by_key(|c| c.close_time);
        Ok(all_prices)
    }

    async fn fetch_trades_from_endpoint(
        &self,
        symbol: &AssetPair,
        endpoint: &str,
        extra_params: Option<QueryParams>,
    ) -> Result<Vec<Trade>> {
        let mut trades = Vec::<Trade>::new();
        let mut last_id: u64 = 0;

        let exchange_symbols = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?;

        loop {
            let mut query = QueryParams::new();
            query.add("symbol", symbol.join(""), true);
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
                    let mut binance_trades = self.from_json::<Vec<Trade>>(resp.as_ref()).await?;
                    binance_trades.sort_by_key(|k| k.time);
                    let fetch_more = binance_trades.len() >= 1000;
                    if fetch_more {
                        // the API will return id >= fromId, thus add one to not include
                        // the last processed id.
                        last_id = binance_trades.iter().last().unwrap().id + 1;
                    };
                    for mut t in binance_trades.into_iter() {
                        let (b, q) = symbol_into_assets(&t.symbol, &exchange_symbols);
                        t.base_asset = b;
                        t.quote_asset = q;
                        trades.push(t);
                    }
                    if !fetch_more {
                        break;
                    }
                }
                Err(err) => {
                    match err.downcast::<ClientError>() {
                        Ok(client_error) => {
                            match client_error.into() {
                                // ApiErrorType::UnavailableSymbol means the symbol is not
                                // available in the exchange, so we can just skip it.
                                ApiError::Api(ApiErrorKind::UnavailableSymbol) => break,
                                err => {
                                    return Err(
                                        anyhow::Error::msg("couldn't fetch trades").context(err)
                                    )
                                }
                            }
                        }
                        Err(err) => return Err(err),
                    }
                }
            }
        }
        Ok(trades)
    }

    pub async fn fetch_trades(&self, endpoint: &str, symbol: &AssetPair) -> Result<Vec<Trade>> {
        self.fetch_trades_from_endpoint(symbol, endpoint, None)
            .await
    }
}

impl BinanceFetcher<RegionGlobal> {
    pub fn new() -> Self {
        Self {
            api_client: ApiClient::new(ENDPOINT_CONCURRENCY),
            config: None,
            credentials: Credentials::<RegionGlobal>::new(),
            domain: API_DOMAIN_GLOBAL,
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(ENDPOINT_CONCURRENCY),
            config: Some(config),
            credentials: Credentials::<RegionGlobal>::new(),
            domain: API_DOMAIN_GLOBAL,
        }
    }

    pub async fn fetch_fiat_orders(&self, tx_type: &str) -> Result<Vec<FiatOrder>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            data: Vec<FiatOrder>,
        }

        let mut orders: Vec<FiatOrder> = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            let mut current_page = 1usize;
            loop {
                let mut query = QueryParams::new();
                query.add("transactionType", tx_type, true);
                query.add("page", current_page, true);
                query.add("beginTime", curr_start.timestamp_millis(), true);
                query.add("endTime", end.timestamp_millis(), true);
                query.add("timestamp", now.timestamp_millis(), false);
                query.add("recvWindow", 60000, true);
                query.add("rows", 500, true);
                query = self.sign_request(query);

                let resp = self
                    .api_client
                    .make_request(
                        &format!("{}{}", self.domain, EndpointsGlobal::FiatOrders.to_string(),),
                        Some(query),
                        Some(self.default_headers()),
                        true,
                    )
                    .await?;

                let Response { data } = self.from_json(&resp.as_ref()).await?;
                if data.len() > 0 {
                    orders.extend(data.into_iter().filter(|x| x.status == "Successful"));
                    current_page += 1;
                } else {
                    break;
                }
            }
            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        Ok(orders)
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders("0").await
    }

    pub async fn fetch_fiat_withdraws(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders("1").await
    }

    pub async fn fetch_margin_trades(&self, symbol: &AssetPair) -> Result<Vec<Trade>> {
        let mut trades = Vec::<Trade>::new();

        for is_isolated in vec!["TRUE", "FALSE"] {
            let mut extra_params = QueryParams::new();
            extra_params.add("isIsolated", is_isolated, true);
            let result_trades = self
                .fetch_trades_from_endpoint(
                    symbol,
                    &EndpointsGlobal::MarginTrades.to_string(),
                    Some(extra_params),
                )
                .await?;
            trades.extend(result_trades);
        }

        Ok(trades)
    }

    pub async fn fetch_margin_loans(
        &self,
        asset: &String,
        isolated_symbol: Option<&AssetPair>,
    ) -> Result<Vec<MarginLoan>> {
        #[derive(Deserialize)]
        struct Response {
            rows: Vec<MarginLoan>,
            total: u16,
        }

        let mut loans = Vec::<MarginLoan>::new();

        // does archived = true fetches records from the last 6 months?
        for archived in &["true", "false"] {
            // fetch in batches of 90 days from `start_date` to `now()`
            let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
            let now = Utc::now().naive_utc();
            loop {
                // the API only allows 90 days between start and end
                let end = std::cmp::min(curr_start + Duration::days(89), now);
                let mut current_page: usize = 1;
                loop {
                    let mut query = QueryParams::new();
                    query.add("asset", &asset, true);
                    if let Some(s) = isolated_symbol {
                        query.add("isolatedSymbol", s.join(""), true);
                    }
                    query.add("startTime", curr_start.timestamp_millis(), true);
                    query.add("endTime", end.timestamp_millis(), true);
                    query.add("size", 100, true);
                    query.add("archived", archived, true);
                    query.add("current", current_page, true);
                    query.add("recvWindow", 60000, true);
                    query.add("timestamp", Utc::now().timestamp_millis(), false);
                    query = self.sign_request(query);

                    let resp = self
                        .api_client
                        .make_request(
                            &format!(
                                "{}{}",
                                self.domain,
                                EndpointsGlobal::MarginLoans.to_string()
                            ),
                            Some(query),
                            Some(self.default_headers()),
                            true,
                        )
                        .await?;

                    let loans_resp = self.from_json::<Response>(resp.as_ref()).await?;

                    loans.extend(loans_resp.rows);
                    if loans_resp.total >= 100 {
                        current_page += 1;
                    } else {
                        break;
                    }
                }
                curr_start = end + Duration::milliseconds(1);
                if end == now {
                    break;
                }
            }
        }

        Ok(loans)
    }

    pub async fn fetch_margin_repays(
        &self,
        asset: &String,
        isolated_symbol: Option<&AssetPair>,
    ) -> Result<Vec<MarginRepay>> {
        #[derive(Deserialize)]
        struct Response {
            rows: Vec<MarginRepay>,
            total: u16,
        }

        let mut repays = Vec::<MarginRepay>::new();

        // does archived = true fetches records from the last 6 months?
        for archived in &["true"] {
            // fetch in batches of 90 days from `start_date` to `now()`
            let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
            let now = Utc::now().naive_utc();
            loop {
                // the API only allows 90 days between start and end
                let end = std::cmp::min(curr_start + Duration::days(89), now);
                let mut current_page: usize = 1;
                loop {
                    let mut query = QueryParams::new();
                    query.add("asset", &asset, true);
                    if let Some(s) = isolated_symbol {
                        query.add("isolatedSymbol", s.join(""), true);
                    }
                    query.add("startTime", curr_start.timestamp_millis(), true);
                    query.add("endTime", end.timestamp_millis(), true);
                    query.add("size", 100, true);
                    query.add("archived", archived, true);
                    query.add("current", current_page, true);
                    query.add("recvWindow", 60000, true);
                    query.add("timestamp", Utc::now().timestamp_millis(), false);
                    query = self.sign_request(query);
                    let resp = self
                        .api_client
                        .make_request(
                            &format!(
                                "{}{}",
                                self.domain,
                                EndpointsGlobal::MarginRepays.to_string()
                            ),
                            Some(query),
                            Some(self.default_headers()),
                            true,
                        )
                        .await?;
                    let repays_resp = self.from_json::<Response>(resp.as_ref()).await?;

                    repays.extend(repays_resp.rows);
                    if repays_resp.total >= 100 {
                        current_page += 1;
                    } else {
                        break;
                    }
                }
                curr_start = end + Duration::milliseconds(1);
                if end == now {
                    break;
                }
            }
        }

        Ok(repays)
    }

    pub async fn fetch_deposits(&self) -> Result<Vec<Deposit>> {
        let mut deposits = Vec::<Deposit>::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
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
                    &format!("{}{}", self.domain, EndpointsGlobal::Deposits.to_string()),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let deposit_list: Vec<Deposit> = self.from_json(resp.as_ref()).await?;
            deposits.extend(deposit_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }
        Ok(deposits)
    }

    pub async fn fetch_withdraws(&self) -> Result<Vec<Withdraw>> {
        let mut withdraws = Vec::<Withdraw>::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
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
                    &format!("{}{}", self.domain, EndpointsGlobal::Withdraws.to_string()),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let withdraw_list: Vec<Withdraw> = self.from_json(resp.as_ref()).await?;
            withdraws.extend(withdraw_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }
        Ok(withdraws)
    }
}

impl BinanceFetcher<RegionUs> {
    pub fn new() -> Self {
        Self {
            api_client: ApiClient::new(ENDPOINT_CONCURRENCY),
            config: None,
            credentials: Credentials::<RegionUs>::new(),
            domain: API_DOMAIN_US,
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(ENDPOINT_CONCURRENCY),
            config: Some(config),
            credentials: Credentials::<RegionUs>::new(),
            domain: API_DOMAIN_US,
        }
    }

    pub async fn fetch_fiat_orders(&self, endpoint: &str) -> Result<Vec<FiatOrder>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            asset_log_record_list: Vec<FiatOrder>,
        }

        let mut orders: Vec<FiatOrder> = Vec::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            let mut query = QueryParams::new();
            query.add("fiatCurrency", "USD", true);
            query.add("startTime", curr_start.timestamp_millis(), true);
            query.add("endTime", end.timestamp_millis(), true);
            query.add("timestamp", now.timestamp_millis(), false);
            query.add("recvWindow", 60000, true);
            query = self.sign_request(query);

            let resp = self
                .api_client
                .make_request(
                    &format!("{}{}", self.domain, endpoint),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let Response {
                asset_log_record_list,
            } = self.from_json(&resp.as_ref()).await?;
            orders.extend(asset_log_record_list.into_iter().filter_map(|x| {
                if x.status == "Successful" {
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

        Ok(orders)
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders(&EndpointsUs::FiatDeposits.to_string())
            .await
    }

    pub async fn fetch_fiat_withdraws(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders(&EndpointsUs::FiatWithdraws.to_string())
            .await
    }

    pub async fn fetch_deposits(&self) -> Result<Vec<Deposit>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct DepositResponse {
            deposit_list: Vec<Deposit>,
        }

        let mut deposits = Vec::<Deposit>::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(90), now);

            let mut query = QueryParams::new();
            query.add("timestamp", now.timestamp_millis(), false);
            query.add("recvWindow", 60000, true);
            query.add("startTime", curr_start.timestamp_millis(), true);
            query.add("endTime", end.timestamp_millis(), true);
            query.add("status", 1, true);
            query = self.sign_request(query);

            let resp = self
                .api_client
                .make_request(
                    &format!("{}{}", self.domain, EndpointsUs::Deposits.to_string()),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let DepositResponse { deposit_list } = self.from_json(resp.as_ref()).await?;
            deposits.extend(deposit_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }
        Ok(deposits)
    }

    pub async fn fetch_withdraws(&self) -> Result<Vec<Withdraw>> {
        let mut withdraws = Vec::<Withdraw>::new();

        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct Response {
            withdraw_list: Vec<Withdraw>,
        }

        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
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
                    &format!("{}{}", self.domain, EndpointsUs::Withdraws.to_string()),
                    Some(query),
                    Some(self.default_headers()),
                    true,
                )
                .await?;

            let Response { withdraw_list } = self.from_json(resp.as_ref()).await?;
            withdraws.extend(withdraw_list);

            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }
        Ok(withdraws)
    }
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
