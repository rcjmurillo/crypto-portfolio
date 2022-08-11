use std::{env, marker::PhantomData, sync::Arc};

use anyhow::{anyhow, Result};

use bytes::Bytes;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::prelude::*;
use hex::encode as hex_encode;
use hmac::{Hmac, Mac};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use sha2::Sha256;

use api_client::{errors::Error as ClientError, ApiClient, QueryParams};
use exchange::{Asset, AssetPair, Candle};

use crate::{
    api_model::*,
    errors::{ApiErrorKind, Error as ApiError},
};

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
    CrossedMarginPairs,
    IsolatedMarginPairs,
    AllMarginAssets,
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
            Self::CrossedMarginPairs => "/sapi/v1/margin/allPairs",
            Self::IsolatedMarginPairs => "/sapi/v1/margin/isolated/allPairs",
            Self::AllMarginAssets => "/sapi/v1/margin/allAssets",
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
        symbol: &AssetPair,
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

        let mut all_prices = Vec::new();

        let endpoint = Arc::new(format!("{}{}", self.domain, endpoint));
        let mut handles = Vec::new();
        for (start, end) in ranges {
            let mut query = QueryParams::new();
            query.add("symbol", symbol.join(""), true);
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

        for resp in stream::iter(handles)
            .buffer_unordered(10)
            .collect::<Vec<_>>()
            .await
        {
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
                Err(err) => match err.downcast::<ClientError>() {
                    Ok(client_error) => {
                        return Err(anyhow!(format!(
                            "couldn't fetch prices for symbol: {}",
                            symbol.join("")
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
        symbol: &AssetPair,
        endpoint: &str,
        extra_params: Option<QueryParams>,
    ) -> Result<Vec<Trade>> {
        let mut trades = Vec::<Trade>::new();
        let mut last_id: u64 = 0;

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
                        t.base_asset = symbol.base.clone();
                        t.quote_asset = symbol.quote.clone();
                        trades.push(t);
                    }
                    if !fetch_more {
                        break;
                    }
                }
                Err(err) => match err.downcast::<ClientError>() {
                    Ok(client_error) => {
                        log::debug!("trades error for {:?}: {}", symbol, client_error);
                        return Err(anyhow!(client_error).context(format!(
                            "couldn't fetch trades from {}{:?} for symbol: {}",
                            endpoint,
                            extra_params,
                            symbol.join("")
                        )));
                    }
                    Err(err) => return Err(err),
                },
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
            api_client: ApiClient::new(),
            config: None,
            credentials: Credentials::<RegionGlobal>::new(),
            domain: API_DOMAIN_GLOBAL,
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(),
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

        let mut requests: Vec<_> = vec![];
        // fetch in batches of 90 days from `start_date` to `now()`
        let mut curr_start = self.data_start_date().and_hms(0, 0, 0);
        loop {
            let now = Utc::now().naive_utc();
            // the API only allows 90 days between start and end
            let end = std::cmp::min(curr_start + Duration::days(89), now);

            requests.push(async move {
                let mut orders: Vec<FiatOrder> = Vec::new();
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

                Ok(orders)
            });
            curr_start = end + Duration::milliseconds(1);
            if end == now {
                break;
            }
        }

        // concurrently run up to 30 requests
        let responses = futures::stream::iter(requests).buffer_unordered(10);
        let responses: Vec<Result<Vec<_>>> = responses.collect().await;
        let responses: Result<Vec<Vec<_>>> = responses.into_iter().collect();
        responses.and_then(|orders| {
            let resp_orders: Vec<FiatOrder> = orders.into_iter().flatten().collect();
            Ok(resp_orders)
        })
    }

    pub async fn fetch_fiat_deposits(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders("0").await
    }

    pub async fn fetch_fiat_withdraws(&self) -> Result<Vec<FiatOrder>> {
        self.fetch_fiat_orders("1").await
    }

    /// Fetch from endpoint with not params and deserialize to the specified type
    async fn fetch_from_endpoint<T: DeserializeOwned>(&self, endpoint: &str) -> Result<T> {
        let mut query_params = QueryParams::new();
        query_params.add(
            "timestamp",
            Utc::now().naive_utc().timestamp_millis(),
            false,
        );
        query_params.add("recvWindow", 60000, true);
        query_params = self.sign_request(query_params);
        let resp = self
            .api_client
            .make_request(
                &format!("{}{}", self.domain, endpoint),
                Some(query_params),
                Some(self.default_headers()),
                true,
            )
            .await?;

        self.from_json::<T>(resp.as_ref()).await
    }

    async fn fetch_all_margin_assets(&self) -> Result<Vec<Asset>> {
        #[derive(Deserialize)]
        #[serde(rename_all = "camelCase")]
        struct MarginAsset {
            asset_name: String,
        }
        let resp: Vec<MarginAsset> = self
            .fetch_from_endpoint(&EndpointsGlobal::AllMarginAssets.to_string())
            .await?;
        Ok(resp.into_iter().map(|a| a.asset_name).collect())
    }

    async fn fetch_margin_pairs(&self, endpoint: &str) -> Result<Vec<AssetPair>> {
        #[derive(Deserialize)]
        struct Pair {
            base: String,
            quote: String,
        }

        let resp = self.fetch_from_endpoint::<Vec<Pair>>(endpoint).await?;
        Ok(resp
            .into_iter()
            .map(|p| AssetPair::new(p.base, p.quote))
            .collect())
    }

    pub async fn fetch_margin_trades(&self, symbol: &AssetPair) -> Result<Vec<Trade>> {
        let crossed_margin_pairs = self
            .fetch_margin_pairs(&EndpointsGlobal::CrossedMarginPairs.to_string())
            .await?;
        let isolated_margin_pairs = self
            .fetch_margin_pairs(&EndpointsGlobal::IsolatedMarginPairs.to_string())
            .await?;
        // let all_margin_assets = self.fetch_all_margin_assets().await?;
        let mut trades = Vec::<Trade>::new();

        // log::debug!("crossed_margin_pairs: {:?}", crossed_margin_pairs);
        // log::debug!("isolated_margin_pairs: {:?}", isolated_margin_pairs);
        // log::debug!("all_margin_assets: {:?}", all_margin_assets);

        for is_isolated in &[true, false] {
            let mut extra_params = QueryParams::new();
            extra_params.add(
                "isIsolated",
                if *is_isolated { "TRUE" } else { "FALSE" },
                true,
            );
            if *is_isolated {
                if !isolated_margin_pairs.contains(symbol) {
                    continue;
                }
            } else if !crossed_margin_pairs.contains(symbol) {
                continue;
            }
            let result = self
                .fetch_trades_from_endpoint(
                    symbol,
                    &EndpointsGlobal::MarginTrades.to_string(),
                    Some(extra_params),
                )
                .await;
            match result {
                Ok(result_trades) => trades.extend(result_trades),
                Err(err) => match err.downcast::<ClientError>() {
                    Ok(client_error) => {
                        match client_error.into() {
                            // ApiErrorKind::UnavailableSymbol means the symbol is not
                            // available in the exchange, so we can just ignore it.
                            // Even though the margin pairs are verified above, sometimes
                            // the data returned by the exchange is not accurate.
                            ApiError::Api(ApiErrorKind::UnavailableSymbol) => {
                                log::debug!(
                                    "ignoring symbol {} for margin trades isolation={}",
                                    symbol.join(""),
                                    is_isolated
                                );
                                continue;
                            }
                            err => return Err(anyhow!(err)),
                        }
                    }
                    Err(err) => return Err(err),
                },
            }
        }

        Ok(trades)
    }

    pub async fn fetch_margin_transactions<T: DeserializeOwned>(
        &self,
        asset: &String,
        isolated_symbol: Option<&AssetPair>,
        endpoint: &str,
    ) -> Result<Vec<T>> {
        #[derive(Deserialize)]
        struct Response<U> {
            rows: Vec<U>,
            total: u16,
        }

        let mut txns = Vec::<T>::new();

        // fetch in batches of 90 days from `start_date` to `now()`
        let now = Utc::now().naive_utc();
        let start = self.data_start_date().and_hms(0, 0, 0);
        let archived_cutoff = now - Duration::days(90);
        let archived_cutoff = archived_cutoff.date().and_hms_micro(23, 59, 59, 99999);
        // ranges to query for archived/recent trades
        let ranges = [
            (start, archived_cutoff, true),
            (archived_cutoff + Duration::milliseconds(1), now, false),
        ];

        for (rstart, rend, archived) in ranges {
            let mut curr_start = rstart;

            let mut requests: Vec<_> = vec![];
            loop {
                // the API only allows 90 days between start and end
                let curr_end = std::cmp::min(curr_start + Duration::days(89), rend); // inclusive
                let mut current_page: usize = 1;
                requests.push(async move {
                    let mut txns = Vec::<T>::new();
                    loop {
                        let mut query = QueryParams::new();
                        query.add("asset", &asset, true);
                        if let Some(s) = isolated_symbol {
                            query.add("isolatedSymbol", s.join(""), true);
                        }
                        query.add("startTime", curr_start.timestamp_millis(), true);
                        query.add("endTime", curr_end.timestamp_millis(), true);
                        query.add("size", 100, true);
                        query.add("archived", archived, true);
                        query.add("current", current_page, true);
                        query.add("recvWindow", 60000, true);
                        query.add("timestamp", Utc::now().timestamp_millis(), false);
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

                        let txns_resp = self.from_json::<Response<T>>(resp.as_ref()).await?;
                        txns.extend(txns_resp.rows);
                        if txns_resp.total >= 100 {
                            current_page += 1;
                        } else {
                            break;
                        }
                    }
                    Ok(txns)
                });
                // move to the next date
                curr_start = curr_end + Duration::milliseconds(1);
                if curr_end == rend {
                    break;
                }
            }
            let responses = futures::stream::iter(requests).buffer_unordered(10);
            let responses: Vec<Result<Vec<_>>> = responses.collect().await;
            let responses: Result<Vec<Vec<_>>> = responses.into_iter().collect();
            match responses {
                Ok(v) => {
                    let resp_txns: Vec<T> = v.into_iter().flatten().collect();
                    txns.extend(resp_txns);
                }
                Err(err) => return Err(err),
            }
        }

        Ok(txns)
    }

    pub async fn fetch_margin_loans(
        &self,
        asset: &String,
        isolated_symbol: Option<&AssetPair>,
    ) -> Result<Vec<MarginLoan>> {
        self.fetch_margin_transactions(
            asset,
            isolated_symbol,
            &EndpointsGlobal::MarginLoans.to_string(),
        )
        .await
    }

    pub async fn fetch_margin_repays(
        &self,
        asset: &String,
        isolated_symbol: Option<&AssetPair>,
    ) -> Result<Vec<MarginRepay>> {
        self.fetch_margin_transactions(
            asset,
            isolated_symbol,
            &EndpointsGlobal::MarginRepays.to_string(),
        )
        .await
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
            api_client: ApiClient::new(),
            config: None,
            credentials: Credentials::<RegionUs>::new(),
            domain: API_DOMAIN_US,
        }
    }

    pub fn with_config(config: Config) -> Self {
        Self {
            api_client: ApiClient::new(),
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
