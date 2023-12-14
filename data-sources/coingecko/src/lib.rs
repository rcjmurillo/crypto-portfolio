//! coingecko API V3 client, will be mostly used to fetch price information.
//! The free API will be used, which has a limit of 50 calls/minute.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize};
use std::collections::{HashMap, HashSet};
use std::{convert::TryInto, time::Duration};
use tokio::sync::Mutex;
use tower::{
    limit::{rate::Rate, RateLimit},
    Service, ServiceExt,
};

use api_client::{ApiClient, Cache, SqliteCache, Query, RequestBuilder};
use market::{Market, MarketData};

const REQUESTS_PER_PERIOD: u64 = 1;
const API_HOST: &'static str = "api.coingecko.com";

enum Api {
    CoinsList,
    CoinsMarkets,
    CoinsMarketChartRange,
    SupportedVsCurrencies,
}

impl AsRef<str> for Api {
    fn as_ref(&self) -> &str {
        match self {
            Self::CoinsList => "/api/v3/coins/list",
            Self::CoinsMarkets => "/api/v3/coins/markets",
            Self::CoinsMarketChartRange => "/api/v3/coins/{}/market_chart/range",
            Self::SupportedVsCurrencies => "/api/v3/simple/supported_vs_currencies",
        }
    }
}

impl ToString for Api {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

#[derive(Deserialize, Clone, Debug)]
struct CoinPrice {
    timestamp: u64,
    price: f64,
}

#[derive(Deserialize, Clone, Debug)]
struct CoinInfo {
    id: String,
    symbol: String,
}

/// assign the provided timestamp into a bucket (start, end), so we can fetch a range
/// of prices for the time range (bucket) the provided timestamp falls into. Given
/// responses will be cached it will help to avoid making subsequent requests for
/// timestamps that fall into the same "bucket".
fn bucketize_timestamp(ts: u64, bucket_size: u64) -> (u64, u64) {
    let start_ts = ts - (ts % bucket_size);
    let end_ts = start_ts + bucket_size;
    (start_ts, end_ts)
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    // maps a symbol to it's id in coingecko
    pub symbol_mappings: HashMap<String, String>,
}

pub struct Client<'a> {
    api_service: Mutex<Cache<RateLimit<ApiClient>, SqliteCache>>,
    config: &'a Config,
    markets: Option<Vec<Market>>,
}

impl<'a> Client<'a> {
    pub fn with_config(config: &'a Config) -> Self {
        Self {
            api_service: Mutex::new(Cache::new(
                RateLimit::new(
                    ApiClient::new(),
                    Rate::new(REQUESTS_PER_PERIOD, Duration::from_secs(3)),
                ),
                SqliteCache::new("coingecko").expect("couldn't create kv client"),
            )),
            config,
            markets: None,
        }
    }

    pub async fn init(&mut self) -> Result<()> {
        self.fetch_markets().await?;
        Ok(())
    }

    pub async fn fetch_markets(&mut self) -> Result<()> {
        self.markets = Some(self.fetch_coin_markets().await?);
        Ok(())
    }

    async fn fetch_coin_info(&self, symbol: &str) -> Result<CoinInfo> {
        // todo: cache this vector
        let coins = self.fetch_coins_list().await?;

        // todo: handle this better
        if symbol.to_lowercase().as_str() == "usd" {
            return Ok(CoinInfo {
                id: "usd".to_string(),
                symbol: "usd".to_string(),
            });
        }
        if symbol.to_lowercase().as_str() == "eur" {
            return Ok(CoinInfo {
                id: "eur".to_string(),
                symbol: "eur".to_string(),
            });
        }

        let symbol = symbol.to_lowercase();

        // if there is an explicit mapping in the config file, that takes presedence
        if let Some(id_mapping) = self.config.symbol_mappings.get(&symbol) {
            return Ok(CoinInfo {
                id: id_mapping.clone(),
                symbol: symbol,
            });
        }

        let mut found = coins.into_iter().fold(vec![], |mut acc, c| {
            if c.symbol == symbol {
                acc.push(c);
            }
            acc
        });

        if found.len() > 1 {
            Err(anyhow!(
                "multiple coins found for symbol {}: {:?}",
                symbol,
                found
            ))
        } else {
            found
                .pop()
                .ok_or_else(|| anyhow!("couldn't find coin info for symbol '{}'", symbol))
        }
    }

    async fn supported_vs_currencies(&self) -> Result<Vec<String>> {
        let mut svc = self.api_service.lock().await;

        let request = RequestBuilder::default()
            .url(
                Url::parse(&format!("https://{}", API_HOST))?
                    .join(Api::SupportedVsCurrencies.as_ref())?,
            )
            .cache_response(true)
            .build()?;

        let resp = svc
            .ready()
            .await
            .map_err(|e| anyhow!(e))?
            .call(request)
            .await
            .map_err(|e| anyhow!(e))?;
        self.from_json(&resp)
    }

    async fn fetch_coins_list(&self) -> Result<Vec<CoinInfo>> {
        let mut svc = self.api_service.lock().await;

        let request = RequestBuilder::default()
            .url(Url::parse(&format!("https://{}", API_HOST))?.join(Api::CoinsList.as_ref())?)
            .cache_response(true)
            .build()?;

        let resp = svc
            .ready()
            .await
            .map_err(|e| anyhow!(e))?
            .call(request)
            .await
            .map_err(|e| anyhow!(e))?;
        let coins: Vec<CoinInfo> = self.from_json(&resp)?;

        Ok(coins)
    }

    async fn fetch_coin_markets(&self) -> Result<Vec<Market>> {
        #[derive(Deserialize)]
        struct MarketResponse {
            id: String,
            symbol: String,
        }
        let mut svc = self.api_service.lock().await;
        let url = Api::CoinsMarkets.as_ref();

        let mut markets = Vec::new();

        let mut seen = HashSet::new();
        let vs_currencies = ["usd", "eur", "btc", "eth", "bnb"];

        for vs_coin in vs_currencies {
            log::debug!("fetching markets for vs_currency={}", vs_coin);
            let mut page = 0;
            loop {
                let mut query = Query::new();
                query
                    .cached_param("vs_currency", vs_coin)
                    .cached_param("per_page", 250)
                    .cached_param("order", "market_cap_desc")
                    .cached_param("page", page);

                let request = RequestBuilder::default()
                    .url(Url::parse(&format!("https://{}", API_HOST))?.join(&url)?)
                    .query_params(query)
                    .cache_response(true)
                    .build()?;

                let resp = svc
                    .ready()
                    .await
                    .map_err(|e| anyhow!(e))?
                    .call(request)
                    .await
                    .map_err(|e| anyhow!(e))?;
                let resp_markets: Vec<MarketResponse> = self.from_json(&resp)?;
                if page == 5 || resp_markets.len() == 0 {
                    break;
                }
                log::debug!(
                    "got markets {} for vs_currency={}",
                    resp_markets.len(),
                    vs_coin
                );
                for m in resp_markets {
                    let mkey = (m.id, vs_coin);
                    if !seen.contains(&mkey) {
                        markets.push(Market::new(m.symbol, vs_coin));
                        seen.insert(mkey);
                    }
                }
                page += 1;
            }
        }

        Ok(markets)
    }

    async fn fetch_coin_market_chart_range(
        &self,
        coin_id: &str,
        vs_coin: &str,
        start_ts: u64,
        end_ts: u64,
    ) -> Result<Vec<CoinPrice>> {
        let mut svc = self.api_service.lock().await;
        let url = Api::CoinsMarketChartRange.as_ref().replace("{}", coin_id);

        let mut query = Query::new();
        query
            .cached_param("vs_currency", vs_coin)
            .cached_param("from", start_ts)
            .cached_param("to", end_ts);

        let request = RequestBuilder::default()
            .url(Url::parse(&format!("https://{}", API_HOST))?.join(&url)?)
            .query_params(query)
            .cache_response(true)
            .build()?;

        #[derive(Deserialize)]
        struct CoinPrices {
            prices: Vec<CoinPrice>,
        }

        let resp = svc
            .ready()
            .await
            .map_err(|e| anyhow!(e))?
            .call(request)
            .await
            .map_err(|e| anyhow!(e))?;
        let CoinPrices { prices } = self.from_json(&resp)?;

        Ok(prices)
    }

    fn from_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(&resp_bytes.clone()) {
            Ok(val) => Ok(val),
            Err(err) => Err(anyhow!(err.to_string()).context(format!(
                "couldn't parse coingecko API response: {:?}",
                resp_bytes.clone()
            ))),
        }
    }
}

#[async_trait]
impl MarketData for Client<'_> {
    async fn has_market(&self, m: &Market) -> Result<bool> {
        let supported_vs_currencies = self.supported_vs_currencies().await?;
        // per tests with the API, for any symbol that's in the supported vs currencies, it's possible
        // to get a market with any other symbol as base (except currencies) and that symbol as quote/vs_currency.
        Ok(supported_vs_currencies.contains(&m.quote.to_lowercase())
            && !market::is_fiat(&m.base.to_lowercase().as_str()))
        // Ok(supported_vs_currencies.contains(&m.quote.to_lowercase()))
    }

    fn normalize(&self, market: &Market) -> Result<Market> {
        Ok(Market::new(
            market.base.to_ascii_lowercase(),
            market.quote.to_ascii_lowercase(),
        ))
    }

    async fn markets(&self) -> Result<Vec<Market>> {
        Ok(self.markets.clone().expect("missing markets"))
    }

    async fn price_at(&self, market: &Market, time: &DateTime<Utc>) -> Result<f64> {
        let base_coin_info = self
            .fetch_coin_info(&market.base.to_ascii_lowercase())
            .await?;
        let quote_coin_info = self
            .fetch_coin_info(&market.quote.to_ascii_lowercase())
            .await?;
        let ts = time.timestamp().try_into()?;
        // the API provides hourly granularity if we request a range <= 90 days
        let bucket_size = 90 * 24 * 60 * 60; // seconds
        let (start_ts, end_ts) = bucketize_timestamp(ts, bucket_size);
        let end_ts = std::cmp::min(Utc::now().timestamp().try_into().unwrap(), end_ts);
        let prices = self
            .fetch_coin_market_chart_range(
                &base_coin_info.id,
                &quote_coin_info.symbol,
                start_ts,
                end_ts,
            )
            .await?;

        if prices.len() == 0 {
            return Err(anyhow!(
                "got empty response for market {} prices at {}",
                market,
                time
            ));
        }

        // find the index of the first price with timestamp greater than one we're looking for.
        // The price used will be the one right before this one.
        // note: the API expects the timestamps in seconds but the response includes
        // timestamps in milliseconds.
        let ts_ms = time.timestamp_millis().try_into()?;
        prices
            .iter()
            .find(|p| p.timestamp.abs_diff(ts_ms) <= 60 * 60 * 1000)
            .map(|p| p.price)
            .ok_or(anyhow!(
                "couldn't find price for {} at {} in response list of prices start={} end={} [{:?}, ..., {:?}]",
                market,
                ts_ms,
                start_ts,
                end_ts,
                prices.first(),
                prices.last(),
            ))
    }
}

#[cfg(test)]
mod tests {
    use crate::bucketize_timestamp;

    #[test]
    fn test_bucketize_timestamp() {
        assert_eq!(bucketize_timestamp(10, 15), (0, 15));
        assert_eq!(bucketize_timestamp(7, 15), (0, 15));
        assert_eq!(bucketize_timestamp(0, 15), (0, 15));
        assert_eq!(bucketize_timestamp(15, 15), (15, 30));
        assert_eq!(bucketize_timestamp(16, 15), (15, 30));
        assert_eq!(bucketize_timestamp(77, 20), (60, 80));
        assert_eq!(bucketize_timestamp(98, 20), (80, 100));
    }
}
