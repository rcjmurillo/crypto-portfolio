//! coingecko API V3 client, will be mostly used to fetch price information.
//! The free API will be used, which has a limit of 50 calls/minute.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize};
use std::collections::HashMap;
use std::{convert::TryInto, time::Duration};
use tokio::sync::Mutex;
use tower::{
    limit::{rate::Rate, RateLimit},
    Service,
};

use api_client::{ApiClient, Cache, Query, RequestBuilder};
use market::{Market, MarketData, CURRENCIES};

const REQUESTS_PER_PERIOD: u64 = 1;
const API_HOST: &'static str = "api.coingecko.com";

enum Api {
    CoinsList,
    CoinsMarketChartRange,
    SupportedVsCurrencies,
}

impl AsRef<str> for Api {
    fn as_ref(&self) -> &str {
        match self {
            Self::CoinsList => "/api/v3/coins/list",
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
    api_service: Mutex<Cache<RateLimit<ApiClient>>>,
    config: &'a Config,
}

impl<'a> Client<'a> {
    pub fn with_config(config: &'a Config) -> Self {
        Self {
            api_service: Mutex::new(Cache::new(RateLimit::new(
                ApiClient::new(),
                Rate::new(REQUESTS_PER_PERIOD, Duration::from_secs(3)),
            ))),
            config,
        }
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

        let mut found = coins.into_iter().fold(vec![], |mut acc, c| {
            if c.symbol == symbol {
                acc.push(c);
            }
            acc
        });
        if found.len() > 1 {
            println!("found multiple: {:?}", found);
            found
                .into_iter()
                .find(|c| {
                    self.config
                        .symbol_mappings
                        .get(&symbol)
                        .filter(|sid| *sid == &c.id)
                        .is_some()
                })
                .ok_or(anyhow!("couldn't find symbol {}", symbol))
        } else {
            println!("found a single one: {:?}", found);
            found
                .pop()
                .ok_or(anyhow!("couldn't find symbol {}", symbol))
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

        let resp = svc.call(request).await?;
        self.from_json(&resp)
    }

    async fn fetch_coins_list(&self) -> Result<Vec<CoinInfo>> {
        let mut svc = self.api_service.lock().await;

        let request = RequestBuilder::default()
            .url(Url::parse(&format!("https://{}", API_HOST))?.join(Api::CoinsList.as_ref())?)
            .cache_response(true)
            .build()?;

        let resp = svc.call(request).await?;
        let coins: Vec<CoinInfo> = self.from_json(&resp)?;

        Ok(coins)
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

        let resp = svc.call(request).await?;
        let CoinPrices { prices } = self.from_json(&resp)?;

        Ok(prices)
    }

    fn from_json<T: DeserializeOwned>(&self, resp_bytes: &Bytes) -> Result<T> {
        match serde_json::from_slice(&resp_bytes.clone()) {
            Ok(val) => Ok(val),
            Err(err) => Err(anyhow!(err.to_string())
                .context(format!("couldn't parse: {:?}", resp_bytes.clone()))),
        }
    }
}

#[async_trait]
impl MarketData for Client<'_> {
    async fn has_market(&self, market: &Market) -> Result<bool> {
        log::debug!("checking if market exists: {:?}", market);
        let supported_vs_currencies = self.supported_vs_currencies().await?;
        // per tests with the API, for any symbol that's in the supported vs currencies, it's possible
        // to get a market with any other symbol as base (except currencies) and that symbol as quote/vs_currency.
        Ok(supported_vs_currencies.contains(&market.quote.to_lowercase())
            && !CURRENCIES.contains(&market.base.to_lowercase().as_str()))
    }

    async fn proxy_markets_for(&self, market: &Market) -> Result<Option<Vec<Market>>> {
        log::debug!("getting proxy markets for: {:?}", market);
        let supported_vs_currencies = self.supported_vs_currencies().await?;

        // case when the both symbols are currencies
        match (
            CURRENCIES.contains(&market.base.to_lowercase().as_str()),
            CURRENCIES.contains(&market.quote.to_lowercase().as_str()),
        ) {
            (true, true) => {
                return Ok(Some(vec![
                    Market::new("btc", market.base.to_lowercase()),
                    Market::new("btc", market.quote.to_lowercase()),
                ]))
            }
            (true, false) => return Err(anyhow!("invalid market {}", market)),
            (_, _) => (), // valid market, continue
        }

        // to keep this simple for now, this is going to use some big coins that
        // must be in the supported vs currencies. Try with each one of them and stop
        // when one of them is in the list.
        let common_quote_symbols = vec!["usd", "btc", "eth"];
        for quote_symbol in common_quote_symbols.iter() {
            // just in case, this shouldn't happen
            if *quote_symbol == market.quote {
                continue;
            }
            if supported_vs_currencies.contains(&(*quote_symbol).to_string()) {
                return Ok(Some(vec![
                    Market::new(market.base.to_lowercase(), quote_symbol),
                    Market::new(market.quote.to_lowercase(), quote_symbol),
                ]));
            }
        }
        Err(anyhow!(
            "common quote symbols {:?} not present in supported vs currencies",
            common_quote_symbols
        ))
    }

    async fn price_at(&self, market: &Market, time: &DateTime<Utc>) -> Result<f64> {
        log::debug!("getting price for market: {:?}", market);
        let base_coin_info = self.fetch_coin_info(&market.base).await?;
        let quote_coin_info = self.fetch_coin_info(&market.quote).await?;
        let ts = time.timestamp().try_into()?;
        // the API provides hourly granularity if we request a range <= 90 days
        let bucket_size = 90 * 24 * 60 * 60; // seconds
        let (start_ts, end_ts) = bucketize_timestamp(ts, bucket_size);
        let prices = self
            .fetch_coin_market_chart_range(
                &base_coin_info.id,
                &quote_coin_info.symbol,
                // given the API uses an hourly granularity, substract and add an hour to
                // the range boundaries to make sure the timestamp we're looking for is included.
                start_ts - 60 * 60,
                end_ts + 60 * 60,
            )
            .await?;
        // find the index of the first price with timestamp greater than one we're looking for.
        // The price used will be the one right before this one.
        // note: the API expects the timestamps in seconds but the response includes
        // timestamps in milliseconds.
        let ts_ms = time.timestamp_millis().try_into()?;
        let index = prices
            .iter()
            .position(|p| p.timestamp > ts_ms)
            .ok_or(anyhow!("couldn't find price in list of prices"))?;
        Ok(prices[index - 1].price)
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
