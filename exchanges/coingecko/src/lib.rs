//! coingecko API V3 client, will be mostly used to fetch price information.
//! The free API will be used, which has a limit of 50 calls/minute.

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize};
use std::time::Duration;
use tokio::sync::Mutex;
use tower::{
    limit::{rate::Rate, RateLimit},
    Service,
};

use api_client::{ApiClient, Query, RequestBuilder};
use exchange::{Asset, AssetPair, AssetsInfo};

const REQUESTS_PER_MINUTE: u64 = 50;
const API_HOST: &'static str = "api.coingecko.com";

enum Api {
    CoinsList,
    CoinsMarketChartRange,
}

impl AsRef<str> for Api {
    fn as_ref(&self) -> &str {
        match self {
            Self::CoinsList => "/api/v3/coins/list",
            Self::CoinsMarketChartRange => "/api/v3/coins/{}/market_chart/range",
        }
    }
}

impl ToString for Api {
    fn to_string(&self) -> String {
        self.as_ref().to_string()
    }
}

#[derive(Deserialize, Clone)]
struct CoinPrice {
    timestamp: u64,
    price: f64,
}

#[derive(Deserialize, Clone)]
struct CoinInfo {
    id: String,
    symbol: String,
}

pub struct Client {
    api_service: Mutex<RateLimit<ApiClient>>,
}

impl Client {
    pub fn new() -> Self {
        Self {
            api_service: Mutex::new(RateLimit::new(
                ApiClient::new(),
                Rate::new(REQUESTS_PER_MINUTE, Duration::from_secs(60)),
            )),
        }
    }

    async fn fetch_coin_info(&self, symbol: &str) -> Result<CoinInfo> {
        // todo: cache this vector
        let coins = self.fetch_coins_list().await?;

        coins
            .into_iter()
            .find(|c| c.symbol == symbol)
            .ok_or(anyhow!("couldn't find symbol {}", symbol))
    }

    async fn fetch_coins_list(&self) -> Result<Vec<CoinInfo>> {
        let mut svc = self.api_service.lock().await;
        // await until the service is ready
        futures::future::poll_fn(|cx| svc.poll_ready(cx)).await?;

        let request = RequestBuilder::default()
            .url(Url::parse(&format!("https://{}", API_HOST))?.join(Api::CoinsList.as_ref())?)
            .cache_response(true)
            .build()?;

        #[derive(Deserialize)]
        struct CoinList {
            coins: Vec<CoinInfo>,
        }

        let resp = svc.call(request).await?;
        let CoinList { coins } = self.from_json(&resp)?;

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
        // await until the service is ready
        futures::future::poll_fn(|cx| svc.poll_ready(cx)).await?;
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

    /// assign the provided timestamp into a bucket (start, end), so we can fetch a range
    /// of prices for the time range (bucket) the provided timestamp falls into. Given
    /// responses will be cached it will help to avoid making subsequent requests for
    /// timestamps that fall into the same "bucket".
    fn bucketize_timestamp(&self, ts: u64) -> (u64, u64) {
        // the API provides hourly granularity if we request a range <= 90 days
        let bucket_size = 90 * 24 * 60 * 60; // seconds
        let start_ts = ts - (ts % bucket_size);
        let end_ts = start_ts + bucket_size;
        (start_ts, end_ts)
    }
}

#[async_trait]
impl AssetsInfo for Client {
    async fn price_at(&self, asset_pair: &AssetPair, time: &DateTime<Utc>) -> Result<f64> {
        let quote_coin_info = self.fetch_coin_info(&asset_pair.quote).await?;
        let base_coin_info = self.fetch_coin_info(&asset_pair.base).await?;
        let ts = time.timestamp().try_into()?;
        let (start_ts, end_ts) = self.bucketize_timestamp(ts);
        let prices = self
            .fetch_coin_market_chart_range(
                &base_coin_info.id,
                &quote_coin_info.id,
                // given the API uses an hourly granularity, substract and add an hour to
                // the range boundaries to make sure the timestamp we're looking for is included.
                start_ts - 60,
                end_ts + 60,
            )
            .await?;
        // find the index of the first price with timestamp greater than one we're looking for.
        // The price used will be the one right before this one.
        let index = prices
            .iter()
            .position(|p| p.timestamp > ts)
            .ok_or(anyhow!("couldn't find price in list of prices"))?;
        Ok(prices[index - 1].price)
    }
    async fn usd_price_at(&self, asset: &Asset, time: &DateTime<Utc>) -> Result<f64> {
        self.price_at(&AssetPair::new(asset, &"usd"), time).await
    }
}
