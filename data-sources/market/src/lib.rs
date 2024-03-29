//! Prices data fetching and solving.
//! Defines common ground for fetching prices from any source and
//! a standardized behavior and approximate market (symbol pairs) prices.

mod conversion;

use std::{fmt::Display, ops::Deref, pin::Pin, sync::Arc, task::Poll};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::{ready, Future};
use serde::{Deserialize, Serialize};
use tower::Service;

use crate::conversion::{conversion_chain, MarketType};

pub type Asset = String;

pub const FIAT_CURRENCIES: &[&str] = &["usd", "eur"];

#[derive(Debug, Serialize, Deserialize)]
pub struct Candle {
    pub open_time: u64,
    pub close_time: u64,
    pub open_price: f64,
    pub close_price: f64,
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Market {
    pub base: Asset,
    pub quote: Asset,
}

impl Market {
    pub fn new<A, B>(asset_a: A, asset_b: B) -> Self
    where
        A: ToString,
        B: ToString,
    {
        Self {
            base: asset_a.to_string(),
            quote: asset_b.to_string(),
        }
    }

    /// try to create the string from an incoming string expected to be
    /// a pair of assets joined by '-'.
    pub fn try_from_str(assets: &str) -> Result<Self> {
        let parts: Vec<&str> = assets.split("-").collect();
        if parts.len() == 2 {
            Ok(Self {
                base: parts[0].to_string(),
                quote: parts[1].to_string(),
            })
        } else {
            Err(anyhow!("couldn't parse '{}' into assets", assets))
        }
    }

    pub fn join(&self, sep: &str) -> String {
        format!("{}{}{}", self.base, sep, self.quote)
    }
}

impl Display for Market {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.base, self.quote)
    }
}

impl TryFrom<String> for Market {
    type Error = anyhow::Error;

    fn try_from(assets: String) -> Result<Self> {
        Self::try_from_str(&assets)
    }
}

#[async_trait]
/// Data source for markets
pub trait MarketData {
    async fn has_market(&self, market: &Market) -> Result<bool>;
    fn normalize(&self, market: &Market) -> Result<Market>;
    async fn price_at(&self, market: &Market, time: &DateTime<Utc>) -> Result<f64>;
    // all available markets in the data source
    async fn markets(&self) -> Result<Vec<Market>>;
}

pub fn is_fiat(asset: &str) -> bool {
    FIAT_CURRENCIES.contains(&asset.to_lowercase().as_str())
}

pub async fn price<T, U>(
    market_data: T,
    market: &Market,
    time: &DateTime<Utc>,
) -> Result<Option<f64>>
where
    U: MarketData,
    T: Deref<Target = U>,
{
    if market_data.has_market(market).await? {
        Ok(Some(market_data.price_at(market, time).await?))
    } else {
        let markets = market_data.markets().await?;
        match conversion_chain(&market_data.normalize(market)?, &markets) {
            Some(conv_chain) => {
                let mut price = 1.0;
                for market_type in conv_chain {
                    price *= match market_type {
                        MarketType::AsIs(m) => market_data.price_at(m, time).await?,
                        MarketType::Inverted(m) => 1.0 / market_data.price_at(m, time).await?,
                    };
                }
                Ok(Some(price))
            }
            None => Ok(None),
        }
    }
}

pub async fn usd_price<T: MarketData>(
    market_data: &T,
    asset: &Asset,
    time: &DateTime<Utc>,
) -> Result<f64> {
    if asset.eq_ignore_ascii_case("usd") {
        return Ok(1.0);
    }
    let usd_market = Market::new(asset.clone(), "USD");
    price(market_data, &usd_market, time)
        .await?
        .ok_or_else(|| anyhow!("couldn't find USD price for {:?}", usd_market))
}

#[derive(Clone)]
pub struct Request {
    market: Market,
    time: DateTime<Utc>,
}

impl Request {
    pub fn new(market: Market, time: DateTime<Utc>) -> Self {
        Self { market, time }
    }
}

pub struct MarketPriceService<T>(Arc<T>);

impl<T> MarketPriceService<T>
where
    T: MarketData,
{
    pub fn from_market_data(market_data: T) -> Self {
        Self(Arc::new(market_data))
    }
}

impl<T> Service<Request> for MarketPriceService<T>
where
    T: MarketData + Send + Sync + 'static,
{
    type Response = Option<f64>;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // always ready to make more requests!
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let md = self.0.clone();
        Box::pin(async move { price(md, &req.market, &req.time).await })
    }
}

/// A services that uses a wraps two services and uses the 2nd one as fallback
/// if calling the first one fails.
pub struct Fallback<S1, S2> {
    inner1: S1,
    inner2: S2,
}

impl<S1, S2> Fallback<S1, S2> {
    pub fn new(s1: S1, s2: S2) -> Self {
        Self {
            inner1: s1,
            inner2: s2,
        }
    }
}

impl<S1, S2> Service<Request> for Fallback<S1, S2>
where
    S1: Service<Request, Error = anyhow::Error>,
    S1::Future: Send + 'static,
    S1::Response: Send,
    S1::Error: Send,
    S2: Service<Request, Response = S1::Response, Error = S1::Error>,
    S2::Future: Send + 'static,
{
    type Response = S1::Response;
    type Error = S1::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        // check that both services are ready
        ready!(self.inner1.poll_ready(cx))?;
        ready!(self.inner2.poll_ready(cx))?;

        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let f1 = self.inner1.call(req.clone());
        let f2 = self.inner2.call(req);

        Box::pin(async move {
            match f1.await {
                Ok(resp) => Ok(resp),
                Err(err) => f2.await.map_err(|err2| err2.context(err)),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    use crate::{price, Market, MarketData};

    macro_rules! assert_close {
        ($n1:expr,$n2:expr,$t:expr) => {
            assert!(f64::abs($n1 - $n2) < $t);
        };
    }

    struct TestMarketData;

    impl TestMarketData {
        fn markets(&self) -> Vec<Market> {
            vec![
                ("BTC", "USD"),
                ("BTC", "ETH"),
                ("ETH", "USD"),
                ("USDC", "USD"),
                ("SOL", "ETH"),
                ("SOL", "USD"),
                ("ETH", "BTC"),
                ("WBTC", "USD"),
                ("ADA", "ETH"),
                ("DOT", "ETH"),
            ]
            .into_iter()
            .map(|t| Market::new(t.0.to_string(), t.1.to_string()))
            .collect()
        }
    }

    #[async_trait]
    impl MarketData for TestMarketData {
        async fn has_market(&self, market: &Market) -> Result<bool> {
            Ok(self.markets().contains(market))
        }

        fn normalize(&self, market: &Market) -> Result<Market> {
            Ok(market.clone())
        }

        async fn markets(&self) -> Result<Vec<Market>> {
            Ok(self.markets())
        }

        async fn price_at(&self, market: &Market, _: &DateTime<Utc>) -> Result<f64> {
            Ok(match (market.base.as_str(), market.quote.as_str()) {
                ("BTC", "USD") => 19_000.0,
                ("ETH", "USD") => 1352.0,
                ("USDC", "USD") => 0.9998,
                ("SOL", "ETH") => 0.02354,
                ("SOL", "USD") => 31.82,
                ("ETH", "BTC") => 0.07113673,
                ("BTC", "ETH") => 14.0574355,
                ("WBTC", "USD") => 19_011.0,
                ("ADA", "ETH") => 0.0003274,
                ("DOT", "ETH") => 0.004655,
                _ => panic!("tried to fetch price for nonexistent market {}", market,),
            })
        }
    }

    #[tokio::test]
    async fn test_solve_price() {
        let m = Market::new("BTC".to_string(), "USD".to_string());
        let market_data = TestMarketData;
        assert!(market_data.has_market(&m).await.unwrap());
        assert_eq!(
            price(&market_data, &m, &Utc::now()).await.unwrap().unwrap(),
            // direct BTC-USD
            19_000.0
        );

        let m = Market::new("BTC".to_string(), "USDC".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());

        println!(
            "{} {}",
            price(&market_data, &m, &Utc::now()).await.unwrap().unwrap(),
            19_000.0 * (1.0 / 0.9998)
        );
        assert_close!(
            price(&market_data, &m, &Utc::now()).await.unwrap().unwrap(),
            // BTC-USD * (1 / USDC-USD)
            19_000.0 * (1.0 / 0.9998),
            5.0
        );

        let m = Market::new("SOL".to_string(), "BTC".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_close!(
            price(&market_data, &m, &Utc::now()).await.unwrap().unwrap(),
            // SOL-ETH * ETH-BTC
            0.02354 * 0.07113673,
            0.0000001
        );

        let m = Market::new("ADA".to_string(), "DOT".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_close!(
            price(&market_data, &m, &Utc::now()).await.unwrap().unwrap(),
            // ADA-ETH * (1 / DOT-ETH)
            0.0003274 * (1.0 / 0.004655),
            0.0001
        );
    }
}
