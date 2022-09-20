//! Prices data fetching and solving.
//! Defines common ground for fetching prices from any source and
//! a standardized behavior and approximate market (symbol pairs) prices.

mod conversion;

use std::{fmt::Display, pin::Pin, sync::Arc, task::Poll};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::Future;
use tower::Service;

pub type Asset = String;

pub const FIAT_CURRENCIES: &[&str] = &["usd", "eur"];

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

    /// try to create the string from an incoming string expected to be0
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

// impl PartialEq for &Market {
//     fn eq(&self, other: &Self) -> bool {
//         self.base == other.base && self.quote == other.quote
//     }
// }

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
    /// A list of "proxy" markets that can be used to indirectly compute the price of the
    /// provided market if this one doesn't exist or can't be computed directly.
    /// e.g. given a market DOT-ETH, a list of proxy markets could be:
    ///   [DOT-BTC, ETH-BTC] or [DOT-USD, ETH-USD]
    /// This list of markets must act as a chain where the quote symbol of a market must be
    /// present on the next one.
    /// TODO: maybe there is a way to add a method to this trait like `find_markets_for(Asset::Base(&str))`
    /// and generalize the logic of `proxy_markets_for` in terms of that method.
    async fn proxy_markets_for(&self, market: &Market) -> Result<Option<Vec<Market>>>;
    async fn price_at(&self, market: &Market, time: &DateTime<Utc>) -> Result<f64>;
}

pub fn is_fiat(asset: &str) -> bool {
    FIAT_CURRENCIES.contains(&asset)
}

// TODO: unify with the method below
pub async fn solve_price<T>(market_data: &T, market: &Market, time: &DateTime<Utc>) -> Result<f64>
where
    T: MarketData + ?Sized,
{
    log::debug!("solving price for {:?}", market);
    if market_data.has_market(market).await? {
        Ok(market_data.price_at(market, time).await?)
    } else {
        match market_data.proxy_markets_for(market).await? {
            Some(proxy_markets) => {
                // for each proxy market:
                //   * if it's the first market being processed set the last price to this market's
                //     price.
                //   * if there is a last market and last price, use this market to convert the last
                //     price in terms of the new one.
                //   * set this market as the last one seen to use it on the next loop.
                //
                // The price of a market is always in terms of the quote symbol, thus the proxy
                // markets list should be formed in a way that a market divided in the chain by the
                // previous one will ultimately compute the price of the original market's base in terms
                // of quote symbol.
                // e.g. given a market DOT-VET and a list of proxy market with prices:
                //
                //   0: DOT-BTC -> 0.00034032
                //   1: VET-BTC -> 0.00000113
                //
                // the following operation will give the price of DOT in terms of VET:
                //
                //  0.00034032 / 0.00000113 = ~301.1681
                //
                let mut last_market: Option<&Market> = None;
                let mut last_price = 0.0;
                for proxy_market in proxy_markets.iter() {
                    if market_data.has_market(proxy_market).await? {
                        let proxy_price = market_data.price_at(proxy_market, time).await?;

                        match last_market {
                            Some(lm) => {
                                if (proxy_market.base != lm.base && proxy_market.base != lm.quote)
                                    && (proxy_market.quote != lm.base
                                        && proxy_market.quote != lm.quote)
                                {
                                    return Err(anyhow!("malformed proxy market list, base nor quote found on previous last market previous={} current={}", lm, proxy_market));
                                }
                                last_price /= proxy_price;
                            }
                            None => {
                                last_price = proxy_price;
                            }
                        }
                        last_market.replace(proxy_market);
                    } else {
                        return Err(anyhow!("proxy market not found: {}", proxy_market));
                    }
                }
                if last_price > 0.0 {
                    Ok(last_price)
                } else {
                    Err(anyhow!("couldn't find price for {}", market))
                }
            }
            None => Err(anyhow!("no proxy markets found for {}", market)),
        }
    }
}

pub async fn solve_price_2<T>(market_data: Arc<T>, market: Market, time: DateTime<Utc>) -> Result<f64>
where
    T: MarketData,
{
    log::debug!("solving price for {:?}", market);
    if market_data.has_market(&market).await? {
        Ok(market_data.price_at(&market, &time).await?)
    } else {
        match market_data.proxy_markets_for(&market).await? {
            Some(proxy_markets) => {
                // for each proxy market:
                //   * if it's the first market being processed set the last price to this market's
                //     price.
                //   * if there is a last market and last price, use this market to convert the last
                //     price in terms of the new one.
                //   * set this market as the last one seen to use it on the next loop.
                //
                // The price of a market is always in terms of the quote symbol, thus the proxy
                // markets list should be formed in a way that a market divided in the chain by the
                // previous one will ultimately compute the price of the original market's base in terms
                // of quote symbol.
                // e.g. given a market DOT-VET and a list of proxy market with prices:
                //
                //   0: DOT-BTC -> 0.00034032
                //   1: VET-BTC -> 0.00000113
                //
                // the following operation will give the price of DOT in terms of VET:
                //
                //  0.00034032 / 0.00000113 = ~301.1681
                //
                let mut last_market: Option<&Market> = None;
                let mut last_price = 0.0;
                for proxy_market in proxy_markets.iter() {
                    if market_data.has_market(proxy_market).await? {
                        let proxy_price = market_data.price_at(proxy_market, &time).await?;

                        match last_market {
                            Some(lm) => {
                                if (proxy_market.base != lm.base && proxy_market.base != lm.quote)
                                    && (proxy_market.quote != lm.base
                                        && proxy_market.quote != lm.quote)
                                {
                                    return Err(anyhow!("malformed proxy market list, base nor quote found on previous last market previous={} current={}", lm, proxy_market));
                                }
                                last_price /= proxy_price;
                            }
                            None => {
                                last_price = proxy_price;
                            }
                        }
                        last_market.replace(proxy_market);
                    } else {
                        return Err(anyhow!("proxy market not found: {}", proxy_market));
                    }
                }
                if last_price > 0.0 {
                    Ok(last_price)
                } else {
                    Err(anyhow!("couldn't find price for {}", market))
                }
            }
            None => Err(anyhow!("no proxy markets found for {}", market)),
        }
    }
}

struct Request {
    market: Market,
    time: DateTime<Utc>,
}

struct MarketPriceService<T>(Arc<T>);

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
    type Response = f64;
    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(())) // always ready to make more requests!
    }

    fn call(&mut self, req: Request) -> Self::Future {
        Box::pin(solve_price_2(
            self.0.clone(),
            req.market.clone(),
            req.time.clone(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    use crate::{solve_price, Market, MarketData};

    struct TestMarketData;

    #[async_trait]
    impl MarketData for TestMarketData {
        async fn has_market(&self, market: &Market) -> Result<bool> {
            Ok(match (market.base.as_str(), market.quote.as_str()) {
                ("BTC", "USD")
                | ("BTC", "ETH")
                | ("ETH", "USD")
                | ("USDC", "USD")
                | ("SOL", "ETH")
                | ("SOL", "USD")
                | ("ETH", "BTC")
                | ("WBTC", "USD")
                | ("ADA", "ETH")
                | ("DOT", "ETH") => true,
                _ => false,
            })
        }

        async fn proxy_markets_for(&self, market: &Market) -> Result<Option<Vec<Market>>>
        where
            Self: Sized,
        {
            Ok(match (market.base.as_str(), market.quote.as_str()) {
                ("BTC", "USDC") => Some(vec![
                    Market::new("BTC".to_string(), "USD".to_string()),
                    Market::new("USDC".to_string(), "USD".to_string()),
                ]),
                ("SOL", "BTC") => Some(vec![
                    Market::new("SOL".to_string(), "USD".to_string()),
                    Market::new("ETH".to_string(), "USD".to_string()),
                    Market::new("BTC".to_string(), "ETH".to_string()),
                ]),
                ("WBTC", "BTC") => Some(vec![
                    Market::new("WBTC".to_string(), "USD".to_string()),
                    Market::new("BTC".to_string(), "USD".to_string()),
                ]),
                ("ADA", "DOT") => Some(vec![
                    Market::new("DOT".to_string(), "ETH".to_string()),
                    Market::new("ADA".to_string(), "ETH".to_string()),
                ]),
                _ => None,
            })
        }

        async fn price_at(&self, market: &Market, _: &DateTime<Utc>) -> Result<f64> {
            Ok(match (market.base.as_str(), market.quote.as_str()) {
                ("BTC", "USD") => 20_000.0,
                ("ETH", "USD") => 1700.0,
                ("USDC", "USD") => 0.9998,
                ("SOL", "ETH") => 0.02047,
                ("SOL", "USD") => 31.05,
                ("ETH", "BTC") => 0.0779,
                ("BTC", "ETH") => 0.0017258863752659917,
                ("WBTC", "USD") => 19_998.7,
                ("ADA", "ETH") => 0.00029129,
                ("DOT", "ETH") => 0.00444281,
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
            solve_price(&market_data, &m, &Utc::now()).await.unwrap(),
            // direct BTC-USD
            20_000.0
        );

        let m = Market::new("BTC".to_string(), "USDC".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&market_data, &m, &Utc::now()).await.unwrap(),
            // BTC-USD / USDC-USD
            20_000.0 / 0.9998
        );

        let m = Market::new("SOL".to_string(), "BTC".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&market_data, &m, &Utc::now()).await.unwrap(),
            // SOL-USD / ETH-USD / BTC-ETH
            31.05 / 1700.0 / 0.0017258863752659917
        );

        let m = Market::new("ADA".to_string(), "DOT".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&market_data, &m, &Utc::now()).await.unwrap(),
            // DOT-ETH / ADA-ETH
            0.00444281 / 0.00029129
        );
    }
}
