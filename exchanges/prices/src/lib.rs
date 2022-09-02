//! Prices data fetching and solving.
//! Defines common ground for fetching prices from any source and
//! a standardized behavior and approximate market (symbol pairs) prices.

use std::fmt::Display;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

pub trait Market: Display {
    fn base(&self) -> &str;
    fn quote(&self) -> &str;
}

#[async_trait]
/// Data source for markets
pub trait MarketData<M: Market> {
    async fn has_market(&self, market: &M) -> Result<bool>;
    /// Try to find an existent market for the two given symbols
    async fn market_for(&self, base_symbol: &str, quote_symbol: &str) -> Result<Option<M>>;
    // A list of "proxy" markets that can be used to indirectly compute the price of the
    // provided market if this one doesn't exist or can't be computed directly.
    // e.g. given a market DOT-ETH, a list of proxy markets could be:
    //   [DOT-BTC, ETH-BTC] or [DOT-USD, ETH-USD]
    // This list of markets must act as a chain where the quote symbol of a market must be
    // present on the next one.
    async fn proxy_markets_for(&self, market: &M) -> Result<Option<Vec<M>>>;
    async fn price_at(&self, market: &M, time: &DateTime<Utc>) -> Result<f64>;
}

pub async fn solve_price<M, MD>(
    market: &M,
    time: &DateTime<Utc>,
    market_data: &MD,
) -> Result<Option<f64>>
where
    M: Market,
    MD: MarketData<M>,
{
    if market_data.has_market(market).await? {
        Ok(Some(market_data.price_at(market, time).await?))
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
                // The price of a market is always in terms of the quote symbol, thus to convert it
                // in terms of the current market we need to know the quote symbol of the last market 
                // we saw, if it's the same as the base symbol on the new one the last price is 
                // multiplied by this market's price, otherwise it's divided. The proxy markets' list 
                // must guarantee that the quote symbol of a given market is present on the next one.
                // e.g. given a market DOT-VET and a list of proxy market with prices:
                //   
                //   0: DOT-ETH -> 0.004569
                //   1: ETH-BTC -> 0.0791120
                //   2: VET-BTC -> 0.0000012
                //   
                // the following operation will give the price of DOT in terms of VET:
                //   
                //  0.004569 * 0.0791120 / 0.0000012 = 301.21894
                //
                let mut last_market: Option<&M> = None;
                let mut last_price = 0.0;
                for proxy_market in proxy_markets.iter() {
                    if market_data.has_market(proxy_market).await? {
                        let proxy_price = market_data.price_at(proxy_market, time).await?;

                        match last_market {
                            Some(lm) => {
                                if lm.quote() == proxy_market.base() {
                                    last_price *= proxy_price;
                                } else if lm.quote() == proxy_market.quote() {
                                    last_price /= proxy_price;
                                } else {
                                    return Err(anyhow!(
                                        "proxy symbols is incorrectly formed, last_market={} current_market={}", 
                                        lm, 
                                        proxy_market
                                    ));
                                }
                            }
                            None => {
                                last_price = proxy_price;
                            }
                        }
                        last_market.replace(proxy_market);
                    } else {
                        return Err(anyhow!(
                            "market in proxy markets not found: {}{}",
                            proxy_market.base(),
                            proxy_market.quote()
                        ));
                    }
                }
                if last_price > 0.0 {
                    Ok(Some(last_price))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Display;

    use anyhow::Result;
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    use crate::{solve_price, Market, MarketData};

    struct TestMarket(String, String);

    impl TestMarket {
        fn new(base: String, quote: String) -> Self {
            Self(base, quote)
        }
    }

    impl PartialEq for &TestMarket {
        fn eq(&self, other: &Self) -> bool {
            self.0 == other.0 && self.1 == other.1
        }
    }

    impl Market for TestMarket {
        fn base(&self) -> &str {
            &self.0
        }

        fn quote(&self) -> &str {
            &self.1
        }
    }

    struct TestMarketData;

    #[async_trait]
    impl MarketData<TestMarket> for TestMarketData {
        async fn has_market(&self, market: &TestMarket) -> Result<bool> {
            Ok(match (market.base(), market.quote()) {
                ("BTC", "USD")
                | ("USDC", "USD")
                | ("SOL", "ETH")
                | ("ETH", "BTC")
                | ("WBTC", "USD")
                | ("ADA", "ETH")
                | ("DOT", "BTC") => true,
                _ => false,
            })
        }

        async fn market_for(&self, symbol1: &str, symbol2: &str) -> Result<Option<TestMarket>> {
            let markets = vec![
                ("BTC", "USD"),
                ("USDC", "USD"),
                ("SOL", "ETH"),
                ("ETH", "BTC"),
                ("WBTC", "USD"),
                ("ADA", "ETH"),
                ("DOT", "ETH"),
            ];
            let r = markets
                .iter()
                .find(|p| *p == &(symbol1, symbol2) || *p == &(symbol2, symbol1));
            Ok(r.map(|t| TestMarket::new(t.0.to_string(), t.1.to_string())))
        }

        async fn proxy_markets_for(&self, market: &TestMarket) -> Result<Option<Vec<TestMarket>>>
        where
            Self: Sized,
        {
            Ok(match (market.base(), market.quote()) {
                ("BTC", "USDC") => Some(vec![
                    TestMarket::new("BTC".to_string(), "USD".to_string()),
                    TestMarket::new("USDC".to_string(), "USD".to_string()),
                ]),
                ("SOL", "BTC") => Some(vec![
                    TestMarket::new("SOL".to_string(), "ETH".to_string()),
                    TestMarket::new("ETH".to_string(), "BTC".to_string()),
                ]),
                ("WBTC", "BTC") => Some(vec![
                    TestMarket::new("WBTC".to_string(), "USD".to_string()),
                    TestMarket::new("BTC".to_string(), "USD".to_string()),
                ]),
                ("ADA", "DOT") => Some(vec![
                    TestMarket::new("ADA".to_string(), "ETH".to_string()),
                    TestMarket::new("ETH".to_string(), "BTC".to_string()),
                    TestMarket::new("DOT".to_string(), "BTC".to_string()),
                ]),
                _ => None,
            })
        }

        async fn price_at(&self, market: &TestMarket, _: &DateTime<Utc>) -> Result<f64> {
            Ok(match (market.base(), market.quote()) {
                ("BTC", "USD") => 20_000.0,
                ("USDC", "USD") => 0.9998,
                ("SOL", "ETH") => 0.02047,
                ("ETH", "BTC") => 0.0779,
                ("WBTC", "USD") => 19_998.7,
                ("ADA", "ETH") => 0.0002891,
                ("DOT", "BTC") => 0.0003616,
                _ => panic!("tried to fetch price for nonexistent market {}", market,),
            })
        }
    }

    impl Display for TestMarket {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}{}", self.0, self.1)
        }
    }

    #[tokio::test]
    async fn test_solve_price() {
        let m = TestMarket::new("BTC".to_string(), "USD".to_string());
        let market_data = TestMarketData;
        assert!(market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&m, &Utc::now(), &market_data).await.unwrap(),
            // direct BTC-USD
            Some(20_000.0)
        );

        let m = TestMarket::new("BTC".to_string(), "USDC".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&m, &Utc::now(), &market_data).await.unwrap(),
            // BTC-USD / USDC-USD
            Some(20_000.0 / 0.9998)
        );

        let m = TestMarket::new("SOL".to_string(), "BTC".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&m, &Utc::now(), &market_data).await.unwrap(),
            // SOL-ETH * ETH-BTC
            Some(0.02047 * 0.0779)
        );

        let m = TestMarket::new("ADA".to_string(), "DOT".to_string());
        assert!(!market_data.has_market(&m).await.unwrap());
        assert_eq!(
            solve_price(&m, &Utc::now(), &market_data).await.unwrap(),
            // ADA-ETH * ETH-BTC / DOT-BTC
            Some(0.0002891 * 0.0779 / 0.0003616)
        );
    }
}
