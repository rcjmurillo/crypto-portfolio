use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[async_trait]
/// Layer of abstraction on how to fetch data from exchanges.
/// This allow to handle any incoming transactions/operations and convert them
/// into known structs that can be correctly translated into operations.
pub trait ExchangeDataFetcher {
    async fn trades(&self) -> Result<Vec<Trade>>;
    async fn margin_trades(&self) -> Result<Vec<Trade>>;
    async fn loans(&self) -> Result<Vec<Loan>>;
    async fn repays(&self) -> Result<Vec<Repay>>;
    async fn deposits(&self) -> Result<Vec<Deposit>>;
    async fn withdraws(&self) -> Result<Vec<Withdraw>>;
}

#[async_trait]
pub trait AssetsInfo {
    async fn price_at(&self, asset_pair: &AssetPair, time: &DateTime<Utc>) -> Result<f64>;
}

#[derive(Serialize, Deserialize)]
pub struct Candle {
    pub open_time: u64,
    pub close_time: u64,
    pub open_price: f64,
    pub close_price: f64,
}

#[async_trait]
pub trait ExchangeClient {
    async fn prices(&self, asset_pair: &AssetPair, start: DateTime<Utc>, end: DateTime<Utc>) -> Result<Vec<Candle>>;
}

pub type Asset = String;

#[derive(Clone)]
pub struct AssetPair(Asset, Asset);

impl AssetPair {
    pub fn new<A, B>(asset_a: A, asset_b: B) -> Self
    where
        A: ToString,
        B: ToString,
    {
        Self(asset_a.to_string(), asset_b.to_string())
    }
    /// try to create the string from an incoming string expected to be0
    /// a pair of assets joined by '-'.
    pub fn try_from_str(assets: &str) -> Result<Self> {
        let parts: Vec<&str> = assets.split("-").collect();
        if parts.len() == 2 {
            Ok(Self(parts[0].to_string(), parts[1].to_string()))
        } else {
            Err(anyhow!("couldn't parse '{}' into assets", assets))
        }
    }

    pub fn join(&self, sep: &str) -> String {
        format!("{}{}{}", self.0, sep, self.1)
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Success,
    Failure,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Trade {
    pub source_id: String,
    pub source: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub amount: f64,
    pub price: f64,
    pub fee: f64,
    pub fee_asset: String,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub side: TradeSide,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Deposit {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub fee: Option<f64>,
    pub is_fiat: bool,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Withdraw {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub fee: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Loan {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub status: Status,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Repay {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    pub interest: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub status: Status,
}

pub(crate) mod datetime_from_str {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{de, Deserialize, Deserializer};
    use std::convert::TryInto;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TimestampOrString {
            Timestamp(u64),
            String(String),
        }

        match TimestampOrString::deserialize(deserializer)? {
            // timestamps from the API are in milliseconds
            TimestampOrString::Timestamp(ts) => {
                Ok(Utc.timestamp_millis(ts.try_into().map_err(de::Error::custom)?))
            }
            TimestampOrString::String(s) => Utc
                .datetime_from_str(&s, "%Y-%m-%d %H:%M:%S")
                .map_err(de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use quickcheck::{quickcheck, Arbitrary, Gen, TestResult};

    impl Arbitrary for Status {
        fn arbitrary(g: &mut Gen) -> Self {
            g.choose(&[Self::Success, Self::Failure]).cloned().unwrap()
        }
    }

    impl Arbitrary for Trade {
        fn arbitrary(g: &mut Gen) -> Self {
            let assets = ["ADA", "SOL", "MATIC"];
            let quote_assets = ["BTC", "ETH", "AVAX"];
            let base_asset = g.choose(&assets).take().unwrap();
            let quote_asset = g.choose(&quote_assets).take().unwrap();
            let sides = [TradeSide::Buy, TradeSide::Sell];
            Self {
                source_id: "1".to_string(),
                source: "test".to_string(),
                symbol: format!("{}{}", base_asset, quote_asset),
                base_asset: base_asset.to_string(),
                quote_asset: quote_asset.to_string(),
                // non-zero price and amount
                price: 0.1 + u16::arbitrary(g) as f64,
                amount: 0.1 + u16::arbitrary(g) as f64,
                fee: u16::arbitrary(g).try_into().unwrap(),
                fee_asset: g.choose(&assets).take().unwrap().to_string(),
                time: Utc::now(),
                side: g.choose(&sides).unwrap().clone(),
            }
        }
    }

    impl Arbitrary for Deposit {
        fn arbitrary(g: &mut Gen) -> Self {
            let assets = ["ADA", "SOL", "MATIC", "BTC", "ETH", "AVAX"];
            Self {
                source_id: "1".to_string(),
                source: "test".to_string(),
                asset: g.choose(&assets).take().unwrap().to_string(),
                // non-zero amount
                amount: 0.1 + u16::arbitrary(g) as f64,
                fee: Option::arbitrary(g),
                time: Utc::now(),
                is_fiat: *g.choose(&[true, false]).take().unwrap(),
            }
        }
    }

    impl Arbitrary for Withdraw {
        fn arbitrary(g: &mut Gen) -> Self {
            let assets = ["ADA", "SOL", "MATIC", "BTC", "ETH", "AVAX"];
            Self {
                source_id: "1".to_string(),
                source: "test".to_string(),
                asset: g.choose(&assets).take().unwrap().to_string(),
                // non-zero amount
                amount: 0.1 + u16::arbitrary(g) as f64,
                fee: u16::arbitrary(g) as f64,
                time: Utc::now(),
            }
        }
    }

    impl Arbitrary for Loan {
        fn arbitrary(g: &mut Gen) -> Self {
            let assets = ["ADA", "SOL", "MATIC", "BTC", "ETH", "AVAX"];
            Self {
                source_id: "1".to_string(),
                source: "test".to_string(),
                asset: g.choose(&assets).unwrap().to_string(),
                // non-zero amount
                amount: 0.1 + u16::arbitrary(g) as f64,
                time: Utc::now(),
                status: Status::arbitrary(g),
            }
        }
    }

    impl Arbitrary for Repay {
        fn arbitrary(g: &mut Gen) -> Self {
            let assets = ["ADA", "SOL", "MATIC", "BTC", "ETH", "AVAX"];
            Self {
                source_id: "1".to_string(),
                source: "test".to_string(),
                asset: g.choose(&assets).unwrap().to_string(),
                // non-zero amount
                amount: 0.1 + u16::arbitrary(g) as f64,
                interest: 0.1 + u16::arbitrary(g) as f64,
                time: Utc::now(),
                status: Status::arbitrary(g),
            }
        }
    }
}
