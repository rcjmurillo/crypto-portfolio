use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::{mpsc, Mutex, RwLock};

use crate::db::{
    get_asset_price_bucket, insert_asset_price_bucket, insert_operations, Operation as DbOperation,
};
use binance::{BinanceFetcher, EndpointsGlobal, RegionGlobal};

#[derive(Deserialize)]
pub enum OperationStatus {
    Success,
    Failed,
}

#[async_trait]
/// Layer of abstraction on how to fetch data from exchanges.
/// This allow to handle any incoming transactions/operations and convert them
/// into known structs that can be correctly translated into operations.
pub trait ExchangeDataFetcher {
    // Allows to express any transactions as a list of operations, this gives
    // more freedom at the cost of trusting exchange client will correctly
    // translate its data/transactions into operations that will keep the correct
    // balance.
    async fn operations(&self) -> Result<Vec<Operation>>;
    async fn trades(&self) -> Result<Vec<Trade>>;
    async fn margin_trades(&self) -> Result<Vec<Trade>>;
    async fn loans(&self) -> Result<Vec<Loan>>;
    async fn repays(&self) -> Result<Vec<Repay>>;
    async fn deposits(&self) -> Result<Vec<Deposit>>;
    async fn withdraws(&self) -> Result<Vec<Withdraw>>;
}

#[async_trait]
pub trait AssetsInfo {
    async fn price_at(&self, symbol: &str, time: DateTime<Utc>) -> Result<f64>;
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Trade {
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

impl Into<Vec<Operation>> for Trade {
    fn into(self) -> Vec<Operation> {
        let mut ops = match self.side {
            TradeSide::Buy => vec![
                Operation::BalanceIncrease {
                    asset: self.base_asset.clone(),
                    amount: self.amount,
                },
                Operation::Cost {
                    asset: self.base_asset,
                    amount: self.amount,
                    time: self.time,
                },
                Operation::BalanceDecrease {
                    asset: self.quote_asset.clone(),
                    amount: self.amount * self.price,
                },
                Operation::Revenue {
                    asset: self.quote_asset,
                    amount: self.amount * self.price,
                    time: self.time,
                },
            ],
            TradeSide::Sell => vec![
                Operation::BalanceDecrease {
                    asset: self.base_asset.clone(),
                    amount: self.amount,
                },
                Operation::Revenue {
                    asset: self.base_asset,
                    amount: self.amount,
                    time: self.time,
                },
                Operation::BalanceIncrease {
                    asset: self.quote_asset.clone(),
                    amount: self.amount * self.price,
                },
                Operation::Cost {
                    asset: self.quote_asset,
                    amount: self.amount * self.price,
                    time: self.time,
                },
            ],
        };
        if self.fee_asset != "" && self.fee > 0.0 {
            ops.push(Operation::BalanceDecrease {
                asset: self.fee_asset.clone(),
                amount: self.fee,
            });
            ops.push(Operation::Cost {
                asset: self.fee_asset,
                amount: self.fee,
                time: self.time,
            });
        }

        ops
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Deposit {
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub fee: Option<f64>,
    pub is_fiat: bool,
}

impl Into<Vec<Operation>> for Deposit {
    fn into(self) -> Vec<Operation> {
        let mut ops = vec![Operation::BalanceIncrease {
            asset: self.asset.clone(),
            amount: self.amount,
        }];
        if let Some(fee) = self.fee {
            ops.extend(vec![
                Operation::BalanceDecrease {
                    asset: self.asset.clone(),
                    amount: fee,
                },
                Operation::Cost {
                    asset: self.asset,
                    amount: fee,
                    time: self.time,
                },
            ]);
        }
        ops
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Withdraw {
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub fee: f64,
}

impl Into<Vec<Operation>> for Withdraw {
    fn into(self) -> Vec<Operation> {
        vec![
            Operation::BalanceDecrease {
                asset: self.asset.clone(),
                amount: self.amount,
            },
            Operation::BalanceDecrease {
                asset: self.asset.clone(),
                amount: self.fee,
            },
            Operation::Cost {
                asset: self.asset,
                amount: self.fee,
                time: self.time,
            },
        ]
    }
}

#[derive(Deserialize)]
pub struct Loan {
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub status: OperationStatus,
}

impl Into<Vec<Operation>> for Loan {
    fn into(self) -> Vec<Operation> {
        match self.status {
            OperationStatus::Success => {
                vec![Operation::BalanceIncrease {
                    asset: self.asset,
                    amount: self.amount,
                }]
            }
            OperationStatus::Failed => vec![],
        }
    }
}

#[derive(Deserialize)]
pub struct Repay {
    pub asset: String,
    pub amount: f64,
    pub interest: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub status: OperationStatus,
}

impl Into<Vec<Operation>> for Repay {
    fn into(self) -> Vec<Operation> {
        match self.status {
            OperationStatus::Success => {
                vec![
                    Operation::BalanceDecrease {
                        asset: self.asset.clone(),
                        amount: self.amount + self.interest,
                    },
                    Operation::Cost {
                        asset: self.asset,
                        amount: self.interest,
                        time: self.time,
                    },
                ]
            }
            OperationStatus::Failed => vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetBalance {
    pub amount: f64,
    pub usd_position: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    BalanceIncrease {
        asset: String,
        amount: f64,
    },
    BalanceDecrease {
        asset: String,
        amount: f64,
    },
    Cost {
        asset: String,
        amount: f64,
        time: DateTime<Utc>,
    },
    Revenue {
        asset: String,
        amount: f64,
        time: DateTime<Utc>,
    },
}

impl Into<DbOperation> for Operation {
    fn into(self) -> DbOperation {
        match self {
            Operation::Cost {
                asset,
                amount,
                time,
            } => DbOperation {
                // source_id: "todo".to_string(),
                op_type: "cost".to_string(),
                asset,
                amount,
                timestamp: Some(time),
            },
            Operation::Revenue {
                asset,
                amount,
                time,
            } => DbOperation {
                // source_id: "todo".to_string(),
                op_type: "revenue".to_string(),
                asset,
                amount,
                timestamp: Some(time),
            },
            Operation::BalanceIncrease { asset, amount } => DbOperation {
                // source_id: "todo".to_string(),
                op_type: "balance_increase".to_string(),
                asset,
                amount,
                timestamp: None,
            },
            Operation::BalanceDecrease { asset, amount } => DbOperation {
                // source_id: "todo".to_string(),
                op_type: "balance_decrease".to_string(),
                asset,
                amount,
                timestamp: None,
            },
        }
    }
}

impl TryFrom<DbOperation> for Operation {
    type Error = Error;

    fn try_from(op: DbOperation) -> Result<Operation> {
        match op.op_type.as_str() {
            "cost" => Ok(Operation::Cost {
                asset: op.asset,
                amount: op.amount,
                time: op.timestamp.expect("missing timestamp in cost operation"),
            }),
            "revenue" => Ok(Operation::Revenue {
                asset: op.asset,
                amount: op.amount,
                time: op
                    .timestamp
                    .expect("missing timestamp in revenue operation"),
            }),
            "balance_increase" => Ok(Operation::BalanceIncrease {
                asset: op.asset,
                amount: op.amount,
            }),
            "balance_decrease" => Ok(Operation::BalanceDecrease {
                asset: op.asset,
                amount: op.amount,
            }),
            _ => Err(anyhow!("couldn't convert db operation into operation")),
        }
    }
}

pub struct BalanceTracker<T: AssetsInfo> {
    coin_balances: RwLock<HashMap<String, AssetBalance>>,
    asset_info: T,
}

impl<T: AssetsInfo> BalanceTracker<T> {
    pub fn new(asset_info: T) -> Self {
        BalanceTracker {
            coin_balances: RwLock::new(HashMap::new()),
            asset_info,
        }
    }

    pub async fn track_operation(&self, op: Operation) -> Result<()> {
        match op {
            Operation::BalanceIncrease { asset, amount } => {
                assert!(
                    amount >= 0.0,
                    "balance increase operation amount can't be negative"
                );
                let mut bal = self.coin_balances.write().await;
                let coin_balance = bal.entry(asset).or_default();
                coin_balance.amount += amount;
            }
            Operation::BalanceDecrease { asset, amount } => {
                assert!(
                    amount >= 0.0,
                    "balance decrease operation amount can't be negative"
                );
                let mut bal = self.coin_balances.write().await;
                let coin_balance = bal.entry(asset).or_default();
                coin_balance.amount -= amount;
            }
            Operation::Cost {
                asset,
                amount,
                time,
            } => {
                let usd_price = if asset.starts_with("USD") {
                    1.0
                } else {
                    self.asset_info
                        .price_at(&format!("{}USDT", asset), time)
                        .await?
                };
                let mut bal = self.coin_balances.write().await;
                let coin_balance = bal.entry(asset).or_default();
                coin_balance.usd_position += -amount * usd_price;
            }
            Operation::Revenue {
                asset,
                amount,
                time,
            } => {
                let usd_price = if asset.starts_with("USD") {
                    1.0
                } else {
                    self.asset_info
                        .price_at(&format!("{}USDT", asset), time)
                        .await?
                };
                let mut bal = self.coin_balances.write().await;
                let coin_balance = bal.entry(asset).or_default();
                coin_balance.usd_position += amount * usd_price;
            }
        }
        Ok(())
    }

    pub async fn get_balance(&self, asset: &str) -> Option<AssetBalance> {
        self.coin_balances
            .read()
            .await
            .get(asset)
            .clone()
            .map(|v| v.clone())
    }

    pub async fn balances(&self) -> Vec<(String, AssetBalance)> {
        self.coin_balances
            .read()
            .await
            .clone()
            .into_iter()
            .collect()
    }
}

const OPS_RECEIVE_BATCH_SIZE: usize = 1000;

pub struct OperationsFlusher {
    receiver: mpsc::Receiver<Operation>,
}

impl OperationsFlusher {
    pub fn with_receiver(receiver: mpsc::Receiver<Operation>) -> Self {
        Self { receiver }
    }

    pub async fn receive(&mut self) -> Result<()> {
        let mut batch = Vec::with_capacity(OPS_RECEIVE_BATCH_SIZE);
        let mut num_ops = 0;
        while let Some(op) = self.receiver.recv().await {
            batch.push(op);
            if batch.len() == OPS_RECEIVE_BATCH_SIZE {
                self.flush(batch)?;
                batch = Vec::with_capacity(OPS_RECEIVE_BATCH_SIZE);
                num_ops += OPS_RECEIVE_BATCH_SIZE;
            }
        }

        let r = if batch.len() > 0 {
            num_ops += batch.len();
            self.flush(batch)
        } else {
            Ok(())
        };
        log::info!("fetched {} operations", num_ops);
        r
    }

    fn flush(&self, batch: Vec<Operation>) -> Result<()> {
        let batch_size = batch.len();
        insert_operations(
            batch
                .into_iter()
                .filter_map(|op| op.try_into().map_or(None, |op: DbOperation| Some(op)))
                .collect(),
        )?;
        log::debug!("flushed {} operations into db", batch_size);
        Ok(())
    }
}

/// stores buckets to prices in the db, buckets are periods of time
/// defined by number of days of span.
pub struct AssetPrices {
    fetcher: BinanceFetcher<RegionGlobal>,
}

impl AssetPrices {
    pub fn new() -> Self {
        Self {
            fetcher: BinanceFetcher::<RegionGlobal>::new(),
        }
    }

    async fn asset_price_at(&self, symbol: &str, datetime: DateTime<Utc>) -> Result<f64> {
        // create buckets of `period_days` size for time, a list of klines
        // will be fetch for the bucket where the time falls into if it doesn't
        // exists already in the map.
        // Once made sure the data for the bucket is in the map use it to
        // determine the price of the symbol at `time`.
        let time = datetime.timestamp_millis().try_into()?;
        let period_days = 180;
        let bucket_size_millis = 24 * 3600 * 1000 * period_days;
        let bucket = (time / bucket_size_millis) as u16;

        if let Some(prices) = get_asset_price_bucket(bucket, symbol)? {
            Ok(self.find_price_at(&prices, time))
        } else {
            let symbol_prices = self.fetch_prices_for(symbol, time).await?;
            let price = self.find_price_at(&symbol_prices, time);
            insert_asset_price_bucket(bucket, symbol, symbol_prices)?;
            Ok(price)
        }
    }

    async fn fetch_prices_for(&self, symbol: &str, time: u64) -> Result<Vec<(u64, f64)>> {
        // fetch prices from the start time of the bucket not `time` so other calls
        // can reuse the data for transactions that fall into the same bucket. Also this
        // way it's assured fetched data won't overlap.
        let period_days = 180;
        let bucket_size_millis = 24 * 3600 * 1000 * period_days;
        let start_ts = time - (time % bucket_size_millis);
        let end_ts = start_ts + bucket_size_millis;
        self.fetcher
            .fetch_prices_in_range(
                &EndpointsGlobal::Klines.to_string(),
                symbol,
                start_ts,
                end_ts,
            )
            .await
    }

    fn find_price_at(&self, prices: &Vec<(u64, f64)>, time: u64) -> f64 {
        // find the price at `time` in the vector of candles, it's assumed
        // that the time is the close time of the candle and the data is sorted.
        // With those invariants then the first candle which time is greater than
        // the provided `time` is the one that holds the most accurate price.
        prices
            .iter()
            .find_map(|p| match p.0 > time {
                true => Some(p.1),
                false => None,
            })
            .unwrap_or(0.0)
    }
}

#[async_trait]
impl AssetsInfo for AssetPrices {
    async fn price_at(&self, symbol: &str, time: DateTime<Utc>) -> Result<f64> {
        self.asset_price_at(symbol, time).await
    }
}

async fn ops_from_fetcher<'a>(
    prefix: &'a str,
    c: Box<dyn ExchangeDataFetcher + Send + Sync>,
) -> Vec<Operation> {
    let mut all_ops: Vec<Operation> = Vec::new();
    log::info!("[{}] fetching trades...", prefix);
    all_ops.extend(
        c.trades()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching margin trades...", prefix);
    all_ops.extend(
        c.margin_trades()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching loans...", prefix);
    all_ops.extend(
        c.loans()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching repays...", prefix);
    all_ops.extend(
        c.repays()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching deposits...", prefix);
    all_ops.extend(
        c.deposits()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching withdraws...", prefix);
    all_ops.extend(
        c.withdraws()
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching operations...", prefix);
    all_ops.extend(c.operations().await.unwrap());
    log::info!("[{}] ALL DONE!!!", prefix);
    all_ops
}

pub async fn fetch_ops<'a>(
    fetchers: Vec<(&'static str, Box<dyn ExchangeDataFetcher + Send + Sync>)>,
) -> mpsc::Receiver<Operation> {
    let (tx, rx) = mpsc::channel(10000);

    for (name, f) in fetchers.into_iter() {
        let txc = tx.clone();
        tokio::spawn(async move {
            for op in ops_from_fetcher(name, f).await {
                match txc.send(op).await {
                    Ok(()) => (),
                    Err(err) => println!("could not send operation: {}", err),
                }
            }
        });
    }

    rx
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

    #[test]
    fn trade_buy_into() {
        let t1 = Trade {
            symbol: "DOTETH".into(),
            base_asset: "DOT".into(),
            quote_asset: "ETH".into(),
            price: 0.5,
            amount: 3.0,
            fee: 0.01,
            fee_asset: "ETH".into(),
            time: Utc::now(),
            side: TradeSide::Buy,
        };

        let ops: Vec<Operation> = t1.into();

        assert_eq!(6, ops.len(), "incorrect number of operations");

        assert_eq!(
            ops[0],
            Operation::BalanceIncrease {
                asset: "DOT".into(),
                amount: 3.0
            }
        );
        assert_eq!(
            ops[1],
            Operation::Cost {
                asset: "DOT".into(),
                amount: 3.0,
                time: Utc::now()
            }
        );
        assert_eq!(
            ops[2],
            Operation::BalanceDecrease {
                asset: "ETH".into(),
                amount: 1.5,
            }
        );
        assert_eq!(
            ops[3],
            Operation::Revenue {
                asset: "ETH".into(),
                amount: 1.5,
                time: Utc::now()
            }
        );
        assert_eq!(
            ops[4],
            Operation::BalanceDecrease {
                asset: "ETH".into(),
                amount: 0.01,
            }
        );
        assert_eq!(
            ops[5],
            Operation::Cost {
                asset: "ETH".into(),
                amount: 0.01,
                time: Utc::now()
            }
        );
    }

    #[test]
    fn trade_sell_into() {
        let t1 = Trade {
            symbol: "DOTETH".into(),
            base_asset: "DOT".into(),
            quote_asset: "ETH".into(),
            price: 0.5,
            amount: 3.0,
            fee: 0.01,
            fee_asset: "XCOIN".into(),
            time: Utc::now(),
            side: TradeSide::Sell,
        };

        let ops: Vec<Operation> = t1.into();

        assert_eq!(6, ops.len(), "incorrect number of operations");

        assert_eq!(
            ops[0],
            Operation::BalanceDecrease {
                asset: "DOT".into(),
                amount: 3.0
            }
        );
        assert_eq!(
            ops[1],
            Operation::Revenue {
                asset: "DOT".into(),
                amount: 3.0,
                time: Utc::now(),
            }
        );
        assert_eq!(
            ops[2],
            Operation::BalanceIncrease {
                asset: "ETH".into(),
                amount: 1.5
            }
        );
        assert_eq!(
            ops[3],
            Operation::Cost {
                asset: "ETH".into(),
                amount: 1.5,
                time: Utc::now()
            }
        );
        assert_eq!(
            ops[4],
            Operation::BalanceDecrease {
                asset: "XCOIN".into(),
                amount: 0.01,
            }
        );
        assert_eq!(
            ops[5],
            Operation::Cost {
                asset: "XCOIN".into(),
                amount: 0.01,
                time: Utc::now(),
            }
        );
    }

    #[test]
    fn trade_into_no_fee() {
        let fee_cases = vec![(1.0, ""), (0.0, "ETH"), (0.0, "")];
        for (fee_amount, fee_asset) in fee_cases.into_iter() {
            let t = Trade {
                symbol: "DOTETH".into(),
                base_asset: "DOT".into(),
                quote_asset: "ETH".into(),
                price: 0.5,
                amount: 3.0,
                fee: fee_amount,
                fee_asset: fee_asset.to_string(),
                time: Utc::now(),
                side: TradeSide::Buy,
            };
            let ops: Vec<Operation> = t.into();
            assert_eq!(4, ops.len(), "incorrect number of operations");

            assert_eq!(
                ops[0],
                Operation::BalanceIncrease {
                    asset: "DOT".into(),
                    amount: 3.0
                }
            );
            assert_eq!(
                ops[1],
                Operation::Cost {
                    asset: "DOT".into(),
                    amount: 3.0,
                    time: Utc::now()
                }
            );
            assert_eq!(
                ops[2],
                Operation::BalanceDecrease {
                    asset: "ETH".into(),
                    amount: 1.5,
                }
            );
            assert_eq!(
                ops[3],
                Operation::Revenue {
                    asset: "ETH".into(),
                    amount: 1.5,
                    time: Utc::now()
                }
            );
        }
    }

    #[tokio::test]
    async fn track_operations() -> Result<()> {
        struct TestAssetInfo {
            prices: Mutex<Vec<f64>>,
        }

        impl TestAssetInfo {
            fn new() -> Self {
                Self {
                    prices: Mutex::new(vec![
                        8500.0, 8900.0, 2000.0, 2100.0, 7000.0, 15.0, 25.0, 95.0,
                    ]),
                }
            }
        }

        #[async_trait]
        impl AssetsInfo for TestAssetInfo {
            async fn price_at(&self, _symbol: &str, time: DateTime<Utc>) -> Result<f64> {
                Ok(self.prices.lock().await.remove(0))
            }
        }

        let mut coin_tracker = BalanceTracker::new(TestAssetInfo::new());
        let ops = vec![
            Operation::BalanceIncrease {
                asset: "BTCUSD".into(),
                amount: 0.03,
            },
            Operation::Cost {
                asset: "BTCUSD".into(),
                amount: 0.03,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                asset: "BTCUSD".into(),
                amount: 0.1,
            },
            Operation::Cost {
                asset: "BTCUSD".into(),
                amount: 0.1,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                asset: "ETHUSD".into(),
                amount: 0.5,
            },
            Operation::Cost {
                asset: "ETHUSD".into(),
                amount: 0.5,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                asset: "ETHUSD".into(),
                amount: 0.01,
            },
            Operation::Cost {
                asset: "ETHUSD".into(),
                amount: 0.01,
                time: Utc::now(),
            },
            Operation::BalanceDecrease {
                asset: "ETHUSD".into(),
                amount: 0.2,
            },
            Operation::Revenue {
                asset: "ETHUSD".into(),
                amount: 0.2,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                asset: "DOTUSD".into(),
                amount: 0.5,
            },
            Operation::Cost {
                asset: "DOTUSD".into(),
                amount: 0.5,
                time: Utc::now(),
            },
            Operation::BalanceDecrease {
                asset: "DOTUSD".into(),
                amount: 0.1,
            },
            Operation::Revenue {
                asset: "DOTUSD".into(),
                amount: 0.1,
                time: Utc::now(),
            },
            Operation::BalanceDecrease {
                asset: "DOTUSD".into(),
                amount: 0.2,
            },
            Operation::Revenue {
                asset: "DOTUSD".into(),
                amount: 0.2,
                time: Utc::now(),
            },
        ];

        for op in ops {
            coin_tracker.track_operation(op).await?;
        }

        let mut expected = vec![
            (
                "BTCUSD".to_string(),
                AssetBalance {
                    amount: 0.13,
                    usd_position: -1145.0,
                },
            ),
            (
                "ETHUSD".to_string(),
                AssetBalance {
                    amount: 0.31,
                    usd_position: 379.0,
                },
            ),
            (
                "DOTUSD".to_string(),
                AssetBalance {
                    amount: 0.2,
                    usd_position: 14.0,
                },
            ),
        ];

        expected.sort_by_key(|x| x.0.clone());

        let mut balances = coin_tracker.balances().await;
        balances.sort_by_key(|x| x.0.clone());

        for ((asset_a, balance_a), (asset_b, balance_b)) in expected.iter().zip(balances.iter()) {
            assert_eq!(asset_a, *asset_b);
            assert_eq!(balance_a, *balance_b);
        }

        Ok(())
    }
}
