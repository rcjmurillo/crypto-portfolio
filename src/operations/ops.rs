use async_trait::async_trait;
use serde::Deserialize;
use std::{collections::HashMap, sync::Arc, vec::Vec};
use tokio::sync::{mpsc, Mutex};

use binance::{BinanceFetcher, Region as BinanceRegion};

use crate::{
    cli::Config,
    errors::{Error, ErrorKind},
    result::Result,
};

#[derive(Deserialize)]
pub enum OperationStatus {
    Success,
    Failed,
}

#[async_trait]
pub trait ExchangeDataFetcher {
    async fn trades(&self, symbols: &[String]) -> Result<Vec<Trade>>;
    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<Trade>>;
    async fn loans(&self, symbols: &[String]) -> Result<Vec<Loan>>;
    async fn repays(&self, symbols: &[String]) -> Result<Vec<Repay>>;
    async fn fiat_deposits(&self, symbols: &[String]) -> Result<Vec<FiatDeposit>>;
    async fn withdraws(&self, symbols: &[String]) -> Result<Vec<Withdraw>>;
}

#[async_trait]
pub trait AssetsInfo {
    async fn price_at(&self, symbol: &str, time: u64) -> Result<f64>;
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Trade {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub amount: f64,
    pub price: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub time: u64,
    pub side: TradeSide,
}

impl Into<Vec<Operation>> for Trade {
    fn into(self) -> Vec<Operation> {
        let mut ops = match self.side {
            TradeSide::Buy => vec![
                Operation::Balance {
                    asset: self.base_asset.clone(),
                    amount: self.amount,
                },
                Operation::Cost {
                    asset: self.base_asset,
                    amount: self.amount,
                    time: self.time,
                },
                Operation::Balance {
                    asset: self.quote_asset.clone(),
                    amount: -self.amount * self.price,
                },
                Operation::Revenue {
                    asset: self.quote_asset,
                    amount: self.amount * self.price,
                    time: self.time,
                },
            ],
            TradeSide::Sell => vec![
                Operation::Balance {
                    asset: self.base_asset.clone(),
                    amount: -self.amount,
                },
                Operation::Revenue {
                    asset: self.base_asset,
                    amount: self.amount,
                    time: self.time,
                },
                Operation::Balance {
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
            ops.push(Operation::Balance {
                asset: self.fee_asset.clone(),
                amount: -self.fee,
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
pub struct FiatDeposit {
    pub asset: String,
    pub amount: f64,
    pub fee: f64,
    pub time: u64,
}

impl Into<Vec<Operation>> for FiatDeposit {
    fn into(self) -> Vec<Operation> {
        vec![
            Operation::Balance {
                asset: self.asset.clone(),
                amount: self.amount,
            },
            Operation::Cost {
                asset: self.asset,
                amount: self.fee,
                time: self.time,
            },
        ]
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Withdraw {
    pub asset: String,
    pub amount: f64,
    pub time: u64,
    pub fee: f64,
}

impl Into<Vec<Operation>> for Withdraw {
    fn into(self) -> Vec<Operation> {
        vec![Operation::Cost {
            asset: self.asset,
            amount: self.fee,
            time: self.time,
        }]
    }
}

pub struct Loan {
    pub asset: String,
    pub amount: f64,
    pub time: u64,
    pub status: OperationStatus,
}

impl Into<Vec<Operation>> for Loan {
    fn into(self) -> Vec<Operation> {
        match self.status {
            OperationStatus::Success => {
                vec![Operation::Balance {
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
    pub time: u64,
    pub status: OperationStatus,
}

impl Into<Vec<Operation>> for Repay {
    fn into(self) -> Vec<Operation> {
        match self.status {
            OperationStatus::Success => {
                vec![
                    Operation::Balance {
                        asset: self.asset.clone(),
                        amount: -self.amount,
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

impl AssetBalance {
    fn new() -> Self {
        AssetBalance {
            amount: 0.0,
            usd_position: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Balance {
        asset: String,
        amount: f64,
    },
    Cost {
        asset: String,
        amount: f64,
        time: u64,
    },
    Revenue {
        asset: String,
        amount: f64,
        time: u64,
    },
}

pub struct BalanceTracker<T: AssetsInfo> {
    coin_balances: HashMap<String, AssetBalance>,
    asset_info: T,
}

impl<T: AssetsInfo> BalanceTracker<T> {
    pub fn new(asset_info: T) -> Self {
        BalanceTracker {
            coin_balances: HashMap::new(),
            asset_info,
        }
    }

    pub async fn track_operation(&mut self, op: Operation) -> Result<()> {
        println!("processing: {:?}", op);
        match op {
            Operation::Balance { asset, amount } => {
                let coin_balance = self.coin_balances.entry(asset).or_default();
                coin_balance.amount += amount;
            }
            Operation::Cost {
                asset,
                amount,
                time,
            } => {
                assert!(time > 0, "cost operation with time zero");
                let usd_price = if asset.starts_with("USD") {
                    1.0
                } else {
                    self.asset_info
                        .price_at(&format!("{}USDT", asset), time)
                        .await?
                };
                let coin_balance = self.coin_balances.entry(asset).or_default();
                coin_balance.usd_position += -amount * usd_price;
            }
            Operation::Revenue {
                asset,
                amount,
                time,
            } => {
                assert!(time > 0, "revenue operation with time zero");
                let usd_price = if asset.starts_with("USD") {
                    1.0
                } else {
                    self.asset_info
                        .price_at(&format!("{}USDT", asset), time)
                        .await?
                };
                let coin_balance = self.coin_balances.entry(asset).or_default();
                coin_balance.usd_position += amount * usd_price;
            }
        }
        println!("processing done!");
        Ok(())
    }

    pub fn get_balance(&self, asset: &str) -> Option<&AssetBalance> {
        if let Some(balance) = self.coin_balances.get(asset) {
            Some(balance)
        } else {
            None
        }
    }

    pub fn balances(&self) -> Vec<(&String, &AssetBalance)> {
        self.coin_balances.iter().collect()
    }
}

// stores a map of buckets to prices, buckets are periods of time
// defined by how the key is computed.
type PricesBucket = HashMap<u16, Vec<(u64, f64)>>;

pub struct AssetPrices {
    prices: Mutex<HashMap<String, PricesBucket>>,
    fetcher: BinanceFetcher,
}

impl AssetPrices {
    pub fn new() -> Self {
        Self {
            prices: Mutex::new(HashMap::new()),
            fetcher: BinanceFetcher::new(BinanceRegion::Global, None),
        }
    }

    async fn asset_price_at(&self, symbol: &str, time: u64) -> Result<f64> {
        let period_days = 180;
        let bucket_size_millis = 24 * 3600 * 1000 * period_days;
        let bucket = (time / bucket_size_millis) as u16;
        let mut prices = self.prices.lock().await;
        if !prices.contains_key(symbol) {
            println!("prices not found for {}", symbol);
            let symbol_prices = self.fetch_prices_for(symbol, time).await?;
            let mut prices_bucket = PricesBucket::new();
            prices_bucket.insert(bucket, symbol_prices);
            prices.insert(symbol.to_string(), prices_bucket);
        }

        let prices_bucket = prices.get_mut(symbol).unwrap();
        if !prices_bucket.contains_key(&bucket) {
            println!("prices not found for {} in bucket {}", symbol, bucket);
            let symbol_prices = self.fetch_prices_for(symbol, time).await?;
            prices_bucket.insert(bucket, symbol_prices);
        }

        Ok(self.find_price_at(prices_bucket.get(&bucket).unwrap(), time))
    }

    async fn fetch_prices_for(&self, symbol: &str, time: u64) -> Result<Vec<(u64, f64)>> {
        let period_days = 180;
        let bucket_size_millis = 24 * 3600 * 1000 * period_days;
        let start_ts = time - (time % bucket_size_millis);
        let end_ts = start_ts + bucket_size_millis;
        self.fetcher
            .fetch_prices_in_range(symbol, start_ts, end_ts)
            .await
            .map_err(|err| Error::new(err.to_string(), ErrorKind::FetchFailed))
    }

    fn find_price_at(&self, prices: &Vec<(u64, f64)>, time: u64) -> f64 {
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
    async fn price_at(&self, symbol: &str, time: u64) -> Result<f64> {
        self.asset_price_at(symbol, time).await
    }
}

async fn ops_from_fetcher<'a>(
    prefix: &'a str,
    c: &Box<dyn ExchangeDataFetcher + Send + Sync>,
    symbols: &'a [String],
) -> Vec<Operation> {
    let mut all_ops: Vec<Operation> = Vec::new();
    println!("[{}]> fetching trades...", prefix);
    all_ops.extend(
        c.trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    println!("[{}]> fetching margin trades...", prefix);
    all_ops.extend(
        c.margin_trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    println!("[{}]> fetching loans...", prefix);
    all_ops.extend(
        c.loans(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    println!("[{}]> fetching repays...", prefix);
    all_ops.extend(
        c.repays(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    println!("[{}]> fetching fiat deposits...", prefix);
    all_ops.extend(
        c.fiat_deposits(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    println!("[{}]> fetching coins withdraws...", prefix);
    all_ops.extend(
        c.withdraws(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    println!("[{}]> ALL DONE!!!", prefix);
    all_ops
}

pub async fn fetch_ops<'a>(
    fetchers: Vec<(&'static str, Box<dyn ExchangeDataFetcher + Send + Sync>)>,
    config: Arc<Config>,
) -> mpsc::Receiver<Operation> {
    let (tx, rx) = mpsc::channel(1000);

    for (name, f) in fetchers.into_iter() {
        let txc = tx.clone();
        let conf = config.clone();
        tokio::spawn(async move {
            for op in ops_from_fetcher(name, &f, &conf.symbols[..]).await {
                match txc.send(op).await {
                    Ok(()) => (),
                    Err(err) => println!("could not send operation: {}", err),
                }
            }
        });
    }

    println!("\nDONE getting operations...");

    rx
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
            time: 123,
            side: TradeSide::Buy,
        };

        let ops: Vec<Operation> = t1.into();

        assert_eq!(6, ops.len(), "incorrect number of operations");

        assert_eq!(
            ops[0],
            Operation::Balance {
                asset: "DOT".into(),
                amount: 3.0
            }
        );
        assert_eq!(
            ops[1],
            Operation::Cost {
                asset: "DOT".into(),
                amount: 3.0,
                time: 123
            }
        );
        assert_eq!(
            ops[2],
            Operation::Balance {
                asset: "ETH".into(),
                amount: -1.5,
            }
        );
        assert_eq!(
            ops[3],
            Operation::Revenue {
                asset: "ETH".into(),
                amount: 1.5,
                time: 123
            }
        );
        assert_eq!(
            ops[4],
            Operation::Balance {
                asset: "ETH".into(),
                amount: -0.01,
            }
        );
        assert_eq!(
            ops[5],
            Operation::Cost {
                asset: "ETH".into(),
                amount: 0.01,
                time: 123
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
            time: 123,
            side: TradeSide::Sell,
        };

        let ops: Vec<Operation> = t1.into();

        assert_eq!(6, ops.len(), "incorrect number of operations");

        assert_eq!(
            ops[0],
            Operation::Balance {
                asset: "DOT".into(),
                amount: -3.0
            }
        );
        assert_eq!(
            ops[1],
            Operation::Revenue {
                asset: "DOT".into(),
                amount: 3.0,
                time: 123,
            }
        );
        assert_eq!(
            ops[2],
            Operation::Balance {
                asset: "ETH".into(),
                amount: 1.5
            }
        );
        assert_eq!(
            ops[3],
            Operation::Cost {
                asset: "ETH".into(),
                amount: 1.5,
                time: 123
            }
        );
        assert_eq!(
            ops[4],
            Operation::Balance {
                asset: "XCOIN".into(),
                amount: -0.01,
            }
        );
        assert_eq!(
            ops[5],
            Operation::Cost {
                asset: "XCOIN".into(),
                amount: 0.01,
                time: 123,
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
                time: 123,
                side: TradeSide::Buy,
            };
            let ops: Vec<Operation> = t.into();
            assert_eq!(4, ops.len(), "incorrect number of operations");

            assert_eq!(
                ops[0],
                Operation::Balance {
                    asset: "DOT".into(),
                    amount: 3.0
                }
            );
            assert_eq!(
                ops[1],
                Operation::Cost {
                    asset: "DOT".into(),
                    amount: 3.0,
                    time: 123
                }
            );
            assert_eq!(
                ops[2],
                Operation::Balance {
                    asset: "ETH".into(),
                    amount: -1.5,
                }
            );
            assert_eq!(
                ops[3],
                Operation::Revenue {
                    asset: "ETH".into(),
                    amount: 1.5,
                    time: 123
                }
            );
        }
    }

    #[tokio::test]
    async fn track_operations() -> Result<()> {
        struct TestAssetInfo {}

        #[async_trait]
        impl AssetsInfo for TestAssetInfo {
            async fn price_at(&self, _symbol: &str, time: u64) -> Result<f64> {
                let prices = vec![8500.0, 8900.0, 2000.0, 2100.0, 7000.0, 15.0, 25.0, 95.0];
                Ok(prices[(time - 1) as usize])
            }
        }

        let mut coin_tracker = BalanceTracker::new(TestAssetInfo {});
        let ops = vec![
            Operation::Balance {
                asset: "BTCUSD".into(),
                amount: 0.03,
            },
            Operation::Cost {
                asset: "BTCUSD".into(),
                amount: 0.03,
                time: 1,
            },
            Operation::Balance {
                asset: "BTCUSD".into(),
                amount: 0.1,
            },
            Operation::Cost {
                asset: "BTCUSD".into(),
                amount: 0.1,
                time: 2,
            },
            Operation::Balance {
                asset: "ETHUSD".into(),
                amount: 0.5,
            },
            Operation::Cost {
                asset: "ETHUSD".into(),
                amount: 0.5,
                time: 3,
            },
            Operation::Balance {
                asset: "ETHUSD".into(),
                amount: 0.01,
            },
            Operation::Cost {
                asset: "ETHUSD".into(),
                amount: 0.01,
                time: 4,
            },
            Operation::Balance {
                asset: "ETHUSD".into(),
                amount: -0.2,
            },
            Operation::Revenue {
                asset: "ETHUSD".into(),
                amount: 0.2,
                time: 5,
            },
            Operation::Balance {
                asset: "DOTUSD".into(),
                amount: 0.5,
            },
            Operation::Cost {
                asset: "DOTUSD".into(),
                amount: 0.5,
                time: 6,
            },
            Operation::Balance {
                asset: "DOTUSD".into(),
                amount: -0.1,
            },
            Operation::Revenue {
                asset: "DOTUSD".into(),
                amount: 0.1,
                time: 7,
            },
            Operation::Balance {
                asset: "DOTUSD".into(),
                amount: -0.2,
            },
            Operation::Revenue {
                asset: "DOTUSD".into(),
                amount: 0.2,
                time: 8,
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

        let mut balances = coin_tracker.balances();
        balances.sort_by_key(|x| x.0.clone());

        for ((asset_a, balance_a), (asset_b, balance_b)) in expected.iter().zip(balances.iter()) {
            assert_eq!(asset_a, *asset_b);
            assert_eq!(balance_a, *balance_b);
        }

        Ok(())
    }
}
