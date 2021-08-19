use async_trait::async_trait;
use serde::Deserialize;
use std::{
    collections::HashMap,
    convert::{From, TryInto},
    sync::Arc,
    vec::Vec,
};
use tokio::sync::mpsc;

use crate::{cli::Config, custom_ops::FileDataFetcher, result::Result};
use binance::{BinanceFetcher, Region as BinanceRegion};

pub enum OperationStatus {
    Success,
    Failed,
}

pub trait IntoOperations {
    fn into_ops(self) -> Vec<Operation>;
}

#[async_trait]
pub trait ExchangeDataFetcher {
    async fn trades(&self, _symbols: &[String]) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }

    async fn margin_trades(&self, _symbols: &[String]) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }

    async fn loans(&self, _symbols: &[String]) -> Result<Vec<Loan>> {
        Ok(Vec::new())
    }

    async fn repays(&self, _symbols: &[String]) -> Result<Vec<Repay>> {
        Ok(Vec::new())
    }

    async fn fiat_deposits(&self, _symbols: &[String]) -> Result<Vec<Deposit>> {
        Ok(Vec::new())
    }

    async fn withdraws(&self, _symbols: &[String]) -> Result<Vec<Withdraw>> {
        Ok(Vec::new())
    }
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
    pub price: f64,
    #[serde(skip)]
    pub cost: f64,
    pub amount: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub time: u64,
    pub side: TradeSide,
}

impl IntoOperations for Trade {
    fn into_ops(self) -> Vec<Operation> {
        let mut ops = Vec::new();
        // determines if the first operation is going to increase or to decrease
        // the balance, then the second operation does the opposit.
        let mut sign = match self.side {
            TradeSide::Buy => 1.0,
            TradeSide::Sell => -1.0,
        };

        ops.push(Operation {
            asset: self.base_asset.to_string(),
            amount: self.amount * sign,
            cost: self.cost * sign,
        });
        sign *= -1.0; // invert sign
        ops.push(Operation {
            asset: self.quote_asset.to_string(),
            amount: self.price * self.amount * sign,
            cost: self.cost * sign,
        });

        if self.fee_asset != "" && self.fee > 0.0 {
            ops.push(Operation {
                asset: self.fee_asset.to_string(),
                amount: -self.fee,
                cost: 0.0,
            });
        }

        ops
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Deposit {
    pub asset: String,
    pub amount: f64,
}

impl IntoOperations for Deposit {
    fn into_ops(self) -> Vec<Operation> {
        vec![Operation {
            asset: self.asset,
            amount: self.amount,
            cost: 0.0,
        }]
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Withdraw {
    pub asset: String,
    pub amount: f64,
    pub time: String,
    pub fee: f64,
}

impl IntoOperations for Withdraw {
    fn into_ops(self) -> Vec<Operation> {
        vec![Operation {
            asset: self.asset.clone(),
            amount: -self.fee,
            cost: 0.0,
        }]
    }
}

pub struct Loan {
    pub asset: String,
    pub amount: f64,
    pub timestamp: u64,
    pub status: OperationStatus,
}

impl IntoOperations for Loan {
    fn into_ops(self) -> Vec<Operation> {
        match self.status {
            OperationStatus::Success => {
                vec![Operation {
                    asset: self.asset.clone(),
                    amount: self.amount,
                    cost: 0.0,
                }]
            }
            OperationStatus::Failed => vec![],
        }
    }
}

pub struct Repay {
    pub asset: String,
    pub amount: f64,
    pub interest: f64,
    pub timestamp: u64,
    pub status: OperationStatus,
}

impl IntoOperations for Repay {
    fn into_ops(self) -> Vec<Operation> {
        match self.status {
            OperationStatus::Success => {
                vec![Operation {
                    asset: self.asset,
                    amount: -(self.amount + self.interest),
                    cost: 0.0,
                }]
            }
            OperationStatus::Failed => vec![],
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoinBalance {
    pub amount: f64,
    pub cost: f64,
}

impl CoinBalance {
    fn new() -> Self {
        CoinBalance {
            amount: 0.0,
            cost: 0.0,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct Operation {
    pub asset: String,
    pub amount: f64,
    pub cost: f64,
}

pub struct BalanceTracker {
    coin_balances: HashMap<String, CoinBalance>,
}

impl BalanceTracker {
    pub fn new() -> Self {
        BalanceTracker {
            coin_balances: HashMap::new(),
        }
    }

    pub fn track_operation(&mut self, op: Operation) {
        let coin_balance = self
            .coin_balances
            .entry(String::from(op.asset))
            .or_insert(CoinBalance::new());
        coin_balance.amount += op.amount;
        coin_balance.cost += op.cost;
    }

    pub fn get_cost(&self, symbol: &str) -> Option<f64> {
        if let Some(balance) = self.coin_balances.get(symbol) {
            Some(balance.cost)
        } else {
            None
        }
    }

    pub fn balances(&self) -> Vec<(&String, &CoinBalance)> {
        self.coin_balances.iter().collect()
    }
}

async fn ops_from_fetcher<'a, T>(prefix: &str, c: &'a T, symbols: &'a [String]) -> Vec<Operation>
where
    T: ExchangeDataFetcher + Sync,
{
    let mut all_ops = Vec::new();
    println!("[{}]> fetching trades...", prefix);
    all_ops.extend(
        c.trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into_ops()),
    );
    println!("[{}]> fetching margin trades...", prefix);
    all_ops.extend(
        c.margin_trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into_ops()),
    );
    println!("[{}]> fetching loans...", prefix);
    all_ops.extend(
        c.loans(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into_ops()),
    );
    println!("[{}]> fetching repays...", prefix);
    all_ops.extend(
        c.repays(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into_ops()),
    );
    println!("[{}]> fetching fiat deposits...", prefix);
    all_ops.extend(
        c.fiat_deposits(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into_ops()),
    );
    println!("[{}]> fetching coins withdraws...", prefix);
    all_ops.extend(
        c.withdraws(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into_ops()),
    );
    println!("[{}]> ALL DONE!!!", prefix);
    all_ops
}

pub async fn fetch_pipeline(
    file_data_fetcher: Option<Arc<FileDataFetcher>>,
    config: Arc<Config>,
) -> mpsc::Receiver<Operation> {
    let (tx, rx) = mpsc::channel(1000);
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    let tx3 = tx.clone();

    let conf1 = config.clone();
    let conf2 = config.clone();
    let conf3 = config.clone();

    let _h1 = tokio::spawn(async move {
        let config = match &conf1.binance {
            Some(c) => Some(c.clone().try_into().unwrap()),
            None => None,
        };
        let binance_client = BinanceFetcher::new(BinanceRegion::Global, &config);
        for op in ops_from_fetcher("binance", &binance_client, &conf1.symbols[..]).await {
            match tx1.send(op).await {
                Ok(()) => (),
                Err(err) => println!("could not send operation: {}", err),
            }
        }
    });

    let _h2 = tokio::spawn(async move {
        let config = match &conf2.binance_us {
            Some(c) => Some(c.clone().try_into().unwrap()),
            None => None,
        };
        let binance_us_client = BinanceFetcher::new(BinanceRegion::Us, &config);
        for op in ops_from_fetcher("binance US", &binance_us_client, &conf2.symbols[..]).await {
            match tx2.send(op).await {
                Ok(()) => (),
                Err(err) => println!("could not send operation: {}", err),
            }
        }
    });

    if let Some(file_data_fetcher) = file_data_fetcher {
        tokio::spawn(async move {
            for op in ops_from_fetcher(
                "custom file operations",
                &*file_data_fetcher,
                &conf3.symbols[..],
            )
            .await
            {
                match tx3.send(op).await {
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
    fn trade_buy_into_ops() {
        let t1 = Trade {
            symbol: "DOTETH".into(),
            base_asset: "DOT".into(),
            quote_asset: "ETH".into(),
            price: 0.5,
            cost: 2.0,
            amount: 3.0,
            fee: 0.01,
            fee_asset: "ETH".into(),
            time: 0,
            side: TradeSide::Buy,
        };

        let ops = t1.into_ops();

        assert_eq!(3, ops.len(), "incorrect number of operations");

        assert_eq!(
            ops[0],
            Operation {
                asset: "DOT".into(),
                amount: 3.0,
                cost: 2.0
            }
        );
        assert_eq!(
            ops[1],
            Operation {
                asset: "ETH".into(),
                amount: -1.5,
                cost: -2.0
            }
        );
        assert_eq!(
            ops[2],
            Operation {
                asset: "ETH".into(),
                amount: -0.01,
                cost: 0.0
            }
        );
    }

    #[test]
    fn trade_sell_into_ops() {
        let t1 = Trade {
            symbol: "DOTETH".into(),
            base_asset: "DOT".into(),
            quote_asset: "ETH".into(),
            price: 0.5,
            cost: 2.0,
            amount: 3.0,
            fee: 0.01,
            fee_asset: "ETH".into(),
            time: 0,
            side: TradeSide::Sell,
        };

        let ops = t1.into_ops();

        assert_eq!(3, ops.len(), "incorrect number of operations");

        assert_eq!(
            ops[0],
            Operation {
                asset: "DOT".into(),
                amount: -3.0,
                cost: -2.0
            }
        );
        assert_eq!(
            ops[1],
            Operation {
                asset: "ETH".into(),
                amount: 1.5,
                cost: 2.0
            }
        );
        assert_eq!(
            ops[2],
            Operation {
                asset: "ETH".into(),
                amount: -0.01,
                cost: 0.0
            }
        );
    }

    #[test]
    fn trade_into_ops_no_fee() {
        let fee_cases = vec![(1.0, ""), (0.0, "ETH"), (0.0, "")];
        for (fee_amount, fee_asset) in fee_cases.into_iter() {
            let t = Trade {
                symbol: "DOTETH".into(),
                base_asset: "DOT".into(),
                quote_asset: "ETH".into(),
                price: 0.5,
                cost: 2.0,
                amount: 3.0,
                fee: fee_amount,
                fee_asset: fee_asset.to_string(),
                time: 0,
                side: TradeSide::Buy,
            };
            let ops = t.into_ops();
            assert_eq!(2, ops.len(), "incorrect number of operations");

            assert_eq!(
                ops[0],
                Operation {
                    asset: "DOT".into(),
                    amount: 3.0,
                    cost: 2.0
                }
            );
            assert_eq!(
                ops[1],
                Operation {
                    asset: "ETH".into(),
                    amount: -1.5,
                    cost: -2.0
                }
            );
        }
    }

    #[test]
    fn track_operations() {
        let mut coin_tracker = BalanceTracker::new();
        let ops = vec![
            Operation {
                asset: "BTCUSD".into(),
                amount: 0.03,
                cost: 250.0,
            },
            Operation {
                asset: "BTCUSD".into(),
                amount: 0.1,
                cost: 500.0,
            },
            Operation {
                asset: "ETHUSD".into(),
                amount: 0.5,
                cost: 1500.0,
            },
            Operation {
                asset: "ETHUSD".into(),
                amount: -0.01,
                cost: -300.0,
            },
            Operation {
                asset: "ETHUSD".into(),
                amount: 0.2,
                cost: 500.0,
            },
            Operation {
                asset: "DOTUSD".into(),
                amount: 0.4,
                cost: 1500.0,
            },
            Operation {
                asset: "DOTUSD".into(),
                amount: -0.01,
                cost: -300.0,
            },
            Operation {
                asset: "DOTUSD".into(),
                amount: -0.9,
                cost: -2800.0,
            },
        ];

        for op in ops {
            coin_tracker.track_operation(op);
        }

        let mut expected = vec![
            (
                "BTCUSD".to_string(),
                CoinBalance {
                    amount: 0.13,
                    cost: 750.0,
                },
            ),
            (
                "ETHUSD".to_string(),
                CoinBalance {
                    amount: 0.69,
                    cost: 1700.0,
                },
            ),
            (
                "DOTUSD".to_string(),
                CoinBalance {
                    amount: -0.51,
                    cost: -1600.0,
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
    }
}
