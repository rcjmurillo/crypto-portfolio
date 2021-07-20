use std::collections::HashMap;
use std::convert::From;
use std::vec::Vec;

use crate::binance::{BinanceFetcher, BinanceRegion};
use crate::private::all_symbols;
#[cfg(feature = "private_ops")]
use crate::private::PrivateOps;
use crate::result::Result;
use crate::tracker::{IntoOperations, Operation};

use async_stream::stream;

use async_trait::async_trait;
use serde::Deserialize;
use tokio_stream::StreamExt;

#[async_trait]
pub trait DataFetcher {
    async fn symbol_price_at(&self, symbol: &str, time: u64) -> f64;
}

#[async_trait]
pub trait ExchangeClient {
    type Trade;
    type Loan;
    type Repay;
    type Deposit;
    type Withdraw;

    async fn trades(&self, symbols: &[String]) -> Result<Vec<Self::Trade>>
    where
        Self::Trade: Into<Trade>;

    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<Self::Trade>>
    where
        Self::Trade: Into<Trade>;

    async fn loans(&self, symbols: &[String]) -> Result<Vec<Self::Loan>>
    where
        Self::Loan: Into<Loan>;

    async fn repays(&self, symbols: &[String]) -> Result<Vec<Self::Repay>>
    where
        Self::Repay: Into<Repay>;

    async fn deposits(&self, symbols: &[String]) -> Result<Vec<Self::Deposit>>
    where
        Self::Deposit: Into<Deposit>;

    async fn withdraws(&self, symbols: &[String]) -> Result<Vec<Self::Withdraw>>
    where
        Self::Withdraw: Into<Withdraw>;
}

pub enum TradeSide {
    Buy,
    Sell,
}

pub struct Trade {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price: f64,
    pub cost: f64,
    pub amount: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub time: u64,
    pub side: TradeSide,
}

#[async_trait]
impl IntoOperations for Trade {
    fn into_ops(self) -> Vec<Operation> {
        let mut ops = Vec::new();
        // determines if the first operation is going to increase or to decrease
        // the balance, then the second operation does the opposit.
        let mut sign = if let TradeSide::Buy = self.side {
            1.0
        } else {
            -1.0
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

        if self.fee_asset != "" {
            ops.push(Operation {
                asset: self.fee_asset.to_string(),
                amount: -self.fee,
                cost: 0.0,
            });
        }

        ops
    }
}

#[derive(Deserialize, Debug)]
pub struct Deposit {
    pub asset: String,
    pub amount: f64,
}

#[async_trait]
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
    pub time: u64,
    pub fee: f64,
}

#[async_trait]
impl IntoOperations for Withdraw {
    fn into_ops(self) -> Vec<Operation> {
        vec![Operation {
            asset: self.asset.clone(),
            amount: -self.fee,
            cost: 0.0,
        }]
    }
}

pub enum OperationStatus {
    Success,
    Failed,
}

pub struct Loan {
    pub asset: String,
    pub amount: f64,
    pub timestamp: u64,
    pub status: OperationStatus,
}

#[async_trait]
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

#[async_trait]
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

impl From<&HashMap<String, String>> for Deposit {
    fn from(data: &HashMap<String, String>) -> Self {
        Deposit {
            asset: String::from("USD"),
            amount: data.get("amount").unwrap().parse::<f64>().unwrap(),
        }
    }
}

async fn ops_from_client<'a, T>(prefix: &str, c: &'a T, symbols: &'a [String]) -> Vec<Operation>
where
    T: ExchangeClient,
    T::Trade: Into<Trade>,
    T::Loan: Into<Loan>,
    T::Repay: Into<Repay>,
    T::Deposit: Into<Deposit>,
    T::Withdraw: Into<Withdraw>,
{
    let mut all_ops = Vec::new();
    println!("[{}]> fetching trades...", prefix);
    all_ops.extend(
        c.trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching margin trades...", prefix);
    all_ops.extend(
        c.margin_trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching loans...", prefix);
    all_ops.extend(
        c.loans(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching repays...", prefix);
    all_ops.extend(
        c.repays(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching fiat deposits...", prefix);
    all_ops.extend(
        c.deposits(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching coins withdraws...", prefix);
    all_ops.extend(
        c.withdraws(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> ALL DONE!!!", prefix);
    all_ops
}

pub fn fetch_pipeline<'a>() -> impl StreamExt<Item = Result<Operation>> + 'a {
    stream! {

        let mut ops = Vec::new();

        let h1 = tokio::spawn(async {
            let binance_client = BinanceFetcher::new(BinanceRegion::Global);    
            ops_from_client("binance", &binance_client, &all_symbols()[..]).await
        });

        let h2 = tokio::spawn(async {
            let binance_us_client = BinanceFetcher::new(BinanceRegion::Us);
            ops_from_client("binance US", &binance_us_client, &all_symbols()[..]).await
        });

        #[cfg(feature="private_ops")]
        {
            let h3 = tokio::spawn(async {
                let private_ops = PrivateOps::new();
                ops_from_client("private ops", &private_ops, &all_symbols()[..]).await
            });
            ops.extend(h3.await.unwrap());
        }

        ops.extend(h1.await.unwrap());
        ops.extend(h2.await.unwrap());

        println!("\nDONE getting operations...");
        for op in ops {
            yield Ok(op);
        }

        ()
    }
}
