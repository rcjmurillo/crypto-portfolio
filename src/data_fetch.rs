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

    async fn trades<T>(&self, symbols: &[String]) -> Result<Vec<Self::Trade>>
    where
        T: From<Self::Trade> + IntoOperations;

    async fn margin_trades<T>(&self, symbols: &[String]) -> Result<Vec<Self::Trade>>
    where
        T: From<Self::Trade> + IntoOperations;

    async fn loans<T>(&self, symbols: &[String]) -> Result<Vec<Self::Loan>>
    where
        T: From<Self::Loan> + IntoOperations;

    async fn repays<T>(&self, symbols: &[String]) -> Result<Vec<Self::Repay>>
    where
        T: From<Self::Repay> + IntoOperations;

    async fn deposits<T>(&self, symbols: &[String]) -> Result<Vec<Self::Deposit>>
    where
        T: From<Self::Deposit> + IntoOperations;

    async fn withdraws<T>(&self, symbols: &[String]) -> Result<Vec<Self::Withdraw>>
    where
        T: From<Self::Withdraw> + IntoOperations;
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

        if self.base_asset == "" || self.quote_asset == "" {
            panic!(
                "found empty base or quote asset in into_ops for symbol {}",
                self.symbol
            );
        }

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

async fn ops_from_client<'a, T>(c: &'a mut T, symbols: &'a [String]) -> Vec<Operation>
where
    T: ExchangeClient,
    Trade: From<T::Trade>,
    T::Trade: Into<Trade>,
    Loan: From<T::Loan>,
    T::Loan: Into<Loan>,
    Repay: From<T::Repay>,
    T::Repay: Into<Repay>,
    Deposit: From<T::Deposit>,
    T::Deposit: Into<Deposit>,
    Withdraw: From<T::Withdraw>,
    T::Withdraw: Into<Withdraw>,
{
    let mut all_ops = Vec::new();
    println!("> fetching trades...");
    all_ops.extend(
        c.trades::<Trade>(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| Trade::from(t).into_ops()),
    );
    println!("> fetching margin trades...");
    all_ops.extend(
        c.margin_trades::<Trade>(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("> fetching loans...");
    all_ops.extend(
        c.loans::<Loan>(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("> fetching repays...");
    all_ops.extend(
        c.repays::<Repay>(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("> fetching fiat deposits...");
    all_ops.extend(
        c.deposits::<Deposit>(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("> fetching coins withdraws...");
    all_ops.extend(
        c.withdraws::<Withdraw>(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("> ALL DONE!!!");
    all_ops
}

pub fn fetch_pipeline<'a>() -> impl StreamExt<Item = Result<Operation>> + 'a {
    stream! {
        let mut binance_client = BinanceFetcher::new(BinanceRegion::Global);
        let mut binance_us_client = BinanceFetcher::new(BinanceRegion::Us);

        println!("========== binance ==========");
        let mut ops = ops_from_client(&mut binance_client, &all_symbols()[..]).await;

        #[cfg(feature="private_ops")]
        {
            println!("========== private ops ==========");
            let mut private_ops = PrivateOps::new();
            ops.extend(ops_from_client(&mut private_ops, &all_symbols()[..]).await);
        }

        println!("========== binance US ==========");
        ops.extend(ops_from_client(&mut binance_us_client, &all_symbols()[..]).await);

        println!("\nDONE getting operations...");
        for op in ops {
            yield Ok(op);
        }

        ()
    }
}
