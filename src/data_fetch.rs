use std::collections::HashMap;
use std::convert::From;
use std::vec::Vec;

use crate::result::Result;
use crate::tracker::{IntoOperations, Operation};
#[cfg(feature = "private_ops")]
use crate::private::PrivateOps;

use async_trait::async_trait;
use serde::Deserialize;

pub enum OperationStatus {
    Success,
    Failed,
}

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

impl From<&HashMap<String, String>> for Deposit {
    fn from(data: &HashMap<String, String>) -> Self {
        Deposit {
            asset: String::from("USD"), // fixme: get asset from hashmap
            amount: data.get("amount").unwrap().parse::<f64>().unwrap(),
        }
    }
}
