use std::fs::File;

use async_trait::async_trait;
use serde::Deserialize;

use crate::{
    binance::{BinanceFetcher, Region as BinanceRegion},
    operations::{Deposit, ExchangeDataFetcher, Loan, Repay, Trade, Withdraw},
    result::Result,
};

#[derive(Debug, Deserialize, Clone)]
struct FileData {
    trades: Vec<Trade>,
    withdraws: Vec<Withdraw>,
    fiat_deposits: Option<Vec<Deposit>>,
}

pub struct FileDataFetcher {
    data: FileData,
}

impl FileDataFetcher {
    pub fn from_file(file: File) -> Result<Self> {
        Ok(Self {
            data: serde_json::from_reader(file)?,
        })
    }
}

#[async_trait]
impl ExchangeDataFetcher for FileDataFetcher {
    type Trade = Trade;
    type Loan = Loan;
    type Repay = Repay;
    type Deposit = Deposit;
    type Withdraw = Withdraw;

    async fn trades(&self, _: &[String]) -> Result<Vec<Trade>> {
        let c = BinanceFetcher::new(BinanceRegion::Global, &None);

        let mut trades = self.data.trades.clone();
        for t in trades.iter_mut() {
            let usd_price = if t.base_asset.starts_with("USD") {
                1.0
            } else {
                c.price_at(&format!("{}USDT", t.base_asset), t.time)
                    .await
                    .expect(&format!("could not fetch USD price for {}", t.base_asset))
            };
            t.cost = t.amount * usd_price;
        }

        Ok(trades)
    }
    async fn margin_trades(&self, _: &[String]) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
    async fn loans(&self, _: &[String]) -> Result<Vec<Loan>> {
        Ok(Vec::new())
    }
    async fn repays(&self, _: &[String]) -> Result<Vec<Repay>> {
        Ok(Vec::new())
    }
    async fn fiat_deposits(&self, _: &[String]) -> Result<Vec<Deposit>> {
        // fixme: don't clone this every time
        Ok(self.data.fiat_deposits.clone().unwrap_or(Vec::new()))
    }
    async fn withdraws(&self, _: &[String]) -> Result<Vec<Withdraw>> {
        Ok(self.data.withdraws.clone())
    }
}
