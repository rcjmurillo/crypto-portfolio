use std::fs::File;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json;

use crate::{
    errors::{Error, ErrorKind},
    operations::{Deposit, ExchangeDataFetcher, Loan, Repay, Trade, Withdraw},
    result::Result,
};
use binance::{BinanceFetcher, Region as BinanceRegion};

#[derive(Debug, Deserialize, Clone)]
struct FileData {
    trades: Vec<Trade>,
    withdraws: Vec<Withdraw>,
    fiat_deposits: Option<Vec<Deposit>>,
}

#[derive(Clone)]
pub struct FileDataFetcher {
    data: FileData,
}

impl FileDataFetcher {
    pub fn from_file(file: File) -> Result<Self> {
        Ok(Self {
            data: serde_json::from_reader(file).map_err(|e| {
                Error::new(
                    format!("couldn't parse custom operations file: {}", e),
                    ErrorKind::Cli,
                )
            })?,
        })
    }
}

#[async_trait]
impl ExchangeDataFetcher for FileDataFetcher {
    async fn trades(&self, _: &[String]) -> Result<Vec<Trade>> {
        let c = BinanceFetcher::new(BinanceRegion::Global, None);

        let mut trades = self.data.trades.clone();
        for t in trades.iter_mut() {
            let usd_price = if t.base_asset.starts_with("USD") {
                1.0
            } else {
                c.fetch_price_at(&format!("{}USDT", t.base_asset), t.time).await?
            };
            t.usd_cost = t.amount * usd_price;
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
