use std::fs::File;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json;

use crate::errors::Error;
use exchange::{Deposit, ExchangeDataFetcher, Loan, Repay, Trade, Withdraw};

#[derive(Debug, Deserialize, Clone)]
struct FileData {
    trades: Vec<Trade>,
    deposits: Option<Vec<Deposit>>,
    withdraws: Option<Vec<Withdraw>>,
}

#[derive(Clone)]
pub struct FileDataFetcher {
    data: FileData,
}

impl FileDataFetcher {
    pub fn from_file(file: File) -> Result<Self> {
        Ok(Self {
            data: serde_json::from_reader(file).map_err(|e| {
                anyhow!("couldn't parse custom operations file: {}", e).context(Error::Cli)
            })?,
        })
    }
}

#[async_trait]
impl ExchangeDataFetcher for FileDataFetcher {
    async fn trades(&self) -> Result<Vec<Trade>> {
        Ok(self.data.trades.clone())
    }
    async fn margin_trades(&self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
    async fn loans(&self) -> Result<Vec<Loan>> {
        Ok(Vec::new())
    }
    async fn repays(&self) -> Result<Vec<Repay>> {
        Ok(Vec::new())
    }
    async fn deposits(&self) -> Result<Vec<Deposit>> {
        Ok(self.data.deposits.clone().unwrap_or(Vec::new()))
    }
    async fn withdraws(&self) -> Result<Vec<Withdraw>> {
        Ok(self.data.withdraws.clone().unwrap_or(Vec::new()))
    }
}
