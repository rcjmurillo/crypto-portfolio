use std::fs::File;

use anyhow::Result;
use async_trait::async_trait;
use serde::Deserialize;
use serde_json;

use crate::{
    errors::{Error, ErrorKind},
    operations::{
        Deposit, ExchangeDataFetcher, FiatDeposit, Loan, Operation, Repay, Trade, Withdraw,
    },
};

#[derive(Debug, Deserialize, Clone)]
struct FileData {
    trades: Vec<Trade>,
    deposits: Option<Vec<Deposit>>,
    withdraws: Option<Vec<Withdraw>>,
    fiat_deposits: Option<Vec<FiatDeposit>>,
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
    async fn operations(&self) -> Result<Vec<Operation>> {
        Ok(Vec::new())
    }
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
    async fn fiat_deposits(&self) -> Result<Vec<FiatDeposit>> {
        // fixme: don't clone this every time
        Ok(self.data.fiat_deposits.clone().unwrap_or(Vec::new()))
    }
    async fn deposits(&self) -> Result<Vec<Deposit>> {
        Ok(self.data.deposits.clone().unwrap_or(Vec::new()))
    }
    async fn withdraws(&self) -> Result<Vec<Withdraw>> {
        Ok(self.data.withdraws.clone().unwrap_or(Vec::new()))
    }
}
