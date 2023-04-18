use std::fs::File;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json;

use crate::errors::Error;
use exchange::{Deposit, ExchangeDataFetcher, Loan, Repay, Trade, Withdraw, operations::{Operation, into_ops}};

#[derive(Debug, Deserialize, Clone)]
struct FileData {
    trades: Vec<Trade>,
    deposits: Option<Vec<Deposit>>,
    withdrawals: Option<Vec<Withdraw>>,
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

impl FileDataFetcher {
    async fn trades(&self) -> Result<Vec<Trade>> {
        Ok(self.data.trades.clone())
    }
    async fn deposits(&self) -> Result<Vec<Deposit>> {
        Ok(self.data.deposits.clone().unwrap_or(Vec::new()))
    }
    async fn withdrawals(&self) -> Result<Vec<Withdraw>> {
        Ok(self.data.withdrawals.clone().unwrap_or(Vec::new()))
    }
}

#[async_trait]
impl ExchangeDataFetcher for FileDataFetcher {
    async fn fetch(&self) -> Result<Vec<Operation>> {
        let mut operations = Vec::new();
        operations.extend(into_ops(self.trades().await?));
        operations.extend(into_ops(self.deposits().await?));
        operations.extend(into_ops(self.withdrawals().await?));
        Ok(operations)
    }
}
