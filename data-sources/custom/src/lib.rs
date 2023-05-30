use std::fs::File;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use data_sync::RecordStorage;
use serde::Deserialize;
use serde_json::{self, Value};

use data_sync::{filter_after, into_records, DataFetcher};

#[derive(Debug, Deserialize, Clone)]
struct FileData {
    trades: Vec<Value>,
    deposits: Option<Vec<Value>>,
    withdrawals: Option<Vec<Value>>,
}

#[derive(Clone)]
pub struct FileDataFetcher {
    name: String,
    data: FileData,
}

impl FileDataFetcher {
    pub fn from_file(name: &str, path: &str) -> Result<Self> {
        Ok(Self {
            name: name.to_string(),
            data: serde_json::from_reader(File::open(&path)?)
                .map_err(|e| anyhow!("couldn't parse custom operations file ({path}): {}", e))?,
        })
    }
}

impl FileDataFetcher {
    async fn trades(&self, last_operation: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        // only return trades that are newer than the last one in the db
        Ok(match last_operation {
            Some(last_operation) => filter_after(&self.data.trades, last_operation.created_at)?,
            None => self.data.trades.clone(),
        })
    }
    async fn deposits(&self, last_operation: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        // only return deposits that are newer than the last one in the db
        Ok(match (last_operation, self.data.deposits.as_ref()) {
            (_, None) => Vec::new(),
            (Some(last_operation), Some(deposits)) => {
                filter_after(deposits, last_operation.created_at)?
            }
            (None, Some(deposits)) => deposits.clone(),
        })
    }
    async fn withdrawals(&self, last_operation: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        // only return withdrawals that are newer than the last one in the db
        Ok(match (last_operation, self.data.withdrawals.as_ref()) {
            (_, None) => Vec::new(),
            (Some(last_operation), Some(withdrawals)) => {
                filter_after(withdrawals, last_operation.created_at)?
            }
            (None, Some(withdrawals)) => withdrawals.clone(),
        })
    }
}

#[async_trait]
impl DataFetcher for FileDataFetcher {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: RecordStorage + Send + Sync,
    {
        let source_name = format!("custom-{}", self.name);
        log::info!("[{source_name}] fetching trades...");
        let last_trade = storage.get_latest(&source_name, "trade")?;
        let ops = into_records(
            &self.trades(last_trade.as_ref()).await?,
            "trade",
            &source_name,
            "source_id",
            "time",
        )?;
        storage.insert(ops)?;

        log::info!("[{source_name}] fetching deposits...");
        let last_deposit = storage.get_latest(&source_name, "deposit")?;
        let ops = into_records(
            &self.deposits(last_deposit.as_ref()).await?,
            "deposit",
            &source_name,
            "source_id",
            "time",
        )?;
        storage.insert(ops)?;

        log::info!("[{source_name}] fetching withdrawals...");
        let last_withdrawal = storage.get_latest(&source_name, "withdrawal")?;
        let ops = into_records(
            &self.withdrawals(last_withdrawal.as_ref()).await?,
            "withdrawal",
            &source_name,
            "source_id",
            "time",
        )?;
        storage.insert(ops)?;

        Ok(())
    }
}
