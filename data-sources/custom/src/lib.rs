use std::fs::File;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use data_sync::OperationStorage;
use serde::Deserialize;
use serde_json;

use data_sync::DataFetcher;
use exchange::{Deposit, Trade, Withdraw};

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
            data: serde_json::from_reader(file)
                .map_err(|e| anyhow!("couldn't parse custom operations file: {}", e))?,
        })
    }
}

impl FileDataFetcher {
    async fn trades(&self, last_operation: Option<data_sync::Operation>) -> Result<Vec<Trade>> {
        // only return trades that are newer than the last one in the db
        Ok(match last_operation {
            Some(last_operation) => self
                .data
                .trades
                .iter()
                .filter(|t| t.time > last_operation.created_at)
                .cloned()
                .collect(),
            None => self.data.trades.clone(),
        })
    }
    async fn deposits(&self, last_operation: Option<data_sync::Operation>) -> Result<Vec<Deposit>> {
        // only return deposits that are newer than the last one in the db
        Ok(match (last_operation, self.data.deposits.as_ref()) {
            (_, None) => Vec::new(),
            (Some(last_operation), Some(deposits)) => deposits
                .iter()
                .filter(|d| d.time > last_operation.created_at)
                .cloned()
                .collect(),
            (None, Some(deposits)) => deposits.clone(),
        })
    }
    async fn withdrawals(
        &self,
        last_operation: Option<data_sync::Operation>,
    ) -> Result<Vec<Withdraw>> {
        // only return withdrawals that are newer than the last one in the db
        Ok(match (last_operation, self.data.withdrawals.as_ref()) {
            (_, None) => Vec::new(),
            (Some(last_operation), Some(withdrawals)) => withdrawals
                .iter()
                .filter(|w| w.time > last_operation.created_at)
                .cloned()
                .collect(),
            (None, Some(withdrawals)) => withdrawals.clone(),
        })
    }
}

#[async_trait]
impl DataFetcher for FileDataFetcher {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: OperationStorage + Send + Sync,
    {
        log::info!("syncing custom operations");

        let last_trade = storage.get_latest("custom", "trade")?;
        let trades = self.trades(last_trade).await?;
        let ops = trades
            .iter()
            .map(|t| data_sync::Operation {
                id: 0,
                source_id: t.source_id.to_string(),
                source: "custom".to_string(),
                op_type: "trade".to_string(),
                data: serde_json::to_string(t).unwrap(),
                created_at: t.time,
                imported_at: None,
            })
            .collect::<Vec<data_sync::Operation>>();
        log::info!("inserted {} new trade operations into the DB", ops.len());
        storage.insert(ops)?;

        let last_deposit = storage.get_latest("custom", "deposit")?;

        let deposits = self.deposits(last_deposit).await?;
        let ops = deposits
            .iter()
            .map(|d| data_sync::Operation {
                id: 0,
                source_id: d.source_id.to_string(),
                source: "custom".to_string(),
                op_type: "deposit".to_string(),
                data: serde_json::to_string(d).unwrap(),
                created_at: d.time,
                imported_at: None,
            })
            .collect::<Vec<data_sync::Operation>>();

        log::info!("inserted {} new deposit operations into the DB", ops.len());
        storage.insert(ops)?;

        let last_withdrawal = storage.get_latest("custom", "withdrawal")?;
        let withdrawals = self.withdrawals(last_withdrawal).await?;
        let ops = withdrawals
            .iter()
            .map(|w| data_sync::Operation {
                id: 0,
                source_id: w.source_id.to_string(),
                source: "custom".to_string(),
                op_type: "withdrawal".to_string(),
                data: serde_json::to_string(w).unwrap(),
                created_at: w.time,
                imported_at: None,
            })
            .collect::<Vec<data_sync::Operation>>();
        log::info!(
            "inserted {} new withdrawls operations into the DB",
            ops.len()
        );
        storage.insert(ops)?;

        Ok(())
    }
}
