use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use rusqlite::{ffi::Error as FfiError, params, Connection, Error, ErrorCode};
use serde_json;

use crate::operations::{storage::Storage, Operation as OperationType};
use crate::{AssetPair, Candle};

const DB_NAME: &'static str = "operations.db";

#[derive(Debug)]
pub struct Operation {
    pub op_id: u8,
    pub source_id: String,
    pub source: String,
    pub op_type: String,
    pub for_asset: Option<String>,
    pub for_amount: Option<f64>,
    pub asset: String,
    pub amount: f64,
    pub timestamp: Option<i64>,
}

impl From<OperationType> for Operation {
    fn from(op: OperationType) -> Operation {
        match op {
            OperationType::Cost {
                id,
                source_id,
                source,
                for_asset,
                for_amount,
                asset,
                amount,
                time,
            } => Operation {
                op_id: id,
                source_id,
                source,
                op_type: "cost".to_string(),
                for_asset: Some(for_asset),
                for_amount: Some(for_amount),
                asset,
                amount,
                timestamp: Some(time.timestamp()),
            },
            OperationType::Revenue {
                id,
                source_id,
                source,
                asset,
                amount,
                time,
            } => Operation {
                op_id: id,
                source_id,
                source,
                op_type: "revenue".to_string(),
                asset,
                amount,
                timestamp: Some(time.timestamp()),
                for_asset: None,
                for_amount: None,
            },
            OperationType::BalanceIncrease {
                id,
                source_id,
                source,
                asset,
                amount,
            } => Operation {
                op_id: id,
                source_id,
                source,
                op_type: "balance_increase".to_string(),
                asset,
                amount,
                for_asset: None,
                for_amount: None,
                timestamp: None,
            },
            OperationType::BalanceDecrease {
                id,
                source_id,
                source,
                asset,
                amount,
            } => Operation {
                op_id: id,
                source_id,
                source,
                op_type: "balance_decrease".to_string(),
                asset,
                amount,
                for_asset: None,
                for_amount: None,
                timestamp: None,
            },
        }
    }
}

pub fn create_tables() -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS operations (
            op_id       INTEGER, 
            source_id   VARCHAR(100), 
            source      VARCHAR(25),
            type        VARCHAR(20),
            for_asset   VARCHAR(15),
            for_amount  FLOAT,
            asset       VARCHAR(15),
            amount      FLOAT,
            timestamp   TIMESTAMP NULL,
            PRIMARY KEY (op_id, source_id, source, type, asset)
        )",
        [],
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS asset_price_buckets (
            bucket  INTEGER,
            asset   VARCHAR(15),
            prices  JSON,
            PRIMARY KEY (bucket, asset)
        )",
        [],
    )?;

    Ok(())
}

pub fn insert_operations(ops: Vec<Operation>) -> Result<(usize, usize)> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt = conn.prepare_cached(
        "INSERT INTO 
         operations (op_id, source_id, source, type, for_asset, for_amount, asset, amount, timestamp) 
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    let mut inserted = 0;
    let mut skipped = 0;
    for op in ops {
        inserted += match stmt.execute(params![
            op.op_id,
            op.source_id,
            op.source,
            op.op_type,
            op.for_asset,
            op.for_amount,
            op.asset,
            op.amount,
            op.timestamp,
        ]) {
            Ok(inserted) => inserted,
            Err(err) => match err {
                Error::SqliteFailure(FfiError { code, .. }, ..) => {
                    match code {
                        ErrorCode::ConstraintViolation => {
                            // already exists, skip it
                            skipped += 1;
                            continue;
                        }
                        _ => return Err(anyhow::Error::new(err)),
                    }
                }
                err => return Err(anyhow::Error::new(err)),
            },
        };
    }

    Ok((inserted, skipped))
}

pub fn get_operations() -> Result<Vec<Operation>> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt = conn.prepare(
        "SELECT op_id, source_id, source,type, for_asset, for_amount, asset, amount, timestamp 
         FROM operations
         ORDER BY timestamp ASC",
    )?;
    let op_iter = stmt.query_map([], |row| {
        Ok(Operation {
            op_id: row.get(0)?,
            source_id: row.get(1)?,
            source: row.get(2)?,
            op_type: row.get(3)?,
            for_asset: row.get(4)?,
            for_amount: row.get(5)?,
            asset: row.get(6)?,
            amount: row.get(7)?,
            timestamp: row.get(8)?,
        })
    })?;

    op_iter
        .map(|o| o.map_err(|e| anyhow!("couldn't fetch operation from db").context(e)))
        .collect()
}

pub fn insert_asset_price_bucket(
    bucket: u16,
    asset_pair: &AssetPair,
    prices: Vec<Candle>,
) -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt =
        conn.prepare("INSERT INTO asset_price_buckets (bucket, asset, prices) VALUES (?, ?, ?)")?;

    match stmt.execute(params![
        bucket,
        asset_pair.join("-"),
        serde_json::to_string(&prices).context("error while converting prices into JSON")?
    ]) {
        Ok(_) => (),
        Err(err) => match err {
            Error::SqliteFailure(FfiError { code, .. }, ..) => {
                match code {
                    ErrorCode::ConstraintViolation => (), // already exists, skip it
                    _ => return Err(anyhow::Error::new(err)),
                }
            }
            err => return Err(anyhow::Error::new(err)),
        },
    };

    Ok(())
}

pub fn get_asset_price_bucket(bucket: u16, asset: &AssetPair) -> Result<Option<Vec<Candle>>> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt =
        conn.prepare("SELECT prices FROM asset_price_buckets WHERE bucket = ?1 AND asset = ?2")?;
    let mut iter = stmt.query_map(params![bucket, asset.join("-")], |row| {
        let prices: String = row.get(0)?;
        Ok(prices)
    })?;

    if let Some(s) = iter.next() {
        Ok(Some(serde_json::from_str(&s?).map_err(|e| {
            anyhow!(e).context("couldn't fetch asset prices bucket from db")
        })?))
    } else {
        Ok(None)
    }
}

pub struct Db;

#[async_trait]
impl Storage for Db {
    async fn get_ops(&self) -> Result<Vec<OperationType>> {
        get_operations()?
            .into_iter()
            .map(|op| op.try_into())
            .collect()
    }
    async fn insert_ops(&self, ops: Vec<OperationType>) -> Result<(usize, usize)> {
        insert_operations(ops.into_iter().map(|op| op.into()).collect())
    }
}
