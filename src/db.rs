use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, TimeZone, Utc};
use rusqlite::{params, Connection};
use serde_json;

const DB_NAME: &'static str = "operations.db";

#[derive(Debug)]
pub struct Operation {
    //pub source_id: String,
    pub op_type: String,
    pub asset: String,
    pub amount: f64,
    pub timestamp: Option<DateTime<Utc>>,
}

pub fn create_tables() -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS operations (
            type        VARCHAR(20),
            asset       VARCHAR(15),
            amount      FLOAT,
            timestamp   TIMESTAMP NULL
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

pub fn insert_operations(ops: Vec<Operation>) -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt = conn
        .prepare("INSERT INTO operations (type, asset, amount, timestamp) VALUES (?, ?, ?, ?)")?;

    for op in ops {
        stmt.execute(params![
            op.op_type,
            op.asset,
            op.amount,
            op.timestamp.map(|t| t.timestamp())
        ])?;
    }

    Ok(())
}

pub fn get_operations() -> Result<Vec<Operation>> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt = conn.prepare("SELECT type, asset, amount, timestamp FROM operations")?;
    let op_iter = stmt.query_map([], |row| {
        let timestamp: Option<i64> = row.get(3)?;
        Ok(Operation {
            op_type: row.get(0)?,
            asset: row.get(1)?,
            amount: row.get(2)?,
            timestamp: timestamp.map(|ts| Utc.timestamp(ts, 0)),
        })
    })?;

    op_iter
        .map(|o| o.map_err(|e| anyhow!("couldn't fetch operation from db").context(e)))
        .collect()
}

pub fn insert_asset_price_bucket(bucket: u16, asset: &str, prices: Vec<(u64, f64)>) -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt =
        conn.prepare("INSERT INTO asset_price_buckets (bucket, asset, prices) VALUES (?, ?, ?)")?;

    stmt.execute(params![
        bucket,
        asset,
        serde_json::to_string(&prices).context("error while converting prices into JSON")?
    ])?;

    Ok(())
}

pub fn get_asset_price_bucket(bucket: u16, asset: &str) -> Result<Option<Vec<(u64, f64)>>> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt =
        conn.prepare("SELECT prices FROM asset_price_buckets WHERE bucket = ?1 AND asset = ?2")?;
    let mut iter = stmt.query_map(params![bucket, asset], |row| {
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
