use anyhow::{anyhow, Result};
use rusqlite::{params, Connection};

const DB_NAME: &'static str = "operations.db";

pub struct Operation {
    //pub source_id: String,
    pub op_type: String,
    pub asset: String,
    pub amount: f64,
    pub timestamp: Option<u32>,
}

pub fn create_tables() -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS operations (
            -- source_id   VARCHAR(25) PRIMARY KEY,
            type        VARCHAR(20),
            asset       VARCHAR(15),
            amount      FLOAT,
            timestamp   UNSIGNED BIG INT NULL
        )",
        [],
    )?;

    Ok(())
}

pub fn insert_operations(ops: Vec<Operation>) -> Result<()> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt = conn.prepare(
        "INSERT INTO operations (type, asset, amount, timestamp) VALUES (?, ?, ?, ?)",
    )?;

    for op in ops {
        println!("inserting ts={:?}", op.timestamp.unwrap_or(0));
        stmt.execute(params![
            //op.source_id,
            op.op_type,
            op.asset,
            op.amount,
            op.timestamp
        ])?;
    }

    Ok(())
}

pub fn get_operations() -> Result<Vec<Operation>> {
    let conn = Connection::open(DB_NAME)?;

    let mut stmt =
        conn.prepare("SELECT type, asset, amount, timestamp FROM operations")?;
    let op_iter = stmt.query_map([], |row| {
        Ok(Operation {
            //source_id: row.get(0)?,
            op_type: row.get(0)?,
            asset: row.get(1)?,
            amount: row.get(2)?,
            timestamp: row.get(3)?,
        })
    })?;

    op_iter
        .map(|o| o.map_err(|e| anyhow!("couldn't fetch operation from db").context(e)))
        .collect()
}
