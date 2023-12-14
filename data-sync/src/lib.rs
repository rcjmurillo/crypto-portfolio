pub mod sqlite;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc, NaiveDateTime};

#[async_trait]
/// Layer of abstraction on how to fetch data from multiple sources
pub trait DataFetcher {
    fn name(&self) -> &str;
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: RecordStorage + Send + Sync;
}

#[derive(Debug, Clone)]
pub struct Record {
    pub id: usize,
    pub source_id: String,
    pub source: String,
    pub tx_type: String,
    pub data: String,
    pub created_at: DateTime<Utc>,
    pub imported_at: Option<DateTime<Utc>>,
}

pub struct RecordValue<'a>(pub &'a serde_json::Value);

impl TryFrom<RecordValue<'_>> for DateTime<Utc> {
    type Error = anyhow::Error;

    fn try_from(value: RecordValue) -> std::result::Result<Self, Self::Error> {
        match value.0 {
            serde_json::Value::String(ref s) => Ok(Utc.from_utc_datetime(&NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")?)),                
            serde_json::Value::Number(ref n) => Utc
                .timestamp_millis_opt(n.as_i64().ok_or(anyhow!("could not parse {n} as i64"))?)
                .single()
                .ok_or(anyhow!("could not parse {n} as UTC datetime")),
            _ => Err(anyhow!("could not parse {} as UTC datetime", value.0)),
        }
    }
}

pub fn filter_after(
    rows: &Vec<serde_json::Value>,
    time: DateTime<Utc>,
) -> Result<Vec<serde_json::Value>> {
    rows.iter()
        .try_fold(Vec::with_capacity(rows.len()), |mut acc, r| {
            let dt: DateTime<Utc> = RecordValue(&r["time"]).try_into()?;
            if dt > time {
                acc.push(r.clone());
            }
            Ok(acc)
        })
}

pub fn into_records(
    rows: &Vec<serde_json::Value>,
    tx_type: &str,
    source_name: &str,
    source_id_field: &str,
    time_field: &str,
) -> Result<Vec<Record>> {
    rows.iter()
        .inspect(|t| log::debug!("processing row: {:?}", t))
        .map(|t| {
            Ok(Record {
                id: 0,
                source_id: t[source_id_field].to_string(),
                source: source_name.to_string(),
                tx_type: tx_type.to_string(),
                data: serde_json::to_string(t).unwrap(),
                created_at: RecordValue(&t[time_field]).try_into()?,
                imported_at: None,
            })
        })
        .collect::<Result<Vec<Record>>>()
}

pub trait RecordStorage {
    fn get_all(&self, source: &str, record_type: Option<&str>) -> Result<Vec<Record>>;
    fn get_latest(&self, source: &str, record_type: &str) -> Result<Option<Record>>;
    fn insert(&self, records: Vec<Record>) -> Result<()>;
}

pub async fn sync_records<F, S>(fetcher: F, storage: S) -> Result<()>
where
    F: DataFetcher + Send + Sync,
    S: RecordStorage + Send + Sync,
{
    match fetcher.sync(storage).await {
        Ok(()) => log::info!("finished syncing transactions for {}", fetcher.name()),
        Err(err) => log::error!("failed to sync transactions for {}: {}", fetcher.name(), err),
    };
    Ok(())
}
#[cfg(test)]
mod tests {}
