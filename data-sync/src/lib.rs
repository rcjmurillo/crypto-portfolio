use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc, NaiveDateTime};

#[async_trait]
/// Layer of abstraction on how to fetch data from multiple sources
pub trait DataFetcher {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: RecordStorage + Send + Sync;
}

#[derive(Debug, Clone)]
pub struct Record {
    pub id: usize,
    pub source_id: String,
    pub source: String,
    pub op_type: String,
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
    op_type: &str,
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
                op_type: op_type.to_string(),
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

pub async fn sync_records<F, S>(name: &str, fetcher: F, storage: S) -> Result<()>
where
    F: DataFetcher + Send + Sync,
    S: RecordStorage + Send + Sync,
{
    match fetcher.sync(storage).await {
        Ok(()) => log::info!("finished syncing operations for {}", name),
        Err(err) => log::error!("failed to sync operations for {}: {}", name, err),
    };
    Ok(())
}

pub fn create_tables() -> Result<()> {
    // read schema from file
    let operations_schema = std::fs::read_to_string("./data-sync/schemas/operations.sql")?;
    let db = rusqlite::Connection::open("./operations.db")?;
    db.execute(&operations_schema, rusqlite::params![])?;
    Ok(())
}

pub struct SqliteStorage {
    db_path: String,
}

impl SqliteStorage {
    pub fn new(db_path: &str) -> Result<Self> {
        Ok(Self {
            db_path: db_path.to_string(),
        })
    }
}

impl RecordStorage for SqliteStorage {
    fn get_all(&self, source: &str, op_type: Option<&str>) -> Result<Vec<Record>> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "id, source_id, source, op_type, data, created_at, imported_at";
        let (where_clause, params) = match op_type {
            Some(op_type) => ("WHERE source = ?1 AND op_type = ?2", vec![source, op_type]),
            None => ("WHERE source = ?1", vec![source]),
        };
        let mut stmt = db.prepare(&format!(
            "SELECT {fields} FROM operations WHERE {where_clause}"
        ))?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            let created_at: String = row.get(5)?;
            let imported_at: Option<String> = row.get(6)?;
            records.push(Record {
                id: row.get(0)?,
                source_id: row.get(1)?,
                source: row.get(2)?,
                op_type: row.get(3)?,
                data: row.get(4)?,
                created_at: DateTime::parse_from_rfc3339(&created_at).unwrap().into(),
                imported_at: imported_at.map(|s| DateTime::parse_from_rfc3339(&s).unwrap().into()),
            });
        }
        Ok(records)
    }

    fn get_latest(&self, source: &str, record_type: &str) -> Result<Option<Record>> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "id, source_id, source, op_type, data, created_at, imported_at";
        let mut stmt = db.prepare(&format!(
            "SELECT {fields} FROM operations WHERE source = ?1 AND op_type = ?2
             ORDER BY created_at DESC LIMIT 1"
        ))?;
        let mut rows = stmt.query(rusqlite::params![source, record_type])?;
        match rows.next()? {
            Some(row) => {
                let created_at: String = row.get(5)?;
                let imported_at: Option<String> = row.get(6)?;
                Ok(Some(Record {
                    id: row.get(0)?,
                    source_id: row.get(1)?,
                    source: row.get(2)?,
                    op_type: row.get(3)?,
                    data: row.get(4)?,
                    created_at: DateTime::parse_from_rfc3339(&created_at)
                        .expect(&format!("couldn't parse created_at {created_at}"))
                        .into(),
                    imported_at: imported_at.map(|s| {
                        DateTime::parse_from_rfc3339(&s)
                            .expect(&format!("couldn't parse imported_at '{s}'"))
                            .into()
                    }),
                }))
            }
            None => Ok(None),
        }
    }

    fn insert(&self, records: Vec<Record>) -> Result<()> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "source_id, source, op_type, data, created_at";
        let mut stmt = db.prepare(&format!(
            "INSERT INTO operations ({fields}) VALUES (?1, ?2, ?3, ?4, ?5)"
        ))?;
        let mut inserted = 0;
        let mut existing = 0;
        for record in records.iter() {
            match stmt.execute(rusqlite::params![
                record.source_id,
                record.source,
                record.op_type,
                record.data,
                record.created_at.to_rfc3339(),
            ]) {
                Ok(_) => inserted += 1,
                Err(err) => {
                    if let Some(rusqlite::ErrorCode::ConstraintViolation) = err.sqlite_error_code()
                    {
                        existing += 1;
                    } else {
                        return Err(anyhow!("failed to insert record: {}", err));
                    }
                }
            }
        }
        let source = records
            .iter()
            .last()
            .map(|r| r.source.as_str())
            .unwrap_or("");
        if inserted > 0 {
            log::info!("inserted {inserted} records into DB from {}", source);
        }
        if existing > 0 {
            log::warn!("skipped {existing} existing records from {}", source);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {}
