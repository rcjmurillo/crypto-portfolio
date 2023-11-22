use crate::{Record, RecordStorage};
use chrono::DateTime;

pub fn create_tables() -> anyhow::Result<()> {
    // read schema from file
    let transactions_schema = std::fs::read_to_string("./data-sync/schemas/transactions.sql")?;
    let db = rusqlite::Connection::open("./transactions.db")?;
    db.execute(&transactions_schema, rusqlite::params![])?;
    Ok(())
}

pub struct SqliteStorage {
    db_path: String,
}

impl SqliteStorage {
    pub fn new(db_path: &str) -> anyhow::Result<Self> {
        Ok(Self {
            db_path: db_path.to_string(),
        })
    }
}

impl RecordStorage for SqliteStorage {
    fn get_all(&self, source: &str, tx_type: Option<&str>) -> anyhow::Result<Vec<Record>> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "id, source_id, source, tx_type, data, created_at, imported_at";
        let (where_clause, params) = match tx_type {
            Some(tx_type) => ("WHERE source = ?1 AND tx_type = ?2", vec![source, tx_type]),
            None => ("WHERE source = ?1", vec![source]),
        };
        let mut stmt = db.prepare(&format!(
            "SELECT {fields} FROM transactions WHERE {where_clause}"
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
                tx_type: row.get(3)?,
                data: row.get(4)?,
                created_at: DateTime::parse_from_rfc3339(&created_at).unwrap().into(),
                imported_at: imported_at.map(|s| DateTime::parse_from_rfc3339(&s).unwrap().into()),
            });
        }
        Ok(records)
    }

    fn get_latest(&self, source: &str, record_type: &str) -> anyhow::Result<Option<Record>> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "id, source_id, source, tx_type, data, created_at, imported_at";
        let mut stmt = db.prepare(&format!(
            "SELECT {fields} FROM transactions WHERE source = ?1 AND tx_type = ?2
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
                    tx_type: row.get(3)?,
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

    fn insert(&self, records: Vec<Record>) -> anyhow::Result<()> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "source_id, source, tx_type, data, created_at";
        let mut stmt = db.prepare(&format!(
            "INSERT INTO transactions ({fields}) VALUES (?1, ?2, ?3, ?4, ?5)"
        ))?;
        let mut inserted = 0;
        let mut existing = 0;
        for record in records.iter() {
            match stmt.execute(rusqlite::params![
                record.source_id,
                record.source,
                record.tx_type,
                record.data,
                record.created_at.to_rfc3339(),
            ]) {
                Ok(_) => inserted += 1,
                Err(err) => {
                    if let Some(rusqlite::ErrorCode::ConstraintViolation) = err.sqlite_error_code()
                    {
                        existing += 1;
                    } else {
                        return Err(anyhow::anyhow!("failed to insert record: {}", err));
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
