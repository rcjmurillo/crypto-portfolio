use anyhow::Result;
use chrono::{DateTime, Utc};

#[derive(Debug)]
pub struct Operation {
    pub id: usize,
    pub source_id: String,
    pub source: String,
    pub op_type: String,
    pub data: String,
    pub created_at: DateTime<Utc>,
    pub imported_at: Option<DateTime<Utc>>,
}

pub trait OperationStorage {
    fn get_all(&self, source: &str, record_type: Option<&str>) -> Result<Vec<Operation>>;
    fn get_latest(&self, source: &str, record_type: &str) -> Result<Option<Operation>>;
    fn insert(&self, records: Vec<Operation>) -> Result<()>;
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

impl OperationStorage for SqliteStorage {
    fn get_all(&self, source: &str, op_type: Option<&str>) -> Result<Vec<Operation>> {
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
            records.push(Operation {
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

    fn get_latest(&self, source: &str, record_type: &str) -> Result<Option<Operation>> {
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
                Ok(Some(Operation {
                    id: row.get(0)?,
                    source_id: row.get(1)?,
                    source: row.get(2)?,
                    op_type: row.get(3)?,
                    data: row.get(4)?,
                    created_at: DateTime::parse_from_rfc3339(&created_at)
                        .expect(&format!("couldn't parse created_at {created_at}"))
                        .into(),
                    imported_at: imported_at.map(|s| {
                        // DateTime::from_utc(
                        //     NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S")
                        //         .expect(&format!("couldn't parse imported_at '{s}'"))
                        //         .into(),
                        //     Utc,
                        // )

                        DateTime::parse_from_rfc3339(&s)
                            .expect(&format!("couldn't parse imported_at '{s}'"))
                            .into()
                    }),
                }))
            }
            None => Ok(None),
        }
    }

    fn insert(&self, records: Vec<Operation>) -> Result<()> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let fields = "source_id, source, op_type, data, created_at";
        let mut stmt = db.prepare(&format!(
            "INSERT INTO operations ({fields}) VALUES (?1, ?2, ?3, ?4, ?5)"
        ))?;
        for record in records {
            stmt.execute(rusqlite::params![
                record.source_id,
                record.source,
                record.op_type,
                record.data,
                record.created_at.to_rfc3339(),
            ])?;
        }
        Ok(())
    }
}

pub fn create_tables() -> Result<()> {
    // read schema from file
    let operations_schema = std::fs::read_to_string("./data-sync/schemas/operations.sql")?;
    let db = rusqlite::Connection::open("./operations.db")?;
    db.execute(&operations_schema, rusqlite::params![])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    
}
