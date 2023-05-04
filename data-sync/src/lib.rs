use anyhow::Result;

pub struct Operation {
    pub id: usize,
    pub source_id: String,
    pub source: String,
    pub op_type: String,
    pub data: String,
    pub created_at: u16,
    pub imported_at: u16,
}

pub trait OperationStorage {
    fn get_all(&self, source: &str, record_type: Option<&str>) -> Result<Vec<Operation>>;
    fn get_latest(&self, source: &str, record_type: &str) -> Result<Option<Operation>>;
    fn insert(&self, records: Vec<Operation>) -> Result<()>;
}

pub struct SqliteStorage {
    db: rusqlite::Connection,
}

impl SqliteStorage {
    pub fn new(db: rusqlite::Connection) -> Self {
        Self { db }
    }
}

impl OperationStorage for SqliteStorage {
    fn get_all(&self, source: &str, op_type: Option<&str>) -> Result<Vec<Operation>> {
        let fields = "id, source_id, source, op_type, data, created_at, imported_at";
        let (where_clause, params) = match op_type {
            Some(op_type) => ("WHERE source = ?1 AND op_type = ?2", vec![source, op_type]),
            None => ("WHERE source = ?1", vec![source]),
        };
        let mut stmt = self.db.prepare(&format!(
            "SELECT {fields} FROM operations WHERE {where_clause}"
        ))?;
        let mut rows = stmt.query(rusqlite::params_from_iter(params))?;
        let mut records = Vec::new();
        while let Some(row) = rows.next()? {
            records.push(Operation {
                id: row.get(0)?,
                source_id: row.get(1)?,
                source: row.get(2)?,
                op_type: row.get(3)?,
                data: row.get(4)?,
                created_at: row.get(5)?,
                imported_at: row.get(6)?,
            });
        }
        Ok(records)
    }

    fn get_latest(&self, source: &str, record_type: &str) -> Result<Option<Operation>> {
        let fields = "id, source_id, source, op_type, data, created_at, imported_at";
        let mut stmt = self.db.prepare(&format!(
            "SELECT {fields} FROM operations WHERE source = ?1 AND op_type = ?2 
             ORDER BY created_at DESC LIMIT 1"
        ))?;
        let mut rows = stmt.query(rusqlite::params![source, record_type])?;
        match rows.next()? {
            Some(row) => Ok(Some(Operation {
                id: row.get(0)?,
                source_id: row.get(1)?,
                source: row.get(2)?,
                op_type: row.get(3)?,
                data: row.get(4)?,
                created_at: row.get(5)?,
                imported_at: row.get(6)?,
            })),
            None => Ok(None),
        }
    }

    fn insert(&self, records: Vec<Operation>) -> Result<()> {
        let fields = "source_id, source, op_type, data, created_at, imported_at";
        let mut stmt = self.db.prepare(&format!(
            "INSERT INTO operations ({fields}) VALUES (?1, ?2, ?3, ?4, ?5, ?6)"
        ))?;
        for record in records {
            stmt.execute(rusqlite::params![
                record.source_id,
                record.source,
                record.op_type,
                record.data,
                record.created_at,
                record.imported_at,
            ])?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
