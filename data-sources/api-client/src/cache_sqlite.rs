use crate::Storage;

pub struct SqliteCache {
    db_path: String,
}

impl SqliteCache {
    pub fn new(name: &str) -> anyhow::Result<Self> {
        let db_path = format!(".cache/{}.db", name);
        let db = rusqlite::Connection::open(&db_path)?;
        create_table(&db)?;
        Ok(Self {
            db_path
        })
    }
}

#[async_trait::async_trait]
impl Storage for SqliteCache {
    type Output = bytes::Bytes;
    type Error = anyhow::Error;

    async fn get(&self, key: &str) -> anyhow::Result<Option<Self::Output>> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let mut stmt = db.prepare("SELECT value FROM responses WHERE key = ?1")?;
        let mut rows = stmt.query(rusqlite::params![key])?;
        match rows.next()? {
            Some(row) => {
                let value: Vec<u8> = row.get(0)?;
                Ok(Some(value.into()))
            }
            None => Ok(None),
        }
    }

    async fn set(&mut self, key: &str, value: &bytes::Bytes) -> anyhow::Result<()> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let mut stmt = db.prepare("INSERT INTO responses (key, value) VALUES (?1, ?2)")?;
        stmt.execute(rusqlite::params![key, value.to_vec()])?;
        Ok(())
    }

    async fn exists(&self, key: &str) -> anyhow::Result<bool> {
        let db = rusqlite::Connection::open(&self.db_path)?;
        let mut stmt = db.prepare("SELECT COUNT(*) FROM responses WHERE key = ?1")?;
        let mut rows = stmt.query(rusqlite::params![key])?;
        let count: i64 = rows.next()?.unwrap().get(0)?;
        Ok(count > 0)
    }
}

fn create_table(db: &rusqlite::Connection) -> anyhow::Result<()> {
    db.execute(
        "CREATE TABLE IF NOT EXISTS responses (
            key TEXT PRIMARY KEY,
            value BLOB NOT NULL
        )",
        [],
    )?;
    Ok(())
}
