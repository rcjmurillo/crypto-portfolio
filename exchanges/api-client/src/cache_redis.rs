use anyhow::anyhow;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use redis::Commands;

use crate::Storage;
use redis::{AsyncCommands, Client};

pub struct RedisCache {
    client: Client,
    conn: Option<redis::aio::Connection>,
}

impl RedisCache {
    pub fn new(host: String, port: u32) -> Result<Self> {
        let client = Client::open(format!("redis://{}:{}", host, port))?;
        Ok(Self { client, conn: None })
    }
    pub async fn connect(&mut self) -> Result<()> {
        if let None = self.conn {
            self.conn = Some(self.client.get_async_connection().await?);
        }
        Ok(())
    }
}

#[async_trait]
impl Storage for RedisCache {
    type Output = Bytes;
    async fn get(&self, key: &str) -> Result<Option<Self::Output>> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(conn.get(key).await?)
    }

    async fn set(&mut self, key: &str, value: &Bytes) -> Result<()> {
        let mut conn = self.client.get_async_connection().await?;
        let r: String = conn.set(key, value.to_vec()).await?;
        if r == "OK" {
            Ok(())
        } else {
            Err(anyhow!("failed to set key = {}", key))
        }
    }

    async fn exists(&self, key: &str) -> Result<bool> {
        let mut conn = self.client.get_async_connection().await?;
        Ok(conn.exists(key).await?)
    }
}
