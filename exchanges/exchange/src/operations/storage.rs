use std::vec;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use redis::Client;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::operations::Operation;

#[async_trait]
pub trait Storage {
    async fn get_ops(&self) -> Result<Vec<Operation>>;
    async fn insert_ops(&self, ops: Vec<Operation>) -> Result<(usize, usize)>;
}

pub trait Record {
    type Id;
    fn id(&self) -> &Self::Id;
    fn datetime(&self) -> DateTime<Utc>;
}

#[derive(Deserialize)]
pub struct RecordId<T> {
    id: T,
}

pub struct Redis {
    client: Client,
}

impl Redis {
    pub fn new(host: String, port: u32) -> Result<Self> {
        let client = Client::open(format!("redis://{}:{}", host, port))?;
        Ok(Self { client })
    }

    pub async fn get_ids<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<Vec<RecordId<T>>>> {
        log::debug!("fetching ids for {}", key);
        let mut conn = self.client.get_async_connection().await?;
        let r: Option<Bytes> = redis::cmd("JSON.GET")
            .arg(&[key, "$[*][*].id"])
            .query_async(&mut conn)
            .await?;
        match r {
            Some(bytes) => {
                log::trace!("bytes from redis {:?}", bytes);
                let x: Vec<T> = serde_json::from_slice(&bytes)?;
                log::debug!("fetched {} records for {}", x.len(), key);
                Ok(Some(x.into_iter().map(|s| RecordId { id: s }).collect()))
            }
            None => Ok(None),
        }
    }

    pub async fn get_records<T: DeserializeOwned>(&self, key: &str) -> Result<Option<Vec<T>>> {
        log::debug!("getting records for {}", key);
        let mut conn = self.client.get_async_connection().await?;
        let r: Option<Bytes> = redis::cmd("JSON.GET")
            .arg(key)
            .arg("$[*]")
            .query_async(&mut conn)
            .await?;

        match r {
            Some(bytes) => {
                log::trace!("bytes from redis {:?}", bytes);
                let x: Vec<T> = serde_json::from_slice(&bytes)?;
                log::debug!("fetched {} records for {}", x.len(), key);
                Ok(if x.is_empty() { None } else { Some(x) })
            }
            None => Ok(None),
        }
    }

    pub async fn insert_records<'a, IdType: PartialEq, T: Record<Id = IdType> + Serialize>(
        &self,
        key: &str,
        records: &Vec<T>,
    ) -> Result<usize>
    where
        for<'de> IdType: Deserialize<'de>,
    {
        // only insert non-existing records
        let existing_ids: Vec<IdType> = self
            .get_ids(key)
            .await?
            .unwrap_or_else(|| vec![])
            .into_iter()
            .map(|r| r.id)
            .collect();
        let mut to_insert: Vec<&T> = records
            .into_iter()
            .filter(|r| !existing_ids.contains(r.id()))
            .collect();
        // to_insert.sort();

        log::debug!(
            "inserting {} of {} records for {}",
            to_insert.len(),
            records.len(),
            key
        );

        if to_insert.is_empty() {
            return Ok(0);
        }

        let payload = serde_json::to_string(&to_insert)?;

        log::debug!("storing into redis {}", payload);

        let cmd = if existing_ids.is_empty() {
            "JSON.SET"
        } else {
            "JSON.ARRAPPEND"
        };

        let mut conn = self.client.get_async_connection().await?;
        let r: String = redis::cmd(cmd)
            .arg(&[key, "$", &payload])
            .query_async(&mut conn)
            .await?;

        if r == "OK" {
            Ok(to_insert.len())
        } else {
            Err(anyhow!("couldn't insert records: {:?}", r))
        }
    }
}
