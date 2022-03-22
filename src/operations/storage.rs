use anyhow::Result;
use async_trait::async_trait;

use crate::operations::Operation;

#[async_trait]
pub trait Storage {
    async fn get_ops(&self) -> Result<Vec<Operation>>;
    async fn insert_ops(&self, ops: Vec<Operation>) -> Result<usize>;
}
