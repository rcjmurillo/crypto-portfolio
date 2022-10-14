use async_trait::async_trait;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use tower::Service;

use crate::{Request, Response};

#[async_trait]
pub trait Storage {
    type Output;
    async fn get(&self, key: &str) -> Result<Option<Self::Output>>;
    async fn set(&mut self, key: &str, value: &Bytes) -> Result<()>;
    async fn exists(&self, key: &str) -> Result<bool>;
}

pub struct Cache<S, ST> {
    storage: Arc<RwLock<ST>>,
    inner: Arc<RwLock<S>>,
}

impl<S, ST> Cache<S, ST>
where
    S: Service<Request, Response = Response, Error = anyhow::Error>,
    S: Send + 'static,
    S::Error: Send + Sync + 'static,
    S::Future: Send,
    ST: Storage<Output = Bytes> + Send + Sync + 'static,
{
    pub fn new(inner: S, storage: ST) -> Self {
        Cache {
            storage: Arc::new(RwLock::new(storage)),
            inner: Arc::new(RwLock::new(inner)),
        }
    }

    /// Check if the cache already contains a response for the provided request
    // pub async fn check(&self, req: &Request) -> Result<bool> {
    //     Ok(req.cache_response && self.storage.read().await.exists(&req.cache_key()).await?)
    // }

    /// Check if the cache already contains a response for the provided request, if so
    /// return immediately, otherwise, await until the inner service is ready.
    pub async fn await_until_ready(&self, req: &Request) -> Result<()> {
        let is_cached =
            req.cache_response && self.storage.read().await.exists(&req.cache_key()).await?;
        if !is_cached {
            // if the there is no cached response, await until the service is ready, which will
            // poll the inner service.
            let mut inner = self.inner.write().await;
            futures::future::poll_fn(|cx| inner.poll_ready(cx)).await?;
        }
        Ok(())
    }
}

impl<S, ST> Service<Request> for Cache<S, ST>
where
    S: Service<Request, Response = Response, Error = anyhow::Error> + Send + Sync + 'static,
    S::Future: Send + 'static,
    S::Error: Send + Sync + 'static,
    ST: Storage<Output = Bytes> + Send + Sync + 'static,
{
    type Response = ST::Output;

    type Error = anyhow::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        panic!("use await_until_ready() instead to poll for readiness")
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let inner = self.inner.clone();
        let entries = self.storage.clone();
        let fut = async move {
            if req.cache_response {
                let k = req.cache_key();
                let entry = { entries.read().await.get(&k).await? };
                match entry {
                    Some(val) => Ok(val),
                    None => {
                        let resp = {
                            let mut inner_locked = inner.write().await;
                            inner_locked.call(req).await?
                        };
                        entries.write().await.set(&k, &resp.bytes).await?;
                        Ok(resp.bytes)
                    }
                }
            } else {
                let mut inner_locked = inner.write().await;
                Ok(inner_locked.call(req).await?.bytes)
            }
        };
        Box::pin(fut)
    }
}
