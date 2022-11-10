use async_trait::async_trait;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;

use anyhow::Result;
use bytes::Bytes;
use tower::{buffer::Buffer, BoxError, Service};

use crate::{Request, Response};

#[async_trait]
pub trait Storage {
    type Output;
    type Error;
    async fn get(&self, key: &str) -> Result<Option<Self::Output>, Self::Error>;
    async fn set(&mut self, key: &str, value: &Bytes) -> Result<(), Self::Error>;
    async fn exists(&self, key: &str) -> Result<bool>;
}

pub struct Cache<S, ST>
where
    S: Service<Request>,
{
    storage: Arc<RwLock<ST>>,
    inner: Buffer<S, Request>,
}

impl<S, ST> Cache<S, ST>
where
    S: Service<Request, Response = Response> + Send + 'static,
    S::Error: Into<BoxError> + Send + Sync + 'static,
    S::Future: Send,
{
    pub fn new(inner: S, storage: ST) -> Self {
        Cache {
            storage: Arc::new(RwLock::new(storage)),
            inner: Buffer::new(inner, 1000),
        }
    }
}

impl<S, ST> Service<Request> for Cache<S, ST>
where
    S: Service<Request, Response = Response> + Send + Sync + 'static,
    S::Error: From<BoxError> + From<ST::Error> + std::error::Error,
    ST: Storage<Output = Bytes> + Send + Sync + 'static,
    Buffer<S, Request>: Service<Request, Response = S::Response, Error = BoxError>,
    <Buffer<S, Request> as Service<Request>>::Future: Send,
{
    type Response = ST::Output;

    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(|e| e.into())
    }

    fn call(&mut self, req: Request) -> Self::Future {
        if req.cache_response {
            let k = req.cache_key();
            let storage = self.storage.clone();
            let fut = self.inner.call(req);
            Box::pin(async move {
                let found = { storage.read().await.get(&k).await? };
                match found {
                    Some(b) => Ok(b),
                    None => {
                        let resp = fut.await?;
                        storage.write().await.set(&k, &resp.bytes).await?;
                        Ok(resp.bytes)
                    }
                }
            })
        } else {
            let resp = self.inner.call(req);
            Box::pin(async { Ok(resp.await?.bytes) })
        }
    }
}
