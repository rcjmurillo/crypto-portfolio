use std::collections::HashMap;
use std::marker::PhantomData;
use std::task::Poll;
use std::{future::Future, pin::Pin, sync::Arc};
use tokio::sync::RwLock;

use anyhow::Result;
use bytes::Bytes;
use tower::Service;

use crate::{Request, Response};

pub struct Cache<S> {
    entries: Arc<RwLock<HashMap<String, Arc<Bytes>>>>,
    inner: S,
}

impl<S> Cache<S>
where
    S: Service<Request, Response = Response, Error = anyhow::Error>,
    S::Error: Send + Sync + 'static,
{
    pub fn new(inner: S) -> Self {
        Cache {
            entries: Arc::new(RwLock::new(HashMap::new())),
            inner,
        }
    }

    pub async fn call(&mut self, req: Request) -> Result<Arc<Bytes>> {
        let key = req
            .query_params
            .as_ref()
            .map(|q| format!("{}{}", req.url.path(), q.materialize().cacheable_query))
            .unwrap_or(req.url.path().to_string());

        if let Some(value) = self.entries.read().await.get(&key) {
            log::debug!("cache hit for {}", key);
            return Ok(value.clone());
        }

        // await until the service is ready
        futures::future::poll_fn(|cx| self.inner.poll_ready(cx)).await?;
        let resp = self.inner.call(req).await?;
        let value = Arc::new(resp.bytes);
        self.entries.write().await.insert(key, value.clone());
        Ok(value)
    }
}
