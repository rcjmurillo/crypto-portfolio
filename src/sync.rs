use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

use crate::result::Result;

pub struct ValueLock<T> {
    entries: Mutex<HashMap<T, Arc<Semaphore>>>,
    capacity: usize,
}

impl<T> ValueLock<T>
where
    T: Eq + Hash + Sized,
{
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: 1
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity
        }
    }

    pub async fn lock_for(&self, key: T) -> Result<OwnedSemaphorePermit> {
        let mut entries = self.entries.lock().await;
        let sem = entries.entry(key).or_insert_with(|| Arc::new(Semaphore::new(self.capacity)));
        Ok(sem.clone().acquire_owned().await.unwrap())
    }
}
