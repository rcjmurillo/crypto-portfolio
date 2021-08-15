use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;

use reqwest::Result;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

/// A semaphore that allows to acquire permits for arbitrary values of type T.
pub struct ValueSemaphore<T> {
    entries: Mutex<HashMap<T, Arc<Semaphore>>>,
    capacity: usize,
}

impl<T> ValueSemaphore<T>
where
    T: Eq + Hash + Sized,
{
    /// Creates a new value semaphore where for each value 1 permit can be acquired 
    /// at the same time.
    pub fn new() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity: 1,
        }
    }

    /// Creates a new value semaphore where for each value `capacity` permits can
    /// be acquired at the same time.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    /// Acquires a permit for value `val`. If there are not enough permits available
    /// the caller will await until a new permit is available.
    /// The permit returned will be valid until dropped.
    pub async fn acquire_for(&self, val: T) -> Result<OwnedSemaphorePermit> {
        let sem = {
            // this context prevents holding the lock on self.entries
            // in case a future awaits below on acquiring a permit from the
            // semaphore.
            // thus other futures can still acquire permits for other entries.
            let mut entries = self.entries.lock().await;
            // if `val` does not exist in the entries map, create a new semaphore
            // with `capacity` permits.
            entries
                .entry(val)
                .or_insert_with(|| Arc::new(Semaphore::new(self.capacity)))
                .clone()
        };
        // try to get a permit for `val` or await until one is available
        Ok(sem.acquire_owned().await.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout, Duration};

    async fn incr_counter(
        counter: &Mutex<u8>,
        value_sem: &ValueSemaphore<u64>,
        value: u64,
    ) -> OwnedSemaphorePermit {
        // acquire permit on `value`
        let l = value_sem.acquire_for(value).await.unwrap();
        {
            let mut c = counter.lock().await;
            *c += 1;
        }
        l
    }

    #[tokio::test]
    async fn await_on_same_value() {
        // only 1 future should hold the permit per value at the same time
        let value_sem = Arc::new(ValueSemaphore::<u64>::with_capacity(1));
        let value_sem_2 = value_sem.clone();
        let c = Arc::new(Mutex::new(0u8));
        let c2 = c.clone();
        let c3 = c.clone();

        let h1 = tokio::spawn(async move { incr_counter(&c, &value_sem, 1).await });
        let h2 = tokio::spawn(async move { incr_counter(&c2, &value_sem_2, 1).await });

        let perm1 = h1.await.unwrap();
        assert_eq!(1, *c3.lock().await);
        // before releasing the first permit task2 should not be able to acquire it,
        // thus a timeout should occur.
        match timeout(Duration::from_millis(100), h2).await {
            Ok(_) => assert!(false, "permit should not be acquired"),
            Err(_) => {
                // count is still one
                assert_eq!(1, *c3.lock().await);
            }
        }
        drop(perm1); // let the task2 to acquire the permit

        // wait a bit so task2 can acquire the permit and increase the counter
        sleep(Duration::from_millis(100)).await;
        assert_eq!(2, *c3.lock().await);
    }

    #[tokio::test]
    async fn await_on_different_values() {
        // only 1 future should hold the permit per value at the same time
        let value_sem = Arc::new(ValueSemaphore::<u64>::with_capacity(1));
        let value_sem_2 = value_sem.clone();
        let value_sem_3 = value_sem.clone();
        let c = Arc::new(Mutex::new(0u8));
        let c2 = c.clone();
        let c3 = c.clone();
        let c4 = c.clone();

        let h1 = tokio::spawn(async move { incr_counter(&c, &value_sem, 1).await });
        let h2 = tokio::spawn(async move { incr_counter(&c2, &value_sem_2, 1).await });
        let h3 = tokio::spawn(async move { incr_counter(&c3, &value_sem_3, 2).await });

        let perm1 = h1.await.unwrap();
        // task3 should be able to acquire a permit as it's awaiting on a different value
        let _perm3 = h3.await.unwrap();
        // before releasing the first permit task2 should not be able to acquire it,
        // thus a timeout should occur.
        match timeout(Duration::from_millis(100), h2).await {
            Ok(_) => assert!(false, "permit should not be acquired"),
            Err(_) => {
                // task1 and task3 should have increased the counter
                assert_eq!(2, *c4.lock().await);
            }
        }
        drop(perm1); // let task2 acquire the permit

        // wait a bit so task2 can acquire the permit and increase the counter
        sleep(Duration::from_millis(100)).await;
        assert_eq!(3, *c4.lock().await);
    }

    #[tokio::test]
    async fn await_on_different_capacities() {
        // only 2 futures should acquire permits per value at the same time
        let value_sem = Arc::new(ValueSemaphore::<u64>::with_capacity(2));
        let value_sem_2 = value_sem.clone();
        let value_sem_3 = value_sem.clone();
        let c = Arc::new(Mutex::new(0u8));
        let c2 = c.clone();
        let c3 = c.clone();
        let c4 = c.clone();

        let h1 = tokio::spawn(async move { incr_counter(&c, &value_sem, 1).await });
        let h2 = tokio::spawn(async move { incr_counter(&c2, &value_sem_2, 1).await });
        let h3 = tokio::spawn(async move { incr_counter(&c3, &value_sem_3, 1).await });

        let perm1 = h1.await.unwrap();
        let _perm2 = h2.await.unwrap();
        // before releasing the permits task3 should not be able to acquire it,
        // thus a timeout should occur.
        match timeout(Duration::from_millis(100), h3).await {
            Ok(_) => assert!(false, "permit should not be acquired"),
            Err(_) => {
                // but task1 and task3 should have increased the counter
                assert_eq!(2, *c4.lock().await);
            }
        }
        drop(perm1); // let task2 to acquire the permit

        // wait a bit so task2 can acquire the permit and increase the counter
        sleep(Duration::from_millis(100)).await;
        assert_eq!(3, *c4.lock().await);
    }
}
