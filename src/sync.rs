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
            capacity: 1,
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
            capacity,
        }
    }

    pub async fn lock_for(&self, key: T) -> Result<OwnedSemaphorePermit> {
        let sem = {
            // this context prevents holding the lock on self.entries
            // in case a future awaits below on acquiring a permit from the
            // semaphore.
            // Thus other futures can still grab locks for other entries.
            let mut entries = self.entries.lock().await;
            entries
                .entry(key)
                .or_insert_with(|| Arc::new(Semaphore::new(self.capacity)))
                .clone()
        };
        Ok(sem.acquire_owned().await.unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, timeout, Duration};

    async fn incr_counter(
        counter: &Mutex<u8>,
        value_lock: &ValueLock<u64>,
        value: u64,
    ) -> OwnedSemaphorePermit {
        // lock on `value`
        let l = value_lock.lock_for(value).await.unwrap();
        {
            let mut c = counter.lock().await;
            *c += 1;
        }
        l
    }

    #[tokio::test]
    async fn lock_on_same_value() {
        // only 1 futures should hold the lock per value at the same time
        let value_lock = Arc::new(ValueLock::<u64>::with_capacity(1));
        let value_lock_2 = value_lock.clone();
        let c = Arc::new(Mutex::new(0u8));
        let c2 = c.clone();
        let c3 = c.clone();

        let h1 = tokio::spawn(async move { incr_counter(&c, &value_lock, 1).await });
        let h2 = tokio::spawn(async move { incr_counter(&c2, &value_lock_2, 1).await });

        let lock1 = h1.await.unwrap();
        assert_eq!(1, *c3.lock().await);
        // before dropping the first lock task2 should not be able to grab it,
        // thus a timeout should occur.
        match timeout(Duration::from_millis(100), h2).await {
            Ok(_) => assert!(false, "lock should not be grabbed"),
            Err(_) => {
                // count is still one
                assert_eq!(1, *c3.lock().await);
            }
        }
        drop(lock1); // let the task2 to grab the lock

        // wait a bit so task2 can grab the lock and increase the counter
        sleep(Duration::from_millis(100)).await;
        assert_eq!(2, *c3.lock().await);
    }

    #[tokio::test]
    async fn lock_on_different_values() {
        // only 1 futures should hold the lock per value at the same time
        let value_lock = Arc::new(ValueLock::<u64>::with_capacity(1));
        let value_lock_2 = value_lock.clone();
        let value_lock_3 = value_lock.clone();
        let c = Arc::new(Mutex::new(0u8));
        let c2 = c.clone();
        let c3 = c.clone();
        let c4 = c.clone();

        let h1 = tokio::spawn(async move { incr_counter(&c, &value_lock, 1).await });
        let h2 = tokio::spawn(async move { incr_counter(&c2, &value_lock_2, 1).await });
        let h3 = tokio::spawn(async move { incr_counter(&c3, &value_lock_3, 2).await });

        let lock1 = h1.await.unwrap();
        // task3 should be able to grab the lock as it's locking on a different value
        let _lock3 = h3.await.unwrap();
        // before dropping the first lock task2 should not be able to grab it,
        // thus a timeout should occur.
        match timeout(Duration::from_millis(100), h2).await {
            Ok(_) => assert!(false, "lock should not be grabbed"),
            Err(_) => {
                // task1 and task3 should have increased the counter
                assert_eq!(2, *c4.lock().await);
            }
        }
        drop(lock1); // let task2 grab the lock

        // wait a bit so task2 can grab the lock and increase the counter
        sleep(Duration::from_millis(100)).await;
        assert_eq!(3, *c4.lock().await);
    }

    #[tokio::test]
    async fn lock_on_different_capacities() {
        // only 2 futures should hold the lock per value at the same time
        let value_lock = Arc::new(ValueLock::<u64>::with_capacity(2));
        let value_lock_2 = value_lock.clone();
        let value_lock_3 = value_lock.clone();
        let c = Arc::new(Mutex::new(0u8));
        let c2 = c.clone();
        let c3 = c.clone();
        let c4 = c.clone();

        let h1 = tokio::spawn(async move { incr_counter(&c, &value_lock, 1).await });
        let h2 = tokio::spawn(async move { incr_counter(&c2, &value_lock_2, 1).await });
        let h3 = tokio::spawn(async move { incr_counter(&c3, &value_lock_3, 1).await });

        let lock1 = h1.await.unwrap();
        let _lock2 = h2.await.unwrap();
        // before dropping the locks task3 should not be able to grab it,
        // thus a timeout should occur.
        match timeout(Duration::from_millis(100), h3).await {
            Ok(_) => assert!(false, "lock should not be grabbed"),
            Err(_) => {
                // but task1 and task3 should have increased the counter
                assert_eq!(2, *c4.lock().await);
            }
        }
        drop(lock1); // let task2 to grab the lock

        // wait a bit so task2 can grab the lock and increase the counter
        sleep(Duration::from_millis(100)).await;
        assert_eq!(3, *c4.lock().await);
    }
}
