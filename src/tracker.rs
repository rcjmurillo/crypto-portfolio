use std::collections::HashMap;
use std::sync::{Arc, Mutex};

pub trait IntoOperations {
    fn into_ops(&self) -> Vec<Operation>;
}

#[derive(Debug, Clone)]
pub struct CoinBalance {
    pub amount: f64,
    cost: f64,
}

impl CoinBalance {
    fn new() -> Self {
        CoinBalance {
            amount: 0.0,
            cost: 0.0,
        }
    }
}

pub struct Operation {
    pub asset: String,
    pub amount: f64,
    pub cost: f64,
}

pub struct CoinTracker {
    coin_balances: Arc<Mutex<HashMap<String, CoinBalance>>>,
}

impl CoinTracker {
    pub fn new() -> Self {
        CoinTracker {
            coin_balances: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn track_operation(&self, op: Operation) {
        let balances = Arc::clone(&self.coin_balances);
        let mut balances = balances.lock().unwrap();
        let coin_balance = balances
            .entry(String::from(op.asset))
            .or_insert(CoinBalance::new());
        coin_balance.amount += op.amount;
        coin_balance.cost += op.cost;
    }

    pub fn get_cost(&self, symbol: &str) -> Option<f64> {
        if let Some(balance) = self.coin_balances.lock().unwrap().get(symbol) {
            Some(balance.cost)
        } else {
            None
        }
    }

    pub fn balances(&self) -> Vec<(String, CoinBalance)> {
        self.coin_balances
            .lock()
            .unwrap()
            .iter()
            .filter_map(|(k, v)| {
                if v.amount >= 0.0 && v.cost >= 0.0 {
                    Some((k.clone(), (*v).clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}
