use chrono::{DateTime, Utc};

use crate::operations::storage::Storage as OperationsStorage;

pub struct Sale {
    asset: String,
    amount: f64,
    datetime: DateTime<Utc>
}

pub enum MatchResult {
    Profit{amount: f64, usd_price: f64, datetime: DateTime<Utc>},
    Loss{amount: f64, usd_price: f64, datetime: DateTime<Utc>},
}

pub trait SaleMatcher {
    fn match_sale(&self, &Sale) -> MatchResult;
}


struct FifoMatcher<S: OperationsStorage> {
    ops_storage: S
};

impl SaleMatcher for FifoMatcher {
    fn new(ops_storage: impl OperationsStorage) -> Self {
        Self {
            ops_storage
        }
    }

    fn match_sale(&self, &Sale) -> MatchResult {

    }
}