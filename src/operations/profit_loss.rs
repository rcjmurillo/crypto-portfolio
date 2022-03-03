use std::collections::{Vec, VecDeque};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::operations::{AssetsInfo, storage::Storage as OperationsStorage, Operation};

#[derive(Debug)]
pub struct Sale {
    asset: String,
    amount: f64,
    datetime: DateTime<Utc>
}

pub struct Purchase {
    amount: f64,
    price: f64,
    datetime: DateTime<Utc>
}

pub enum MatchResult {
    Profit{usd_amount: f64, purchases: Vec<Purchase>},
    Loss{usd_amount: f64, purchases: Vec<Purchase>},
}

/// A way to define a matcher that for any given sale of an asset will find 
/// its respective purchase operations, then computing the profit or loss.
/// 
/// The concrete implementation will define the match strategy, consider the following
/// scenarios:
/// 
/// Scenario 1:
///     operation=purchase asset=A time=1 amount=10 price=$2
///     operation=sale     asset=A time=4 amount=7  price=$5
///     ^ this sale has a profit of: 7*5 - 7*2 = $21
/// 
/// Scenario 2:
///     operation=purchase asset=A time=1 amount=10 price=$2
///     operation=purchase asset=A time=3 amount=5  price=$3
///     operation=sale     asset=A time=4 amount=15 price=$5
///     ^ this sale has an amount that spans both purchase operations,
///       thus the profit is the revenue of the sale operation minus the 
///       sum of the cost of each purchase: 15*5 - 5*3 - 10*2 = $40 
/// 
/// Scenario 3:
///     operation=purchase asset=A time=1 amount=10 price=$2
///     operation=purchase asset=A time=3 amount=5  price=$3
///     operation=sale     asset=A time=4 amount=4  price=$5
///     ^ either of the purchase operations can be used to calculate the profit/loss 
///       of this sale, but which one?
/// 
/// Scenario 4:
///     operation=purchase asset=A time=1 amount=10 price=$2
///     operation=purchase asset=A time=3 amount=5  price=$3
///     operation=sale     asset=A time=4 amount=7  price=$5
///     ^ should the profit/loss be computed using the first purchase? or
///       using the cost of the second one plus the partial cost of the first one?
pub trait SaleMatcher {
    fn match_sale(&self, &Sale) -> Result<MatchResult>;
}


enum ConsumeStrategy {
    Fifo,
    Lifo,
}


struct OperationsStream {
    ops: VecDeque<Operation>,
    assets_info: AssetsInfo,
} 

impl PurchasesStream {
    pub from_ops(ops: Vec<Operation>, strategy: ConsumeStrategy, assets_info: AssetsInfo) -> Self {
        ops.sort_by_key(|op| match strategy {
            // the oldest operations will appear first when consuming the queue
            ConsumeStrategy::Fifo => op.datetime.timestamp,
            // the newest operations will appear first when consuming the queue
            ConsumeStrategy::Lifo => -op.datetime.timestamp,
        });
        Self {
            ops: ops.into(),
            assets_info,
        }
    }
    fn consume(&self, sale: &Sale) -> Result<MatchResult> {
        // consume purchase operations until we fulfill this amount
        let mut fulfill_amount = sale.amount;
        let mut consumed_ops = Vec::new();
        let mut cost = 0.0;
        
        while fulfill_amount > 0 {
            // todo: if peek is not available, pop the purchase and put it back if needed
            if let Some(purchase) = self.ops.peek() {
                if let &Operation::Cost{for_amount, asset, amount, time, ..} = purchase {
                    let price = self.assets_info.price_at(asset, time)?;
                    if fulfill_amount > *for_amount {
                        fulfill_amount -= *for_amount; 
                        cost += for_amount * price; 
                        *for_amount = 0;
                    } else {
                        // keep track how much we used from the purchase
                        *for_amount -= fulfill_amount;
                        cost += fulfill_amount * price; 
                        fulfill_amount = 0;
                    }
                    if for_amount == 0 {
                        self.ops.pop_front();
                    }
                    consumed_ops.push(purchase.clone());
                }
            } else {
                return Err(anyhow!("inconsistency found!!! there are not enough purchase operations to fulfill this sale operation: {:?}", sale));
            }
        }

        Ok(match (sale.revenue, cost) {
            (r, c) if r >= c => MatchResult::Profit {usd_amount: r - c, consumed_ops},
            (r, c) => MatchResult::Loss {usd_amount: c - r, consumed_ops},
        })
    }
}


/// A matcher using the FIFO strategy for computing profit/loss of sale operations, 
/// this strategy (first-in, first-out) works by using the oldest-to-newer purchase 
/// operations when there are a multiple operations which can be used to compute the 
/// profit/loss of a sale.
struct FifoMatcher<S> {
    ops_storage: S,
    assets_info: AssetsInfo,
};

impl<S: OperationsStorage> SaleMatcher for FifoMatcher<S> {
    fn new(ops_storage: S) -> Self {
        Self {
            ops_storage
        }
    }

    fn match_sale(&self, &Sale) -> MatchResult {
        // figure out the balance for the asset before the time of the sale
        let purchase_ops = 
    }
}