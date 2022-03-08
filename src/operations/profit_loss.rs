use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::operations::{storage::Storage as OperationsStorage, AssetsInfo, Operation};

#[derive(Debug)]
pub struct Sale {
    asset: String,
    amount: f64,
    datetime: DateTime<Utc>,
}

pub struct Purchase {
    amount: f64,
    price: f64,
    datetime: DateTime<Utc>,
}

pub enum MatchResult {
    Profit {
        usd_amount: f64,
        purchases: Vec<Purchase>,
    },
    Loss {
        usd_amount: f64,
        purchases: Vec<Purchase>,
    },
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
#[async_trait]
pub trait SaleMatcher {
    async fn match_sale(&mut self, sale: &Sale) -> Result<MatchResult>;
}

enum ConsumeStrategy {
    Fifo,
    Lifo,
}

struct OperationsStream<'a> {
    ops: VecDeque<Operation>,
    assets_info: &'a (dyn AssetsInfo + Sync + Send),
}

impl<'a> OperationsStream<'a> {
    pub fn from_ops(
        mut ops: Vec<Operation>,
        strategy: ConsumeStrategy,
        assets_info: &'a (dyn AssetsInfo + Sync + Send),
    ) -> Self {
        ops.sort_by_key(|op| match strategy {
            // the oldest operations will appear first when consuming the queue
            ConsumeStrategy::Fifo => match op {
                Operation::BalanceIncrease { .. } | Operation::BalanceDecrease { .. } => 0,
                Operation::Cost { time, .. } | Operation::Revenue { time, .. } => time.timestamp(),
            },
            // the newest operations will appear first when consuming the queue
            ConsumeStrategy::Lifo => match op {
                Operation::BalanceIncrease { .. } | Operation::BalanceDecrease { .. } => 0,
                Operation::Cost { time, .. } | Operation::Revenue { time, .. } => -time.timestamp(),
            },
        });
        Self {
            ops: ops.into(),
            assets_info,
        }
    }

    async fn consume(&mut self, sale: &Sale) -> Result<MatchResult> {
        // consume purchase operations until we fulfill this amount
        let mut fulfill_amount = sale.amount;
        let mut consumed_ops = Vec::new();
        // the total cost of purchasing the amount from the sale
        let mut cost = 0.0;
        while fulfill_amount > 0.0 {
            if let Some(mut purchase) = self.ops.front_mut() {
                let (purchase_amount, asset, time) = match &mut purchase {
                    Operation::Cost {
                        ref mut for_amount,
                        ref asset,
                        ref time,
                        ..
                    } => (for_amount, asset, time),
                    _ => continue,
                };
                // pick the amount that will be used to fulfill the sale
                let amount_fulfilled = if *purchase_amount < fulfill_amount {
                    *purchase_amount
                } else {
                    fulfill_amount
                };
                let price = self.assets_info.price_at(asset, time).await?;
                fulfill_amount -= amount_fulfilled;
                cost += amount_fulfilled * price;
                // update the amount used from this purchase to fulfill the
                // sale, if there is an amount left, it could be still to
                // fulfill more sales.
                *purchase_amount -= amount_fulfilled;
                consumed_ops.push(Purchase {
                    // the amount fulfilled from the operation
                    amount: amount_fulfilled,
                    price,
                    datetime: *time,
                });
                // if the whole amount from the purchase was used, remove it
                // from the queue as we can't use it anymore to fulfill more sales.
                if *purchase_amount == 0.0 {
                    self.ops.pop_front();
                }
            } else {
                return Err(anyhow!("inconsistency found!!! there are not enough purchase operations to fulfill this sale operation: {:?}", sale));
            }
        }

        Ok(match (sale.amount, cost) {
            (r, c) if r >= c => MatchResult::Profit {
                usd_amount: r - c,
                purchases: consumed_ops,
            },
            (r, c) => MatchResult::Loss {
                usd_amount: c - r,
                purchases: consumed_ops,
            },
        })
    }
}

/// A matcher using the FIFO strategy for computing profit/loss of sale operations,
/// this strategy (first-in, first-out) works by using the oldest-to-newer purchase
/// operations when there are a multiple operations which can be used to compute the
/// profit/loss of a sale.
struct FifoMatcher<'a> {
    purchases_stream: OperationsStream<'a>,
}

impl<'a> FifoMatcher<'a> {
    async fn new(
        ops_storage: &'a (dyn OperationsStorage + Sync + Send),
        assets_info: &'a (dyn AssetsInfo + Sync + Send),
    ) -> Result<FifoMatcher<'a>> {
        Ok(Self {
            purchases_stream: OperationsStream::from_ops(
                ops_storage.get_ops().await?,
                ConsumeStrategy::Fifo,
                assets_info,
            ),
        })
    }
}

#[async_trait]
impl<'a> SaleMatcher for FifoMatcher<'a> {
    async fn match_sale(&mut self, sale: &Sale) -> Result<MatchResult> {
        // figure out the balance for the asset before the time of the sale
        self.purchases_stream.consume(sale).await
    }
}
