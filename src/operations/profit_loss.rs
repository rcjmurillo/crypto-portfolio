use std::collections::VecDeque;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::operations::{AssetsInfo, Operation};

#[derive(Debug)]
pub struct Sale {
    pub asset: String,
    pub amount: f64,
    pub datetime: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Purchase {
    amount: f64,
    price: f64,
    datetime: DateTime<Utc>,
}

#[derive(Debug)]
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

pub enum ConsumeStrategy {
    Fifo,
    Lifo,
}

/// A stream of operations that can be consumed using a ConsumeStrategy for computing
/// profit/loss of sale operations:
/// - FIFO strategy:
///     This strategy (first-in, first-out) works by using the oldest-to-newer purchase
///     operations when there are a multiple operations which can be used to compute the
///     profit/loss of a sale.
/// - LIFO strategy:
///     This strategy (Last-in, first-out) works by using the newer-to-oldest purchase
///     operations when there are a multiple operations which can be used to compute the
///     profit/loss of a sale.
pub struct OperationsStream<'a> {
    ops: VecDeque<Operation>,
    assets_info: &'a (dyn AssetsInfo + Sync + Send),
}

impl<'a> OperationsStream<'a> {
    pub fn from_ops(
        ops: Vec<Operation>,
        strategy: ConsumeStrategy,
        assets_info: &'a (dyn AssetsInfo + Sync + Send),
    ) -> Self {
        let mut ops = ops
            .into_iter()
            .filter(|op| matches!(op, Operation::Cost { .. }))
            .collect::<Vec<Operation>>();
        ops.sort_by_key(|op| match strategy {
            // the oldest operations will appear first when consuming the queue
            ConsumeStrategy::Fifo => match op {
                Operation::Cost { time, .. } => time.timestamp(),
                _ => 0,
            },
            // the newest operations will appear first when consuming the queue
            ConsumeStrategy::Lifo => match op {
                Operation::Cost { time, .. } => -time.timestamp(),
                _ => 0,
            },
        });
        Self {
            ops: ops.into(),
            assets_info,
        }
    }

    pub async fn consume(&mut self, sale: &Sale) -> Result<MatchResult> {
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
                let price = self.assets_info.price_at(&format!("{}USDT", asset), time).await?;
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

    #[cfg(test)]
    fn ops(&self) -> Vec<&Operation> {
        self.ops.iter().collect()
    }
}

#[async_trait]
impl<'a> SaleMatcher for OperationsStream<'a> {
    async fn match_sale(&mut self, sale: &Sale) -> Result<MatchResult> {
        // consume purchase amounts from the stream to fulfill the sale amount
        self.consume(sale).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use quickcheck::{quickcheck, Gen, TestResult};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::operations::{AssetPrices, Operation::*, storage::Storage};

    struct DummyStorage {
        ops: Vec<Operation>,
    }

    #[async_trait]
    impl Storage for DummyStorage {
        async fn get_ops(&self) -> Result<Vec<Operation>> {
            Ok(self.ops.clone())
        }
        async fn insert_ops(&self, ops: Vec<Operation>) -> Result<(usize, usize)> {
            Ok((ops.len(), 0))
        }
    }

    struct DummyAssetsInfo {
        last_price: Arc<Mutex<f64>>,
    }

    impl DummyAssetsInfo {
        pub fn new() -> Self {
            Self {
                last_price: Arc::new(Mutex::new(0.0)),
            }
        }
    }

    #[async_trait]
    impl AssetsInfo for DummyAssetsInfo {
        async fn price_at(&self, _symbol: &str, _time: &DateTime<Utc>) -> Result<f64> {
            let nums: Vec<_> = (1usize..10).map(|n| n as f64).collect();
            let mut gen = Gen::new(100);
            let mut p: f64 = *gen.choose(&nums[..]).unwrap();
            let mut last_price = self.last_price.lock().await;
            while p == *last_price {
                p = *gen.choose(&nums[..]).unwrap();
            }
            *last_price = p;
            Ok(p)
        }
    }

    fn valid_sequence(ops: &Vec<Operation>) -> bool {
        // check that for every revenue operation there are enough prior purchase operations
        // to cover the amount.
        let mut filtered_ops: Vec<&Operation> = ops
            .iter()
            .filter(|op| matches!(op, Cost { .. }) || matches!(op, Revenue { .. }))
            .collect();
        // sort by time, cost and revenue operations are guaranteed to have a time
        filtered_ops.sort_by_key(|op| op.time().unwrap());

        // make sure we have at least one revenue op
        if !filtered_ops.iter().any(|op| matches!(op, Revenue { .. })) {
            return false;
        }
        for (i, op) in filtered_ops.iter().enumerate() {
            if let Operation::Revenue {
                amount,
                asset: rev_asset,
                ..
            } = op
            {
                let mut prior_amount = 0.0;
                // go through all the ops that are before this one and verify if
                // they can cover amount sold.
                for j in 0..i {
                    match filtered_ops[j] {
                        // cost ops increase the availble amount
                        Cost {
                            for_amount,
                            ref for_asset,
                            ..
                        } if for_asset == rev_asset => {
                            prior_amount += for_amount;
                        }
                        // previous revenue ops reduce the total available amount
                        Revenue {
                            amount, ref asset, ..
                        } => {
                            if asset == rev_asset {
                                prior_amount -= amount;
                            }
                        }
                        _ => (),
                    }
                }
                if prior_amount < *amount {
                    return false;
                }
            }
        }
        true
    }

    fn into_sales(ops: &Vec<Operation>) -> Vec<Sale> {
        ops.iter()
            .filter_map(|op| match op {
                Operation::Revenue {
                    asset,
                    amount,
                    time,
                    ..
                } => Some(Sale {
                    asset: asset.clone(),
                    amount: *amount,
                    datetime: *time,
                }),
                _ => None,
            })
            .collect()
    }

    quickcheck! {
        fn ops_stream_cost_only(ops: Vec<Operation>) -> bool {
            let asset_prices = AssetPrices::new();
            let stream = OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, &asset_prices);
            stream.ops().iter().all(|op| matches!(op, Operation::Cost{..}))
        }
    }

    #[test]
    fn can_consume_from_fifo_operations_stream() {
        fn prop(ops: Vec<Operation>) -> TestResult {
            if !valid_sequence(&ops) {
                return TestResult::discard();
            }
            let sales = into_sales(&ops);
            let assets_info = DummyAssetsInfo::new();
            let mut stream = OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, &assets_info);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("could not build tokio runtime");
            for s in sales {
                if !rt.block_on(stream.consume(&s)).is_ok() {
                    return TestResult::failed();
                }
            }
            TestResult::passed()
        }
        quickcheck(prop as fn(ops: Vec<Operation>) -> TestResult);
    }

    #[test]
    fn consume_at_least_one_or_more_cost_ops() {
        fn prop(ops: Vec<Operation>) -> TestResult {
            if ops.len() > 10 || !valid_sequence(&ops) {
                return TestResult::discard();
            }
            let sales = into_sales(&ops);
            let assets_info = DummyAssetsInfo::new();
            let mut stream = OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, &assets_info);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("could not build tokio runtime");
            for s in sales {
                match rt.block_on(stream.consume(&s)) {
                    Ok(
                        MatchResult::Profit { purchases, .. } | MatchResult::Loss { purchases, .. },
                    ) => {
                        if purchases.len() == 0 {
                            return TestResult::failed();
                        }
                    }
                    Err(_) => return TestResult::failed(),
                }
            }
            TestResult::passed()
        }
        quickcheck(prop as fn(ops: Vec<Operation>) -> TestResult);
    }

    #[test]
    fn consume_ops_stream_fifo() {
        fn prop(ops: Vec<Operation>) -> TestResult {
            if ops.len() != 3 || !valid_sequence(&ops) {
                return TestResult::discard();
            }
            // op0 must be Cost
            // op1 must be Cost
            // op2 must be Revenue
            // all three ops must be for the same asset
            let valid = match (&ops[0], &ops[1], &ops[2]) {
                (
                    Cost { for_asset: a1, .. },
                    Cost { for_asset: a2, .. },
                    Revenue { asset: a3, .. },
                ) => a1 == a2 && a2 == a3,
                _ => false,
            };
            if !valid {
                return TestResult::discard();
            }

            // check that the first cost op's amount has been partially or fully consumed
            if let Revenue {
                amount,
                asset,
                time,
                ..
            } = &ops[3]
            {
                let orig_amounts = [ops[0].amount(), ops[1].amount()];
                let sale = Sale {
                    amount: *amount,
                    asset: asset.clone(),
                    datetime: *time,
                };
                let assets_info = DummyAssetsInfo::new();
                let mut stream =
                    OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, &assets_info);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("could not build tokio runtime");

                match rt.block_on(stream.consume(&sale)) {
                    Ok(_) => {
                        let ops = stream.ops();
                        // if the sale consumed the whole amount, only one or none ops must remain
                        if sale.amount >= orig_amounts[0] && ops.len() == 2 {
                            return TestResult::failed();
                        } else {
                            // both ops must remain in the queue
                            if ops.len() < 2
                                || ops[0].amount() >= orig_amounts[0]
                                || ops[1].amount() >= orig_amounts[1]
                            {
                                return TestResult::failed();
                            }
                        }
                    }
                    Err(_) => return TestResult::failed(),
                }
            }
            TestResult::passed()
        }

        quickcheck(prop as fn(ops: Vec<Operation>) -> TestResult);
    }
}
