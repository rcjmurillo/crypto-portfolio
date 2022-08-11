use std::fmt;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::operations::Operation;
use crate::{AssetPair, AssetsInfo};

#[derive(Debug)]
pub struct Sale {
    pub asset: String,
    pub amount: f64,
    pub datetime: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Purchase {
    source: String,
    amount: f64,
    cost: f64,
    price: f64,
    paid_with: String,
    paid_with_amount: f64,
    datetime: DateTime<Utc>,
    sale_result: OperationResult,
}

#[derive(Debug)]
pub enum OperationResult {
    Profit(f64),
    Loss(f64),
}

#[derive(Debug)]
pub struct MatchResult {
    result: OperationResult,
    purchases: Vec<Purchase>,
}

impl fmt::Display for MatchResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { result, purchases } = self;
        let (result_str, usd_amount) = match result {
            OperationResult::Profit(usd_amount) => ("Profit", usd_amount),
            OperationResult::Loss(usd_amount) => ("Loss", usd_amount),
        };
        write!(
            f,
            "{} of ${}:\n> Purchases:\n{}",
            result_str,
            usd_amount,
            purchases
                .iter()
                .map(|p| format!("\t{:?}", p))
                .collect::<Vec<String>>()
                .join("\n")
        )
    }
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
    ops: Vec<Operation>,
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
            // the oldest operations will appear first when iterating
            ConsumeStrategy::Fifo => match op {
                Operation::Cost { time, .. } => time.timestamp(),
                _ => 0,
            },
            // the newest operations will appear first when iterating
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
        let mut amount_to_fulfill = sale.amount;
        let mut consumed_ops = Vec::new();
        // the total cost of purchasing the amount from the sale
        let mut cost = 0.0;
        let mut ops_iter = self.ops.iter_mut();
        // .filter(|op| {
        //     if let Operation::Cost { for_asset, .. } = op {
        //         for_asset == &sale.asset
        //     } else {
        //         false
        //     }
        // });
        while amount_to_fulfill > 0.0 {
            if let Some(mut purchase) = ops_iter.next() {
                let (source, purchased_amount, paid_amount, asset, time) = match &mut purchase {
                    Operation::Cost {
                        ref source,
                        ref mut for_amount,
                        ref mut amount,
                        ref for_asset,
                        ref asset,
                        ref time,
                        ..
                    } => {
                        if for_asset == &sale.asset {
                            (source, for_amount, amount, asset, time)
                        } else {
                            continue;
                        }
                    }
                    _ => continue,
                };
                if *purchased_amount == 0.0 && *paid_amount == 0.0 {
                    continue;
                }
                // purchased_amount that will be used to fulfill the sale
                let (amount_fulfilled, paid_amount_used) = if *purchased_amount < amount_to_fulfill
                {
                    (*purchased_amount, *paid_amount)
                } else {
                    (
                        amount_to_fulfill,
                        // compute the partial amount paid for the amount used from the purchase
                        *paid_amount * (amount_to_fulfill / *purchased_amount),
                    )
                };
                let price = self
                    .assets_info
                    .usd_price_at(&asset, time)
                    .await?;
                amount_to_fulfill -= amount_fulfilled;
                let purchase_cost = paid_amount_used * price;
                cost += purchase_cost;
                // update the amount used from this purchase to fulfill the
                // sale, if there is an amount left, it could be still used to
                // fulfill more sales.
                *purchased_amount -= amount_fulfilled;
                *paid_amount -= paid_amount_used;

                let price_at_sale = self
                    .assets_info
                    .usd_price_at(&sale.asset, &sale.datetime)
                    .await?;

                let sale_revenue = amount_fulfilled * price_at_sale;
                consumed_ops.push(Purchase {
                    source: source.clone(),
                    // the amount fulfilled from the operation
                    amount: amount_fulfilled,
                    cost: paid_amount_used * price,
                    price: self
                        .assets_info
                        .usd_price_at(&sale.asset, time)
                        .await?,
                    paid_with: asset.to_string(),
                    paid_with_amount: paid_amount_used,
                    datetime: *time,
                    sale_result: match (sale_revenue, purchase_cost) {
                        (r, c) if r >= c => OperationResult::Profit(r - c),
                        (r, c) => OperationResult::Loss(c - r),
                    },
                });
                // if the whole amount from the purchase was used, remove it
                // from the queue as we can't use it anymore to fulfill more sales.
                if *purchased_amount == 0.0 {
                    // TODO: delete op from vec
                }
            } else {
                return Err(anyhow!("inconsistency found!!! there are not enough purchase operations to fulfill this sale operation: {:?}", sale));
            }
        }

        let sale_asset_price = self
            .assets_info
            .usd_price_at(&sale.asset, &sale.datetime)
            .await?;
        let revenue = sale.amount * sale_asset_price;
        Ok(match (revenue, cost) {
            (r, c) if r >= c => MatchResult {
                result: OperationResult::Profit(r - c),
                purchases: consumed_ops,
            },
            (r, c) => MatchResult {
                result: OperationResult::Loss(c - r),
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

    use crate::{
        exchange::Asset,
        operations::{storage::Storage, Operation::*}
    };

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
        async fn price_at(&self, _asset_pair: &AssetPair, _time: &DateTime<Utc>) -> Result<f64> {
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

        async fn usd_price_at(&self, asset: &Asset, time: &DateTime<Utc>) -> Result<f64> {
            self.price_at(&AssetPair::new(asset, "USD"), time).await
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
            let asset_prices = DummyAssetsInfo::new();
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
                        MatchResult { purchases, .. },
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
