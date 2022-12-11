use std::{
    fmt::{self, Display},
    iter::Peekable, ops::Deref,
};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::operations::{Amount, Operation};
use market::{Market, MarketData};

#[derive(Debug)]
pub struct Sale {
    pub amount: Amount,
    pub datetime: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Purchase {
    pub source: String,
    pub amount: f64,
    pub cost: f64,
    pub price: f64,
    pub paid_with: Vec<Amount>,
    pub datetime: DateTime<Utc>,
    pub sale_result: OperationResult,
}

#[derive(Debug, Clone)]
struct OperationCostBasis {
    operation: Operation,
    remaining_amount: f64,
}

impl OperationCostBasis {
    fn from_operation(operation: Operation) -> Self {
        let remaining_amount = operation.amount().value;
        Self {
            operation,
            remaining_amount,
        }
    }

    /// Computes the cost basis vector for the provided amount. It'll consume the most possible amount
    /// from this operation to determine the cost basis, which will be a vector of costs given those
    /// can be in different assets.
    /// The first element of the tuple represents the amount consumed from this operation, it may be
    /// not possible to consume all the provided amount, so the caller knows the remaining amount to
    /// compute the cost from another operation.
    /// It also updates the remaining amount available for consumption for other operations.
    fn cost_basis_for(&mut self, amount: f64) -> Option<(f64, Vec<Amount>)> {
        // no amount to consume, just return
        if self.remaining_amount == 0.0 {
            return None;
        }
        // the amount to consume for the asset acquisition is min(amount, self.remaining_amount).
        // std::cmp::min can be used for floats.
        let amount_consumed = if amount <= self.remaining_amount {
            amount
        } else {
            self.remaining_amount
        };
        match &self.operation {
            Operation::Acquire {
                amount,
                price,
                costs,
                ..
            } => {
                // the cost basis for the acquisition of asset amount is the price of that asset per unit multiplied
                // by the number of units consumed from this acquisition operation.
                let mut all_costs = vec![Amount::new(
                    amount_consumed * price.value,
                    price.asset.clone(),
                )];
                // plus any additional costs associated with the operation.
                if let Some(op_costs) = costs {
                    for c in op_costs {
                        // use the cost per unit so costs for partial amounts of the transaction can be computed
                        // correctly.
                        let cost_per_unit = c.value / amount.value;
                        all_costs.push(Amount::new(
                            amount_consumed * cost_per_unit,
                            c.asset.clone(),
                        ));
                    }
                }
                self.remaining_amount -= amount_consumed;
                Some((amount_consumed, all_costs))
            }
            Operation::Receive { .. } => {
                unreachable!("found a Receive operation when none is expected")
            }
            Operation::Send { .. } => unreachable!("found a Send operation when none is expected"),
            Operation::Dispose { .. } => {
                unreachable!("found a Dispose operation when none is expected")
            }
        }
    }
}

impl Deref for OperationCostBasis {
    type Target = Operation;
    fn deref(&self) -> &Self::Target {
        &self.operation
    }
}

#[derive(Debug)]
pub enum OperationResult {
    Profit(f64),
    Loss(f64),
}

impl Display for OperationResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperationResult::Loss(n) => write!(f, "-{n}"),
            OperationResult::Profit(n) => write!(f, "+{n}"),
        }
    }
}

#[derive(Debug)]
pub struct MatchResult {
    pub result: OperationResult,
    pub purchases: Vec<Purchase>,
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
pub struct OperationsStream<T> {
    ops: Peekable<<Vec<OperationCostBasis> as IntoIterator>::IntoIter>,
    market_data: T,
}

impl<T> OperationsStream<T>
where
    T: MarketData,
{
    pub fn from_ops(ops: Vec<Operation>, strategy: ConsumeStrategy, market_data: T) -> Self {
        let mut ops = ops
            .into_iter()
            .filter(|op| matches!(op, Operation::Acquire { .. }))
            .map(|op| OperationCostBasis::from_operation(op))
            .collect::<Vec<OperationCostBasis>>();
        ops.sort_by_key(|op| match strategy {
            ConsumeStrategy::Fifo => op.time().timestamp(),
            // the newest operations will appear first when iterating
            ConsumeStrategy::Lifo => -op.time().timestamp(),
        });
        Self {
            ops: ops.into_iter().peekable(),
            market_data,
        }
    }

    pub async fn consume(&mut self, sale: &Sale) -> Result<MatchResult> {
        log::debug!("consuming sale {:?}", sale);

        // consume operations until we fulfill this amount
        let mut amount_to_fulfill = sale.amount.value;
        let mut consumed_ops = Vec::new();
        // the total cost of purchasing the amount from the sale
        let mut total_cost = 0.0;

        while amount_to_fulfill > 0.0 {
            if let Some(op) = self.ops.peek_mut() {
                let cost_basis = op.cost_basis_for(amount_to_fulfill);
                if let Some((amount_fulfilled, costs)) = cost_basis {
                    amount_to_fulfill -= amount_fulfilled;

                    let mut paid_with = vec![];
                    let mut op_cost = total_cost;
                    for c in costs {
                        // compute the cost in USD
                        let usd_price = market::usd_price(
                            &self.market_data,
                            &c.asset,
                            op.time(),
                        )
                        .await?;
                        op_cost += c.value * usd_price;
                        paid_with.push(c);
                    }
                    total_cost += op_cost;

                    let usd_price_at_sale =
                        market::usd_price(&self.market_data, &sale.amount.asset, &sale.datetime).await?;

                    let sale_revenue = amount_fulfilled * usd_price_at_sale;
                    consumed_ops.push(Purchase {
                        source: op.source().to_string(),
                        // the amount fulfilled from the operation
                        amount: amount_fulfilled,
                        cost: op_cost,
                        price: market::usd_price(
                            &self.market_data,
                            &sale.amount.asset,
                            op.time(),
                        )
                        .await?,
                        paid_with,
                        datetime: *op.time(),
                        sale_result: match (sale_revenue, op_cost) {
                            (r, c) if r >= c => OperationResult::Profit(r - c),
                            (r, c) => OperationResult::Loss(c - r),
                        },
                    });
                } else {
                    // nothing more to consume, move to the next operation
                    self.ops.next();
                }
            } else {
                return Err(anyhow!("inconsistency found!!! there are not enough purchase operations to fulfill this dispose operation: {:?}", sale));
            }
        }

        let dispose_usd_price =
            market::usd_price(&self.market_data, &sale.amount.asset, &sale.datetime).await?;
        let revenue = sale.amount.value * dispose_usd_price;
        Ok(match (revenue, total_cost) {
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
    fn ops(&self) -> Vec<OperationCostBasis> {
        self.ops.clone().collect()
    }
}

#[async_trait]
impl<T> SaleMatcher for OperationsStream<T>
where
    T: MarketData + Send + Sync,
{
    async fn match_sale(&mut self, sale: &Sale) -> Result<MatchResult> {
        // consume purchase amounts from the stream to fulfill the sale amount
        self.consume(sale).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::TimeZone;
    use proptest::{collection::vec, option::of, prelude::*, sample::select};
    use quickcheck::{quickcheck, Gen, TestResult};
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::operations::{storage::Storage, Operation::*};
    use market::Asset;

    prop_compose! {
        fn amount()(value in 0.01 .. 1000f64, asset in select(vec!["BTC", "ETH", "AVAX", "ADA", "MATIC", "ALGO"])) -> Amount {
            Amount { value, asset: asset.to_string() }
        }
    }

    prop_compose! {
        fn datetime()(year in 2000..2020i32, month in 1..12u32, day in 1..28u32, hour in 0..23u32, minute in 0..59u32) -> DateTime<Utc> {
            Utc.with_ymd_and_hms(year, month, day, hour, minute, 0).unwrap()
        }
    }

    prop_compose! {
        fn operation()(
            source_id in "[a-zA-Z0-9]{5}",
            source in "test",
            amount in amount(),
            price in amount(),
            costs in of(vec(amount(), 3)),
            time in datetime(),
            optype in select(vec!["acquire", "dispose", "send", "receive"])
        ) -> Operation {
            match optype {
                "acquire" => Operation::Acquire {
                    source_id,
                    source,
                    amount,
                    price,
                    costs,
                    time
                },
                "dispose" => Operation::Dispose {
                    source_id,
                    source,
                    amount,
                    price,
                    costs,
                    time
                },
                "send" => Operation::Send {
                    source_id,
                    source,
                    amount,
                    sender: Some("test-sender".to_string()),
                    recipient: Some("test-recipient".to_string()),
                    costs,
                    time
                },
                "receive" => Operation::Receive {
                    source_id,
                    source,
                    amount,
                    sender: Some("test-sender".to_string()),
                    recipient: Some("test-recipient".to_string()),
                    costs,
                    time
                },
                _ => unreachable!()
            }
        }
    }

    proptest! {
        #[test]
        fn test_operation_cost_basis_consume_all_amount(operation in operation()) {
            prop_assume!(matches!(operation, Operation::Acquire{ .. }));
            let mut op = OperationCostBasis::from_operation(operation.clone());
            let r = op.cost_basis_for(op.amount().value);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value);

            // test it's only possible to consume no more than the available amount
            let mut op = OperationCostBasis::from_operation(operation);
            let r = op.cost_basis_for(op.amount().value + 0.1);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value);
        }
    }

    proptest! {
        #[test]
        fn test_operation_cost_basis_consume_partial_amount(operation in operation()) {
            prop_assume!(matches!(operation, Operation::Acquire{ .. }));
            let mut op = OperationCostBasis::from_operation(operation);
            let r = op.cost_basis_for(op.amount().value * 0.77);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value * 0.77);
        }
    }

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

    struct DummyMarketData {
        last_price: Arc<Mutex<f64>>,
    }

    impl DummyMarketData {
        pub fn new() -> Self {
            Self {
                last_price: Arc::new(Mutex::new(0.0)),
            }
        }
    }

    #[async_trait]
    impl MarketData for DummyMarketData {
        async fn has_market(&self, _: &Market) -> Result<bool> {
            Ok(true)
        }
        fn normalize(&self, market: &Market) -> Result<Market> {
            Ok(market.clone())
        }
        async fn markets(&self) -> Result<Vec<Market>> {
            Ok(vec![])
        }
        async fn price_at(&self, _market: &Market, _time: &DateTime<Utc>) -> Result<f64> {
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

    // fn valid_sequence(ops: &Vec<Operation>) -> bool {
    //     // check that for every revenue operation there are enough prior purchase operations
    //     // to cover the amount.
    //     let mut filtered_ops: Vec<&Operation> = ops
    //         .iter()
    //         .filter(|op| matches!(op, Cost { .. }) || matches!(op, Revenue { .. }))
    //         .collect();
    //     // sort by time, cost and revenue operations are guaranteed to have a time
    //     filtered_ops.sort_by_key(|op| op.time().unwrap());

    //     // make sure we have at least one revenue op
    //     if !filtered_ops.iter().any(|op| matches!(op, Revenue { .. })) {
    //         return false;
    //     }
    //     for (i, op) in filtered_ops.iter().enumerate() {
    //         if let Operation::Revenue {
    //             amount,
    //             asset: rev_asset,
    //             ..
    //         } = op
    //         {
    //             let mut prior_amount = 0.0;
    //             // go through all the ops that are before this one and verify if
    //             // they can cover amount sold.
    //             for j in 0..i {
    //                 match filtered_ops[j] {
    //                     // cost ops increase the availble amount
    //                     Cost {
    //                         for_amount,
    //                         ref for_asset,
    //                         ..
    //                     } if for_asset == rev_asset => {
    //                         prior_amount += for_amount;
    //                     }
    //                     // previous revenue ops reduce the total available amount
    //                     Revenue {
    //                         amount, ref asset, ..
    //                     } => {
    //                         if asset == rev_asset {
    //                             prior_amount -= amount;
    //                         }
    //                     }
    //                     _ => (),
    //                 }
    //             }
    //             if prior_amount < *amount {
    //                 return false;
    //             }
    //         }
    //     }
    //     true
    // }

    // fn into_sales(ops: &Vec<Operation>) -> Vec<Sale> {
    //     ops.iter()
    //         .filter_map(|op| match op {
    //             Operation::Revenue {
    //                 asset,
    //                 amount,
    //                 time,
    //                 ..
    //             } => Some(Sale {
    //                 asset: asset.clone(),
    //                 amount: *amount,
    //                 datetime: *time,
    //             }),
    //             _ => None,
    //         })
    //         .collect()
    // }

    // quickcheck! {
    //     fn ops_stream_cost_only(ops: Vec<Operation>) -> bool {
    //         let market_data = DummyMarketData::new();
    //         let stream = OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, market_data);
    //         stream.ops().iter().all(|op| matches!(op, Operation::Cost{..}))
    //     }
    // }

    // #[test]
    // fn can_consume_from_fifo_operations_stream() {
    //     fn prop(ops: Vec<Operation>) -> TestResult {
    //         if !valid_sequence(&ops) {
    //             return TestResult::discard();
    //         }
    //         let sales = into_sales(&ops);
    //         let market_data = DummyMarketData::new();
    //         let mut stream = OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, market_data);
    //         let rt = tokio::runtime::Builder::new_current_thread()
    //             .enable_all()
    //             .build()
    //             .expect("could not build tokio runtime");
    //         for s in sales {
    //             if !rt.block_on(stream.consume(&s)).is_ok() {
    //                 return TestResult::failed();
    //             }
    //         }
    //         TestResult::passed()
    //     }
    //     quickcheck(prop as fn(ops: Vec<Operation>) -> TestResult);
    // }

    // #[test]
    // fn consume_at_least_one_or_more_cost_ops() {
    //     fn prop(ops: Vec<Operation>) -> TestResult {
    //         if ops.len() > 10 || !valid_sequence(&ops) {
    //             return TestResult::discard();
    //         }
    //         let sales = into_sales(&ops);
    //         let market_data = DummyMarketData::new();
    //         let mut stream = OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, market_data);
    //         let rt = tokio::runtime::Builder::new_current_thread()
    //             .enable_all()
    //             .build()
    //             .expect("could not build tokio runtime");
    //         for s in sales {
    //             match rt.block_on(stream.consume(&s)) {
    //                 Ok(MatchResult { purchases, .. }) => {
    //                     if purchases.len() == 0 {
    //                         return TestResult::failed();
    //                     }
    //                 }
    //                 Err(_) => return TestResult::failed(),
    //             }
    //         }
    //         TestResult::passed()
    //     }
    //     quickcheck(prop as fn(ops: Vec<Operation>) -> TestResult);
    // }

    // #[test]
    // fn consume_ops_stream_fifo() {
    //     fn prop(ops: Vec<Operation>) -> TestResult {
    //         if ops.len() != 3 || !valid_sequence(&ops) {
    //             return TestResult::discard();
    //         }
    //         // op0 must be Cost
    //         // op1 must be Cost
    //         // op2 must be Revenue
    //         // all three ops must be for the same asset
    //         let valid = match (&ops[0], &ops[1], &ops[2]) {
    //             (
    //                 Cost { for_asset: a1, .. },
    //                 Cost { for_asset: a2, .. },
    //                 Revenue { asset: a3, .. },
    //             ) => a1 == a2 && a2 == a3,
    //             _ => false,
    //         };
    //         if !valid {
    //             return TestResult::discard();
    //         }

    //         // check that the first cost op's amount has been partially or fully consumed
    //         if let Revenue {
    //             amount,
    //             asset,
    //             time,
    //             ..
    //         } = &ops[3]
    //         {
    //             let orig_amounts = [ops[0].amount(), ops[1].amount()];
    //             let sale = Sale {
    //                 amount: *amount,
    //                 asset: asset.clone(),
    //                 datetime: *time,
    //             };
    //             let market_data = DummyMarketData::new();
    //             let mut stream =
    //                 OperationsStream::from_ops(ops, ConsumeStrategy::Fifo, market_data);
    //             let rt = tokio::runtime::Builder::new_current_thread()
    //                 .enable_all()
    //                 .build()
    //                 .expect("could not build tokio runtime");

    //             match rt.block_on(stream.consume(&sale)) {
    //                 Ok(_) => {
    //                     let ops = stream.ops();
    //                     // if the sale consumed the whole amount, only one or none ops must remain
    //                     if sale.amount >= orig_amounts[0] && ops.len() == 2 {
    //                         return TestResult::failed();
    //                     } else {
    //                         // both ops must remain in the queue
    //                         if ops.len() < 2
    //                             || ops[0].amount() >= orig_amounts[0]
    //                             || ops[1].amount() >= orig_amounts[1]
    //                         {
    //                             return TestResult::failed();
    //                         }
    //                     }
    //                 }
    //                 Err(_) => return TestResult::failed(),
    //             }
    //         }
    //         TestResult::passed()
    //     }

    //     quickcheck(prop as fn(ops: Vec<Operation>) -> TestResult);
    // }
}
