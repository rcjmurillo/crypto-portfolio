use std::{
    fmt::{self},
    iter::Peekable,
    ops::Deref,
};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use crate::operations::{Amount, Operation};

#[derive(Debug)]
pub struct Disposal {
    pub amount: Amount,
    pub datetime: DateTime<Utc>,
}

#[derive(Debug)]
pub struct Acquisition {
    pub source: String,
    pub amount: f64,
    pub paid_with: Vec<Amount>,
    pub datetime: DateTime<Utc>,
}

#[derive(Debug, Clone)]
struct ConsumableOperation {
    operation: Operation,
    pub remaining_amount: f64,
}

impl ConsumableOperation {
    fn from_operation(operation: Operation) -> Self {
        let remaining_amount = operation.amount().value;
        Self {
            operation,
            remaining_amount,
        }
    }

    /// Computes the costs for consuming the provided amount from the operation. It'll consume the
    /// most possible amount from this operation to determine the cost basis, which will be a vector
    /// of costs given those can be in different assets.
    /// The first element of the tuple represents the amount consumed from this operation, it may be
    /// not possible to consume all the provided amount, so the caller knows the remaining amount to
    /// compute the cost from another operation.
    /// It also updates the remaining amount available for consumption for other operations.
    fn consume_amount(&mut self, amount: f64) -> Option<(f64, Vec<Amount>)> {
        // no amount to consume, just return
        if self.remaining_amount == 0.0 {
            return None;
        }
        // the amount to consume for the asset acquisition is min(amount, self.remaining_amount).
        // std::cmp::min can't be used for floats.
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
                // the cost for the acquisition of asset amount is the price of that asset per unit multiplied
                // by the number of units consumed from this acquisition operation.
                let mut all_costs = vec![Amount::new(
                    amount_consumed * price.value,
                    price.asset.clone(),
                )];
                // plus any additional costs associated with the operation.
                if let Some(op_costs) = costs {
                    for c in op_costs {
                        // compute the cost proportional to the consumed amount
                        let percentage_consumed = amount_consumed / amount.value;
                        all_costs.push(Amount::new(percentage_consumed * c.value, c.asset.clone()));
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

impl Deref for ConsumableOperation {
    type Target = Operation;
    fn deref(&self) -> &Self::Target {
        &self.operation
    }
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
pub enum ConsumeStrategy {
    Fifo,
    Lifo,
}

/// Computes the cost basis for a given disposal operation by search the related operations
/// and that compose the whole disposed amount and its cost.
///
/// A resolution strategy will be provided, consider the following
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
pub struct CostBasisResolver {
    ops: Peekable<<Vec<ConsumableOperation> as IntoIterator>::IntoIter>,
}

impl CostBasisResolver {
    pub fn from_ops(ops: Vec<Operation>, strategy: ConsumeStrategy) -> Self {
        let mut ops = ops
            .into_iter()
            .filter(|op| matches!(op, Operation::Acquire { .. }))
            .map(|op| ConsumableOperation::from_operation(op))
            .collect::<Vec<ConsumableOperation>>();
        ops.sort_by_key(|op| match strategy {
            ConsumeStrategy::Fifo => op.time().timestamp(),
            // the newest operations will appear first when iterating
            ConsumeStrategy::Lifo => -op.time().timestamp(),
        });
        Self {
            ops: ops.into_iter().peekable(),
        }
    }

    pub async fn resolve(&mut self, disposal: &Disposal) -> Result<Vec<Acquisition>> {
        log::debug!("resolving for disposal {:?}", disposal);

        // consume operations until we fulfill this amount if possible
        let mut amount_to_fulfill = disposal.amount.value;
        let mut consumed_ops = Vec::new();

        while amount_to_fulfill > 0.0 {
            if let Some(op) = self.ops.peek_mut() {
                if let Some((amount_fulfilled, costs)) = op.consume_amount(amount_to_fulfill) {
                    amount_to_fulfill -= amount_fulfilled;
                    consumed_ops.push(Acquisition {
                        source: op.source().to_string(),
                        // the amount fulfilled from the operation
                        amount: amount_fulfilled,
                        paid_with: costs,
                        datetime: *op.time(),
                    });
                    if op.remaining_amount == 0.0 {
                        // operation consumed, move to the next one
                        self.ops.next();
                    }
                } else {
                    // nothing more to consume, move to the next operation
                    self.ops.next();
                }
            } else {
                return Err(anyhow!("missing cost basis! there are not enough acquisition operations to fulfill disposal operation: {:?}", disposal));
            }
        }

        Ok(consumed_ops)
    }

    #[cfg(test)]
    fn ops(&self) -> Vec<ConsumableOperation> {
        self.ops.clone().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use chrono::TimeZone;
    use proptest::{collection::vec, option::of, prelude::*, sample::select};
    use quickcheck::Gen;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    use crate::operations::{storage::Storage, Operation::*};

    prop_compose! {
        fn amount(assets: Option<Vec<&'static str>>)(value in 1.0 .. 100.0f64, asset in select(match assets {
            Some(a) => a,
            None => vec!["BTC", "ETH", "AVAX", "ADA", "MATIC", "ALGO"]
        })) -> Amount {
            Amount { value, asset: asset.to_string() }
        }
    }

    prop_compose! {
        fn datetime()(year in 2000..2020i32, month in 1..12u32, day in 1..28u32, hour in 0..23u32, minute in 0..59u32) -> DateTime<Utc> {
            Utc.with_ymd_and_hms(year, month, day, hour, minute, 0).unwrap()
        }
    }

    prop_compose! {
        fn operation(optypes: Vec<&'static str>, assets: Option<Vec<&'static str>>, custom_dt: Option<DateTime<Utc>>)(
            source_id in "[a-zA-Z0-9]{5}",
            source in "test",
            amount in amount(assets),
            price in amount(None),
            costs in of(vec(amount(None), 3)),
            time in datetime().prop_map(move |v| match custom_dt {
                Some(dt) => dt,
                None => v
            }),
            optype in select(optypes)
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

    prop_compose! {
        fn dispose_acquire_op_sequence()(
            dispose_op in operation(vec!["dispose"], None, None),
            acquire_ops in vec(operation(vec!["acquire"], None, None), 20)
        ) -> Vec<Operation> {
            let mut ops = vec![dispose_op];
            ops.extend(acquire_ops);
            ops
        }
    }

    fn op_sequence_in_order(
        op_types: Vec<&'static str>,
        datetimes: Vec<DateTime<Utc>>,
        assets: Vec<&'static str>,
    ) -> impl Strategy<Value = Vec<Operation>> {
        let mut ops = Vec::new();
        for (t, dt) in op_types.iter().zip(datetimes) {
            ops.push(operation(vec![t], Some(assets.clone()), Some(dt)))
        }
        ops
    }

    prop_compose! {
        fn op_sequence(n: usize)(
            op_seqs in vec(dispose_acquire_op_sequence(), 1..=n)
        ) -> Vec<Operation> {
            let mut all_ops = Vec::new();
            for ops in op_seqs {
                all_ops.extend(ops);
            }
            all_ops
        }
    }

    proptest! {
        #[test]
        fn test_consumable_operation_consume_all_amount(
            operation in operation(vec!["acquire", "dispose", "send", "receive"], None, None)
        ) {
            prop_assume!(matches!(operation, Operation::Acquire{ .. }));
            let mut op = ConsumableOperation::from_operation(operation.clone());
            let r = op.consume_amount(op.amount().value);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value);
            prop_assert_eq!(op.remaining_amount, 0.0);

            // test it's only possible to consume no more than the available amount
            let mut op = ConsumableOperation::from_operation(operation);
            let r = op.consume_amount(op.amount().value + 1.0);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value);
            prop_assert_eq!(op.remaining_amount, 0.0);
        }
    }

    proptest! {
        #[test]
        fn test_consumable_operation_consume_partial_amount(
            operation in operation(vec!["acquire", "dispose", "send", "receive"], None, None)
        ) {
            prop_assume!(matches!(operation, Operation::Acquire{ .. }));
            let mut op = ConsumableOperation::from_operation(operation);
            let to_consume = op.amount().value * 0.77;
            let r = op.consume_amount(to_consume);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value * 0.77);
            prop_assert_eq!(op.remaining_amount, op.amount().value - to_consume);
        }
    }

    proptest! {
        #[test]
        fn test_cost_basis_resolver_acquire_only(
            ops in vec(operation(vec!["acquire", "dispose", "send", "receive"], None, None), 1..100)
        ) {
            let stream = CostBasisResolver::from_ops(ops, ConsumeStrategy::Fifo);
            assert!(stream.ops().iter().all(|op| matches!(op.operation, Operation::Acquire{..})));
        }
    }

    proptest! {
        #[test]
        fn test_can_consume_from_fifo_operations_stream(ops in op_sequence(50)) {
            let sales = into_disposals(&ops);
            let mut stream = CostBasisResolver::from_ops(ops, ConsumeStrategy::Fifo);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("could not build tokio runtime");
            for s in sales {
                rt.block_on(stream.resolve(&s)).unwrap();
            }
        }
    }

    proptest! {
        #[test]
        fn test_can_consume_from_lifo_operations_stream(ops in op_sequence(50)) {
            let sales = into_disposals(&ops);
            let mut stream = CostBasisResolver::from_ops(ops, ConsumeStrategy::Lifo);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("could not build tokio runtime");
            for s in sales {
                rt.block_on(stream.resolve(&s)).unwrap();
            }
        }
    }

    proptest! {
        #[test]
        fn test_consume_at_least_one_or_more_acquire_ops(ops in op_sequence(20)) {
            let sales = into_disposals(&ops);
            let mut stream = CostBasisResolver::from_ops(ops, ConsumeStrategy::Fifo);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("could not build tokio runtime");
            for s in sales {
                match rt.block_on(stream.resolve(&s)) {
                    Ok(ops) => {
                        prop_assert!(ops.len() > 0);
                    }
                    Err(err) => prop_assert!(false, "{}", err)
                }
            }
        }
    }

    proptest! {
        #[test]
        fn test_cost_basis_resolver_fifo(mut ops in op_sequence_in_order(
            vec!["acquire", "acquire", "dispose"],
            vec![
                Utc.with_ymd_and_hms(2000, 2, 11, 1, 4, 31).unwrap(),
                Utc.with_ymd_and_hms(2000, 2, 11, 3, 45, 7).unwrap(),
                Utc.with_ymd_and_hms(2000, 2, 11, 8, 0, 9).unwrap(),
            ],
            vec!["BTC"],
        )) {
            // op0 must be Acquire
            // op1 must be Acquire
            // op2 must be Dispose
            // all three ops must be for the same asset
            println!("{} {} {}", ops[0].amount().asset, ops[1].amount().asset, ops[2].amount().asset);
            let all_same_asset = match (&ops[0], &ops[1], &ops[2]) {
                (
                    Acquire { amount: a1, .. },
                    Acquire { amount: a2, .. },
                    Dispose { amount: a3, .. },
                ) => a1.asset == a2.asset && a2.asset == a3.asset,
                _ => false,
            };
            prop_assume!(all_same_asset);
            prop_assume!(ops[0].amount().value + ops[1].amount().value >= ops[2].amount().value);

            ops.sort_by(|a, b| {
                if a.time() > b.time() {
                    std::cmp::Ordering::Greater
                } else if a.time() == b.time() {
                    std::cmp::Ordering::Equal
                } else {
                    std::cmp::Ordering::Less
                }
            });

            // prop_assume!(ops[0].time() < ops[1].time());

            // check that the first cost op's amount has been partially or fully consumed
            if let Dispose {
                amount,
                time,
                ..
            } = &ops[2]
            {
                let orig_amounts = [ops[0].amount().clone(), ops[1].amount().clone()];
                let sale = Disposal {
                    amount: amount.clone(),
                    datetime: *time,
                };
                let mut stream = CostBasisResolver::from_ops(ops, ConsumeStrategy::Fifo);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("could not build tokio runtime");

                match rt.block_on(stream.resolve(&sale)) {
                    Ok(result) => {
                        let ops = stream.ops();
                        // if the sale consumed the whole amount, only one or none ops must remain
                        if sale.amount.value >= orig_amounts[0].value {
                            prop_assert!(ops.len() == 1, "expected only 1 operation remaining, got {:?}", ops);
                            prop_assert_eq!(ops[0].remaining_amount, orig_amounts[1].value - (sale.amount.value - orig_amounts[0].value));
                        } else {
                            // both ops must remain in the queue
                            prop_assert!(ops.len() == 2);
                            // it should have partially consumed the first amount
                            prop_assert!(ops[0].remaining_amount < orig_amounts[0].value);
                            // it shouldn't have consumed the 2nd amount
                            prop_assert_eq!(ops[1].remaining_amount, orig_amounts[1].value);
                        }

                    }
                    Err(e) => prop_assert!(false, "{}", e),
                }
            }
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

    fn into_disposals(ops: &Vec<Operation>) -> Vec<Disposal> {
        ops.iter()
            .filter_map(|op| match op {
                Operation::Dispose { amount, time, .. } => Some(Disposal {
                    amount: amount.clone(),
                    datetime: *time,
                }),
                _ => None,
            })
            .collect()
    }
}
