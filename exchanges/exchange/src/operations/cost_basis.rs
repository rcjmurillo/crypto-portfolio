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
    use crate::operations::{storage::Storage, Operation::*};
    use async_trait::async_trait;
    use chrono::TimeZone;
    use proptest::{collection::vec, option::of, prelude::*, sample::select};

    fn assert_approx_eq(a: f64, b: f64, p: f64) {
        assert!((a - b).abs() < p);
    }

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

    // fn op_sequence_in_order(
    //     op_types: Vec<&'static str>,
    //     datetimes: Vec<DateTime<Utc>>,
    //     assets: Vec<&'static str>,
    // ) -> impl Strategy<Value = Vec<Operation>> {
    //     let mut ops = Vec::new();
    //     for (t, dt) in op_types.iter().zip(datetimes) {
    //         ops.push(operation(vec![t], Some(assets.clone()), Some(dt)))
    //     }
    //     ops
    // }

    fn op_sequence_in_order(
        op_types: Vec<&'static str>,
        assets: Vec<&'static str>,
    ) -> impl Strategy<Value = Vec<Operation>> {
        let mut ops = Vec::new();
        let mut dt = Utc::now();
        for t in op_types {
            ops.push(operation(vec![t], Some(assets.clone()), Some(dt)));
            dt = dt + chrono::Duration::hours(1);
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

    fn check_operation_costs(operation: &Operation, consumed_amount: f64, costs: &Vec<Amount>) {
        if let Operation::Acquire {
            amount,
            price,
            costs: op_costs,
            ..
        } = operation
        {
            // test every cost of the operation has been correctly computed
            // relative to the consumed amount.
            if let Some(op_costs) = op_costs {
                let percentage_consumed = consumed_amount / amount.value;
                let mut costs_iter = costs.iter();
                let first_cost = costs_iter.next().unwrap();
                assert_eq!(&first_cost.asset, &price.asset);
                assert_approx_eq(first_cost.value, price.value * consumed_amount, 0.0001);
                assert_eq!(costs_iter.len(), op_costs.len());
                for (consumed_cost, cost) in costs_iter.zip(op_costs) {
                    assert_eq!(cost.asset, consumed_cost.asset);
                    assert_approx_eq(
                        consumed_cost.value,
                        cost.value * percentage_consumed,
                        0.0001,
                    );
                }
            }
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
            let (consumed_amount, costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value);
            prop_assert_eq!(op.remaining_amount, 0.0);
            check_operation_costs(&operation, consumed_amount, &costs);

            // test it's only possible to consume no more than the available amount
            let mut op = ConsumableOperation::from_operation(operation.clone());
            let r = op.consume_amount(op.amount().value + 1.0);
            prop_assert!(r.is_some());
            let (consumed_amount, _costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, op.amount().value);
            prop_assert_eq!(op.remaining_amount, 0.0);
            check_operation_costs(&operation, consumed_amount, &costs);
        }
    }

    proptest! {
        #[test]
        fn test_consumable_operation_consume_partial_amount(
            operation in operation(vec!["acquire", "dispose", "send", "receive"], None, None)
        ) {
            prop_assume!(matches!(operation, Operation::Acquire{ .. }));
            let mut op = ConsumableOperation::from_operation(operation.clone());
            let to_consume = op.amount().value * 0.77;
            let r = op.consume_amount(to_consume);
            prop_assert!(r.is_some());
            let (consumed_amount, costs) = r.unwrap();
            prop_assert_eq!(consumed_amount, to_consume);
            prop_assert_eq!(op.remaining_amount, op.amount().value - to_consume);
            check_operation_costs(&operation, consumed_amount, &costs);
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
        fn test_cost_basis_resolver_fifo(ops in op_sequence_in_order(
            vec!["acquire", "acquire", "acquire", "dispose"],
            vec!["BTC"],
        )) {
            // all three ops must be for the same asset
            prop_assume!(ops.iter().all(|op| op.amount().asset == "BTC"));
            // check that the sum of the first n ops is greater or equal than the last op
            let sum = ops.iter().fold(0.0, |acc, op| acc + op.amount().value);
            let last_value = ops.last().unwrap().amount().value;
            prop_assume!(sum - last_value >= last_value);

            // check that the first cost op's amount has been partially or fully consumed
            if let Some(Dispose {amount, time, ..}) = &ops.last() {
                // create a vector with all the original amounts but the last one
                let orig_amounts = ops.iter().take(ops.len() - 1).map(|op| op.amount().clone()).collect::<Vec<_>>();
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
                    Ok(acquisitions) => {
                        let ops = stream.ops();
                        // calculate the number of operations that should have been fully consumed by the sale.
                        // Each consumed operation accounts for part of the consumed amount.
                        let mut remaining_sale_amount = sale.amount.value;
                        let mut fully_consumed_ops = 0;
                        let mut partially_consumed_ops = 0;
                        for amount in orig_amounts.iter() {
                            if remaining_sale_amount > amount.value {
                                fully_consumed_ops += 1;
                                remaining_sale_amount -= amount.value;
                            } else {
                                partially_consumed_ops += 1;
                                break;
                            }
                        }

                        // check that the number of ops consumed
                        prop_assert_eq!(acquisitions.len(), fully_consumed_ops + partially_consumed_ops);

                        // check that the number of ops remaining
                        prop_assert_eq!(ops.len(), orig_amounts.len() - fully_consumed_ops);

                        if partially_consumed_ops > 0 {
                            // check the remaining sale amount was consumed from the first op remaining in the cost resolver
                            prop_assert_eq!(
                                ops[0].remaining_amount, 
                                orig_amounts[fully_consumed_ops].value - remaining_sale_amount
                            );
                        } else {
                            // check that the sale was fully consumed
                            prop_assert_eq!(ops.len(), 0);
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
