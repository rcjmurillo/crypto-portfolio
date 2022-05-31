use crate::{
    operations::Operation,
    {Deposit, Loan, Repay, Status, Trade, TradeSide, Withdraw},
};

// pub struct Vec<Operation>(Vec<Operation>);

// impl Vec<Operation> {
//     pub fn new() -> Self {
//         Self(vec![])
//     }
// }

// impl From<Vec<Operation>> for Vec<Operation> {
//     fn from(ops: Vec<Operation>) -> Self {
//         ops.0
//     }
// }

// impl Deref for Vec<Operation> {
//     type Target = Vec<Operation>;

//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }

impl From<Trade> for Vec<Operation> {
    fn from(trade: Trade) -> Self {
        let mut ops = match trade.side {
            TradeSide::Buy => vec![
                Operation::BalanceIncrease {
                    id: 1,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    asset: trade.base_asset.clone(),
                    amount: trade.amount,
                },
                Operation::Cost {
                    id: 2,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    for_asset: trade.base_asset.clone(),
                    for_amount: trade.amount,
                    asset: trade.quote_asset.clone(),
                    amount: trade.amount * trade.price,
                    time: trade.time,
                },
                Operation::BalanceDecrease {
                    id: 3,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    asset: trade.quote_asset.clone(),
                    amount: trade.amount * trade.price,
                },
                Operation::Revenue {
                    id: 4,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    asset: trade.quote_asset.clone(),
                    amount: trade.amount * trade.price,
                    time: trade.time,
                },
            ],
            TradeSide::Sell => vec![
                Operation::BalanceDecrease {
                    id: 1,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    asset: trade.base_asset.clone(),
                    amount: trade.amount,
                },
                Operation::Revenue {
                    id: 2,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    asset: trade.base_asset.clone(),
                    amount: trade.amount,
                    time: trade.time,
                },
                Operation::BalanceIncrease {
                    id: 3,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    asset: trade.quote_asset.clone(),
                    amount: trade.amount * trade.price,
                },
                Operation::Cost {
                    id: 4,
                    source_id: trade.source_id.clone(),
                    source: trade.source.clone(),
                    for_asset: trade.quote_asset.clone(),
                    for_amount: trade.amount * trade.price,
                    asset: trade.base_asset.clone(),
                    amount: trade.amount,
                    time: trade.time,
                },
            ],
        };
        if trade.fee_asset != "" && trade.fee > 0.0 {
            ops.push(Operation::BalanceDecrease {
                id: 5,
                source_id: trade.source_id.clone(),
                source: trade.source.clone(),
                asset: trade.fee_asset.clone(),
                amount: trade.fee,
            });
            ops.push(Operation::Cost {
                id: 6,
                source_id: trade.source_id,
                source: trade.source,
                for_asset: match trade.side {
                    TradeSide::Buy => trade.base_asset,
                    TradeSide::Sell => trade.quote_asset,
                },
                for_amount: 0.0,
                asset: trade.fee_asset,
                amount: trade.fee,
                time: trade.time,
            });
        }

        ops
    }
}

impl From<Deposit> for Vec<Operation> {
    fn from(deposit: Deposit) -> Self {
        let mut ops = vec![Operation::BalanceIncrease {
            id: 1,
            source_id: deposit.source_id.clone(),
            source: deposit.source.clone(),
            asset: deposit.asset.clone(),
            amount: deposit.amount,
        }];
        if let Some(fee) = deposit.fee.filter(|f| f > &0.0) {
            ops.extend(vec![
                Operation::BalanceDecrease {
                    id: 2,
                    source_id: deposit.source_id.clone(),
                    source: deposit.source.clone(),
                    asset: deposit.asset.clone(),
                    amount: fee,
                },
                Operation::Cost {
                    id: 3,
                    source_id: deposit.source_id,
                    source: deposit.source,
                    for_asset: deposit.asset.clone(),
                    for_amount: 0.0,
                    asset: deposit.asset,
                    amount: fee,
                    time: deposit.time,
                },
            ]);
        }
        ops
    }
}

impl From<Withdraw> for Vec<Operation> {
    fn from(withdraw: Withdraw) -> Self {
        let mut ops = vec![Operation::BalanceDecrease {
            id: 1,
            source_id: withdraw.source_id.clone(),
            source: withdraw.source.clone(),
            asset: withdraw.asset.clone(),
            amount: withdraw.amount,
        }];
        if withdraw.fee > 0.0 {
            ops.extend(vec![
                Operation::BalanceDecrease {
                    id: 2,
                    source_id: format!("{}-fee", &withdraw.source_id),
                    source: withdraw.source.clone(),
                    asset: withdraw.asset.clone(),
                    amount: withdraw.fee,
                },
                Operation::Cost {
                    id: 3,
                    source_id: withdraw.source_id,
                    source: withdraw.source,
                    for_asset: withdraw.asset.clone(),
                    for_amount: 0.0,
                    asset: withdraw.asset,
                    amount: withdraw.fee,
                    time: withdraw.time,
                },
            ]);
        }
        ops
    }
}

impl From<Loan> for Vec<Operation> {
    fn from(loan: Loan) -> Self {
        match loan.status {
            Status::Success => vec![Operation::BalanceIncrease {
                id: 1,
                source_id: loan.source_id,
                source: loan.source,
                asset: loan.asset,
                amount: loan.amount,
            }],
            Status::Failure => vec![],
        }
    }
}

impl From<Repay> for Vec<Operation> {
    fn from(repay: Repay) -> Self {
        match repay.status {
            Status::Success => vec![
                Operation::BalanceDecrease {
                    id: 1,
                    source_id: repay.source_id.clone(),
                    source: repay.source.clone(),
                    asset: repay.asset.clone(),
                    amount: repay.amount + repay.interest,
                },
                Operation::Cost {
                    id: 2,
                    source_id: repay.source_id,
                    source: repay.source,
                    for_asset: repay.asset.clone(),
                    for_amount: 0.0,
                    asset: repay.asset,
                    amount: repay.interest,
                    time: repay.time,
                },
            ],
            Status::Failure => vec![],
        }
    }
}
