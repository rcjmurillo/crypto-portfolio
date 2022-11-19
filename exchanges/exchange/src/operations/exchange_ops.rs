use crate::{
    operations::{Amount, Operation},
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
        assert!(
            trade.base_asset != "",
            "missing base asset on trade {trade:?}"
        );
        assert!(
            trade.quote_asset != "",
            "missing quote asset on trade {trade:?}"
        );

        // the only cost for trades are the fees if any
        let has_fees = trade.fee_asset != "" && trade.fee > 0.0;
        let costs = if has_fees {
            Some(vec![Amount::new(trade.fee, trade.fee_asset)])
        } else {
            None
        };

        let mut ops = match trade.side {
            TradeSide::Buy => {
                vec![
                    Operation::Acquire {
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.base_amount(), trade.base_asset.clone()),
                        price: Amount::new(trade.quote_amount(), trade.quote_asset.clone()),
                        costs: costs,
                        time: trade.time,
                    },
                    Operation::Dispose {
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.quote_amount(), trade.quote_asset.clone()),
                        price: Amount::new(trade.base_amount(), trade.base_asset),
                        costs: None,
                        time: trade.time,
                    },
                ]
            }
            TradeSide::Sell => {
                vec![
                    Operation::Dispose {
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.base_amount(), trade.base_asset.clone()),
                        price: Amount::new(trade.quote_amount(), trade.quote_asset.clone()),
                        costs: costs,
                        time: trade.time,
                    },
                    Operation::Acquire {
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.quote_amount(), trade.quote_asset.clone()),
                        price: Amount::new(trade.base_amount(), trade.base_asset.clone()),
                        costs: None,
                        time: trade.time,
                    },
                ]
            }
        };
        if has_fees {
            ops.push(Operation::Dispose {
                source_id: trade.source_id.clone(),
                source: trade.source.clone(),
                amount: Amount::new(trade.fee, trade.fee_asset.clone()),
                price: Amount::new(trade.fee, trade.fee_asset.clone()),
                costs: None,
                time: trade.time,
            });
        }
        ops
    }
}

impl From<Deposit> for Vec<Operation> {
    fn from(deposit: Deposit) -> Self {
        let mut ops = vec![Operation::Receive {
            source_id: deposit.source_id.clone(),
            source: deposit.source.clone(),
            amount: Amount::new(deposit.amount, deposit.asset.clone()),
            sender: None,
            recipient: None,
            costs: deposit
                .fee
                .filter(|f| *f > 0.0)
                .map(|f| vec![Amount::new(f, deposit.asset.clone())]),
            time: deposit.time,
        }];
        if let Some(fee) = deposit.fee.filter(|f| f > &0.0) {
            ops.extend(vec![Operation::Dispose {
                source_id: deposit.source_id.clone(),
                source: deposit.source.clone(),
                amount: Amount::new(fee, deposit.asset.clone()),
                price: Amount::new(fee, deposit.asset.clone()),
                costs: None,
                time: deposit.time,
            }]);
        }
        ops
    }
}

impl From<Withdraw> for Vec<Operation> {
    fn from(withdraw: Withdraw) -> Self {
        let mut ops = vec![Operation::Send {
            source_id: withdraw.source_id.clone(),
            source: withdraw.source.clone(),
            amount: Amount::new(withdraw.amount, withdraw.asset.clone()),
            sender: None,
            recipient: None,
            costs: if withdraw.fee > 0.0 {
                Some(vec![Amount::new(withdraw.fee, withdraw.asset.clone())])
            } else {
                None
            },
            time: withdraw.time,
        }];
        if withdraw.fee > 0.0 {
            ops.extend(vec![Operation::Dispose {
                source_id: withdraw.source_id.clone(),
                source: withdraw.source.clone(),
                amount: Amount::new(withdraw.fee, withdraw.asset.clone()),
                price: Amount::new(withdraw.fee, withdraw.asset.clone()),
                costs: None,
                time: withdraw.time,
            }]);
        }
        ops
    }
}

impl From<Loan> for Vec<Operation> {
    fn from(loan: Loan) -> Self {
        match loan.status {
            Status::Success => vec![Operation::Acquire {
                source_id: loan.source_id,
                source: loan.source,
                amount: Amount::new(loan.amount, loan.asset),
                price: Amount::new(0.0, loan.asset),
                costs: None,
                time: loan.time,
            }],
            Status::Failure => vec![],
        }
    }
}

impl From<Repay> for Vec<Operation> {
    fn from(repay: Repay) -> Self {
        match repay.status {
            Status::Success => vec![Operation::Acquire {
                source_id: repay.source_id,
                source: repay.source,
                amount: Amount::new(repay.amount, repay.asset),
                price: Amount::new(0.0, repay.asset),
                costs: if repay.interest > 0.0 {
                    Some(vec![Amount::new(repay.interest, repay.asset)])
                } else {
                    None
                },
                time: repay.time,
            }],
            Status::Failure => vec![],
        }
    }
}
