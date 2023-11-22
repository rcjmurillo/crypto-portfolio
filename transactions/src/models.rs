use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{Amount, OperationType, Operation};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum Status {
    Success,
    Failure,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TradeSide {
    Buy,
    Sell,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Trade {
    pub source_id: String,
    pub source: String,
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub amount: f64,
    pub price: f64,
    pub fee: f64,
    pub fee_asset: String,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub side: TradeSide,
}

impl Trade {
    pub fn base_amount(&self) -> f64 {
        self.amount
    }

    pub fn quote_amount(&self) -> f64 {
        self.amount * self.price
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Deposit {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub fee: Option<f64>,
    pub is_fiat: bool,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct Withdraw {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub fee: f64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Loan {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub status: Status,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Repay {
    pub source_id: String,
    pub source: String,
    pub asset: String,
    pub amount: f64,
    pub interest: f64,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub status: Status,
}

pub(crate) mod datetime_from_str {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{de, Deserialize, Deserializer};
    use std::convert::TryInto;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TimestampOrString {
            Timestamp(u64),
            String(String),
        }

        match TimestampOrString::deserialize(deserializer)? {
            // timestamps from the API are in milliseconds
            TimestampOrString::Timestamp(ts) => Utc
                .timestamp_millis_opt(ts.try_into().map_err(de::Error::custom)?)
                .single()
                .ok_or_else(|| de::Error::custom("invalid timestamp")),
            TimestampOrString::String(s) => Utc
                .datetime_from_str(&s, "%Y-%m-%dT%H:%M:%SZ")
                .map_err(de::Error::custom),
        }
    }

    pub fn serialize<S>(date: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(date.format("%Y-%m-%dT%H:%M:%SZ").to_string().as_str())
    }
}

/// Convert models into transactions

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
            Some(vec![Amount::new(trade.fee, trade.fee_asset.clone())])
        } else {
            None
        };

        let mut ops = match trade.side {
            TradeSide::Buy => {
                vec![
                    Operation {
                        op_type: OperationType::Acquire,
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.base_amount(), trade.base_asset.clone()),
                        price: Some(Amount::new(trade.quote_amount(), trade.quote_asset.clone())),
                        costs: costs,
                        time: trade.time,
                        recipient: None,
                        sender: None,
                    },
                    Operation {
                        op_type: OperationType::Dispose,
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.quote_amount(), trade.quote_asset.clone()),
                        price: Some(Amount::new(trade.base_amount(), trade.base_asset)),
                        costs: None,
                        time: trade.time,
                        recipient: None,
                        sender: None,
                    },
                ]
            }
            TradeSide::Sell => {
                vec![
                    Operation {
                        op_type: OperationType::Dispose,
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.base_amount(), trade.base_asset.clone()),
                        price: Some(Amount::new(trade.quote_amount(), trade.quote_asset.clone())),
                        costs: costs,
                        time: trade.time,
                        recipient: None,
                        sender: None,
                    },
                    Operation {
                        op_type: OperationType::Acquire,
                        source_id: trade.source_id.clone(),
                        source: trade.source.clone(),
                        amount: Amount::new(trade.quote_amount(), trade.quote_asset.clone()),
                        price: Some(Amount::new(trade.base_amount(), trade.base_asset.clone())),
                        costs: None,
                        time: trade.time,
                        recipient: None,
                        sender: None,
                    },
                ]
            }
        };
        if has_fees {
            ops.push(Operation {
                op_type: OperationType::Dispose,
                source_id: trade.source_id.clone(),
                source: trade.source.clone(),
                amount: Amount::new(trade.fee, trade.fee_asset.clone()),
                price: Some(Amount::new(trade.fee, trade.fee_asset.clone())),
                costs: None,
                time: trade.time,
                recipient: None,
                sender: None,
            });
        }
        ops
    }
}

impl From<Deposit> for Vec<Operation> {
    fn from(deposit: Deposit) -> Self {
        let mut ops = vec![Operation {
            op_type: OperationType::Receive,
            source_id: deposit.source_id.clone(),
            source: deposit.source.clone(),
            amount: Amount::new(deposit.amount, deposit.asset.clone()),
            costs: deposit
                .fee
                .filter(|f| *f > 0.0)
                .map(|f| vec![Amount::new(f, deposit.asset.clone())]),
            time: deposit.time,
            price: None,
            sender: None,
            recipient: None,
        }];
        if let Some(fee) = deposit.fee.filter(|f| f > &0.0) {
            ops.extend(vec![Operation {
                op_type: OperationType::Dispose,
                source_id: deposit.source_id.clone(),
                source: deposit.source.clone(),
                amount: Amount::new(fee, deposit.asset.clone()),
                price: Some(Amount::new(fee, deposit.asset.clone())),
                costs: None,
                time: deposit.time,
                sender: None,
                recipient: None,
            }]);
        }
        ops
    }
}

impl From<Withdraw> for Vec<Operation> {
    fn from(withdraw: Withdraw) -> Self {
        let mut ops = vec![Operation {
            op_type: OperationType::Send,
            source_id: withdraw.source_id.clone(),
            source: withdraw.source.clone(),
            amount: Amount::new(withdraw.amount, withdraw.asset.clone()),
            costs: if withdraw.fee > 0.0 {
                Some(vec![Amount::new(withdraw.fee, withdraw.asset.clone())])
            } else {
                None
            },
            time: withdraw.time,
            price: None,
            sender: None,
            recipient: None,
        }];
        if withdraw.fee > 0.0 {
            ops.extend(vec![Operation {
                op_type: OperationType::Dispose,
                source_id: withdraw.source_id.clone(),
                source: withdraw.source.clone(),
                amount: Amount::new(withdraw.fee, withdraw.asset.clone()),
                price: Some(Amount::new(withdraw.fee, withdraw.asset.clone())),
                costs: None,
                time: withdraw.time,
                sender: None,
                recipient: None,
            }]);
        }
        ops
    }
}

impl From<Loan> for Vec<Operation> {
    fn from(loan: Loan) -> Self {
        match loan.status {
            Status::Success => vec![Operation {
                op_type: OperationType::Acquire,
                source_id: loan.source_id,
                source: loan.source,
                amount: Amount::new(loan.amount, loan.asset.clone()),
                price: Some(Amount::new(0.0, loan.asset)),
                costs: None,
                time: loan.time,
                sender: None,
                recipient: None,
            }],
            Status::Failure => vec![],
        }
    }
}

impl From<Repay> for Vec<Operation> {
    fn from(repay: Repay) -> Self {
        match repay.status {
            Status::Success => vec![Operation {
                op_type: OperationType::Acquire,
                source_id: repay.source_id,
                source: repay.source,
                amount: Amount::new(repay.amount, repay.asset.clone()),
                price: Some(Amount::new(0.0, repay.asset.clone())),
                costs: if repay.interest > 0.0 {
                    Some(vec![Amount::new(repay.interest, repay.asset)])
                } else {
                    None
                },
                time: repay.time,
                sender: None,
                recipient: None,
            }],
            Status::Failure => vec![],
        }
    }
}
