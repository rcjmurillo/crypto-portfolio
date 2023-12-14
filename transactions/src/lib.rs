pub mod cost_basis;
mod models;
pub use models::*;

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use serde_json::Value;
use std::fmt::{self, Display};

type Asset = String;

#[derive(Debug, Clone, PartialEq)]
pub struct Amount {
    pub value: f64,
    pub asset: Asset,
}

impl Amount {
    pub fn new<T>(value: f64, asset: T) -> Self
    where
        T: Into<Asset>,
    {
        Self {
            value,
            asset: asset.into(),
        }
    }
}

impl Display for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, self.asset)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OperationType {
    Acquire,
    Dispose,
    Send,
    Receive,
}

impl fmt::Display for OperationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            OperationType::Acquire => "acquire",
            OperationType::Dispose => "dispose",
            OperationType::Send => "send",
            OperationType::Receive => "receive",
        };
        write!(f, "{s}")
    }
}

/// Types of transactions used to express any type of transaction
#[derive(Debug, Clone, PartialEq)]
pub struct Operation {
    pub op_type: OperationType,
    pub source_id: String,
    pub source: String,
    pub sender: Option<String>,
    pub recipient: Option<String>,
    pub amount: Amount,
    pub price: Option<Amount>,
    pub costs: Option<Vec<Amount>>,
    pub time: DateTime<Utc>,
}

impl Operation {
    pub fn id(&self) -> String {
        format!(
            "{}-{}-{}",
            self.source,
            self.op_type.to_string(),
            self.source_id
        )
    }
}

#[cfg(test)]
mod tests {}
