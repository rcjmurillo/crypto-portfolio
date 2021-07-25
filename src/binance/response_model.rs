use serde::{Deserialize, Serialize};

use crate::operations::{self, OperationStatus, TradeSide};

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FiatDeposit {
    order_id: String,
    offset: Option<u64>,
    payment_channel: String,
    payment_method: String,
    pub order_status: String,
    #[serde(with = "string_or_float")]
    pub amount: f64,
    #[serde(with = "string_or_float")]
    transaction_fee: f64,
    #[serde(with = "string_or_float")]
    platform_fee: f64,
}

impl From<FiatDeposit> for operations::Deposit {
    fn from(d: FiatDeposit) -> Self {
        Self {
            asset: "USD".to_string(), // fixme: grab the actual asset from the API
            amount: d.amount,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Withdraw {
    id: String,
    withdraw_order_id: Option<String>,
    amount: f64,
    transaction_fee: f64,
    address: String,
    asset: String,
    tx_id: String,
    apply_time: u64,
    status: u16,
}

impl From<Withdraw> for operations::Withdraw {
    fn from(w: Withdraw) -> Self {
        Self {
            asset: w.asset,
            amount: w.amount,
            time: w.apply_time,
            fee: w.transaction_fee,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    pub symbol: String,
    pub id: u64,
    order_id: i64,
    #[serde(with = "string_or_float")]
    price: f64,
    #[serde(with = "string_or_float")]
    pub qty: f64,
    #[serde(with = "string_or_float")]
    commission: f64,
    commission_asset: String,
    pub time: u64,
    is_buyer: bool,
    // computed
    #[serde(skip)]
    pub cost: f64,
    #[serde(skip)]
    pub base_asset: String,
    #[serde(skip)]
    pub quote_asset: String,
}

impl From<Trade> for operations::Trade {
    fn from(t: Trade) -> Self {
        Self {
            symbol: t.symbol,
            base_asset: t.base_asset,
            quote_asset: t.quote_asset,
            price: t.price,
            cost: t.cost,
            amount: t.qty,
            fee: t.commission,
            fee_asset: t.commission_asset,
            time: t.time,
            side: if t.is_buyer {
                TradeSide::Buy
            } else {
                TradeSide::Sell
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginBorrow {
    tx_id: u64,
    asset: String,
    #[serde(with = "string_or_float")]
    principal: f64,
    timestamp: u64,
    status: String,
}

impl From<MarginBorrow> for operations::Loan {
    fn from(m: MarginBorrow) -> Self {
        Self {
            asset: m.asset,
            amount: m.principal,
            timestamp: m.timestamp,
            status: match m.status.as_str() {
                "CONFIRMED" => OperationStatus::Success,
                _ => OperationStatus::Failed,
            },
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginRepay {
    //isolated_symbol: String,
    #[serde(with = "string_or_float")]
    amount: f64,
    asset: String,
    #[serde(with = "string_or_float")]
    interest: f64,
    #[serde(with = "string_or_float")]
    principal: f64,
    status: String,
    timestamp: u64,
    tx_id: u64,
}

impl From<MarginRepay> for operations::Repay {
    fn from(r: MarginRepay) -> Self {
        Self {
            asset: r.asset,
            amount: r.amount,
            interest: r.interest,
            timestamp: r.timestamp,
            status: match r.status.as_str() {
                "CONFIRMED" => OperationStatus::Success,
                _ => OperationStatus::Failed,
            },
        }
    }
}

#[derive(Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}

pub(crate) mod string_or_float {
    use std::fmt;

    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn serialize<T, S>(value: &T, serializer: S) -> Result<S::Ok, S::Error>
    where
        T: fmt::Display,
        S: Serializer,
    {
        serializer.collect_str(value)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum StringOrFloat {
            String(String),
            Float(f64),
        }

        match StringOrFloat::deserialize(deserializer)? {
            StringOrFloat::String(s) => s.parse().map_err(de::Error::custom),
            StringOrFloat::Float(i) => Ok(i),
        }
    }
}
