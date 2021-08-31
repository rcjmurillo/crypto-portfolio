use serde::{Deserialize, Serialize};

fn default_currency_usd() -> String {
    "USD".into()
}

fn default_zero() -> f64 {
    0.0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FiatDeposit {
    #[serde(default = "default_currency_usd")]
    pub fiat_currency: String,
    #[serde(with = "string_or_float")]
    pub amount: f64,
    #[serde(alias = "totalFee", with = "string_or_float")]
    pub transaction_fee: f64,
    #[serde(with = "string_or_float", default = "default_zero")]
    pub platform_fee: f64,
    #[serde(alias = "orderStatus")]
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Withdraw {
    id: String,
    withdraw_order_id: Option<String>,
    tx_id: String,
    address: String,
    status: u16,
    #[serde(with = "string_or_float")]
    pub amount: f64,
    #[serde(with = "string_or_float")]
    pub transaction_fee: f64,
    pub coin: String,
    pub apply_time: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    order_id: i64,
    pub symbol: String,
    pub id: u64,
    #[serde(with = "string_or_float")]
    pub price: f64,
    #[serde(with = "string_or_float")]
    pub qty: f64,
    #[serde(with = "string_or_float")]
    pub commission: f64,
    pub commission_asset: String,
    pub time: u64,
    pub is_buyer: bool,

    // computed
    #[serde(skip)]
    pub usd_cost: f64,
    #[serde(skip)]
    pub base_asset: String,
    #[serde(skip)]
    pub quote_asset: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginLoan {
    tx_id: u64,
    pub asset: String,
    #[serde(with = "string_or_float")]
    pub principal: f64,
    pub timestamp: u64,
    pub status: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginRepay {
    tx_id: u64,
    #[serde(with = "string_or_float")]
    principal: f64,
    #[serde(with = "string_or_float")]
    pub amount: f64,
    pub asset: String,
    #[serde(with = "string_or_float")]
    pub interest: f64,
    pub status: String,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Symbol {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SymbolPrice {
    pub symbol: String,
    #[serde(with = "string_or_float")]
    pub price: f64,
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
