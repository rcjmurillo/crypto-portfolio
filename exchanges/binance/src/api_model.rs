use chrono::{DateTime, Utc};
use serde::Deserialize;

fn default_currency_usd() -> String {
    "USD".into()
}

fn default_zero() -> f64 {
    0.0
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FiatOrder {
    #[serde(alias="orderId", alias="orderNo")]
    pub id: String,
    #[serde(default = "default_currency_usd")]
    pub fiat_currency: String,
    #[serde(with = "float_from_str")]
    pub amount: f64,
    #[serde(alias = "totalFee", with = "float_from_str")]
    pub transaction_fee: f64,
    #[serde(with = "float_from_str", default = "default_zero")]
    pub platform_fee: f64,
    #[serde(alias = "orderStatus")]
    pub status: String,
    #[serde(with = "datetime_from_str")]
    pub create_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Deposit {
    #[serde(alias="txId")]
    pub id: String,
    #[serde(with = "float_from_str")]
    pub amount: f64,
    #[serde(alias = "asset")]
    pub coin: String,
    pub status: u8,
    #[serde(with = "datetime_from_str")]
    pub insert_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Withdraw {
    pub id: String,
    status: u16,
    #[serde(with = "float_from_str")]
    pub amount: f64,
    #[serde(with = "float_from_str")]
    pub transaction_fee: f64,
    #[serde(alias = "asset")]
    pub coin: String,
    #[serde(with = "datetime_from_str", alias = "apply_time")]
    pub apply_time: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Trade {
    order_id: i64,
    pub symbol: String,
    pub id: u64,
    #[serde(with = "float_from_str")]
    pub price: f64,
    #[serde(with = "float_from_str")]
    pub qty: f64,
    #[serde(with = "float_from_str")]
    pub commission: f64,
    pub commission_asset: String,
    #[serde(with = "datetime_from_str")]
    pub time: DateTime<Utc>,
    pub is_buyer: bool,

    // computed
    #[serde(skip)]
    pub base_asset: String,
    #[serde(skip)]
    pub quote_asset: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginLoan {
    pub tx_id: u64,
    pub asset: String,
    #[serde(with = "float_from_str")]
    pub principal: f64,
    #[serde(with = "datetime_from_str")]
    pub timestamp: DateTime<Utc>,
    pub status: String,
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct MarginRepay {
    pub tx_id: u64,
    #[serde(with = "float_from_str")]
    pub principal: f64,
    #[serde(with = "float_from_str")]
    pub amount: f64,
    pub asset: String,
    #[serde(with = "float_from_str")]
    pub interest: f64,
    pub status: String,
    #[serde(with = "datetime_from_str")]
    pub timestamp: DateTime<Utc>,
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
    #[serde(with = "float_from_str")]
    pub price: f64,
}

pub(crate) mod float_from_str {
    use serde::{de, Deserialize, Deserializer};

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

pub(crate) mod datetime_from_str {
    use chrono::{DateTime, TimeZone, Utc};
    use serde::{de, Deserialize, Deserializer};

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum TimestampOrString {
            Timestamp(i64),
            String(String),
        }

        Ok(match TimestampOrString::deserialize(deserializer)? {
            // timestamps from the API are in milliseconds
            TimestampOrString::Timestamp(ts) => Utc.timestamp_millis(ts),
            TimestampOrString::String(s) => Utc
                .datetime_from_str(&s, "%Y-%m-%d %H:%M:%S")
                .map_err(de::Error::custom)?,
        })
    }
}
