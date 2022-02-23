use crate::client::Identifiable;
use chrono::{DateTime, Utc};
use serde::Deserialize;

fn default_buy() -> TransactionSide {
    TransactionSide::Buy
}

#[derive(Debug, Deserialize, Clone)]
pub struct Pagination {
    pub next_uri: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AccountCurrency {
    pub code: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Account {
    pub id: String,
    pub currency: AccountCurrency,
}

impl Identifiable<String> for Account {
    fn id(&self) -> &String {
        &self.id
    }
}

#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub enum TransactionSide {
    Buy,
    Sell,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Amount {
    pub amount: String,
    pub currency: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Product {
    pub id: String,
    base_currency: String,
    quote_currency: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Resource {
    pub id: String,
    resource: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Fill {
    pub trade_id: u32,
    pub product_id: String,
    pub price: String,
    pub size: String,
    pub fee: String,
    pub side: String,
    pub created_at: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct Transaction {
    pub id: String,
    transaction: Option<Resource>,
    #[serde(alias = "type")]
    pub tx_type: Option<String>,
    pub status: String,
    pub updated_at: String,
    pub payout_at: Option<String>,
    pub fee: Option<Amount>,
    pub amount: Amount,
    pub subtotal: Option<Amount>,

    // computed
    #[serde(skip, default = "default_buy")]
    pub side: TransactionSide,
}

impl Transaction {
    pub fn update_time(&self) -> DateTime<Utc> {
        self.updated_at.parse::<DateTime<Utc>>().expect(&format!(
            "couldn't parse updated_at time '{}'",
            self.updated_at
        ))
    }
}

impl Identifiable<String> for Transaction {
    fn id(&self) -> &String {
        &self.id
    }
}

#[derive(Deserialize, Clone)]
pub struct Response<T> {
    pub pagination: Pagination,
    pub data: Vec<T>,
}
