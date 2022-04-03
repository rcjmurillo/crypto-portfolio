use std::convert::TryFrom;

use anyhow::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::join_all;

use coinbase::{Amount, CoinbaseFetcher, Config, Fill, Pro, Std, Transaction, TransactionSide};

use crate::{
    cli::ExchangeConfig,
    operations::{self as ops},
    ExchangeDataFetcher,
};

impl TryFrom<ExchangeConfig> for Config {
    type Error = Error;
    fn try_from(c: ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_date: c.start_date()?,
            symbols: c.symbols.clone(),
        })
    }
}

impl Into<ops::Trade> for Fill {
    fn into(self) -> ops::Trade {
        let assets: Vec<_> = self.product_id.split("-").collect();
        let base_asset = assets[0].to_string();
        let quote_asset = assets[1].to_string();
        ops::Trade {
            source_id: self.trade_id.to_string(),
            source: "coinbase".to_string(),
            symbol: self.product_id.clone(),
            base_asset,
            quote_asset,
            price: self
                .price
                .parse::<f64>()
                .expect(&format!("couldn't parse price '{}' into f64", self.price)),
            amount: self
                .size
                .parse::<f64>()
                .expect(&format!("couldn't parse size '{}' into f64", self.size)),
            fee: self
                .fee
                .parse::<f64>()
                .expect(&format!("couldn't parse fee '{}' into f64", self.fee)),
            // the fee asset is always the quote asset in coinbase API
            fee_asset: assets[1].to_string(),
            time: self
                .created_at
                .parse::<DateTime<Utc>>()
                .expect(&format!("couldn't parse time '{}'", self.created_at)),
            side: match self.side.as_str() {
                "buy" => ops::TradeSide::Buy,
                "sell" => ops::TradeSide::Sell,
                _ => panic!("invalid transaction side {}", self.side),
            },
        }
    }
}

impl Into<ops::Deposit> for Transaction {
    fn into(self) -> ops::Deposit {
        let subtotal: Amount = self.subtotal.expect("missing subtotal in transaction");
        let fee: Amount = self.fee.expect("missing fee in transaction");
        let payout_at = self.payout_at.expect("missing payout_at in transaction");
        ops::Deposit {
            source_id: self.id,
            source: "coinbase".to_string(),
            asset: subtotal.currency,
            amount: subtotal.amount.parse::<f64>().expect(&format!(
                "couldn't parse amount '{}' into f64",
                subtotal.amount
            )),
            fee: Some(
                fee.amount
                    .parse::<f64>()
                    .expect(&format!("couldn't parse amount '{}' into f64", fee.amount)),
            ),
            time: payout_at
                .parse::<DateTime<Utc>>()
                .expect(&format!("couldn't parse time '{}'", payout_at)),
            is_fiat: true,
        }
    }
}

impl Into<ops::Withdraw> for Transaction {
    fn into(self) -> ops::Withdraw {
        let subtotal: Amount = self.subtotal.expect("missing subtotal in transaction");
        let fee: Amount = self.fee.expect("missing fee in transaction");
        let payout_at = self.payout_at.expect("missing payout_at in transaction");
        ops::Withdraw {
            source_id: self.id,
            source: "coinbase".to_string(),
            asset: subtotal.currency,
            amount: subtotal
                .amount
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("couldn't parse amount '{}' into f64", subtotal.amount)),
            fee: fee
                .amount
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("couldn't parse amount '{}' into f64", fee.amount)),
            time: payout_at
                .parse::<DateTime<Utc>>()
                .unwrap_or_else(|_| panic!("couldn't parse time '{}'", payout_at)),
        }
    }
}

impl From<TransactionSide> for ops::TradeSide {
    fn from(ts: TransactionSide) -> ops::TradeSide {
        match ts {
            TransactionSide::Buy => ops::TradeSide::Buy,
            TransactionSide::Sell => ops::TradeSide::Sell,
        }
    }
}

impl Into<ops::Trade> for Transaction {
    fn into(self) -> ops::Trade {
        let to_f64 = |amount_str: &str| {
            amount_str
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("couldn't parse amount '{}' into f64", amount_str))
        };

        let base_asset = self.amount.currency;
        let subtotal = self.subtotal.expect("missing subtotal in transaction");
        let subtotal_amount = to_f64(&subtotal.amount);
        let quote_asset = subtotal.currency;
        let amount = to_f64(&self.amount.amount);
        let fee = self.fee.expect("missing fee in transaction");
        ops::Trade {
            source_id: self.id,
            source: "coinbase".to_string(),
            symbol: format!("{}{}", base_asset, quote_asset),
            base_asset,
            quote_asset,
            amount: amount,
            price: subtotal_amount / amount,
            fee: to_f64(&fee.amount),
            fee_asset: fee.currency,
            time: self
                .updated_at
                .parse::<DateTime<Utc>>()
                .unwrap_or_else(|_| panic!("couldn't parse time '{}'", self.updated_at)),
            side: self.side.into(),
        }
    }
}

struct Operations(Vec<ops::Operation>);

impl Into<Operations> for Transaction {
    // fixme: convert into TryFrom
    fn into(self) -> Operations {
        let for_amount = self
            .amount
            .amount
            .parse::<f64>()
            .unwrap_or_else(|_| {
                panic!(
                    "couldn't parse for_amount '{}' into f64",
                    self.amount.amount
                )
            })
            .abs();
        let amount = self
            .native_amount
            .amount
            .parse::<f64>()
            .unwrap_or_else(|_| {
                panic!(
                    "couldn't parse amount '{}' into f64",
                    self.native_amount.amount
                )
            })
            .abs();
        Operations(if amount > 0.0 {
            vec![
                ops::Operation::BalanceIncrease {
                    id: 1,
                    source_id: self.id.clone(),
                    source: "coinbase".to_string(),
                    asset: self.amount.currency.clone(),
                    amount: amount,
                },
                ops::Operation::Cost {
                    id: 2,
                    source_id: self.id.clone(),
                    source: "coinbase".to_string(),
                    for_asset: self.amount.currency.clone(),
                    for_amount: for_amount,
                    asset: self.native_amount.currency.clone(),
                    amount: amount,
                    time: self.update_time(),
                },
            ]
        } else {
            vec![
                ops::Operation::BalanceDecrease {
                    id: 1,
                    source_id: self.id.clone(),
                    source: "coinbase".to_string(),
                    asset: self.amount.currency.clone(),
                    amount: amount,
                },
                ops::Operation::Revenue {
                    id: 2,
                    source_id: self.id.clone(),
                    source: "coinbase".to_string(),
                    asset: self.amount.currency.clone(),
                    amount: amount,
                    time: self.update_time(),
                },
            ]
        })
    }
}

#[async_trait]
impl ExchangeDataFetcher for CoinbaseFetcher<Std> {
    async fn operations(&self) -> Result<Vec<ops::Operation>> {
        let operations = self
            .fetch_transactions()
            .await?
            .into_iter()
            .filter_map(|t| -> Option<Operations> {
                match t.tx_type.as_ref().expect("missing type in transaction") == "trade"
                    && t.status == "completed"
                {
                    true => Some(t.into()),
                    false => None,
                }
            })
            .map(|ops| ops.0)
            .flatten()
            .collect();
        Ok(operations)
    }
    async fn trades(&self) -> Result<Vec<ops::Trade>> {
        let mut all_trades = Vec::new();
        all_trades.extend(self.fetch_buys().await?.into_iter().map(|d| d.into()));
        all_trades.extend(self.fetch_sells().await?.into_iter().map(|d| d.into()));
        Ok(all_trades)
    }
    async fn margin_trades(&self) -> Result<Vec<ops::Trade>> {
        Ok(Vec::new())
    }
    async fn loans(&self) -> Result<Vec<ops::Loan>> {
        Ok(Vec::new())
    }
    async fn repays(&self) -> Result<Vec<ops::Repay>> {
        Ok(Vec::new())
    }
    async fn deposits(&self) -> Result<Vec<ops::Deposit>> {
        Ok(self
            .fetch_fiat_deposits()
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect())
    }
    async fn withdraws(&self) -> Result<Vec<ops::Withdraw>> {
        Ok(self
            .fetch_withdraws()
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect())
    }
}

#[async_trait]
impl ExchangeDataFetcher for CoinbaseFetcher<Pro> {
    async fn operations(&self) -> Result<Vec<ops::Operation>> {
        Ok(Vec::new())
    }
    async fn trades(&self) -> Result<Vec<ops::Trade>> {
        let mut handles = Vec::new();
        for product_id in self.config.symbols.iter() {
            handles.push(self.fetch_fills(product_id));
        }
        Ok(join_all(handles)
            .await
            .into_iter()
            .map(|r| r.map_err(|e| e))
            .collect::<Result<Vec<Vec<Fill>>>>()?
            .into_iter()
            .flatten()
            .map(|x| x.into())
            .collect())
    }
    async fn margin_trades(&self) -> Result<Vec<ops::Trade>> {
        Ok(Vec::new())
    }
    async fn loans(&self) -> Result<Vec<ops::Loan>> {
        Ok(Vec::new())
    }
    async fn repays(&self) -> Result<Vec<ops::Repay>> {
        Ok(Vec::new())
    }
    async fn deposits(&self) -> Result<Vec<ops::Deposit>> {
        Ok(Vec::new())
    }
    async fn withdraws(&self) -> Result<Vec<ops::Withdraw>> {
        Ok(Vec::new())
    }
}
