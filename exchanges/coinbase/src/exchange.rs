use anyhow::{Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::join_all;

use crate::{
    api_model::{Amount, Fill, Transaction, TransactionSide},
    client::{CoinbaseFetcher, Config, Pro, Std},
};

use exchange::{
    operations::Operation, Deposit, ExchangeDataFetcher, Loan, Repay, Trade, TradeSide, Withdraw,
};

impl Into<Trade> for Fill {
    fn into(self) -> Trade {
        let assets: Vec<_> = self.product_id.split("-").collect();
        let base_asset = assets[0].to_string();
        let quote_asset = assets[1].to_string();
        Trade {
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
                "buy" => TradeSide::Buy,
                "sell" => TradeSide::Sell,
                _ => panic!("invalid transaction side {}", self.side),
            },
        }
    }
}

impl Into<Deposit> for Transaction {
    fn into(self) -> Deposit {
        let subtotal: Amount = self.subtotal.expect("missing subtotal in transaction");
        let fee: Amount = self.fee.expect("missing fee in transaction");
        let payout_at = self.payout_at.expect("missing payout_at in transaction");
        Deposit {
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

impl Into<Withdraw> for Transaction {
    fn into(self) -> Withdraw {
        let subtotal: Amount = self.subtotal.expect("missing subtotal in transaction");
        let fee: Amount = self.fee.expect("missing fee in transaction");
        let payout_at = self.payout_at.expect("missing payout_at in transaction");
        let amount = subtotal.amount;
        Withdraw {
            source_id: self.id,
            source: "coinbase".to_string(),
            asset: subtotal.currency,
            amount: amount
                .parse::<f64>()
                .unwrap_or_else(|_| panic!("couldn't parse amount '{}' into f64", amount)),
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

impl From<TransactionSide> for TradeSide {
    fn from(ts: TransactionSide) -> TradeSide {
        match ts {
            TransactionSide::Buy => TradeSide::Buy,
            TransactionSide::Sell => TradeSide::Sell,
        }
    }
}

impl Into<Trade> for Transaction {
    fn into(self) -> Trade {
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
        let updated_at = self.updated_at;
        Trade {
            source_id: self.id,
            source: "coinbase".to_string(),
            symbol: format!("{}{}", base_asset, quote_asset),
            base_asset,
            quote_asset,
            amount: amount,
            price: subtotal_amount / amount,
            fee: to_f64(&fee.amount),
            fee_asset: fee.currency,
            time: updated_at
                .parse::<DateTime<Utc>>()
                .unwrap_or_else(|_| panic!("couldn't parse time '{}'", updated_at)),
            side: self.side.into(),
        }
    }
}

// struct Operations(Vec<Operation>);

// impl Into<Operations> for Transaction {
//     // fixme: convert into TryFrom
//     fn into(self) -> Operations {
//         let for_amount = self
//             .amount
//             .amount
//             .parse::<f64>()
//             .unwrap_or_else(|_| {
//                 panic!(
//                     "couldn't parse for_amount '{}' into f64",
//                     self.amount.amount
//                 )
//             })
//             .abs();
//         let amount = self
//             .native_amount
//             .amount
//             .parse::<f64>()
//             .unwrap_or_else(|_| {
//                 panic!(
//                     "couldn't parse amount '{}' into f64",
//                     self.native_amount.amount
//                 )
//             })
//             .abs();
//         Operations(if amount > 0.0 {
//             vec![
//                 Operation::BalanceIncrease {
//                     id: 1,
//                     source_id: self.id.clone(),
//                     source: "coinbase".to_string(),
//                     asset: self.amount.currency.clone(),
//                     amount: amount,
//                 },
//                 Operation::Cost {
//                     id: 2,
//                     source_id: self.id.clone(),
//                     source: "coinbase".to_string(),
//                     for_asset: self.amount.currency.clone(),
//                     for_amount: for_amount,
//                     asset: self.native_amount.currency.clone(),
//                     amount: amount,
//                     time: self.update_time(),
//                 },
//             ]
//         } else {
//             vec![
//                 Operation::BalanceDecrease {
//                     id: 1,
//                     source_id: self.id.clone(),
//                     source: "coinbase".to_string(),
//                     asset: self.amount.currency.clone(),
//                     amount: amount,
//                 },
//                 Operation::Revenue {
//                     id: 2,
//                     source_id: self.id.clone(),
//                     source: "coinbase".to_string(),
//                     asset: self.amount.currency.clone(),
//                     amount: amount,
//                     time: self.update_time(),
//                 },
//             ]
//         })
//     }
// }

fn into_ops<T>(typed_ops: Vec<T>) -> Vec<Operation>
where
    T: Into<Vec<Operation>>,
{
    typed_ops.into_iter().flat_map(|s| s.into()).collect()
}

impl CoinbaseFetcher<Std> {
    // async fn operations(&self) -> Result<Vec<Operation>> {
    //     let operations = self
    //         .fetch_transactions()
    //         .await?
    //         .into_iter()
    //         .filter_map(|t| -> Option<Operations> {
    //             match t.tx_type.as_ref().expect("missing type in transaction") == "trade"
    //                 && t.status == "completed"
    //             {
    //                 true => Some(t.into()),
    //                 false => None,
    //             }
    //         })
    //         .map(|ops| ops.0)
    //         .flatten()
    //         .collect();
    //     Ok(operations)
    // }
    async fn trades(&self) -> Result<Vec<Trade>> {
        let mut all_trades = Vec::new();
        all_trades.extend(self.fetch_buys().await?.into_iter().map(|d| d.into()));
        all_trades.extend(self.fetch_sells().await?.into_iter().map(|d| d.into()));
        Ok(all_trades)
    }
    async fn deposits(&self) -> Result<Vec<Deposit>> {
        Ok(self
            .fetch_fiat_deposits()
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect())
    }
    async fn withdrawals(&self) -> Result<Vec<Withdraw>> {
        Ok(self
            .fetch_withdrawals()
            .await?
            .into_iter()
            .map(|d| d.into())
            .collect())
    }
}

#[async_trait]
impl ExchangeDataFetcher for CoinbaseFetcher<Std> {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: data_sync::OperationStorage + Send + Sync,
    {
        let mut ops = Vec::new();
        ops.extend(into_ops(self.trades().await?));
        ops.extend(into_ops(self.deposits().await?));
        ops.extend(into_ops(self.withdrawals().await?));
        Ok(())
    }
}

impl CoinbaseFetcher<Pro> {
    async fn trades(&self) -> Result<Vec<Trade>> {
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
    async fn margin_trades(&self) -> Result<Vec<Trade>> {
        Ok(Vec::new())
    }
    async fn loans(&self) -> Result<Vec<Loan>> {
        Ok(Vec::new())
    }
    async fn repays(&self) -> Result<Vec<Repay>> {
        Ok(Vec::new())
    }
    async fn deposits(&self) -> Result<Vec<Deposit>> {
        Ok(Vec::new())
    }
    async fn withdrawals(&self) -> Result<Vec<Withdraw>> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl ExchangeDataFetcher for CoinbaseFetcher<Pro> {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: data_sync::OperationStorage + Send + Sync,
    {
        let mut ops = Vec::new();
        ops.extend(into_ops(self.trades().await?));
        ops.extend(into_ops(self.margin_trades().await?));
        ops.extend(into_ops(self.loans().await?));
        ops.extend(into_ops(self.repays().await?));
        ops.extend(into_ops(self.deposits().await?));
        ops.extend(into_ops(self.withdrawals().await?));
        Ok(())
    }
}
