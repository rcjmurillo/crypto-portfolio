use std::convert::TryFrom;

use binance::{BinanceFetcher, Config, FiatDeposit, MarginLoan, MarginRepay, Trade, Withdraw};

use async_trait::async_trait;
use futures::future::join_all;

use crate::{
    cli::ExchangeConfig,
    errors::Error,
    operations::{self as ops, ExchangeDataFetcher, OperationStatus, TradeSide},
    result::Result,
};

impl TryFrom<ExchangeConfig> for Config {
    type Error = Error;
    fn try_from(c: ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_datetime: c.start_date()?,
        })
    }
}

impl From<FiatDeposit> for ops::Deposit {
    fn from(d: FiatDeposit) -> Self {
        Self {
            asset: d.fiat_currency,
            amount: d.amount,
        }
    }
}

impl From<Withdraw> for ops::Withdraw {
    fn from(w: Withdraw) -> Self {
        Self {
            asset: w.coin,
            amount: w.amount,
            time: w.apply_time,
            fee: w.transaction_fee,
        }
    }
}

impl From<Trade> for ops::Trade {
    fn from(t: Trade) -> Self {
        Self {
            symbol: t.symbol,
            base_asset: t.base_asset,
            quote_asset: t.quote_asset,
            price: t.price,
            usd_cost: t.usd_cost,
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

impl From<MarginLoan> for ops::Loan {
    fn from(m: MarginLoan) -> Self {
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

impl From<MarginRepay> for ops::Repay {
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

#[async_trait]
impl ExchangeDataFetcher for BinanceFetcher {
    async fn trades(&self, symbols: &[String]) -> Result<Vec<ops::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in symbols.iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_trades(symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<ops::Trade>> {
        // Processing binance margin trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in symbols.iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_margin_trades(symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn loans(&self, symbols: &[String]) -> Result<Vec<ops::Loan>> {
        let mut handles = Vec::new();
        let exchange_symbols = self.fetch_exchange_symbols().await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        for symbol in symbols.into_iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = binance::symbol_into_assets(&symbol, &exchange_symbols);
                handles.push(self.fetch_margin_loans(asset, symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn repays(&self, symbols: &[String]) -> Result<Vec<ops::Repay>> {
        let mut handles = Vec::new();
        let exchange_symbols = self.fetch_exchange_symbols().await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        for symbol in symbols.into_iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = binance::symbol_into_assets(&symbol, &exchange_symbols);
                handles.push(self.fetch_margin_repays(asset, symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn fiat_deposits(&self, _: &[String]) -> Result<Vec<ops::Deposit>> {
        Ok(self.fetch_fiat_deposits().await?.into_iter().map(|x| x.into()).collect())
    }

    async fn withdraws(&self, _: &[String]) -> Result<Vec<ops::Withdraw>> {
        match self.fetch_withdraws().await {
            Ok(w) => Ok(w.into_iter().map(|x| x.into()).collect()),
            Err(e) => Err(e.into())
        }
    }
}

fn flatten_results<T, U>(results: Vec<binance::Result<Vec<T>>>) -> Result<Vec<U>>
where
    T: Into<U>,
{
    Ok(results
        .into_iter()
        .map(|r| r.map_err(|e| e.into()))
        .collect::<Result<Vec<Vec<T>>>>()?
        .into_iter()
        .flatten()
        .map(|x| x.into())
        .collect())
}
