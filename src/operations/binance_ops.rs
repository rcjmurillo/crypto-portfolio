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
            asset: "USD".to_string(), // fixme: grab the actual asset from the API
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
impl ExchangeDataFetcher for BinanceFetcher<'_> {
    type Trade = Trade;
    type Loan = MarginLoan;
    type Repay = MarginRepay;
    type Deposit = FiatDeposit;
    type Withdraw = Withdraw;

    async fn trades(&self, symbols: &[String]) -> Result<Vec<Trade>> {
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

    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<Trade>> {
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

    async fn loans(&self, symbols: &[String]) -> Result<Vec<MarginLoan>> {
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

    async fn repays(&self, symbols: &[String]) -> Result<Vec<MarginRepay>> {
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

    async fn fiat_deposits(&self, _: &[String]) -> Result<Vec<FiatDeposit>> {
        Ok(Vec::new())
    }

    async fn withdraws(&self, _: &[String]) -> Result<Vec<Withdraw>> {
        self.fetch_withdraws().await.map_err(|e| e.into())
    }
}

fn flatten_results<T>(results: Vec<binance::Result<Vec<T>>>) -> Result<Vec<T>> {
    Ok(results
        .into_iter()
        .map(|r| r.map_err(|e| e.into()))
        .collect::<Result<Vec<Vec<T>>>>()?
        .into_iter()
        .flatten()
        .collect())
}
