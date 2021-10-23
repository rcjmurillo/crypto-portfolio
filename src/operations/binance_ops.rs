use std::convert::{TryFrom, TryInto};

use binance::{
    BinanceGlobalFetcher, BinanceUsFetcher, Config, Deposit, FiatDeposit, MarginLoan, MarginRepay,
    Trade, Withdraw,
};

use anyhow::{Error as AnyhowError, Result};
use async_trait::async_trait;
use chrono::{TimeZone, Utc};
use futures::future::join_all;

use crate::{
    cli::ExchangeConfig,
    errors::{Error, ErrorKind},
    operations::{
        self as ops, AssetsInfo, ExchangeDataFetcher, Operation, OperationStatus, TradeSide,
    },
};

impl TryFrom<&ExchangeConfig> for Config {
    type Error = AnyhowError;
    fn try_from(c: &ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_date: c.start_date()?,
            symbols: c.symbols.clone(),
        })
    }
}

impl From<FiatDeposit> for ops::Deposit {
    fn from(d: FiatDeposit) -> Self {
        Self {
            asset: d.fiat_currency,
            amount: d.amount,
            fee: Some(d.platform_fee),
            time: d
                .update_time
                .unwrap_or(Utc::now().timestamp_millis().try_into().unwrap()),
            is_fiat: true,
        }
    }
}

impl From<Deposit> for ops::Deposit {
    fn from(d: Deposit) -> Self {
        Self {
            asset: d.coin,
            amount: d.amount,
            fee: None,
            time: d.insert_time,
            is_fiat: false,
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
            time: m.timestamp,
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
            amount: r.principal,
            interest: r.interest,
            time: r.timestamp,
            status: match r.status.as_str() {
                "CONFIRMED" => OperationStatus::Success,
                _ => OperationStatus::Failed,
            },
        }
    }
}

#[async_trait]
impl ExchangeDataFetcher for BinanceGlobalFetcher {
    async fn operations(&self) -> Result<Vec<Operation>> {
        Ok(Vec::new())
    }

    async fn trades(&self) -> Result<Vec<ops::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .base_fetcher
            .fetch_exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in self.base_fetcher.symbols().iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.base_fetcher.fetch_trades(symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn margin_trades(&self) -> Result<Vec<ops::Trade>> {
        // Processing binance margin trades
        let all_symbols: Vec<String> = self
            .base_fetcher
            .fetch_exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in self.base_fetcher.symbols().iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_margin_trades(symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn loans(&self) -> Result<Vec<ops::Loan>> {
        let mut handles = Vec::new();
        let exchange_symbols = self.base_fetcher.fetch_exchange_symbols().await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        for symbol in self.base_fetcher.symbols().iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = binance::symbol_into_assets(&symbol, &exchange_symbols);
                handles.push(self.fetch_margin_loans(asset, symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn repays(&self) -> Result<Vec<ops::Repay>> {
        let mut handles = Vec::new();
        let exchange_symbols = self.base_fetcher.fetch_exchange_symbols().await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        for symbol in self.base_fetcher.symbols().iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = binance::symbol_into_assets(&symbol, &exchange_symbols);
                handles.push(self.fetch_margin_repays(asset, symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn deposits(&self) -> Result<Vec<ops::Deposit>> {
        let mut deposits = Vec::new();

        deposits.extend(
            self.fetch_fiat_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Deposit>>(),
        );
        deposits.extend(
            self.fetch_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Deposit>>(),
        );

        Ok(deposits)
    }

    async fn withdraws(&self) -> Result<Vec<ops::Withdraw>> {
        match self.fetch_withdraws().await {
            Ok(w) => Ok(w.into_iter().map(|x| x.into()).collect()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl ExchangeDataFetcher for BinanceUsFetcher {
    async fn operations(&self) -> Result<Vec<Operation>> {
        Ok(Vec::new())
    }

    async fn trades(&self) -> Result<Vec<ops::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .base_fetcher
            .fetch_exchange_symbols()
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in self.base_fetcher.symbols().iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.base_fetcher.fetch_trades(symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
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
        let mut deposits = Vec::new();

        deposits.extend(
            self.fetch_fiat_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Deposit>>(),
        );
        deposits.extend(
            self.fetch_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Deposit>>(),
        );

        Ok(deposits)
    }

    async fn withdraws(&self) -> Result<Vec<ops::Withdraw>> {
        match self.fetch_withdraws().await {
            Ok(w) => Ok(w.into_iter().map(|x| x.into()).collect()),
            Err(e) => Err(e.into()),
        }
    }
}

#[async_trait]
impl AssetsInfo for BinanceGlobalFetcher {
    async fn price_at(&self, symbol: &str, time: u64) -> Result<f64> {
        self.base_fetcher
            .fetch_price_at(symbol, time)
            .await
            .map_err(|err| Error::new(err.to_string(), ErrorKind::FetchFailed).into())
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
