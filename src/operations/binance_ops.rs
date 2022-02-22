use std::{
    collections::HashSet,
    convert::{TryFrom, TryInto},
};

use binance::{
    BinanceFetcher, Config, Deposit, EndpointsGlobal, EndpointsUs, FiatOrder, MarginLoan,
    MarginRepay, RegionGlobal, RegionUs, Trade, Withdraw,
};

use anyhow::{Error as AnyhowError, Result};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::future::join_all;

use crate::{
    cli::ExchangeConfig,
    operations::{
        self as ops, AssetsInfo, ExchangeDataFetcher, Operation, OperationStatus, TradeSide,
    },
};

impl TryFrom<ExchangeConfig> for Config {
    type Error = AnyhowError;
    fn try_from(c: ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_date: c.start_date()?,
            symbols: c.symbols.clone(),
        })
    }
}

impl From<FiatOrder> for ops::Deposit {
    fn from(d: FiatOrder) -> Self {
        Self {
            asset: d.fiat_currency,
            amount: d.amount,
            fee: Some(d.platform_fee),
            time: d.create_time,
            is_fiat: true,
        }
    }
}

impl From<FiatOrder> for ops::Withdraw {
    fn from(d: FiatOrder) -> Self {
        Self {
            asset: d.fiat_currency,
            amount: d.amount,
            fee: d.transaction_fee + d.platform_fee,
            time: d.create_time,
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
impl ExchangeDataFetcher for BinanceFetcher<RegionGlobal> {
    async fn operations(&self) -> Result<Vec<Operation>> {
        Ok(Vec::new())
    }

    async fn trades(&self) -> Result<Vec<ops::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut trades: Vec<Result<Vec<binance::Trade>>> = Vec::new();
        let endpoint = EndpointsGlobal::Trades.to_string();

        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_trades(&endpoint, symbol.clone()));
                if handles.len() >= 10 {
                    trades.extend(join_all(handles).await);
                    handles = Vec::new();
                }
            }
        }
        if handles.len() > 0 {
            trades.extend(join_all(handles).await);
        }
        flatten_results(trades)
    }

    async fn margin_trades(&self) -> Result<Vec<ops::Trade>> {
        // Processing binance margin trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_margin_trades(symbol.clone()));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn loans(&self) -> Result<Vec<ops::Loan>> {
        let mut handles = Vec::new();
        let exchange_symbols = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();

        let mut processed_assets = HashSet::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = binance::symbol_into_assets(&symbol, &exchange_symbols);
                if !processed_assets.contains(&asset) {
                    // fetch cross-margin loans
                    handles.push(self.fetch_margin_loans(asset.clone(), None));
                    processed_assets.insert(asset.clone());
                }
                // fetch margin isolated loans
                handles.push(self.fetch_margin_loans(asset, Some(symbol)));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn repays(&self) -> Result<Vec<ops::Repay>> {
        let mut handles = Vec::new();
        let exchange_symbols = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();

        let mut processed_assets = HashSet::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol) {
                let (asset, _) = binance::symbol_into_assets(&symbol, &exchange_symbols);
                if !processed_assets.contains(&asset) {
                    // fetch cross-margin repays
                    handles.push(self.fetch_margin_repays(asset.clone(), None));
                    processed_assets.insert(asset.clone());
                }
                // fetch margin isolated repays
                handles.push(self.fetch_margin_repays(asset, Some(symbol)));
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
        let mut withdraws = Vec::new();

        withdraws.extend(
            self.fetch_fiat_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Withdraw>>(),
        );

        withdraws.extend(
            self.fetch_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Withdraw>>(),
        );

        Ok(withdraws)
    }
}

#[async_trait]
impl ExchangeDataFetcher for BinanceFetcher<RegionUs> {
    async fn operations(&self) -> Result<Vec<Operation>> {
        Ok(Vec::new())
    }

    async fn trades(&self) -> Result<Vec<ops::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&EndpointsUs::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let endpoint = EndpointsUs::Trades.to_string();
        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol) {
                handles.push(self.fetch_trades(&endpoint, symbol.clone()));
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
        let mut withdraws = Vec::new();

        withdraws.extend(
            self.fetch_fiat_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Withdraw>>(),
        );
        withdraws.extend(
            self.fetch_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<ops::Withdraw>>(),
        );

        Ok(withdraws)
    }
}

#[async_trait]
impl AssetsInfo for BinanceFetcher<RegionGlobal> {
    async fn price_at(&self, symbol: &str, time: DateTime<Utc>) -> Result<f64> {
        self.fetch_price_at(&EndpointsGlobal::Prices.to_string(), symbol, time)
            .await
        //.map_err(|err| anyhow!(err.to_string()).context(Error::FetchFailed))
    }
}

fn flatten_results<T, U>(results: Vec<Result<Vec<T>>>) -> Result<Vec<U>>
where
    T: Into<U>,
{
    Ok(results
        .into_iter()
        .collect::<Result<Vec<Vec<T>>>>()?
        .into_iter()
        .flatten()
        .map(|x| x.into())
        .collect())
}
