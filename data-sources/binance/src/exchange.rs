use chrono::{DateTime, Utc};
use std::{collections::HashSet, vec};

use anyhow::Result;
use async_trait::async_trait;
use futures::prelude::*;

use crate::{
    api_model::{Deposit, FiatOrder, MarginLoan, MarginRepay, Trade, Withdraw},
    client::{ApiGlobal, ApiUs, BinanceFetcher, RegionGlobal, RegionUs},
};
use exchange::{
    self,
    operations::{into_ops, Operation},
    Candle, ExchangeClient, ExchangeDataFetcher,
};
use market::Market;

impl From<FiatOrder> for exchange::Deposit {
    fn from(d: FiatOrder) -> Self {
        Self {
            source_id: d.id,
            source: "binance".to_string(),
            asset: d.fiat_currency,
            amount: d.amount,
            fee: Some(d.platform_fee),
            time: d.create_time,
            is_fiat: true,
        }
    }
}

impl From<FiatOrder> for exchange::Withdraw {
    fn from(d: FiatOrder) -> Self {
        Self {
            source_id: d.id,
            source: "binance".to_string(),
            asset: d.fiat_currency,
            amount: d.amount,
            fee: d.transaction_fee + d.platform_fee,
            time: d.create_time,
        }
    }
}

impl From<Deposit> for exchange::Deposit {
    fn from(d: Deposit) -> Self {
        Self {
            source_id: d.tx_id,
            source: "binance".to_string(),
            asset: d.coin,
            amount: d.amount,
            fee: None,
            time: d.insert_time,
            is_fiat: false,
        }
    }
}

impl From<Withdraw> for exchange::Withdraw {
    fn from(w: Withdraw) -> Self {
        Self {
            source_id: w.id,
            source: "binance".to_string(),
            asset: w.coin,
            amount: w.amount,
            time: w.apply_time,
            fee: w.transaction_fee,
        }
    }
}

impl From<Trade> for exchange::Trade {
    fn from(t: Trade) -> Self {
        Self {
            source_id: t.id.to_string(),
            source: "binance".to_string(),
            symbol: t.symbol,
            base_asset: t.base_asset.expect("missing base_asset in trade"),
            quote_asset: t.quote_asset.expect("missing quote_asset in trade"),
            price: t.price,
            amount: t.qty,
            fee: t.commission,
            fee_asset: t.commission_asset,
            time: t.time,
            side: if t.is_buyer {
                exchange::TradeSide::Buy
            } else {
                exchange::TradeSide::Sell
            },
        }
    }
}

impl From<MarginLoan> for exchange::Loan {
    fn from(m: MarginLoan) -> Self {
        Self {
            source_id: m.tx_id.to_string(),
            source: "binance".to_string(),
            asset: m.asset,
            amount: m.principal,
            time: m.timestamp,
            status: match m.status.as_str() {
                "CONFIRMED" => exchange::Status::Success,
                _ => exchange::Status::Failure,
            },
        }
    }
}

impl From<MarginRepay> for exchange::Repay {
    fn from(r: MarginRepay) -> Self {
        Self {
            source_id: r.tx_id.to_string(),
            source: "binance".to_string(),
            asset: r.asset,
            amount: r.principal,
            interest: r.interest,
            time: r.timestamp,
            status: match r.status.as_str() {
                "CONFIRMED" => exchange::Status::Success,
                _ => exchange::Status::Failure,
            },
        }
    }
}

impl BinanceFetcher<RegionGlobal> {
    async fn trades(&self) -> Result<Vec<exchange::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&ApiGlobal::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let endpoint = ApiGlobal::Trades.to_string();

        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                handles.push(self.fetch_trades(&endpoint, symbol));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn margin_trades(&self) -> Result<Vec<exchange::Trade>> {
        // Processing binance margin trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&ApiGlobal::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                handles.push(self.fetch_margin_trades(symbol));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn loans(&self) -> Result<Vec<exchange::Loan>> {
        let exchange_symbols = self
            .fetch_exchange_symbols(&ApiGlobal::ExchangeInfo.to_string())
            .await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();

        let mut handles = vec![];
        let mut processed_assets = HashSet::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                if !processed_assets.contains(&symbol.base) {
                    // fetch cross-margin loans
                    handles.push(self.fetch_margin_loans(&symbol.base, None));
                    processed_assets.insert(&symbol.base);
                }
                // fetch margin isolated loans
                handles.push(self.fetch_margin_loans(&symbol.base, Some(symbol)));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn repays(&self) -> Result<Vec<exchange::Repay>> {
        let mut handles = Vec::new();
        let exchange_symbols = self
            .fetch_exchange_symbols(&ApiGlobal::ExchangeInfo.to_string())
            .await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();

        let mut processed_assets = HashSet::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                if !processed_assets.contains(&symbol.base) {
                    // fetch cross-margin repays
                    handles.push(self.fetch_margin_repays(&symbol.base, None));
                    processed_assets.insert(symbol.base.clone());
                }
                // fetch margin isolated repays
                handles.push(self.fetch_margin_repays(&symbol.base, Some(symbol)));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn deposits(&self) -> Result<Vec<exchange::Deposit>> {
        let mut deposits = Vec::new();

        deposits.extend(
            self.fetch_fiat_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Deposit>>(),
        );
        deposits.extend(
            self.fetch_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Deposit>>(),
        );

        Ok(deposits)
    }

    async fn withdrawals(&self) -> Result<Vec<exchange::Withdraw>> {
        let mut withdrawals = Vec::new();

        withdrawals.extend(
            self.fetch_fiat_withdrawals()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );

        withdrawals.extend(
            self.fetch_withdrawals()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );

        Ok(withdrawals)
    }
}

#[async_trait]
impl ExchangeDataFetcher for BinanceFetcher<RegionGlobal> {
    async fn sync<S>(&self, _storage: S) -> Result<()>
    where
        S: data_sync::OperationStorage + Send + Sync,
    {
        let mut operations = Vec::new();
        log::info!("[binance] fetching trades...");
        operations.extend(into_ops(self.trades().await?));
        log::info!("[binance] fetching margin trades...");
        operations.extend(into_ops(self.margin_trades().await?));
        log::info!("[binance] fetching loans...");
        operations.extend(into_ops(self.loans().await?));
        log::info!("[binance] fetching repays...");
        operations.extend(into_ops(self.repays().await?));
        log::info!("[binance] fetching deposits...");
        operations.extend(into_ops(self.deposits().await?));
        log::info!("[binance] fetching withdrawals...");
        operations.extend(into_ops(self.withdrawals().await?));
        log::info!("[binance] ALL DONE!!!");
        Ok(())
    }
}

#[async_trait]
impl ExchangeClient for BinanceFetcher<RegionGlobal> {
    async fn prices(
        &self,
        market: &Market,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Candle>> {
        self.fetch_prices_in_range(
            &ApiGlobal::Klines.to_string(),
            market,
            start.timestamp_millis().try_into()?,
            end.timestamp_millis().try_into()?,
        )
        .await
    }
}

impl BinanceFetcher<RegionUs> {
    async fn trades(&self) -> Result<Vec<exchange::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&ApiUs::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let endpoint = ApiUs::Trades.to_string();
        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                handles.push(self.fetch_trades(&endpoint, &symbol));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn deposits(&self) -> Result<Vec<exchange::Deposit>> {
        let mut deposits = Vec::new();

        deposits.extend(
            self.fetch_fiat_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Deposit>>(),
        );
        deposits.extend(
            self.fetch_deposits()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Deposit>>(),
        );

        Ok(deposits)
    }

    async fn withdrawals(&self) -> Result<Vec<exchange::Withdraw>> {
        let mut withdrawals = Vec::new();

        withdrawals.extend(
            self.fetch_fiat_withdrawals()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );
        withdrawals.extend(
            self.fetch_withdrawals()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );

        Ok(withdrawals)
    }
}

#[async_trait]
impl ExchangeDataFetcher for BinanceFetcher<RegionUs> {
    async fn sync<S>(&self, _storage: S) -> Result<()>
    where
        S: data_sync::OperationStorage + Send + Sync,
    {
        let mut operations: Vec<Operation> = Vec::new();

        let trades = self
            .trades()
            .await?
            .into_iter()
            .map(|x| x.into())
            .collect::<Vec<exchange::Trade>>();
        let deposits = self
            .deposits()
            .await?
            .into_iter()
            .map(|x| x.into())
            .collect::<Vec<exchange::Deposit>>();
        let withdrawals = self
            .withdrawals()
            .await?
            .into_iter()
            .map(|x| x.into())
            .collect::<Vec<exchange::Withdraw>>();

        log::info!("[binance US] fetching trades...");
        operations.extend(
            trades
                .into_iter()
                .flat_map(|x| -> Vec<Operation> { x.into() }),
        );
        log::info!("[binance US] fetching deposits...");
        operations.extend(
            deposits
                .into_iter()
                .flat_map(|x| -> Vec<Operation> { x.into() }),
        );
        log::info!("[binance US] fetching withdrawals...");
        operations.extend(
            withdrawals
                .into_iter()
                .flat_map(|x| -> Vec<Operation> { x.into() }),
        );
        log::info!("[binance US] ALL DONE!!!");
        Ok(())
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
