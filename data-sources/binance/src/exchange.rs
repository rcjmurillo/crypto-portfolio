use std::{collections::HashSet, vec};

use anyhow::Result;
use async_trait::async_trait;
use futures::prelude::*;
use serde_json::Value;

use crate::{
    api_model::{Deposit, FiatOrder, MarginLoan, MarginRepay, Trade, Withdraw},
    client::{ApiGlobal, ApiUs, BinanceFetcher, RegionGlobal, RegionUs},
};
use data_sync::DataFetcher;
use operations::Operation;

pub fn into_ops<T>(typed_ops: Vec<T>) -> Vec<Operation>
where
    T: Into<Vec<Operation>>,
{
    typed_ops.into_iter().flat_map(|s| s.into()).collect()
}

impl From<FiatOrder> for operations::Deposit {
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

impl From<FiatOrder> for operations::Withdraw {
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

impl From<Deposit> for operations::Deposit {
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

impl From<Withdraw> for operations::Withdraw {
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

impl From<Trade> for operations::Trade {
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
                operations::TradeSide::Buy
            } else {
                operations::TradeSide::Sell
            },
        }
    }
}

impl From<MarginLoan> for operations::Loan {
    fn from(m: MarginLoan) -> Self {
        Self {
            source_id: m.tx_id.to_string(),
            source: "binance".to_string(),
            asset: m.asset,
            amount: m.principal,
            time: m.timestamp,
            status: match m.status.as_str() {
                "CONFIRMED" => operations::Status::Success,
                _ => operations::Status::Failure,
            },
        }
    }
}

impl From<MarginRepay> for operations::Repay {
    fn from(r: MarginRepay) -> Self {
        Self {
            source_id: r.tx_id.to_string(),
            source: "binance".to_string(),
            asset: r.asset,
            amount: r.principal,
            interest: r.interest,
            time: r.timestamp,
            status: match r.status.as_str() {
                "CONFIRMED" => operations::Status::Success,
                _ => operations::Status::Failure,
            },
        }
    }
}

impl BinanceFetcher<RegionGlobal> {
    async fn trades(
        &self,
        last_record: Option<&data_sync::Record>,
    ) -> Result<Vec<serde_json::Value>> {
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
                handles.push(self.fetch_trades_from_endpoint(
                    symbol,
                    &endpoint,
                    None,
                    last_record.clone(),
                ));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn margin_trades(&self, last_record: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        // Processing binance margin trades
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
                handles.push(self.fetch_trades_from_endpoint(symbol, &endpoint, None, last_record));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn margin_transactions(
        &self,
        endpoint: &str,
        last_record: Option<&data_sync::Record>,
    ) -> Result<Vec<Value>> {
        let exchange_symbols = self
            .fetch_exchange_symbols(&ApiGlobal::ExchangeInfo.to_string())
            .await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();
        let valid_symbols: Vec<&market::Market> = self
            .symbols()
            .iter()
            .filter(|s| all_symbols.contains(&s.join("")))
            .collect();
        let all_base_assets: HashSet<_> = valid_symbols.iter().map(|s| &s.base).collect();

        let mut handles = vec![];
        handles.extend(
            all_base_assets
                .iter()
                .map(|a| self.fetch_margin_transactions(*a, None, endpoint, last_record.clone())),
        );
        handles.extend(self.symbols().iter().map(|s| {
            self.fetch_margin_transactions(&s.base, Some(s), endpoint, last_record.clone())
        }));

        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }

    async fn loans(&self, last_record: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        self.margin_transactions(ApiGlobal::MarginLoans.as_ref(), last_record)
            .await
    }

    async fn repays(&self, last_record: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        self.margin_transactions(ApiGlobal::MarginRepays.as_ref(), last_record)
            .await
    }

    async fn deposits(&self, last_record: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        let mut deposits = Vec::new();

        deposits.extend(self.fetch_fiat_deposits(last_record).await?);
        deposits.extend(self.fetch_deposits(last_record).await?);

        Ok(deposits)
    }

    async fn withdrawals(&self, last_record: Option<&data_sync::Record>) -> Result<Vec<Value>> {
        let mut withdrawals = Vec::new();
        withdrawals.extend(self.fetch_fiat_withdrawals(last_record).await?);
        withdrawals.extend(self.fetch_withdrawals(last_record).await?);
        Ok(withdrawals)
    }
}

#[async_trait]
impl DataFetcher for BinanceFetcher<RegionGlobal> {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: data_sync::RecordStorage + Send + Sync,
    {
        log::info!("[binance] fetching trades...");
        let last_record = storage.get_latest("binance", "trade")?;
        storage.insert(data_sync::into_records(
            &self.trades(last_record.as_ref()).await?,
            "trade",
            "binance",
            "id",
            "time",
        )?)?;
        log::info!("[binance] fetching margin trades...");
        let last_record = storage.get_latest("binance", "margin-trade")?;
        storage.insert(data_sync::into_records(
            &self.margin_trades(last_record.as_ref()).await?,
            "margin-trade",
            "binance",
            "id",
            "time",
        )?)?;
        log::info!("[binance] fetching loans...");
        let last_record = storage.get_latest("binance", "loan")?;
        storage.insert(data_sync::into_records(
            &self.loans(last_record.as_ref()).await?,
            "loan",
            "binance",
            "txId",
            "timestamp",
        )?)?;
        log::info!("[binance] fetching repays...");
        let last_record = storage.get_latest("binance", "repay")?;
        storage.insert(data_sync::into_records(
            &self.repays(last_record.as_ref()).await?,
            "repay",
            "binance",
            "txId",
            "timestamp",
        )?)?;
        log::info!("[binance] fetching deposits...");
        let last_record = storage.get_latest("binance", "deposit")?;
        storage.insert(data_sync::into_records(
            &self.deposits(last_record.as_ref()).await?,
            "deposit",
            "binance",
            "txId",
            "insert_time",
        )?)?;
        log::info!("[binance] fetching withdrawals...");
        let last_record = storage.get_latest("binance", "withdrawal")?;
        storage.insert(data_sync::into_records(
            &self.withdrawals(last_record.as_ref()).await?,
            "withdrawal",
            "binance",
            "txId",
            "applyTime",
        )?)?;

        log::info!("[binance] ALL DONE!!!");
        Ok(())
    }
}

impl BinanceFetcher<RegionUs> {
    async fn trades(&self, last_record: Option<&data_sync::Record>) -> Result<Vec<Value>> {
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
                handles.push(self.fetch_trades_from_endpoint(
                    &symbol,
                    &endpoint,
                    None,
                    last_record,
                ));
            }
        }
        flatten_results(
            stream::iter(handles)
                .buffer_unordered(500)
                .collect::<Vec<_>>()
                .await,
        )
    }
}

#[async_trait]
impl DataFetcher for BinanceFetcher<RegionUs> {
    async fn sync<S>(&self, storage: S) -> Result<()>
    where
        S: data_sync::RecordStorage + Send + Sync,
    {
        log::info!("[binance US] fetching trades...");
        let last_trade = storage.get_latest("binance-us", "trade")?;
        log::debug!("last trade: {:?}", last_trade);
        let trades = self.trades(last_trade.as_ref()).await?;
        storage.insert(data_sync::into_records(
            &trades,
            "trade",
            "binance-us",
            "id",
            "time",
        )?)?;

        log::info!("[binance US] fetching deposits...");
        let last_deposit = storage.get_latest("binance-us", "deposit")?;
        let deposits = self.fetch_deposits(last_deposit.as_ref()).await?;
        storage.insert(data_sync::into_records(
            &deposits,
            "deposit",
            "binance-us",
            "txId",
            "insertTime",
        )?)?;

        log::info!("[binance US] fetching fiat deposits...");
        let last_deposit = storage.get_latest("binance-us", "fiat-deposit")?;
        let deposits = self.fetch_fiat_deposits(last_deposit.as_ref()).await?;
        storage.insert(data_sync::into_records(
            &deposits,
            "fiat-deposit",
            "binance-us",
            "id",
            "createTime",
        )?)?;

        log::info!("[binance US] fetching withdrawals...");
        let last_withdrawal = storage.get_latest("binance-us", "withdrawal")?;
        let withdrawals = self.fetch_withdrawals(last_withdrawal.as_ref()).await?;
        storage.insert(data_sync::into_records(
            &withdrawals,
            "withdrawal",
            "binance-us",
            "id",
            "applyTime",
        )?)?;

        log::info!("[binance US] fetching fiat withdrawals...");
        let last_withdrawal = storage.get_latest("binance-us", "fiat-withdrawal")?;
        let withdrawals = self
            .fetch_fiat_withdrawals(last_withdrawal.as_ref())
            .await?;
        storage.insert(data_sync::into_records(
            &withdrawals,
            "fiat-withdrawal",
            "binance-us",
            "id ",
            "createTime",
        )?)?;

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
