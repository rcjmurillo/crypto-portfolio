use chrono::{DateTime, Utc};
use std::{collections::HashSet, vec};

use anyhow::Result;
use async_trait::async_trait;
use futures::future::join_all;

use crate::{
    api_model::{Deposit, FiatOrder, MarginLoan, MarginRepay, Trade, Withdraw},
    client::{BinanceFetcher, EndpointsGlobal, EndpointsUs, RegionGlobal, RegionUs},
};
use exchange::{self, Asset, AssetPair, AssetsInfo, Candle, ExchangeClient, ExchangeDataFetcher};

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
            source_id: d.id,
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
            base_asset: t.base_asset,
            quote_asset: t.quote_asset,
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

#[async_trait]
impl ExchangeDataFetcher for BinanceFetcher<RegionGlobal> {
    async fn trades(&self) -> Result<Vec<exchange::Trade>> {
        // Processing binance trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?
            .into_iter()
            .map(|x| x.symbol)
            .collect();

        let mut trades: Vec<Result<Vec<Trade>>> = Vec::new();
        let endpoint = EndpointsGlobal::Trades.to_string();

        let mut handles = Vec::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                handles.push(self.fetch_trades(&endpoint, symbol));
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

    async fn margin_trades(&self) -> Result<Vec<exchange::Trade>> {
        // Processing binance margin trades
        let all_symbols: Vec<String> = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
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
        flatten_results(join_all(handles).await)
    }

    async fn loans(&self) -> Result<Vec<exchange::Loan>> {
        let mut handles = Vec::new();
        let exchange_symbols = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
            .await?;
        let all_symbols: Vec<String> = exchange_symbols.iter().map(|x| x.symbol.clone()).collect();

        let mut loans = vec![];
        let mut processed_assets = HashSet::new();
        for symbol in self.symbols().iter() {
            if all_symbols.contains(&symbol.join("")) {
                if !processed_assets.contains(&symbol.base) {
                    // fetch cross-margin loans
                    handles.push(self.fetch_margin_loans(&symbol.base, None));
                    processed_assets.insert(&symbol.base);
                }
                // fetch margin isolated loans
                // handles.push(self.fetch_margin_loans(&symbol.base, Some(symbol)));
                loans.extend(
                    self.fetch_margin_loans(&symbol.base, Some(symbol))
                        .await?
                        .into_iter()
                        .map(|l| l.into())
                        .collect::<Vec<exchange::Loan>>(),
                );
            }
        }
        // flatten_results(join_all(handles).await)

        Ok(loans)
    }

    async fn repays(&self) -> Result<Vec<exchange::Repay>> {
        let mut handles = Vec::new();
        let exchange_symbols = self
            .fetch_exchange_symbols(&EndpointsGlobal::ExchangeInfo.to_string())
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
        flatten_results(join_all(handles).await)
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

    async fn withdraws(&self) -> Result<Vec<exchange::Withdraw>> {
        let mut withdraws = Vec::new();

        withdraws.extend(
            self.fetch_fiat_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );

        withdraws.extend(
            self.fetch_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );

        Ok(withdraws)
    }
}

#[async_trait]
impl ExchangeClient for BinanceFetcher<RegionGlobal> {
    async fn prices(
        &self,
        asset_pair: &AssetPair,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<Candle>> {
        self.fetch_prices_in_range(
            &EndpointsGlobal::Klines.to_string(),
            asset_pair,
            start.timestamp_millis().try_into()?,
            end.timestamp_millis().try_into()?,
        )
        .await
    }
}

#[async_trait]
impl ExchangeDataFetcher for BinanceFetcher<RegionUs> {
    async fn trades(&self) -> Result<Vec<exchange::Trade>> {
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
            if all_symbols.contains(&symbol.join("")) {
                handles.push(self.fetch_trades(&endpoint, &symbol));
            }
        }
        flatten_results(join_all(handles).await)
    }

    async fn margin_trades(&self) -> Result<Vec<exchange::Trade>> {
        Ok(Vec::new())
    }

    async fn loans(&self) -> Result<Vec<exchange::Loan>> {
        Ok(Vec::new())
    }

    async fn repays(&self) -> Result<Vec<exchange::Repay>> {
        Ok(Vec::new())
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

    async fn withdraws(&self) -> Result<Vec<exchange::Withdraw>> {
        let mut withdraws = Vec::new();

        withdraws.extend(
            self.fetch_fiat_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );
        withdraws.extend(
            self.fetch_withdraws()
                .await?
                .into_iter()
                .map(|x| x.into())
                .collect::<Vec<exchange::Withdraw>>(),
        );

        Ok(withdraws)
    }
}

#[async_trait]
impl AssetsInfo for BinanceFetcher<RegionGlobal> {
    async fn price_at(&self, asset_pair: &AssetPair, time: &DateTime<Utc>) -> Result<f64> {
        self.fetch_price_at(
            &EndpointsGlobal::Prices.to_string(),
            &asset_pair.join(""),
            time,
        )
        .await
    }

    async fn usd_price_at(&self, asset: &Asset, time: &DateTime<Utc>) -> Result<f64> {
        self.price_at(&AssetPair::new(asset, "USD"), time).await
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
