use std::{
    collections::{HashMap, HashSet},
    fmt::Display,
};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};
use tracing::{span, Level};

use market::{self, Asset, Market, MarketData};

use crate::ExchangeDataFetcher;

#[derive(Clone, Debug, Deserialize)]
pub enum OperationStatus {
    Success,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetBalance {
    pub amount: f64,
    pub usd_position: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Amount {
    pub value: f64,
    pub asset: Asset,
}

impl Amount {
    pub fn new<T>(value: f64, asset: T) -> Self
    where
        T: Into<Asset>,
    {
        Self {
            value,
            asset: asset.into(),
        }
    }
}

impl Display for Amount {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}{}", self.value, self.asset)
    }
}

/// Types of operations used to express any type of transaction
#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Acquire {
        source_id: String,
        source: String,
        amount: Amount,
        price: Amount,
        costs: Option<Vec<Amount>>,
        time: DateTime<Utc>,
    },
    Dispose {
        source_id: String,
        source: String,
        amount: Amount,
        price: Amount,
        costs: Option<Vec<Amount>>,
        time: DateTime<Utc>,
    },
    Send {
        source_id: String,
        source: String,
        amount: Amount,
        sender: Option<String>,
        recipient: Option<String>,
        costs: Option<Vec<Amount>>,
        time: DateTime<Utc>,
    },
    Receive {
        source_id: String,
        source: String,
        amount: Amount,
        sender: Option<String>,
        recipient: Option<String>,
        costs: Option<Vec<Amount>>,
        time: DateTime<Utc>,
    },
}

impl Operation {
    fn id(&self) -> String {
        match self {
            Self::Acquire { source_id, .. } => format!("asset-acquire-{source_id}"),
            Self::Dispose { source_id, .. } => format!("balance_decrease-{source_id}"),
            Self::Send { source_id, .. } => format!("send-{source_id}"),
            Self::Receive { source_id, .. } => format!("receive-{source_id}"),
        }
    }

    pub fn time(&self) -> &DateTime<Utc> {
        match self {
            Self::Acquire { time, .. }
            | Self::Dispose { time, .. }
            | Self::Send { time, .. }
            | Self::Receive { time, .. } => time,
        }
    }

    pub fn amount(&self) -> &Amount {
        match self {
            Self::Acquire { amount, .. }
            | Self::Dispose { amount, .. }
            | Self::Send { amount, .. }
            | Self::Receive { amount, .. } => amount,
        }
    }

    pub fn costs(&self) -> &Option<Vec<Amount>> {
        match self {
            Self::Acquire { costs, .. }
            | Self::Dispose { costs, .. }
            | Self::Send { costs, .. }
            | Self::Receive { costs, .. } => costs,
        }
    }

    pub fn source(&self) -> &str {
        match self {
            Self::Acquire { source, .. }
            | Self::Dispose { source, .. }
            | Self::Send { source, .. }
            | Self::Receive { source, .. } => source,
        }
    }
}

pub struct BalanceTracker<T> {
    coin_balances: RwLock<HashMap<String, AssetBalance>>,
    operations_seen: RwLock<HashSet<String>>,
    batch: RwLock<Vec<Operation>>,
    market_data: T,
}

impl<T: MarketData> BalanceTracker<T> {
    pub fn new(asset_info: T) -> Self {
        BalanceTracker {
            coin_balances: RwLock::new(HashMap::new()),
            operations_seen: RwLock::new(HashSet::new()),
            batch: RwLock::new(Vec::new()),
            market_data: asset_info,
        }
    }

    async fn track_costs(
        &self,
        amount: &Amount,
        costs: &Vec<Amount>,
        time: &DateTime<Utc>,
        balance: &mut HashMap<String, AssetBalance>,
    ) -> Result<()> {
        let coin_balance = balance.entry(amount.asset.clone()).or_default();
        for cost in costs {
            let span = span!(Level::DEBUG, "tracking asset acquisition cost");
            let _enter = span.enter();
            let usd_market = Market::new(cost.asset.clone(), "USD");
            let usd_price = market::price(&self.market_data, &usd_market, &time)
                .await?
                .ok_or_else(|| anyhow!("couldn't find price for {:?}", usd_market))?;
            coin_balance.usd_position -= cost.value * usd_price;
        }
        Ok(())
    }

    async fn track_operation(
        &self,
        op: &Operation,
        balance: &mut HashMap<String, AssetBalance>,
    ) -> Result<()> {
        match op {
            Operation::Acquire {
                amount,
                price,
                costs,
                time,
                ..
            } => {
                let span = span!(Level::DEBUG, "tracking asset acquisition");
                let _enter = span.enter();
                assert!(
                    amount.value >= 0.0,
                    "asset acquisition operation amount can't be negative"
                );
                let coin_balance = balance.entry(amount.asset.clone()).or_default();
                coin_balance.amount += amount.value;

                let usd_price = market::usd_price(&self.market_data, &price.asset, time).await?;
                coin_balance.usd_position -= amount.value * price.value * usd_price;

                if let Some(costs) = costs {
                    span!(Level::DEBUG, "tracking asset acquisition cost");
                    self.track_costs(amount, costs, time, balance).await?;
                }
            }
            Operation::Dispose {
                amount,
                price,
                costs,
                time,
                ..
            } => {
                let span = span!(Level::DEBUG, "tracking asset disposal");
                let _enter = span.enter();
                assert!(
                    amount.value >= 0.0,
                    "asset disposal operation amount can't be negative"
                );
                let coin_balance = balance.entry(amount.asset.clone()).or_default();
                coin_balance.amount -= amount.value;

                let usd_price = market::usd_price(&self.market_data, &price.asset, &time).await?;
                let coin_balance = balance.entry(amount.asset.clone()).or_default();
                coin_balance.usd_position += amount.value * price.value * usd_price;

                if let Some(costs) = costs {
                    span!(Level::DEBUG, "tracking asset disposal cost");
                    self.track_costs(amount, costs, time, balance).await?;
                }
            }
            Operation::Send { amount, .. } => {
                let span = span!(Level::DEBUG, "tracking asset send");
                let _enter = span.enter();
                assert!(
                    amount.value >= 0.0,
                    "asset send operation amount can't be negative"
                );
                let coin_balance = balance.entry(amount.asset.clone()).or_default();
                coin_balance.amount += amount.value;
            }
            Operation::Receive { amount, .. } => {
                let span = span!(Level::DEBUG, "tracking asset receive");
                let _enter = span.enter();
                assert!(
                    amount.value >= 0.0,
                    "asset receive operation amount can't be negative"
                );
                let coin_balance = balance.entry(amount.asset.clone()).or_default();
                coin_balance.amount -= amount.value;
            }
        }
        Ok(())
    }

    pub async fn process_batch(&self) -> Result<()> {
        let mut bal = self.coin_balances.write().await;
        for op in self.batch.read().await.iter() {
            self.track_operation(op, &mut *bal).await?;
        }
        self.batch.write().await.clear();
        Ok(())
    }

    pub async fn batch_operation(&self, op: Operation) {
        if self.operations_seen.read().await.contains(&op.id()) {
            log::warn!("ignoring duplicate operation {:?}", op);
            return;
        }
        self.operations_seen.write().await.insert(op.id());
        self.batch.write().await.push(op);
    }

    pub async fn get_balance(&self, asset: &str) -> Option<AssetBalance> {
        self.coin_balances
            .read()
            .await
            .get(asset)
            .clone()
            .map(|v| v.clone())
    }

    pub async fn balances(&self) -> Vec<(String, AssetBalance)> {
        self.coin_balances
            .read()
            .await
            .clone()
            .into_iter()
            .collect()
    }
}

async fn ops_from_fetcher<'a>(
    prefix: &'a str,
    c: Box<dyn ExchangeDataFetcher + Send + Sync>,
) -> Result<Vec<Operation>> {
    let mut all_ops: Vec<Operation> = Vec::new();
    log::info!("[{}] fetching trades...", prefix);
    all_ops.extend(
        c.trades()
            .await?
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching margin trades...", prefix);
    all_ops.extend(
        c.margin_trades()
            .await?
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching loans...", prefix);
    all_ops.extend(
        c.loans()
            .await?
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching repays...", prefix);
    all_ops.extend(
        c.repays()
            .await?
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching deposits...", prefix);
    all_ops.extend(
        c.deposits()
            .await?
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] fetching withdraws...", prefix);
    all_ops.extend(
        c.withdraws()
            .await?
            .into_iter()
            .flat_map(|t| -> Vec<Operation> { t.into() }),
    );
    log::info!("[{}] ALL DONE!!!", prefix);
    Ok(all_ops)
}

pub async fn fetch_ops<'a>(
    fetchers: Vec<(&'static str, Box<dyn ExchangeDataFetcher + Send + Sync>)>,
) -> mpsc::Receiver<Operation> {
    let (tx, rx) = mpsc::channel(100_000);

    for (name, f) in fetchers.into_iter() {
        let txc = tx.clone();
        // tokio::spawn(async move {
        match ops_from_fetcher(name, f).await {
            Ok(ops) => {
                for op in ops {
                    match txc.send(op).await {
                        Ok(()) => (),
                        Err(err) => log::error!("could not send operation: {}", err),
                    }
                }
                log::debug!("finished sending ops for fetcher {}", name);
            }
            Err(err) => {
                log::error!("failed to fetch operations from {}: {}", name, err);
                break;
            }
        };
        // });
    }

    rx
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Deposit, Loan, Repay, Status, Trade, TradeSide, Withdraw};
    use Operation::*;

    use async_trait::async_trait;
    use chrono::TimeZone;
    use proptest::{option::of, prelude::*, sample::select};
    use tokio::sync::Mutex;

    prop_compose! {
        fn datetime()(year in 2000..2010i32, month in 1..12u32, day in 1..28u32, hour in 0..23u32, minute in 0..59u32) -> DateTime<Utc> {
            Utc.with_ymd_and_hms(year, month, day, hour, minute, 0).unwrap()
        }
    }

    prop_compose! {
        fn trade(side: TradeSide)(
            source_id in any::<u8>().prop_map(|v| v.to_string()),
            source in "test",
            side in prop_oneof![Just(side)],
            base_asset in select(vec!["ADA", "SOL", "MATIC"]).prop_map(|a| a.to_string()),
            quote_asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            price in 0.1 .. 100000f64,
            amount in 0.1 .. 1000f64,
            fee in 0.0 .. 1000f64,
            fee_asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            time in datetime(),
        ) -> Trade {
            Trade {
                source_id,
                source,
                side,
                symbol: format!("{base_asset}{quote_asset}"),
                base_asset,
                quote_asset,
                price,
                amount,
                fee,
                fee_asset,
                time,
            }
        }
    }

    prop_compose! {
        fn deposit()(
            source_id in any::<u8>().prop_map(|v| v.to_string()),
            source in "test",
            asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            amount in 0.1 .. 1000f64,
            fee in of(0.0 .. 1000f64),
            fee_asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            time in datetime(),
        ) -> Deposit {
            let is_fiat = &asset == "USD";
            Deposit {
                source_id,
                source,
                asset,
                amount,
                time,
                fee,
                is_fiat
            }
        }
    }

    prop_compose! {
        fn withdraw()(
            source_id in any::<u8>().prop_map(|v| v.to_string()),
            source in "test",
            asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            amount in 0.1 .. 1000f64,
            fee in 0.0 .. 1000f64,
            fee_asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            time in datetime(),
        ) -> Withdraw {
            Withdraw {
                source_id,
                source,
                asset,
                amount,
                time,
                fee,
            }
        }
    }

    prop_compose! {
        fn loan()(
            source_id in any::<u8>().prop_map(|v| v.to_string()),
            source in "test",
            asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            amount in 0.1 .. 1000f64,
            time in datetime(),
            status in prop_oneof![Just(Status::Success)]
        ) -> Loan {
            Loan {
                source_id,
                source,
                asset,
                amount,
                time,
                status
            }
        }
    }

    prop_compose! {
        fn repay()(
            source_id in any::<u8>().prop_map(|v| v.to_string()),
            source in "test",
            asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
            amount in 0.1 .. 1000f64,
            interest in 0.01 .. 10f64,
            time in datetime(),
            status in prop_oneof![Just(Status::Success), Just(Status::Failure)]
        ) -> Repay {
            Repay {
                source_id,
                source,
                asset,
                amount,
                interest,
                time,
                status
            }
        }
    }

    proptest! {
        #[test]
        fn trade_buy_into_operations(trade in trade(TradeSide::Buy)) {
            let t = trade.clone();
            let ops: Vec<Operation> = trade.into();

            let mut expected_num_ops = 2;
            if t.fee > 0.0 {
                expected_num_ops += 1;
            }
            prop_assert_eq!(ops.len(), expected_num_ops);

            match &ops[0] {
                Acquire { amount, price, costs, .. } => {
                    prop_assert_eq!(&amount.asset, &t.base_asset);
                    prop_assert_eq!(amount.value, t.base_amount());
                    prop_assert_eq!(&price.asset, &t.quote_asset);
                    prop_assert_eq!(price.value, t.quote_amount());

                    if t.fee > 0.0 {
                        let costs = costs.as_ref().unwrap();
                        prop_assert_eq!(costs.len(), 1);
                        let cost = &costs[0];

                        prop_assert_eq!(&cost.asset, &t.fee_asset);
                        prop_assert_eq!(cost.value, t.fee);
                    } else {
                        prop_assert!(!matches!(costs, None));
                    }
                }
                _ => {
                    prop_assert!(false, "op 1 is not Acquire");
                }
            }

            match &ops[1] {
                Dispose { amount, price, costs, .. } => {
                    prop_assert_eq!(&amount.asset, &t.quote_asset);
                    prop_assert_eq!(amount.value, t.quote_amount());
                    prop_assert_eq!(&price.asset, &t.base_asset);
                    prop_assert_eq!(price.value, t.base_amount());
                    prop_assert!(matches!(costs, None));
                }
                _ => {
                    prop_assert!(false, "op 2 is not Dispose");
                }
            }

            if t.fee > 0.0 {
                match &ops[2] {
                    Dispose { amount, price, costs, .. } => {
                        prop_assert_eq!(&amount.asset, &t.fee_asset);
                        prop_assert_eq!(amount.value, t.fee);
                        prop_assert_eq!(&price.asset, &t.fee_asset);
                        prop_assert_eq!(price.value, t.fee);
                        prop_assert!(matches!(costs, None));
                    }
                    _ => {
                        prop_assert!(false, "op 3 is not Dispose");
                    }
                }
            }
        }
    }

    proptest! {
        #[test]
        fn trade_sell_into_operations(trade in trade(TradeSide::Sell)) {
            let t = trade.clone();
            let ops: Vec<Operation> = trade.into();

            let mut expected_num_ops = 2;
            if t.fee > 0.0 {
                expected_num_ops += 1;
            }
            prop_assert_eq!(ops.len(), expected_num_ops);

            match &ops[0] {
                Dispose { amount, price, costs, .. } => {
                    prop_assert_eq!(&amount.asset, &t.base_asset);
                    prop_assert_eq!(amount.value, t.base_amount());
                    prop_assert_eq!(&price.asset, &t.quote_asset);
                    prop_assert_eq!(price.value, t.quote_amount());

                    if t.fee > 0.0 {
                        let costs = costs.as_ref().unwrap();
                        let cost = &costs[0];
                        prop_assert_eq!(&cost.asset, &t.fee_asset);
                        prop_assert_eq!(cost.value, t.fee);
                    }

                }
                _ => {
                    prop_assert!(false, "op 1 is not Dispose");
                }
            }

            match &ops[1] {
                Acquire { amount, price, costs, .. } => {
                    prop_assert_eq!(&amount.asset, &t.quote_asset);
                    prop_assert_eq!(amount.value, t.quote_amount());
                    prop_assert_eq!(&price.asset, &t.base_asset);
                    prop_assert_eq!(price.value, t.base_amount());
                    prop_assert!(matches!(costs, None));
                }
                _ => {
                    prop_assert!(false, "op 2 is not Acquire");
                }
            }

            if t.fee > 0.0 {
                match &ops[2] {
                    Dispose { amount, price, costs, .. } => {
                        prop_assert_eq!(&amount.asset, &t.fee_asset);
                        prop_assert_eq!(amount.value, t.fee);
                        prop_assert_eq!(&price.asset, &t.fee_asset);
                        prop_assert_eq!(price.value, t.fee);
                        prop_assert!(matches!(costs, None));
                    }
                    _ => prop_assert!(false, "op 3 is not Dispose")
                }
            }
        }
    }

    proptest! {
        #[test]
        fn deposit_into_operations(deposit in deposit()) {
            let d = deposit.clone();
            let ops: Vec<Operation> = deposit.into();

            let num_ops = if d.fee.filter(|f| f > &0.0).is_some() {
                2
            } else {
                1
            };

            prop_assert_eq!(
                ops.len(),
                num_ops,
                "incorrect number of ops expected {} got {}",
                num_ops,
                ops.len()
            );

            if let Receive { amount, costs, .. } = &ops[0] {
                prop_assert_eq!(&amount.asset, &d.asset);
                prop_assert_eq!(amount.value, d.amount);

                if let Some(fee) = d.fee.filter(|f| *f > 0.0) {
                    prop_assert!(matches!(costs, Some(..)));
                    let costs = costs.as_ref().unwrap();
                    prop_assert_eq!(costs.len(), 1);
                    let cost = &costs[0];
                    prop_assert_eq!(&cost.asset, &amount.asset);
                    prop_assert_eq!(cost.value, fee);
                }
            } else {
                prop_assert!(false, "op 1 is not Receive")
            }

            if let Some(fee) = d.fee.filter(|f| *f > 0.0) {
                if let Dispose { amount: fee_amount, costs, .. } = &ops[1] {
                    prop_assert_eq!(&fee_amount.asset, &d.asset);
                    prop_assert_eq!(fee_amount.value, fee);
                    prop_assert!(matches!(costs, None));
                } else {
                    prop_assert!(false, "op 2 is not Dispose");
                }
            }
        }
    }

    proptest! {
        #[test]
        fn withdraw_into_operations(withdraw in withdraw()) {
            let w = withdraw.clone();
            let ops: Vec<Operation> = withdraw.into();

            let num_ops = if w.fee > 0.0 {
                2
            } else {
                1
            };

            prop_assert_eq!(
                ops.len(),
                num_ops,
                "incorrect number of ops expected {} got {}",
                num_ops,
                ops.len()
            );

            if let Send { amount, costs, .. } = &ops[0] {
                prop_assert_eq!(&amount.asset, &w.asset);
                prop_assert_eq!(amount.value, w.amount);

                if w.fee > 0.0 {
                    prop_assert!(matches!(costs, Some(..)));
                    let costs = costs.as_ref().unwrap();
                    prop_assert_eq!(costs.len(), 1);
                    let cost = &costs[0];
                    prop_assert_eq!(&cost.asset, &amount.asset);
                    prop_assert_eq!(cost.value, w.fee);
                }
            } else {
                prop_assert!(false, "op 1 is not Send")
            }

            if w.fee > 0.0 {
                if let Dispose { amount: fee_amount, costs, .. } = &ops[1] {
                    prop_assert_eq!(&fee_amount.asset, &w.asset);
                    prop_assert_eq!(fee_amount.value, w.fee);
                    prop_assert!(matches!(costs, None));
                } else {
                    prop_assert!(false, "op 2 is not Dispose");
                }
            }
        }
    }

    proptest! {
        fn loan_into_operations(loan in loan()) {
            let l = loan.clone();
            let ops: Vec<Operation> = loan.into();

            let num_ops = match l.status {
                Status::Success => 1,
                Status::Failure => 0,
            };
            prop_assert_eq!(ops.len(), num_ops);
            if num_ops > 0 {
                if let Acquire { amount, price, costs, .. } = &ops[0] {
                    prop_assert_eq!(&amount.asset, &l.asset);
                    prop_assert_eq!(amount.value, l.amount);
                    prop_assert_eq!(&price.asset, &l.asset);
                    prop_assert_eq!(price.value, 0.0);
                    prop_assert!(matches!(costs, None));
                } else {
                    prop_assert!(false, "op 1 is not Acquire");
                }
            }
        }
    }

    proptest! {
        fn repay_into_operations(repay in repay()) {
            let r = repay.clone();
            let ops: Vec<Operation> = repay.into();

            let num_ops = match r.status {
                Status::Success => 1,
                Status::Failure => 0,
            };
            prop_assert_eq!(ops.len(), num_ops);
            if num_ops > 0 {
                if let Dispose { amount, price, costs, .. } = &ops[0] {
                    prop_assert_eq!(&amount.asset, &r.asset);
                    prop_assert_eq!(amount.value, r.amount + r.interest);
                    prop_assert_eq!(&price.asset, &r.asset);
                    prop_assert_eq!(price.value, 0.0);
                    prop_assert!(matches!(costs, None));
                } else {
                    prop_assert!(false, "op 1 is not Dispose");
                }
            }
        }
    }

    #[tokio::test]
    async fn track_operations() -> Result<()> {
        struct TestMarketData {
            prices: Mutex<Vec<f64>>,
        }

        impl TestMarketData {
            fn new() -> Self {
                Self {
                    prices: Mutex::new(vec![7000.0, 25.0, 95.0]),
                }
            }
        }

        #[async_trait]
        impl MarketData for TestMarketData {
            async fn has_market(&self, market: &Market) -> Result<bool> {
                Ok(true)
            }

            fn normalize(&self, market: &Market) -> Result<Market> {
                Ok(market.clone())
            }

            async fn markets(&self) -> Result<Vec<Market>> {
                Ok(vec![
                    Market::new("BTC", "USD"),                    
                    Market::new("ETH", "USD"),                    
                    Market::new("DOT", "USD"),                    
                ])
            }

            async fn price_at(&self, market: &Market, _time: &DateTime<Utc>) -> Result<f64> {
                Ok(match (market.base.as_str(), market.quote.as_str()) {
                    ("USDT" | "USD", "USDT" | "USD") => 1.0,
                    _ => self.prices.lock().await.remove(0),
                })
            }
        }

        let coin_tracker = BalanceTracker::new(TestMarketData::new());
        let ops = vec![
            Operation::Acquire {
                source_id: "1".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.03, "BTC"),
                price: Amount::new(255.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Acquire {
                source_id: "3".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.1, "BTC"),
                price: Amount::new(890.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Acquire {
                source_id: "5".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.5, "ETH"),
                price: Amount::new(1000.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Acquire {
                source_id: "7".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.01, "ETH"),
                price: Amount::new(21.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Dispose {
                source_id: "9".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.2, "ETH"),
                price: Amount::new(1000.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Acquire {
                source_id: "11".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.5, "DOT"),
                price: Amount::new(7.5, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Dispose {
                source_id: "13".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.1, "DOT"),
                price: Amount::new(15.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
            Operation::Dispose {
                source_id: "15".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.2, "DOT"),
                price: Amount::new(13.0, "USD"),
                costs: None,
                time: Utc::now(),
            },
        ];

        for op in ops {
            coin_tracker.batch_operation(op).await;
        }
        coin_tracker.process_batch().await?;

        let mut expected = vec![
            (
                "BTC".to_string(),
                AssetBalance {
                    amount: 0.13,
                    usd_position: -0.03 * 255. - 0.1 * 890.,
                },
            ),
            (
                "ETH".to_string(),
                AssetBalance {
                    amount: 0.31,
                    usd_position: -0.5 * 1000. - 0.01 * 21. + 0.2 * 1000.,
                },
            ),
            (
                "DOT".to_string(),
                AssetBalance {
                    amount: 0.2,
                    usd_position: -0.5 * 7.5 + 0.1 * 15. + 0.2 * 13.,
                },
            ),
        ];

        expected.sort_by_key(|x| x.0.clone());

        let mut balances = coin_tracker.balances().await;
        balances.sort_by_key(|x| x.0.clone());

        for ((asset_a, balance_a), (asset_b, balance_b)) in expected.iter().zip(balances.iter()) {
            assert_eq!(asset_a, asset_b);
            assert_eq!(balance_a, balance_b);
        }

        Ok(())
    }
}
