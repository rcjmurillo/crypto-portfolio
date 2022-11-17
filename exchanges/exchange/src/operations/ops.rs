use std::ascii::AsciiExt;
use std::collections::{HashMap, HashSet};
use std::convert::{TryFrom, TryInto};

use anyhow::{anyhow, Error, Result};
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use serde::Deserialize;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, span, Level};

use crate::operations::{db, storage::Storage};
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
    value: f64,
    asset: Asset,
}

/**
 * TODO: replace this with the following operations
 * Acquire -> acquire an asset, buy, airdrop, interest, etc.
 * Dispose -> diposes an assset either by selling, fees, etc.
 * Send -> send an asset from one wallet or entity to another
 * Receive -> receive an asset from another entity, wallet, etc.
 * Both operations will have an optional Cost,
 * Also amounts will be expresed as a new Amount(value, asset)
 */

/// Types of operations used to express any type of transaction
#[derive(Debug, Clone, PartialEq)]
pub enum Operation {
    Acquire {
        source_id: String,
        source: String,
        amount: Amount,
        costs: Option<Vec<Amount>>,
        time: DateTime<Utc>,
    },
    Dispose {
        source_id: String,
        source: String,
        amount: Amount,
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
        for cost in costs {
            let span = span!(Level::DEBUG, "tracking asset acquisition cost");
            let _enter = span.enter();
            let usd_market = Market::new(cost.asset.clone(), "USD");
            let usd_price = market::solve_price(&self.market_data, &usd_market, &time)
                .await?
                .ok_or_else(|| anyhow!("couldn't find price for {:?}", usd_market))?;
            let coin_balance = balance.entry(amount.asset.clone()).or_default();
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

                if let Some(costs) = costs {
                    let span = span!(Level::DEBUG, "tracking asset acquisition cost");
                    self.track_costs(amount, costs, time, balance);
                }
            }
            Operation::Dispose {
                amount,
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

                let usd_market = Market::new(amount.asset.clone(), "USD");
                let usd_price = market::solve_price(&self.market_data, &usd_market, &time)
                    .await?
                    .ok_or_else(|| anyhow!("couldn't find price for {:?}", usd_market))?;
                let coin_balance = balance.entry(amount.asset.clone()).or_default();
                coin_balance.usd_position += amount.value * usd_price;

                if let Some(costs) = costs {
                    let span = span!(Level::DEBUG, "tracking asset disposal cost");
                    self.track_costs(amount, costs, time, balance);
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

/// Construct to create pipelines that process operations.
/// Receives and processes operations, then passes them through
/// to the next processor through the `sender` channel. The last processor
/// won't be provided with a sender channel.
#[async_trait]
pub trait OperationsProcesor {
    async fn process(
        &self,
        mut receiver: mpsc::Receiver<Operation>,
        sender: Option<mpsc::Sender<Operation>>,
    ) -> Result<()>;
}

const OPS_RECEIVE_BATCH_SIZE: usize = 1000;

/// Flushes batched operations into the db
pub struct OperationsFlusher<S> {
    ops_storage: S,
}

impl<S: Storage> OperationsFlusher<S> {
    pub fn new(ops_storage: S) -> Self {
        Self { ops_storage }
    }
    async fn flush(&self, batch: Vec<Operation>) -> Result<usize> {
        let mut seen_ops = HashSet::new();
        let (inserted, skipped) = self
            .ops_storage
            .insert_ops(
                batch
                    .into_iter()
                    .inspect(|op| {
                        if seen_ops.contains(&op.id()) {
                            log::warn!("duplicate op {} {:?}", op.id(), op);
                        } else {
                            seen_ops.insert(op.id());
                        }
                    })
                    .filter_map(|op| op.try_into().map_or(None, |op| Some(op)))
                    .collect(),
            )
            .await?;
        log::debug!(
            "flushed {} operations into db, skipped = {}",
            inserted,
            skipped
        );
        Ok(inserted)
    }
}

#[async_trait]
impl<S: Storage + Send + Sync> OperationsProcesor for OperationsFlusher<S> {
    async fn process(
        &self,
        mut receiver: mpsc::Receiver<Operation>,
        sender: Option<mpsc::Sender<Operation>>,
    ) -> Result<()> {
        log::info!("syncing operations to db...");
        let mut batch = Vec::with_capacity(OPS_RECEIVE_BATCH_SIZE);
        let mut num_ops = 0;
        let mut num_fetched = 0;
        while let Some(op) = receiver.recv().await {
            batch.push(op.clone());
            if batch.len() == OPS_RECEIVE_BATCH_SIZE {
                log::info!("batched {}", OPS_RECEIVE_BATCH_SIZE);
                num_ops += self.flush(batch).await?;
                num_fetched += OPS_RECEIVE_BATCH_SIZE;
                batch = Vec::with_capacity(OPS_RECEIVE_BATCH_SIZE);
            }
            if let Some(sender) = sender.as_ref() {
                sender.send(op).await?;
            }
        }
        log::debug!("channel for receiving ops closed in operations flusher");
        if batch.len() > 0 {
            log::info!("batched {}", batch.len());
            num_fetched += batch.len();
            num_ops += self.flush(batch).await?;
        };
        log::info!("fetched={} inserted={}", num_fetched, num_ops);
        Ok(())
    }
}

/// Fetches prices for assets and stores on the db based on the processed
/// operations.
pub struct PricesFetcher<T> {
    market_data: T,
}

impl<T> PricesFetcher<T> {
    pub fn new(market_data: T) -> Self {
        Self { market_data }
    }
}

#[async_trait]
impl<T: MarketData + Send + Sync> OperationsProcesor for PricesFetcher<T> {
    async fn process(
        &self,
        mut receiver: mpsc::Receiver<Operation>,
        sender: Option<mpsc::Sender<Operation>>,
    ) -> Result<()> {
        while let Some(op) = receiver.recv().await {
            log::debug!("processing op {:?}", op);
            // match &op {
            //     Operation::Cost { asset, time, .. } | Operation::Revenue { asset, time, .. } => {
            //         if asset.to_ascii_lowercase() == "usd" {
            //             return Ok(1.0);
            //         }
            //         self.market_data.price_at(&Market::new(asset, "usd"), time).await?;
            //     }
            //     _ => (),
            // }
            if let Some(sender) = sender.as_ref() {
                sender.send(op).await?;
            }
        }
        debug!("channel for receiving ops closed in prices fetcher");
        Ok(())
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
    use quickcheck::{quickcheck, Arbitrary, Gen, TestResult};
    use tokio::sync::Mutex;
    use Operation::*;

    use proptest::{option::of, prelude::*, sample::select};

    impl Arbitrary for Operation {
        fn arbitrary(g: &mut Gen) -> Self {
            let assets: &[&str] = &["BTC", "ETH", "SOL", "AVAX", "USD"];
            let source_id = u8::arbitrary(g).to_string();
            let recipient = u8::arbitrary(g).to_string();
            let sender = u8::arbitrary(g).to_string();
            let source = g
                .choose(&["binance", "coinbase", "other-source"])
                .unwrap()
                .to_string();
            // non-zero amounts
            let amount = Amount {
                value: 0.1 + u16::arbitrary(g) as f64,
                asset: g.choose(&assets).unwrap().to_string(),
            };
            let cost = Amount {
                value: 0.1 + u16::arbitrary(g) as f64,
                asset: g.choose(&assets).unwrap().to_string(),
            };

            match g.choose(&[0, 1, 2, 3]).unwrap() {
                &0 => Acquire {
                    source_id,
                    source,
                    amount,
                    costs: Some(vec![cost]),
                    time: Utc::now(),
                },
                &1 => Dispose {
                    source_id,
                    source,
                    amount,
                    costs: Some(vec![cost]),
                    time: Utc::now(),
                },
                &2 => Send {
                    source_id,
                    source,
                    amount,
                    costs: Some(vec![cost]),
                    sender: Some(sender),
                    recipient: Some(recipient),
                    time: Utc::now(),
                },
                &3 => Receive {
                    source_id,
                    source,
                    amount,
                    costs: Some(vec![cost]),
                    sender: Some(sender),
                    recipient: Some(recipient),
                    time: Utc::now(),
                },
                _ => panic!("unexpected index"),
            }
        }
    }

    prop_compose! {
        fn datetime()(year in 2000..2100i32, month in 1..12u32, day in 1..28u32, hour in 0..59u32, minute in 0..59u32) -> DateTime<Utc> {
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
                Acquire { amount, costs, .. } => {
                    prop_assert_eq!(amount.asset, t.base_asset);
                    prop_assert_eq!(amount.value, t.amount);

                    let costs = costs.unwrap();
                    let cost = costs[0];

                    prop_assert_eq!(cost.asset, t.quote_asset);
                    prop_assert_eq!(cost.value, t.amount * t.price);

                    if t.fee > 0.0 {
                        let cost = costs[1];

                        prop_assert_eq!(cost.asset, t.fee_asset);
                        prop_assert_eq!(cost.value, t.fee);
                    }
                }
                _ => {
                    prop_assert!(false, "op 1 is not Acquire");
                }
            }

            match &ops[1] {
                Dispose {
                    amount,
                    costs,
                    ..
                } => {
                    prop_assert_eq!(amount.asset, t.quote_asset);
                    prop_assert_eq!(amount.value, t.amount * t.price);
                    prop_assert!(matches!(costs, None));
                }
                _ => {
                    prop_assert!(false, "op 2 is not Dispose");
                }
            }

            if t.fee > 0.0 {
                match &ops[2] {
                    Dispose {
                        amount,
                        costs,
                        ..
                    } => {
                        prop_assert_eq!(amount.asset, t.fee_asset);
                        prop_assert_eq!(amount.value, t.fee);
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
                Dispose {
                    amount,
                    costs,
                    ..
                } => {
                    prop_assert_eq!(amount.asset, t.base_asset);
                    prop_assert_eq!(amount.value, t.amount);

                    if t.fee > 0.0 {
                        let costs = costs.unwrap();
                        let cost = costs[0];
                        prop_assert_eq!(cost.asset, t.fee_asset);
                        prop_assert_eq!(cost.value, t.fee);
                    }

                }
                _ => {
                    prop_assert!(false, "op 1 is not Dispose");
                }
            }

            match &ops[1] {
                Acquire { amount, costs, .. } => {
                    prop_assert_eq!(amount.asset, t.quote_asset);
                    prop_assert_eq!(amount.value, t.amount * t.price);

                    if market::is_fiat(&amount.asset) {
                        prop_assert!(matches!(costs, None));
                    } else {
                        prop_assert!(matches!(costs, Some(..)));
                        let costs = costs.unwrap();
                        prop_assert_eq!(costs.len(), 1);
                        let cost = costs[0];
                        prop_assert_eq!(cost.asset, t.base_asset);
                        prop_assert_eq!(cost.value, t.amount);
                    }
                }
                _ => {
                    prop_assert!(false, "op 2 is not Acquire");
                }
            }

            if t.fee > 0.0 {
                match &ops[2] {
                    Dispose {
                        amount,
                        costs,
                        ..
                    } => {
                        prop_assert_eq!(amount.asset, t.fee_asset);
                        prop_assert_eq!(amount.value, t.fee);
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
                3
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
                prop_assert_eq!(amount.asset, d.asset);
                prop_assert_eq!(amount.value, d.amount);

                if let Some(fee) = d.fee.filter(|f| *f > 0.0) {
                    prop_assert!(matches!(costs, Some(..)));
                    let costs = costs.unwrap();
                    prop_assert_eq!(costs.len(), 1);
                    let cost = costs[0];
                    prop_assert_eq!(cost.asset, amount.asset);
                    prop_assert_eq!(cost.value, fee);
                }
            } else {
                prop_assert!(false, "op 1 is not Receive")
            }

            if let Some(fee) = d.fee.filter(|f| *f > 0.0) {
                if let Dispose { amount: fee_amount, costs, .. } = &ops[1] {
                    prop_assert_eq!(fee_amount.asset, d.asset);
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
                3
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
                prop_assert_eq!(amount.asset, w.asset);
                prop_assert_eq!(amount.value, w.amount);

                if w.fee > 0.0 {
                    prop_assert!(matches!(costs, Some(..)));
                    let costs = costs.unwrap();
                    prop_assert_eq!(costs.len(), 1);
                    let cost = costs[0];
                    prop_assert_eq!(cost.asset, amount.asset);
                    prop_assert_eq!(cost.value, w.fee);
                }
            } else {
                prop_assert!(false, "op 1 is not Send")
            }

            if w.fee > 0.0 {
                if let Dispose { amount: fee_amount, costs, .. } = &ops[1] {
                    prop_assert_eq!(fee_amount.asset, w.asset);
                    prop_assert_eq!(fee_amount.value, w.fee);
                    prop_assert!(matches!(costs, None));
                } else {
                    prop_assert!(false, "op 2 is not Dispose");
                }
            }
        }
    }

    #[test]
    fn loan_into_operations() {
        fn prop(loan: Loan) -> TestResult {
            if loan.amount == 0.0 {
                return TestResult::discard();
            }

            let d = loan.clone();
            let ops: Vec<Operation> = loan.into();

            let num_ops = match d.status {
                Status::Success => 1,
                Status::Failure => 0,
            };
            if ops.len() != num_ops {
                println!(
                    "incorrect number of ops expected {} got {}",
                    num_ops,
                    ops.len()
                );
                return TestResult::failed();
            }

            if num_ops > 0 {
                match &ops[0] {
                    BalanceIncrease { asset, amount, .. } => {
                        if asset != &d.asset || amount != &d.amount {
                            println!("non-matching fields for op 1");
                            return TestResult::failed();
                        }
                    }
                    _ => {
                        println!("not matching op 1 type");
                        return TestResult::failed();
                    }
                }
            }
            return TestResult::passed();
        }
        quickcheck(prop as fn(Loan) -> TestResult);
    }

    #[test]
    fn repay_into_operations() {
        fn prop(repay: Repay) -> TestResult {
            if repay.amount == 0.0 {
                return TestResult::discard();
            }

            let d = repay.clone();
            let ops: Vec<Operation> = repay.into();

            let num_ops = match d.status {
                Status::Success => 2,
                Status::Failure => 0,
            };
            if ops.len() != num_ops {
                println!(
                    "incorrect number of ops expected {} got {}",
                    num_ops,
                    ops.len()
                );
                return TestResult::failed();
            }

            if num_ops > 0 {
                match &ops[0] {
                    BalanceDecrease { asset, amount, .. } => {
                        if asset != &d.asset || amount != &(d.amount + d.interest) {
                            println!("non-matching fields for op 1");
                            return TestResult::failed();
                        }
                    }
                    _ => {
                        println!("not matching op 1 type");
                        return TestResult::failed();
                    }
                }
                match &ops[1] {
                    Cost {
                        for_asset,
                        for_amount,
                        asset,
                        amount,
                        ..
                    } => {
                        if for_asset != &d.asset
                            || for_amount != &0.0
                            || asset != &d.asset
                            || amount != &d.interest
                        {
                            println!("non-matching fields for op 2");
                            return TestResult::failed();
                        }
                    }
                    _ => {
                        println!("not matching op 2 type");
                        return TestResult::failed();
                    }
                }
            }
            return TestResult::passed();
        }
        quickcheck(prop as fn(Repay) -> TestResult);
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
                Ok(vec![])
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
            Operation::BalanceIncrease {
                id: 1,
                source_id: "1".to_string(),
                source: "test".to_string(),
                asset: "BTC".into(),
                amount: 0.03,
            },
            Operation::Cost {
                id: 2,
                source_id: "2".to_string(),
                source: "test".to_string(),
                for_asset: "BTC".to_string(),
                for_amount: 0.03,
                asset: "USD".into(),
                amount: 255.0,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                id: 3,
                source_id: "3".to_string(),
                source: "test".to_string(),
                asset: "BTC".into(),
                amount: 0.1,
            },
            Operation::Cost {
                id: 4,
                source_id: "4".to_string(),
                source: "test".to_string(),
                for_asset: "BTC".into(),
                for_amount: 0.1,
                asset: "USD".into(),
                amount: 890.0,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                id: 5,
                source_id: "5".to_string(),
                source: "test".to_string(),
                asset: "ETH".into(),
                amount: 0.5,
            },
            Operation::Cost {
                id: 6,
                source_id: "6".to_string(),
                source: "test".to_string(),
                for_asset: "ETH".into(),
                for_amount: 0.5,
                asset: "USD".into(),
                amount: 1000.0,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                id: 7,
                source_id: "7".to_string(),
                source: "test".to_string(),
                asset: "ETH".into(),
                amount: 0.01,
            },
            Operation::Cost {
                id: 8,
                source_id: "1".to_string(),
                source: "test".to_string(),
                for_asset: "ETH".into(),
                for_amount: 0.01,
                asset: "USD".into(),
                amount: 21.0,
                time: Utc::now(),
            },
            Operation::BalanceDecrease {
                id: 9,
                source_id: "9".to_string(),
                source: "test".to_string(),
                asset: "ETH".into(),
                amount: 0.2,
            },
            Operation::Revenue {
                id: 10,
                source_id: "10".to_string(),
                source: "test".to_string(),
                asset: "ETH".into(),
                amount: 0.2,
                time: Utc::now(),
            },
            Operation::BalanceIncrease {
                id: 11,
                source_id: "11".to_string(),
                source: "test".to_string(),
                asset: "DOT".into(),
                amount: 0.5,
            },
            Operation::Cost {
                id: 12,
                source_id: "12".to_string(),
                source: "test".to_string(),
                for_asset: "DOT".into(),
                for_amount: 0.5,
                asset: "USD".into(),
                amount: 7.5,
                time: Utc::now(),
            },
            Operation::BalanceDecrease {
                id: 13,
                source_id: "13".to_string(),
                source: "test".to_string(),
                asset: "DOT".into(),
                amount: 0.1,
            },
            Operation::Revenue {
                id: 14,
                source_id: "14".to_string(),
                source: "test".to_string(),
                asset: "DOT".into(),
                amount: 0.1,
                time: Utc::now(),
            },
            Operation::BalanceDecrease {
                id: 15,
                source_id: "15".to_string(),
                source: "test".to_string(),
                asset: "DOT".into(),
                amount: 0.2,
            },
            Operation::Revenue {
                id: 16,
                source_id: "16".to_string(),
                source: "test".to_string(),
                asset: "DOT".into(),
                amount: 0.2,
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
                    usd_position: -1145.0,
                },
            ),
            (
                "ETH".to_string(),
                AssetBalance {
                    amount: 0.31,
                    usd_position: 379.0,
                },
            ),
            (
                "DOT".to_string(),
                AssetBalance {
                    amount: 0.2,
                    usd_position: 14.0,
                },
            ),
        ];

        expected.sort_by_key(|x| x.0.clone());

        let mut balances = coin_tracker.balances().await;
        balances.sort_by_key(|x| x.0.clone());

        for ((asset_a, balance_a), (asset_b, balance_b)) in expected.iter().zip(balances.iter()) {
            assert_eq!(asset_a, asset_b);
            println!("{}", asset_a);
            assert_eq!(balance_a, balance_b);
        }

        Ok(())
    }
}
