use std::{cmp::Ordering, collections::HashMap};

use std::collections::HashSet;

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};

use operations::OpType;
use tokio::sync::RwLock;
use tracing::{span, Level};

use market::{self, Market, MarketData};

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};

use binance::{ApiGlobal, BinanceFetcher, RegionGlobal};
use operations::{cost_basis::Acquisition, Amount, Operation};

pub async fn asset_balances<T: MarketData>(
    balance_tracker: &BalanceTracker<T>,
    binance_client: BinanceFetcher<RegionGlobal>,
) -> Result<()> {
    let mut coin_balances = HashMap::<String, f64>::new();

    let mut all_assets_usd_unrealized_position = 0.0;

    for (coin, balance) in balance_tracker.balances().await.iter() {
        *coin_balances.entry(coin.to_string()).or_insert(0.0) += balance.amount;
    }

    let mut coin_balances = coin_balances
        .iter()
        .filter(|b| *b.1 > 0.0)
        .collect::<Vec<(&String, &f64)>>();
    let mut all_assets_value = 0f64;

    let all_prices: HashMap<String, f64> = binance_client
        .fetch_all_prices(&ApiGlobal::Prices.to_string())
        .await?
        .into_iter()
        .map(|x| (x.symbol, x.price))
        .collect();

    let get_price = |symbol: &str| {
        if symbol.starts_with("USD") {
            1.0
        } else {
            *all_prices
                .get(&(String::from(symbol) + "USDT"))
                .unwrap_or_else(|| panic!("couldn't get price for {}", symbol))
        }
    };

    coin_balances.sort_by(|a, b| {
        let price_a = get_price(a.0);
        let price_b = get_price(b.0);
        match price_a * a.1 < price_b * b.1 {
            true => Ordering::Greater,
            false => Ordering::Less,
        }
    });

    let mut table = Vec::new();
    for (coin, &amount) in coin_balances {
        let price = get_price(coin);
        // only compute the current value for balances > 0
        let value = price * if amount > 0.0 { amount } else { 0.0 };
        all_assets_value += value;

        if let Some(balance) = balance_tracker.get_balance(&coin[..]).await {
            let usd_unrealized_position = value + balance.usd_position;
            let usd_unrealized_position_pcnt = (value / balance.usd_position.abs() - 1.0) * 100.0;
            all_assets_usd_unrealized_position += usd_unrealized_position;
            table.push(vec![
                coin.cell(),
                format!("{:.6}", amount).cell().justify(Justify::Right),
                format!("{:.4}", price).cell().justify(Justify::Right),
                format!("{:.2}", value).cell().justify(Justify::Right),
                format!("{:.2}", balance.usd_position)
                    .cell()
                    .justify(Justify::Right),
                format!("{:.2}", usd_unrealized_position)
                    .cell()
                    .justify(Justify::Right),
                format!("{:.2}%", usd_unrealized_position_pcnt)
                    .cell()
                    .justify(Justify::Right),
            ]);
        }
    }

    let table = table
        .table()
        .title(vec![
            "Asset".cell().bold(true),
            "Amount".cell().justify(Justify::Right).bold(true),
            "Price USD".cell().justify(Justify::Right).bold(true),
            "Value USD".cell().justify(Justify::Right).bold(true),
            "Position USD".cell().justify(Justify::Right).bold(true),
            "Unrealized Position USD"
                .cell()
                .justify(Justify::Right)
                .bold(true),
            "Unrealized Position %"
                .cell()
                .justify(Justify::Right)
                .bold(true),
        ])
        .bold(true);
    println!();
    assert!(print_stdout(table).is_ok());

    let mut summary_table = vec![];

    summary_table.extend(vec![
        vec![
            "Unrealized USD position".cell(),
            format!("{:.2}", all_assets_usd_unrealized_position).cell(),
        ],
        vec![
            "Assets USD value".cell(),
            format!("{:.2}", all_assets_value).cell(),
        ],
    ]);
    assert!(print_stdout(summary_table.table()).is_ok());

    Ok(())
}

pub async fn sell_detail<T: MarketData>(
    amount: Amount,
    datetime: DateTime<Utc>,
    acquisitions: Vec<Acquisition>,
    market_data: &T,
) -> Result<()> {
    let mut table = Vec::new();

    for p in acquisitions {
        let usd_cost = p.paid_with_cost(&"USD".to_string(), market_data).await?;
        let price_usd = market_data
            .price_at(&Market::new(amount.asset.to_string(), "USD"), &p.datetime)
            .await?;

        table.push(vec![
            p.source.clone().cell(),
            p.amount.cell().justify(Justify::Right),
            usd_cost.cell().justify(Justify::Right),
            price_usd.cell().justify(Justify::Right),
            p.paid_with
                .iter()
                .map(|a| a.to_string())
                .collect::<Vec<String>>()
                .join(",")
                .cell()
                .justify(Justify::Right),
            p.datetime.cell(),
        ]);
    }

    let table = table
        .table()
        .title(vec![
            "Source".cell(),
            "Amount".cell().justify(Justify::Right),
            "Cost basis (USD)".cell().justify(Justify::Right),
            "Price USD".cell().justify(Justify::Right),
            "Purchased with".cell().justify(Justify::Right),
            "Datetime".cell(),
        ])
        .bold(true);

    println!("\nSale of {} at {}:\n", amount, datetime);

    print_stdout(table.table()).map_err(|e| anyhow!(e))
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct AssetBalance {
    pub amount: f64,
    pub usd_position: f64,
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
        match op.op_type {
            OpType::Acquire => {
                let span = span!(Level::DEBUG, "tracking asset acquisition");
                let _enter = span.enter();
                assert!(
                    op.amount.value >= 0.0,
                    "asset acquisition operation amount can't be negative"
                );
                let coin_balance = balance.entry(op.amount.asset.clone()).or_default();
                coin_balance.amount += op.amount.value;

                let price = op
                    .price
                    .as_ref()
                    .expect("missing price in acquire operation");
                let usd_price =
                    market::usd_price(&self.market_data, &price.asset, &op.time).await?;
                coin_balance.usd_position -= op.amount.value * price.value * usd_price;

                if let Some(costs) = op.costs.as_ref() {
                    span!(Level::DEBUG, "tracking asset acquisition cost");
                    self.track_costs(&op.amount, &costs, &op.time, balance)
                        .await?;
                }
            }
            OpType::Dispose => {
                let span = span!(Level::DEBUG, "tracking asset disposal");
                let _enter = span.enter();
                assert!(
                    op.amount.value >= 0.0,
                    "asset disposal operation amount can't be negative"
                );
                let price = op
                    .price
                    .as_ref()
                    .expect("missing price in acquire operation");
                let coin_balance = balance.entry(op.amount.asset.clone()).or_default();
                coin_balance.amount -= op.amount.value;

                let usd_price =
                    market::usd_price(&self.market_data, &price.asset, &op.time).await?;
                let coin_balance = balance.entry(op.amount.asset.clone()).or_default();
                coin_balance.usd_position += op.amount.value * price.value * usd_price;

                if let Some(costs) = op.costs.as_ref() {
                    span!(Level::DEBUG, "tracking asset disposal cost");
                    self.track_costs(&op.amount, &costs, &op.time, balance)
                        .await?;
                }
            }
            OpType::Send => {
                let span = span!(Level::DEBUG, "tracking asset send");
                let _enter = span.enter();
                assert!(
                    op.amount.value >= 0.0,
                    "asset send operation amount can't be negative"
                );
                let coin_balance = balance.entry(op.amount.asset.clone()).or_default();
                coin_balance.amount += op.amount.value;
            }
            OpType::Receive => {
                let span = span!(Level::DEBUG, "tracking asset receive");
                let _enter = span.enter();
                assert!(
                    op.amount.value >= 0.0,
                    "asset receive operation amount can't be negative"
                );
                let coin_balance = balance.entry(op.amount.asset.clone()).or_default();
                coin_balance.amount -= op.amount.value;
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

#[cfg(test)]
mod tests {
    use super::*;
    use operations::{Deposit, Loan, OpType::*, Repay, Status, Trade, TradeSide, Withdraw};

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
            _fee_asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
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
            _fee_asset in select(vec!["BTC", "ETH", "AVAX", "USD"]).prop_map(|a| a.to_string()),
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

            match &ops[0].op_type {
                OpType::Acquire => {
                    let op = &ops[0];
                    prop_assert_eq!(&op.amount.asset, &t.base_asset);
                    prop_assert_eq!(op.amount.value, t.base_amount());
                    prop_assert_eq!(&op.price.as_ref().unwrap().asset, &t.quote_asset);
                    prop_assert_eq!(op.price.as_ref().unwrap().value, t.quote_amount());

                    if t.fee > 0.0 {
                        let costs = op.costs.as_ref().unwrap();
                        prop_assert_eq!(costs.len(), 1);
                        let cost = &costs[0];

                        prop_assert_eq!(&cost.asset, &t.fee_asset);
                        prop_assert_eq!(cost.value, t.fee);
                    } else {
                        prop_assert!(!matches!(op.costs, None));
                    }
                }
                _ => {
                    prop_assert!(false, "op 1 is not Acquire");
                }
            }

            match &ops[1].op_type {
                OpType::Dispose => {
                    let op = &ops[1];
                    prop_assert_eq!(&op.amount.asset, &t.quote_asset);
                    prop_assert_eq!(op.amount.value, t.quote_amount());
                    prop_assert_eq!(&op.price.as_ref().as_ref().unwrap().asset, &t.base_asset);
                    prop_assert_eq!(op.price.as_ref().as_ref().unwrap().value, t.base_amount());
                    prop_assert!(matches!(op.costs, None));
                }
                _ => {
                    prop_assert!(false, "op 2 is not Dispose");
                }
            }

            if t.fee > 0.0 {
                match &ops[2].op_type {
                    OpType::Dispose => {
                        let op = &ops[2];
                        prop_assert_eq!(&op.amount.asset, &t.fee_asset);
                        prop_assert_eq!(op.amount.value, t.fee);
                        prop_assert_eq!(&op.price.as_ref().unwrap().asset, &t.fee_asset);
                        prop_assert_eq!(op.price.as_ref().unwrap().value, t.fee);
                        prop_assert!(matches!(op.costs, None));
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

            match &ops[0].op_type {
                OpType::Dispose => {
                    let op = &ops[0];
                    prop_assert_eq!(&op.amount.asset, &t.base_asset);
                    prop_assert_eq!(op.amount.value, t.base_amount());
                    prop_assert_eq!(&op.price.as_ref().unwrap().asset, &t.quote_asset);
                    prop_assert_eq!(op.price.as_ref().unwrap().value, t.quote_amount());

                    if t.fee > 0.0 {
                        let costs = op.costs.as_ref().unwrap();
                        let cost = &costs[0];
                        prop_assert_eq!(&cost.asset, &t.fee_asset);
                        prop_assert_eq!(cost.value, t.fee);
                    }

                }
                _ => {
                    prop_assert!(false, "op 1 is not Dispose");
                }
            }

            match &ops[1].op_type {
                Acquire => {
                    let op = &ops[1];
                    prop_assert_eq!(&op.amount.asset, &t.quote_asset);
                    prop_assert_eq!(op.amount.value, t.quote_amount());
                    prop_assert_eq!(&op.price.as_ref().unwrap().asset, &t.base_asset);
                    prop_assert_eq!(op.price.as_ref().unwrap().value, t.base_amount());
                    prop_assert!(matches!(op.costs, None));
                }
                _ => {
                    prop_assert!(false, "op 2 is not Acquire");
                }
            }

            if t.fee > 0.0 {
                match &ops[2].op_type {
                    OpType::Dispose => {
                        let op = &ops[2];
                        prop_assert_eq!(&op.amount.asset, &t.fee_asset);
                        prop_assert_eq!(op.amount.value, t.fee);
                        prop_assert_eq!(&op.price.as_ref().unwrap().asset, &t.fee_asset);
                        prop_assert_eq!(op.price.as_ref().unwrap().value, t.fee);
                        prop_assert!(matches!(op.costs, None));
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

            if let Receive = &ops[0].op_type {
                let op = &ops[0];
                prop_assert_eq!(&op.amount.asset, &d.asset);
                prop_assert_eq!(op.amount.value, d.amount);

                if let Some(fee) = d.fee.filter(|f| *f > 0.0) {
                    prop_assert!(matches!(op.costs, Some(..)));
                    let costs = op.costs.as_ref().unwrap();
                    prop_assert_eq!(costs.len(), 1);
                    let cost = &costs[0];
                    prop_assert_eq!(&cost.asset, &op.amount.asset);
                    prop_assert_eq!(cost.value, fee);
                }
            } else {
                prop_assert!(false, "op 1 is not Receive")
            }

            if let Some(fee) = d.fee.filter(|f| *f > 0.0) {
                if let Dispose = &ops[1].op_type {
                    let op = &ops[1];
                    prop_assert_eq!(&op.amount.asset, &d.asset);
                    prop_assert_eq!(op.amount.value, fee);
                    prop_assert!(matches!(op.costs, None));
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

            if let Send = &ops[0].op_type {
                let op = &ops[0];
                prop_assert_eq!(&op.amount.asset, &w.asset);
                prop_assert_eq!(op.amount.value, w.amount);

                if w.fee > 0.0 {
                    prop_assert!(matches!(op.costs, Some(..)));
                    let costs = op.costs.as_ref().unwrap();
                    prop_assert_eq!(costs.len(), 1);
                    let cost = &costs[0];
                    prop_assert_eq!(&cost.asset, &op.amount.asset);
                    prop_assert_eq!(cost.value, w.fee);
                }
            } else {
                prop_assert!(false, "op 1 is not Send")
            }

            if w.fee > 0.0 {
                if let Dispose = &ops[1].op_type {
                    let op = &ops[1];
                    prop_assert_eq!(&op.amount.asset, &w.asset);
                    prop_assert_eq!(op.amount.value, w.fee);
                    prop_assert!(matches!(op.costs, None));
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
                if let Acquire = &ops[0].op_type {
                    let op = &ops[0];
                    prop_assert_eq!(&op.amount.asset, &l.asset);
                    prop_assert_eq!(op.amount.value, l.amount);
                    prop_assert_eq!(&op.price.as_ref().unwrap().asset, &l.asset);
                    prop_assert_eq!(op.price.as_ref().unwrap().value, 0.0);
                    prop_assert!(matches!(op.costs, None));
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
                if let Dispose = &ops[0].op_type {
                    let op = &ops[0];
                    prop_assert_eq!(&op.amount.asset, &r.asset);
                    prop_assert_eq!(op.amount.value, r.amount + r.interest);
                    prop_assert_eq!(&op.price.as_ref().unwrap().asset, &r.asset);
                    prop_assert_eq!(op.price.as_ref().unwrap().value, 0.0);
                    prop_assert!(matches!(op.costs, None));
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
            async fn has_market(&self, _market: &Market) -> Result<bool> {
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
            Operation {
                op_type: Acquire,
                source_id: "1".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.03, "BTC"),
                price: Some(Amount::new(255.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Acquire,
                source_id: "3".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.1, "BTC"),
                price: Some(Amount::new(890.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Acquire,
                source_id: "5".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.5, "ETH"),
                price: Some(Amount::new(1000.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Acquire,
                source_id: "7".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.01, "ETH"),
                price: Some(Amount::new(21.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Dispose,
                source_id: "9".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.2, "ETH"),
                price: Some(Amount::new(1000.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Acquire,
                source_id: "11".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.5, "DOT"),
                price: Some(Amount::new(7.5, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Dispose,
                source_id: "13".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.1, "DOT"),
                price: Some(Amount::new(15.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
            },
            Operation {
                op_type: Dispose,
                source_id: "15".to_string(),
                source: "test".to_string(),
                amount: Amount::new(0.2, "DOT"),
                price: Some(Amount::new(13.0, "USD")),
                costs: None,
                time: Utc::now(),
                sender: None,
                recipient: None,
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
