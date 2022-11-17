use std::{cmp::Ordering, collections::HashMap};

use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use cli_table::{format::Justify, print_stdout, Cell, Style, Table};

use binance::{ApiGlobal, BinanceFetcher, RegionGlobal};
use exchange::operations::{profit_loss::MatchResult, BalanceTracker};
use market::MarketData;

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

pub fn sell_detail(
    asset: &str,
    amount: f64,
    datetime: DateTime<Utc>,
    match_result: MatchResult,
) -> Result<()> {
    let mut table = Vec::new();

    for p in match_result.purchases {
        table.push(vec![
            p.sale_result.cell(),
            p.source.cell(),
            p.amount.cell().justify(Justify::Right),
            p.cost.cell().justify(Justify::Right),
            p.price.cell().justify(Justify::Right),
            format!("{} {}", p.paid_with_amount, p.paid_with)
                .cell()
                .justify(Justify::Right),
            p.datetime.cell(),
        ]);
    }

    let table = table
        .table()
        .title(vec![
            "Sale result (USD)".cell(),
            "Source".cell(),
            "Amount".cell().justify(Justify::Right),
            "Cost basis (USD)".cell().justify(Justify::Right),
            "Price USD".cell().justify(Justify::Right),
            "Purchased with".cell().justify(Justify::Right),
            "Datetime".cell(),
        ])
        .bold(true);

    println!("\nSale of {} {} at {}:\n", amount, asset, datetime);

    print_stdout(table.table()).map_err(|e| anyhow!(e))
}
