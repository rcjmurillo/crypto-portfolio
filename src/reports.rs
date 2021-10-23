use std::{cmp::Ordering, collections::HashMap};

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};

use binance::{BinanceGlobalFetcher, Region as BinanceRegion};

use crate::{
    operations::{AssetsInfo, BalanceTracker},
    result::Result,
};

pub async fn asset_balances<T: AssetsInfo>(balance_tracker: &BalanceTracker<T>) -> Result<()> {
    let binance_client = BinanceGlobalFetcher::new();

    let mut coin_balances = HashMap::<String, f64>::new();

    let mut all_assets_usd_unrealized_position = 0.0;

    for (coin, balance) in balance_tracker.balances().iter() {
        *coin_balances.entry(coin.to_string()).or_insert(0.0) += balance.amount;
    }

    let mut coin_balances = coin_balances
        .iter()
        .filter(|b| *b.1 != 0.0 && !b.0.starts_with("USD") && !b.0.starts_with("EUR"))
        .collect::<Vec<(&String, &f64)>>();
    let mut all_assets_value = 0f64;

    let all_prices: HashMap<String, f64> = binance_client
        .base_fetcher
        .fetch_all_prices()
        .await?
        .into_iter()
        .map(|x| (x.symbol, x.price))
        .collect();

    coin_balances.sort_by(|a, b| {
        let price_a = all_prices.get(&(String::from(a.0) + "USDT")).unwrap();
        let price_b = all_prices.get(&(String::from(b.0) + "USDT")).unwrap();
        match price_a * a.1 < price_b * b.1 {
            true => Ordering::Greater,
            false => Ordering::Less,
        }
    });

    let mut table = Vec::new();
    for (coin, &amount) in coin_balances {
        let price = all_prices.get(&(coin.clone() + "USDT")).unwrap();
        // only compute a the current value for balances > 0
        let value = price * if amount > 0.0 { amount } else { 0.0 };
        all_assets_value += value;

        if let Some(balance) = balance_tracker.get_balance(&coin[..]) {
            let usd_unrealized_position = value + balance.usd_position;
            let usd_unrealized_position_pcnt = (value / balance.usd_position.abs() - 1.0) * 100.0;
            all_assets_usd_unrealized_position += usd_unrealized_position;
            table.push(vec![
                coin.cell(),
                format!("{:.6}", amount).cell().justify(Justify::Right),
                format!("{:.4}", price).cell().justify(Justify::Right),
                format!("{:.2}", balance.usd_position)
                    .cell()
                    .justify(Justify::Right),
                format!("{:.2}", value).cell().justify(Justify::Right),
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
            "Coin".cell().bold(true),
            "Amount".cell().justify(Justify::Right).bold(true),
            "Price USD".cell().justify(Justify::Right).bold(true),
            "Position USD".cell().justify(Justify::Right).bold(true),
            "Value USD".cell().justify(Justify::Right).bold(true),
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
            "Global USD position".cell(),
            format!("{:.2}", all_assets_usd_unrealized_position).cell(),
        ],
        vec![
            "Coins value USD".cell(),
            format!("{:.2}", all_assets_value).cell(),
        ],
        vec![
            "Unrealized USD profit".cell(),
            format!(
                "{:.2}",
                all_assets_value - all_assets_usd_unrealized_position
            )
            .cell(),
        ],
    ]);
    assert!(print_stdout(summary_table.table()).is_ok());

    Ok(())
}
