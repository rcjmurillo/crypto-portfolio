mod binance;
mod private;

#[cfg(feature = "private_ops")]
mod private_ops;

mod data_fetch;
mod errors;
mod result;
mod tracker;
mod utils;

use std::cmp::Ordering;
use std::collections::HashMap;

use crate::binance::{BinanceFetcher, BinanceRegion};
use crate::data_fetch::{fetch_pipeline, Deposit};
#[cfg(feature = "private_ops")]
use crate::private::PrivateOps;
use crate::result::Result;
use crate::tracker::CoinTracker;

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};
use tokio_stream::StreamExt;

#[tokio::main]
pub async fn main() -> Result<()> {
    let coin_tracker = CoinTracker::new();
    let binance_client = BinanceFetcher::new(BinanceRegion::Global);
    let binance_client_us = BinanceFetcher::new(BinanceRegion::Us);

    let s = fetch_pipeline();
    tokio::pin!(s);

    while let Some(Ok(op)) = s.next().await {
        coin_tracker.track_operation(op);
    }
    let mut coins_value = 0f64;

    let mut coin_balances = HashMap::<String, f64>::new();

    // Group coins
    for (coin, balance) in (&coin_tracker).balances() {
        *coin_balances.entry(coin.clone()).or_insert(0.0) += balance.amount;
    }

    let mut coin_balances = coin_balances
        .iter()
        .filter(|b| *b.1 != 0.0 && !b.0.starts_with("USD") && !b.0.starts_with("EUR"))
        .collect::<Vec<(&String, &f64)>>();

    let all_prices: HashMap<String, f64> = binance_client
        .all_prices()?
        .into_iter()
        .map(|x| (x.symbol, x.price))
        .collect();

    // println!("coin balances: {:?}", coin_balances);

    // println!("sorting balances...");
    coin_balances.sort_by(|a, b| {
        let price_a = all_prices.get(&(String::from(a.0) + "USDT")).unwrap();
        let price_b = all_prices.get(&(String::from(b.0) + "USDT")).unwrap();
        if price_a * a.1 < price_b * b.1 {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    });

    // println!("building table...");
    let mut table = Vec::new();
    for (coin, &amount) in coin_balances {
        let price = all_prices.get(&(coin.clone() + "USDT")).unwrap();
        let value = price * amount;
        coins_value += value;
        let coin_cost = coin_tracker.get_cost(&coin[..]).unwrap_or_default();
        let position_pcnt = (value / coin_cost - 1.0) * 100.0;
        let position_usd = value - coin_cost;
        table.push(vec![
            coin.cell(),
            format!("{:.6}", amount).cell().justify(Justify::Right),
            format!("{:.4}", price).cell().justify(Justify::Right),
            format!("{:.2}", coin_cost).cell().justify(Justify::Right),
            format!("{:.2}", value).cell().justify(Justify::Right),
            format!("{:.2}%", position_pcnt)
                .cell()
                .justify(Justify::Right),
            format!("{:.2}", position_usd)
                .cell()
                .justify(Justify::Right),
        ]);
    }

    let table = table
        .table()
        .title(vec![
            "Coin".cell().bold(true),
            "Amount".cell().justify(Justify::Right).bold(true),
            "Price USD".cell().justify(Justify::Right).bold(true),
            "Cost USD".cell().justify(Justify::Right).bold(true),
            "Value USD".cell().justify(Justify::Right).bold(true),
            "Position %".cell().justify(Justify::Right).bold(true),
            "Position USD".cell().justify(Justify::Right).bold(true),
        ])
        .bold(true);
    println!();
    assert!(print_stdout(table).is_ok());

    println!();

    let deposits = binance_client.fetch_fiat_deposits().await?;
    let deposits_us = binance_client_us.fetch_fiat_deposits().await?;

    let mut usd_investment = deposits.iter().map(|x| x.amount).sum::<f64>();
    usd_investment += deposits_us.iter().map(|x| x.amount).sum::<f64>();

    #[cfg(feature = "private_ops")]
    {
        let private_ops = PrivateOps::new();
        usd_investment += private_ops
            .fetch_fiat_deposits()
            .await?
            .iter()
            .map(|x| x.amount)
            .sum::<f64>();
    }

    let summary_table = vec![
        vec![
            "Invested USD".cell(),
            format!("{:.2}", usd_investment).cell(),
        ],
        vec![
            "Coins value USD".cell(),
            format!("{:.2}", coins_value).cell(),
        ],
        vec![
            "Unrealized profit USD".cell(),
            format!("{:.2}", (coins_value - usd_investment)).cell(),
        ],
    ]
    .table();
    assert!(print_stdout(summary_table).is_ok());

    Ok(())
}
