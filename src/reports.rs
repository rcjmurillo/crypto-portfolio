use std::{cmp::Ordering, collections::HashMap, convert::TryInto, sync::Arc};

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};

use binance::{BinanceFetcher, Region as BinanceRegion};

use crate::{
    cli::Config, operations::BalanceTracker,
    operations::ExchangeDataFetcher, result::Result,
};

pub async fn asset_balances(
    balance_tracker: &BalanceTracker,
    fetchers: Vec<(
        &'static str,
        Arc<Box<dyn ExchangeDataFetcher + Send + Sync>>,
    )>,
    config: Arc<Config>,
) -> Result<()> {
    let config_binance = config
        .binance
        .as_ref()
        .and_then(|c| Some(c.clone().try_into().unwrap()));
    let binance_client = BinanceFetcher::new(BinanceRegion::Global, config_binance);

    let mut coin_balances = HashMap::<String, f64>::new();

    for (coin, balance) in balance_tracker
        .balances()
        .iter()
        .filter(|(_, b)| b.amount >= 0.0 && b.cost >= 0.0)
    {
        *coin_balances.entry(coin.to_string()).or_insert(0.0) += balance.amount;
    }

    let mut coin_balances = coin_balances
        .iter()
        .filter(|b| *b.1 != 0.0 && !b.0.starts_with("USD") && !b.0.starts_with("EUR"))
        .collect::<Vec<(&String, &f64)>>();
    let mut all_assets_value = 0f64;

    let all_prices: HashMap<String, f64> = binance_client
        .fetch_all_prices()
        .await?
        .into_iter()
        .map(|x| (x.symbol, x.price))
        .collect();

    coin_balances.sort_by(|a, b| {
        let price_a = all_prices.get(&(String::from(a.0) + "USDT")).unwrap();
        let price_b = all_prices.get(&(String::from(b.0) + "USDT")).unwrap();
        if price_a * a.1 < price_b * b.1 {
            Ordering::Greater
        } else {
            Ordering::Less
        }
    });

    let mut table = Vec::new();
    for (coin, &amount) in coin_balances {
        let price = all_prices.get(&(coin.clone() + "USDT")).unwrap();
        let value = price * amount;
        all_assets_value += value;
        let coin_cost = balance_tracker.get_cost(&coin[..]).unwrap_or_default();
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

    let mut investments: HashMap<String, f64> = HashMap::new();

    for (_, f) in fetchers.iter() {
        f.fiat_deposits(&config.symbols).await?.into_iter().for_each(|x| {
            let b = investments.entry(x.asset).or_insert(0.0);
            *b += x.amount;
        });
    }

    let mut invested_usd: f64 = 0.0;
    let mut summary_table = vec![];
    for (asset, inv) in investments.iter() {
        summary_table.push(vec![
            format!("Invested {}", asset).cell(),
            format!("{:.2}", inv).cell(),
        ]);
        if asset == "USD" {
            invested_usd += inv;
        } else {
            // convert it to USD
            invested_usd += all_prices.get(&(asset.to_owned() + "USDT")).unwrap();
        }
    }
    summary_table.extend(vec![
        vec![
            "Coins value USD".cell(),
            format!("{:.2}", all_assets_value).cell(),
        ],
        vec![
            "Estimated unrealized profit USD".cell(),
            format!("{:.2}", all_assets_value - invested_usd).cell(),
        ],
    ]);
    assert!(print_stdout(summary_table.table()).is_ok());

    Ok(())
}
