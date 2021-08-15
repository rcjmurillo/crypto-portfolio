mod cli;
mod custom_ops;
mod errors;
mod operations;
mod result;

use std::{cmp::Ordering, collections::HashMap, sync::Arc, convert::TryInto};

use cli_table::{format::Justify, print_stdout, Cell, Style, Table};
use structopt::{self, StructOpt};
use tokio::sync::mpsc;

use binance::{BinanceFetcher, Region as BinanceRegion};
use crate::{
    cli::{Args, Config},
    custom_ops::FileDataFetcher,
    operations::{
        BalanceTracker, Deposit, ExchangeDataFetcher, IntoOperations, Loan, Operation, Repay,
        Trade, Withdraw,
    },
    result::Result,
};

async fn ops_from_client<'a, T>(prefix: &str, c: &'a T, symbols: &'a [String]) -> Vec<Operation>
where
    T: ExchangeDataFetcher + Sync,
    T::Trade: Into<Trade>,
    T::Loan: Into<Loan>,
    T::Repay: Into<Repay>,
    T::Deposit: Into<Deposit>,
    T::Withdraw: Into<Withdraw>,
{
    let mut all_ops = Vec::new();
    println!("[{}]> fetching trades...", prefix);
    all_ops.extend(
        c.trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching margin trades...", prefix);
    all_ops.extend(
        c.margin_trades(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching loans...", prefix);
    all_ops.extend(
        c.loans(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching repays...", prefix);
    all_ops.extend(
        c.repays(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching fiat deposits...", prefix);
    all_ops.extend(
        c.fiat_deposits(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> fetching coins withdraws...", prefix);
    all_ops.extend(
        c.withdraws(symbols)
            .await
            .unwrap()
            .into_iter()
            .flat_map(|t| t.into().into_ops()),
    );
    println!("[{}]> ALL DONE!!!", prefix);
    all_ops
}

async fn fetch_pipeline<'a>(
    file_data_fetcher: Option<FileDataFetcher>,
    config: Arc<Config>,
) -> mpsc::Receiver<Operation> {
    let (tx, rx) = mpsc::channel(1000);
    let tx1 = tx.clone();
    let tx2 = tx.clone();
    let tx3 = tx.clone();

    let conf1 = config.clone();
    let conf2 = config.clone();
    let conf3 = config.clone();

    let _h1 = tokio::spawn(async move {
        let config = match &conf1.binance {
            Some(c) => Some(c.clone().try_into().unwrap()),
            None => None
        };
        let binance_client = BinanceFetcher::new(BinanceRegion::Global, &config);
        for op in ops_from_client("binance", &binance_client, &conf1.symbols[..]).await {
            match tx1.send(op).await {
                Ok(()) => (),
                Err(err) => println!("could not send operation: {}", err),
            }
        }
    });

    let _h2 = tokio::spawn(async move {
        let config = match &conf2.binance {
            Some(c) => Some(c.clone().try_into().unwrap()),
            None => None
        };
        let binance_us_client = BinanceFetcher::new(BinanceRegion::Us, &config);
        for op in ops_from_client("binance US", &binance_us_client, &conf2.symbols[..]).await {
            match tx2.send(op).await {
                Ok(()) => (),
                Err(err) => println!("could not send operation: {}", err),
            }
        }
    });

    if let Some(file_data_fetcher) = file_data_fetcher {
        tokio::spawn(async move {
            for op in ops_from_client(
                "custom file operations",
                &file_data_fetcher,
                &conf3.symbols[..],
            )
            .await
            {
                match tx3.send(op).await {
                    Ok(()) => (),
                    Err(err) => println!("could not send operation: {}", err),
                }
            }
        });
    }

    println!("\nDONE getting operations...");

    rx
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::from_args();

    let Args::Portfolio {
        config,
        action: _,
        ops_file,
    } = args;

    let config = Arc::new(config);

    let config_binance = match &config.binance {
        Some(c) => Some(c.clone().try_into()?),
        None => None
    };
    let config_binance_us = match &config.binance_us {
        Some(c) => Some(c.clone().try_into()?),
        None => None
    };

    let mut coin_tracker = BalanceTracker::new();
    let binance_client = BinanceFetcher::new(BinanceRegion::Global, &config_binance);
    let binance_client_us = BinanceFetcher::new(BinanceRegion::Us, &config_binance_us);

    let file_data_fetcher = match ops_file {
        Some(ops_file) => match FileDataFetcher::from_file(ops_file) {
            Ok(fetcher) => Some(fetcher),
            Err(err) => {
                println!("could not process file: {}", err);
                None
            },
        },
        None => None,
    };

    let mut usd_investment: f64 = 0.0;
    if let Some(file_data_fetcher) = file_data_fetcher.as_ref() {
        usd_investment += file_data_fetcher
            .fiat_deposits(&config.symbols)
            .await?
            .iter()
            .map(|x| x.amount)
            .sum::<f64>();
    }

    let mut s = fetch_pipeline(file_data_fetcher, config).await;

    while let Some(op) = s.recv().await {
        coin_tracker.track_operation(op);
    }
    let mut coins_value = 0f64;

    let mut coin_balances = HashMap::<String, f64>::new();

    // Group coins
    for (coin, balance) in (&coin_tracker)
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

    usd_investment += deposits.iter().map(|x| x.amount).sum::<f64>();
    usd_investment += deposits_us.iter().map(|x| x.amount).sum::<f64>();

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
