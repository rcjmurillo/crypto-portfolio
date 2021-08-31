mod cli;
mod custom_ops;
mod errors;
mod operations;
mod reports;
mod result;

use std::{convert::TryInto, sync::Arc};

use structopt::{self, StructOpt};

use binance::{BinanceFetcher, Region as BinanceRegion};

use crate::{
    cli::Args,
    custom_ops::FileDataFetcher,
    operations::{fetch_ops, BalanceTracker, ExchangeDataFetcher},
    result::Result,
};

fn mk_fetchers(
    config: &cli::Config,
    file_fetcher: Option<FileDataFetcher>,
) -> Vec<(
    &'static str,
    Arc<Box<dyn ExchangeDataFetcher + Send + Sync>>,
)> {
    let config_binance = config
        .binance
        .as_ref()
        .and_then(|c| Some(c.clone().try_into().unwrap()));
    let binance_client = BinanceFetcher::new(BinanceRegion::Global, config_binance);
    let config_binance_us = config
        .binance_us
        .as_ref()
        .and_then(|c| Some(c.clone().try_into().unwrap()));
    let binance_client_us = BinanceFetcher::new(BinanceRegion::Us, config_binance_us);

    let mut fetchers = vec![
        (
            "Binance Global",
            Arc::new(Box::new(binance_client) as Box<dyn ExchangeDataFetcher + Send + Sync>),
        ),
        (
            "Binance US",
            Arc::new(Box::new(binance_client_us) as Box<dyn ExchangeDataFetcher + Send + Sync>),
        ),
    ];

    if let Some(file_fetcher) = file_fetcher {
        fetchers.push((
            "Custom Operations",
            Arc::new(Box::new(file_fetcher) as Box<dyn ExchangeDataFetcher + Send + Sync>),
        ));
    }
    fetchers
}

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::from_args();

    let Args::Portfolio {
        config,
        action: _,
        ops_file,
    } = args;

    let mut coin_tracker = BalanceTracker::new();

    let config = Arc::new(config);

    let file_fetcher = match ops_file {
        Some(ops_file) => match FileDataFetcher::from_file(ops_file) {
            Ok(fetcher) => Some(fetcher),
            Err(err) => {
                panic!("could not process file: {}", err);
            }
        },
        None => None,
    };

    let mut s = fetch_ops(mk_fetchers(&config, file_fetcher.clone()), config.clone()).await;

    while let Some(op) = s.recv().await {
        coin_tracker.track_operation(op);
    }

    reports::asset_balances(&coin_tracker, config).await?;
    println!();

    Ok(())
}
