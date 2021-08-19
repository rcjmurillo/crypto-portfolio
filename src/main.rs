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

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::from_args();

    let Args::Portfolio {
        config,
        action: _,
        ops_file,
    } = args;

    let config = Arc::new(config);

    let mut coin_tracker = BalanceTracker::new();

    let file_data_fetcher = match ops_file {
        Some(ops_file) => match FileDataFetcher::from_file(ops_file) {
            Ok(fetcher) => Some(fetcher),
            Err(err) => {
                println!("could not process file: {}", err);
                None
            }
        },
        None => None,
    };

    let config_binance = config.binance.as_ref().and_then(|c| Some(c.clone().try_into().unwrap()));
    let binance_client = BinanceFetcher::new(BinanceRegion::Global, config_binance);
    let config_binance_us = config.binance_us.as_ref().and_then(|c| Some(c.clone().try_into().unwrap()));
    let binance_client_us = BinanceFetcher::new(BinanceRegion::Us, config_binance_us);

    let mut fetchers = vec![
        (
            "Binance Global",
            Box::new(binance_client) as Box<dyn ExchangeDataFetcher + Send + Sync>,
        ),
        (
            "Binance US",
            Box::new(binance_client_us) as Box<dyn ExchangeDataFetcher + Send + Sync>,
        ),
    ];
    
    if let Some(ff) = file_data_fetcher.clone() {  // fixme: avoid this clone
        fetchers.push((
            "Custom Operations",
            Box::new(ff) as Box<dyn ExchangeDataFetcher + Send + Sync>,
        ));
    }

    let mut s = fetch_ops(fetchers, Arc::clone(&config)).await;

    while let Some(op) = s.recv().await {
        coin_tracker.track_operation(op);
    }

    reports::asset_balances(&coin_tracker, file_data_fetcher, &config).await?;
    println!();

    Ok(())
}
