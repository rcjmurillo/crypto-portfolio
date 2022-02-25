mod cli;
mod custom_ops;
mod db;
mod errors;
mod operations;
mod reports;

use std::{convert::TryInto, sync::Arc};

use anyhow::{anyhow, Result};
use structopt::{self, StructOpt};

use binance::{BinanceFetcher, Config, RegionGlobal, RegionUs};
// use coinbase::{CoinbaseFetcher, Config as CoinbaseConfig, Pro, Std};

use crate::{
    cli::{Args, PortfolioAction},
    custom_ops::FileDataFetcher,
    db::{create_tables, get_operations, Operation as DbOperation},
    operations::{
        fetch_ops, AssetPrices, BalanceTracker, ExchangeDataFetcher, OperationsFlusher,
    },
};

fn mk_fetchers(
    config: &cli::Config,
    file_fetcher: Option<FileDataFetcher>,
) -> Vec<(&'static str, Box<dyn ExchangeDataFetcher + Send + Sync>)> {
    let mut fetchers: Vec<(&'static str, Box<dyn ExchangeDataFetcher + Send + Sync>)> = Vec::new();

    // coinbase exchange disabled because it doesn't provide the full set of
    // operations and fees when converting coins.

    // let coinbase_config: Option<CoinbaseConfig> = config
    //     .coinbase
    //     .as_ref()
    //     .and_then(|c| Some(c.try_into().unwrap()));
    // if let Some(config) = coinbase_config {
    //     let coinbase_fetcher = CoinbaseFetcher::<Std>::new(config.clone());
    //     fetchers.push((
    //         "Coinbase",
    //         Box::new(coinbase_fetcher) as Box<dyn ExchangeDataFetcher + Send + Sync>,
    //     ));
    // }

    // let coinbase_config: Option<CoinbaseConfig> = config
    //     .coinbase_pro
    //     .as_ref()
    //     .and_then(|c| Some(c.try_into().unwrap()));
    // if let Some(config) = coinbase_config {
    //     let coinbase_fetcher_pro = CoinbaseFetcher::<Pro>::new(config);
    //     fetchers.push((
    //         "Coinbase Pro",
    //         Box::new(coinbase_fetcher_pro) as Box<dyn ExchangeDataFetcher + Send + Sync>,
    //     ));
    // }

    if let Some(conf) = config.binance.clone() {
        let config_binance: Config = conf.try_into().unwrap();
        let binance_client = BinanceFetcher::<RegionGlobal>::with_config(config_binance);
        fetchers.push(("Binance Global", Box::new(binance_client)));
    }

    if let Some(conf) = config.binance_us.clone() {
        let config_binance_us: Config = conf.try_into().unwrap();
        let binance_client_us = BinanceFetcher::<RegionUs>::with_config(config_binance_us);
        fetchers.push(("Binance US", Box::new(binance_client_us)));
    }

    if let Some(file_fetcher) = file_fetcher {
        fetchers.push(("Custom Operations", Box::new(file_fetcher)));
    }
    fetchers
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();

    create_tables()?;

    let args = Args::from_args();

    let Args::Portfolio {
        config,
        action,
        ops_file,
    } = args;

    match action {
        PortfolioAction::Balances => {
            let coin_tracker = BalanceTracker::new(AssetPrices::new());
            let ops = get_operations()?
                .into_iter()
                .map(|o: DbOperation| o.try_into());

            // let mut handles = Vec::new();
            // let coin_tracker = Arc::new(coin_tracker);
            for op in ops {
                // let ct = Arc::clone(&coin_tracker);
                // handles.push(tokio::spawn(async move {
                    // handles.push(coin_tracker.track_operation(op));
                    if let Err(err) = coin_tracker.track_operation(op?).await {
                        log::error!("couldn't process operation: {:?}", err);
                    }
                // }));
            }

            // join_all(handles)
            //     .await
            //     .into_iter()
            //     .map(|e| e.map_err(|err| anyhow!(err).context("")))
            //     .collect::<Result<()>>()?;

            reports::asset_balances(&coin_tracker).await?;
            println!();
        }
        PortfolioAction::Sync => {
            let config = Arc::new(config);
            let file_fetcher = match ops_file {
                Some(ops_file) => match FileDataFetcher::from_file(ops_file) {
                    Ok(fetcher) => Some(fetcher),
                    Err(err) => {
                        return Err(anyhow!(err).context("could read config from file"));
                    }
                },
                None => None,
            };

            let receiver = fetch_ops(mk_fetchers(&config, file_fetcher.clone())).await;
            let mut flusher = OperationsFlusher::with_receiver(receiver);
            flusher.receive().await?;

            log::info!("fetch done!");
        }
    }

    Ok(())
}
