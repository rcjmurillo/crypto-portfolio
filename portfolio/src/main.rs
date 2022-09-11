#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod cli;
mod custom_ops;
mod errors;
mod reports;

use std::{convert::TryInto, sync::Arc};

use anyhow::{anyhow, Result};
use futures::future::join_all;
use structopt::{self, StructOpt};
use tokio::sync::mpsc;

use binance::{BinanceFetcher, Config, RegionGlobal, RegionUs};
use coingecko::Client as CoinGeckoClient;
// use coinbase::{CoinbaseFetcher, Config as CoinbaseConfig, Pro, Std};

use exchange::operations::{
    db::{create_tables, get_operations, Db, Operation as DbOperation},
    fetch_ops,
    profit_loss::{ConsumeStrategy, OperationsStream, Sale},
    storage::Storage,
    AssetPrices, BalanceTracker, Operation, OperationsFlusher, OperationsProcesor, PricesFetcher,
};

use crate::{
    cli::{Args, PortfolioAction},
    custom_ops::FileDataFetcher,
};
use exchange::ExchangeDataFetcher;

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
    tracing_subscriber::fmt::init();

    create_tables()?;

    let args = Args::from_args();

    let Args::Portfolio {
        config,
        action,
        ops_file,
    } = args;

    match action {
        PortfolioAction::Balances => {
            const BATCH_SIZE: usize = 1000;
            let ops = get_operations()?
                .into_iter()
                .map(|o: DbOperation| o.try_into());
            let coin_tracker = BalanceTracker::new(CoinGeckoClient::with_config(
                config.coingecko.as_ref().expect("missing coingecko config"),
            ));
            let mut handles = Vec::new();
            for (i, op) in ops.into_iter().enumerate() {
                coin_tracker.batch_operation(op?).await;
                if i % BATCH_SIZE == 0 {
                    handles.push(coin_tracker.process_batch());
                }
            }
            handles.push(coin_tracker.process_batch());

            join_all(handles).await.into_iter().try_for_each(|op| {
                op.map_err(|err| anyhow!(err).context("couldn't process batch"))
            })?;

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
            // let prices_fetcher =
            //     PricesFetcher::new(AssetPrices::new(BinanceFetcher::<RegionGlobal>::new()));
            let prices_fetcher = PricesFetcher::new(AssetPrices::new(
                CoinGeckoClient::with_config(config.coingecko.as_ref().expect("missing coingecko config")),
            ));
            let flusher = OperationsFlusher::new(Db);

            // pipeline to process operations
            let (sender, receiver2) = mpsc::channel(100_000);
            let f1 = prices_fetcher.process(receiver, Some(sender));
            let f2 = flusher.process(receiver2, None);

            let results = join_all(vec![f1, f2]).await;

            results.iter().for_each(|r| match r {
                Err(err) => log::error!("{}", err),
                Ok(_) => (),
            });

            log::info!("fetch done!");
        }
        PortfolioAction::RevenueReport {
            asset: report_asset,
        } => {
            let ops_storage = Db;
            let ops = ops_storage.get_ops().await?;

            let mut stream = OperationsStream::from_ops(
                ops.clone(),
                ConsumeStrategy::Fifo,
                CoinGeckoClient::with_config(config.coingecko.as_ref().expect("missing coingecko config")),
            );

            for op in ops {
                if let Operation::Revenue {
                    asset,
                    amount,
                    time,
                    ..
                } = op
                {
                    if &asset == "EUR"
                        || asset.starts_with("USD")
                        || report_asset.as_ref().map_or(false, |a| a != &asset)
                    {
                        continue;
                    }
                    match stream
                        .consume(&Sale {
                            asset: asset.clone(),
                            amount,
                            datetime: time,
                        })
                        .await
                    {
                        Ok(mr) => println!("\nSale of {} {} at {}:\n> {}", amount, asset, time, mr),
                        Err(err) => println!(
                            "error when consuming of {} {} at {}: {}",
                            amount, asset, time, err
                        ),
                    }
                }
            }
        }
    }

    Ok(())
}
