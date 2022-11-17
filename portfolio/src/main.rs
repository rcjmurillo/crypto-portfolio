#[cfg(test)]
#[macro_use]
extern crate quickcheck;

mod cli;
mod custom_ops;
mod errors;
mod reports;

use std::{convert::TryInto, fs::File, sync::Arc};

use anyhow::{anyhow, Result};
use futures::{channel::mpsc::Receiver, future::join_all, stream, StreamExt};
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
    BalanceTracker, Operation, OperationsFlusher, OperationsProcesor, PricesFetcher,
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

async fn mk_ops_receiver(
    config: &cli::Config,
    ops_file: Option<File>,
) -> Result<mpsc::Receiver<Operation>> {
    let file_fetcher = match ops_file {
        Some(ops_file) => match FileDataFetcher::from_file(ops_file) {
            Ok(fetcher) => Some(fetcher),
            Err(err) => {
                return Err(anyhow!(err).context("could read config from file"));
            }
        },
        None => None,
    };

    Ok(fetch_ops(mk_fetchers(&config, file_fetcher.clone())).await)
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

    let mut ops_receiver = mk_ops_receiver(&config, ops_file).await?;

    match action {
        PortfolioAction::Balances => {
            const BATCH_SIZE: usize = 1000;

            let mut cg = CoinGeckoClient::with_config(
                config.coingecko.as_ref().expect("missing coingecko config"),
            );
            cg.init().await?;

            let coin_tracker = BalanceTracker::new(cg);
            let mut handles = Vec::new();
            let mut i = 0;
            while let Some(op) = ops_receiver.recv().await {
                coin_tracker.batch_operation(op).await;
                if i % BATCH_SIZE == 0 {
                    handles.push(coin_tracker.process_batch());
                }
                i += 1;
            }
            handles.push(coin_tracker.process_batch());

            for batch_result in stream::iter(handles)
                .buffer_unordered(1000)
                .collect::<Vec<_>>()
                .await
            {
                log::debug!(
                    "batch processed {:?}",
                    batch_result.map_err(|err| anyhow!(err).context("couldn't process batch"))
                );
            }

            let binance_client = BinanceFetcher::<RegionGlobal>::with_config(
                config.binance.expect("missing binance config").try_into()?,
            );
            reports::asset_balances(&coin_tracker, binance_client).await?;
            println!();
        }
        PortfolioAction::RevenueReport {
            asset: report_asset,
        } => {
            let mut ops = Vec::new();
            while let Some(op) = ops_receiver.recv().await {
                ops.push(op);
            }

            let mut cg = CoinGeckoClient::with_config(
                config.coingecko.as_ref().expect("missing coingecko config"),
            );
            cg.init().await?;
            let mut stream = OperationsStream::from_ops(ops.clone(), ConsumeStrategy::Fifo, cg);

            for op in ops {
                if let Operation::Revenue {
                    asset,
                    amount,
                    time,
                    ..
                } = op
                {
                    assert!(!market::is_fiat(&asset), "there shouldn't be revenue ops for fiat currencies");
                    if report_asset
                            .as_ref()
                            .map_or(false, |a| !a.eq_ignore_ascii_case(&asset))
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
                        Ok(mr) => reports::sell_detail(asset.as_ref(), amount, time, mr)?,
                        Err(err) => println!(
                            "error when consuming {} {} at {}: {}",
                            amount, asset, time, err
                        ),
                    }
                }
            }
        }
    }

    Ok(())
}
