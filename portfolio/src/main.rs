mod cli;
mod config;
mod errors;
mod reports;

use cli::{Action, Args};
use data_sync::SqliteStorage;
use std::fs::File;

use anyhow::{anyhow, Result};

use futures::{stream, StreamExt};
use structopt::StructOpt;
use tokio::sync::mpsc;

use binance::{BinanceFetcher, RegionGlobal};
use coingecko::Client as CoinGeckoClient;
// use coinbase::{CoinbaseFetcher, Config as CoinbaseConfig, Pro, Std};

use custom::FileDataFetcher;
use data_sync::sync_operations;
use exchange::operations::{
    cost_basis::{ConsumeStrategy, CostBasisResolver, Disposal},
    BalanceTracker, Operation,
};

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    data_sync::create_tables()?;

    let args = Args::from_args();

    let Args { action, config } = args;

    match action {
        Action::Balances => {
            const BATCH_SIZE: usize = 1000;

            let mut cg = CoinGeckoClient::with_config(
                config.coingecko.as_ref().expect("missing coingecko config"),
            );
            cg.init().await?;

            let coin_tracker = BalanceTracker::new(cg);
            let mut handles = Vec::new();
            let mut i = 0;
            todo!("load ops from storage");
            // while let Some(op) = ops_receiver.recv().await {
            //     coin_tracker.batch_operation(op).await;
            //     if i % BATCH_SIZE == 0 {
            //         handles.push(coin_tracker.process_batch());
            //     }
            //     i += 1;
            // }
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
        Action::Sync { ops_file } => {
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

            // if let Some(conf) = config.binance.clone() {
            //     let config_binance: Config = conf.try_into().unwrap();
            //     let binance_client = BinanceFetcher::<RegionGlobal>::with_config(config_binance);
            //     fetchers.push(("Binance Global", Box::new(binance_client)));
            // }

            // if let Some(conf) = config.binance_us.clone() {
            //     let config_binance_us: Config = conf.try_into().unwrap();
            //     let binance_client_us = BinanceFetcher::<RegionUs>::with_config(config_binance_us);
            //     fetch_ops("Binance US", binance_client_us, tx.clone()).await?;
            // }

            // match ops_file {
            //     Some(ops_file) => {
            //         let sqlite_storage = SqliteStorage::new("./operations.db")?;
            //         match FileDataFetcher::from_file(ops_file) {
            //             Ok(fetcher) => fetch_ops("Custom Operations", fetcher, sqlite_storage).await?,
            //             Err(err) => {
            //                 return Err(anyhow!(err).context("could read config from file"));
            //             }
            //         }
            //     }
            //     None => (),
            // };

            match ops_file {
                Some(ops_file) => {
                    let sqlite_storage = SqliteStorage::new("./operations.db")?;
                    match FileDataFetcher::from_file(ops_file) {
                        Ok(fetcher) => {
                            sync_operations("custom operations", fetcher, sqlite_storage).await?;
                        }
                        Err(err) => {
                            return Err(anyhow!(err).context("could read config from file"));
                        }
                    }
                }
                None => (),
            };
        }
        Action::RevenueReport {
            asset: report_asset,
        } => {
            let mut ops = Vec::new();
            todo!("load ops from storage");

            let mut cg = CoinGeckoClient::with_config(
                config.coingecko.as_ref().expect("missing coingecko config"),
            );
            cg.init().await?;
            let mut cb_solver = CostBasisResolver::from_ops(ops.clone(), ConsumeStrategy::Fifo);

            for op in ops {
                if let Operation::Dispose { amount, time, .. } = op {
                    assert!(
                        !market::is_fiat(&amount.asset),
                        "there shouldn't be revenue ops for fiat currencies"
                    );
                    if report_asset
                        .as_ref()
                        .map_or(false, |a| !a.eq_ignore_ascii_case(&amount.asset))
                    {
                        continue;
                    }
                    match cb_solver
                        .resolve(&Disposal {
                            amount: amount.clone(),
                            datetime: time,
                        })
                        .await
                    {
                        Ok(acquisitions) => {
                            reports::sell_detail(amount, time, acquisitions, &cg).await?
                        }
                        Err(err) => {
                            println!("error when consuming {} at {}: {}", amount, time, err)
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
