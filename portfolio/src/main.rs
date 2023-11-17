mod cli;
mod config;
mod errors;
mod reports;

use cli::{Action, Args};
use data_sync::SqliteStorage;

use anyhow::{anyhow, Result};

use futures::{stream, StreamExt};
use structopt::StructOpt;

use binance::{BinanceFetcher, RegionGlobal};
use coingecko::Client as CoinGeckoClient;
// use coinbase::{CoinbaseFetcher, Config as CoinbaseConfig, Pro, Std};

use custom::FileDataFetcher;
use operations::{
    cost_basis::{ConsumeStrategy, CostBasisResolver, Disposal},
    OpType, Operation,
};
use reports::BalanceTracker;

#[tokio::main]
pub async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    data_sync::create_tables()?;

    let args = Args::from_args();

    let Args { action, config } = args;

    match action {
        Action::Sync => {
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

            if let Some(conf) = config.binance_us.clone() {
                let config_binance_us: binance::Config = conf.try_into().unwrap();
                let binance_client_us =
                    BinanceFetcher::<binance::RegionUs>::with_config(config_binance_us);
                let sqlite_storage = SqliteStorage::new("./operations.db")?;
                data_sync::sync_records(binance_client_us, sqlite_storage).await?;
            }

            match config.custom_sources {
                Some(custom_sources) => {
                    for custom_source in custom_sources {
                        match FileDataFetcher::from_file(&custom_source.name, &custom_source.file) {
                            Ok(fetcher) => {
                                let sqlite_storage = SqliteStorage::new("./operations.db")?;
                                data_sync::sync_records(fetcher, sqlite_storage).await?;
                            }
                            Err(err) => {
                                return Err(anyhow!(err).context("could read config from file"));
                            }
                        }
                    }
                }
                None => (),
            };
        }
        Action::Balances => {
            const BATCH_SIZE: usize = 1000;

            let mut cg = CoinGeckoClient::with_config(
                config.coingecko.as_ref().expect("missing coingecko config"),
            );
            cg.init().await?;

            let coin_tracker = BalanceTracker::new(cg);
            let ops = Vec::<Operation>::new();
            let mut handles = Vec::new();
            let mut i = 0;
            todo!("load ops from storage");
            for op in ops {
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
                if let OpType::Dispose = op.op_type {
                    assert!(
                        !market::is_fiat(&op.amount.asset),
                        "there shouldn't be revenue ops for fiat currencies"
                    );
                    if report_asset
                        .as_ref()
                        .map_or(false, |a| !a.eq_ignore_ascii_case(&op.amount.asset))
                    {
                        continue;
                    }
                    match cb_solver
                        .resolve(&Disposal {
                            amount: op.amount.clone(),
                            datetime: op.time,
                        })
                        .await
                    {
                        Ok(acquisitions) => {
                            reports::sell_detail(op.amount, op.time, acquisitions, &cg).await?
                        }
                        Err(err) => {
                            println!("error when consuming {} at {}: {}", op.amount, op.time, err)
                        }
                    }
                }
            }
        }
    }

    Ok(())
}
