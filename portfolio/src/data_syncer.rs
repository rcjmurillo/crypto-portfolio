use custom::FileDataFetcher;
use data_sync::sqlite::SqliteStorage;

use anyhow::anyhow;

pub async fn sync_sources(config: crate::config::Config) -> anyhow::Result<()> {
    data_sync::sqlite::create_tables()?;
    
    // coinbase exchange disabled because it doesn't provide the full set of
    // transactions and fees when converting coins.

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
        let config_binance_us = conf.try_into().unwrap();
        let binance_client_us =
            binance::BinanceFetcher::<binance::RegionUs>::with_config(config_binance_us);
        let sqlite_storage = SqliteStorage::new("./transactions.db")?;
        data_sync::sync_records(binance_client_us, sqlite_storage).await?;
    }

    match config.custom_sources {
        Some(custom_sources) => {
            for custom_source in custom_sources {
                match FileDataFetcher::from_file(&custom_source.name, &custom_source.file) {
                    Ok(fetcher) => {
                        let sqlite_storage = SqliteStorage::new("./transactions.db")?;
                        data_sync::sync_records(fetcher, sqlite_storage).await?;
                    }
                    Err(err) => {
                        return Err(anyhow!(err).context("could not read config from file"));
                    }
                }
            }
        }
        None => (),
    };

    Ok(())
}
