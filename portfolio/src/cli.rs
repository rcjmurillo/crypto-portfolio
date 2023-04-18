use std::{
    ffi::{OsStr, OsString},
    fs::{read_to_string, File},
    path::PathBuf, str::FromStr,
};

use anyhow::{anyhow, Error as AnyhowError, Result};
use chrono::{NaiveDate, Utc, DateTime};
use serde::Deserialize;
use structopt::{self, StructOpt};
use toml::{self};

use crate::errors::Error;
use binance::Config as BinanceConfig;
use coinbase::Config as CoinbaseConfig;
use coingecko::Config as CoingeckoConfig;
use market::{Asset, Market};

fn read_config_file(path: &OsStr) -> std::result::Result<Config, OsString> {
    match Config::from_file_path(path.into()) {
        Ok(config) => Ok(config),
        Err(err) => Err(err.to_string().into()),
    }
}

fn read_file(path: &OsStr) -> std::result::Result<File, OsString> {
    match File::open(path) {
        Ok(file) => Ok(file),
        Err(err) => Err(err.to_string().into()),
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ExchangeConfig {
    pub api_key: Option<String>,
    pub secret_key: Option<String>,
    // all the markets to work with, only markets included here will
    // appear in the report.
    pub symbols: Option<Vec<String>>,
    pub assets: Option<Vec<Asset>>,
    // How far back to look for transactions
    start_date: toml::value::Datetime,
}

impl ExchangeConfig {
    pub fn start_datetime(&self) -> Result<DateTime<Utc>> {
        Ok(self.start_date.to_string().parse::<DateTime<Utc>>().unwrap())
    }
}

#[derive(Deserialize)]
pub struct Config {
    // allow to specify a configuration per exchange
    pub binance: Option<ExchangeConfig>,
    pub binance_us: Option<ExchangeConfig>,
    pub coinbase: Option<ExchangeConfig>,
    pub coinbase_pro: Option<ExchangeConfig>,
    pub coingecko: Option<CoingeckoConfig>,
}

impl Config {
    pub fn from_file_path(file_path: PathBuf) -> Result<Self> {
        let contents = read_to_string(file_path.clone()).map_err(|e| {
            anyhow!(Error::Cli).context(format!(
                "couldn't read config file: {:?} error: {}",
                file_path, e
            ))
        })?;
        toml::from_str(contents.as_str()).map_err(|e| {
            anyhow!(Error::Cli).context(format!(
                "couldn't parse config file: {:?} error: {}",
                file_path, e
            ))
        })
    }
}

impl TryFrom<ExchangeConfig> for BinanceConfig {
    type Error = AnyhowError;
    fn try_from(c: ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_date: c.start_datetime()?,
            symbols: c
                .symbols
                .ok_or_else(|| anyhow!("missing symbols in binance config"))?
                .iter()
                .map(|s| Market::try_from_str(&s))
                .collect::<Result<Vec<Market>>>()?,
            api_key: c.api_key.expect("missing api key env var"),
            secret_key: c.secret_key.expect("missing secret key env var"),
        })
    }
}

impl TryFrom<ExchangeConfig> for CoinbaseConfig {
    type Error = AnyhowError;
    fn try_from(c: ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_date: c.start_datetime()?.date_naive(),
            // fixme: decide which one to use by check if c.symbols or c.assets is present
            symbols: c
                .assets
                .ok_or_else(|| anyhow!("missing assets in binance config"))?
                .clone(),
        })
    }
}

#[derive(StructOpt)]
pub enum Action {
    Sync {
        /// Operations file in JSON format
        #[structopt(long = "ops-file", parse(try_from_os_str = read_file))]
        ops_file: Option<File>,
    },
    Balances,
    RevenueReport {
        #[structopt(long)]
        asset: Option<String>,
    },
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct Args {
    /// Action to run
    #[structopt(subcommand)]
    pub action: Action,
    /// Configuration file path
    #[structopt(long = "config", parse(try_from_os_str = read_config_file))]
    pub config: Config,
}
