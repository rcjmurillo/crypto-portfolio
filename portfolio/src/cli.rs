use std::{
    ffi::{OsStr, OsString},
    fs::{read_to_string, File},
    path::PathBuf,
};

use anyhow::{anyhow, Context, Error as AnyhowError, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use structopt::{self, StructOpt};
use toml;

use crate::errors::Error;
use binance::Config as BinanceConfig;
use coinbase::Config as CoinbaseConfig;
use exchange::{Asset, AssetPair};

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
    // all the asset pairs to work with, only asset pairs included here will
    // appear in the report.
    pub symbols: Option<Vec<String>>,
    pub assets: Option<Vec<Asset>>,
    // How far back to look for transactions
    start_date: toml::Value,
}

impl ExchangeConfig {
    pub fn start_date(&self) -> Result<NaiveDate> {
        if let Some(start_date) = self.start_date.as_datetime() {
            Ok(start_date.to_string().parse::<NaiveDate>().unwrap())
        } else {
            return Err(anyhow!("could not parse date from config").context(Error::Cli));
        }
    }
}

#[derive(Deserialize)]
pub struct Config {
    // allow to specify a configuration per exchange
    pub binance: Option<ExchangeConfig>,
    pub binance_us: Option<ExchangeConfig>,
    pub coinbase: Option<ExchangeConfig>,
    pub coinbase_pro: Option<ExchangeConfig>,
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
            start_date: c.start_date()?,
            symbols: c
                .symbols
                .ok_or_else(|| anyhow!("missing symbols in binance config"))?
                .iter()
                .map(|s| AssetPair::try_from_str(&s))
                .collect::<Result<Vec<AssetPair>>>()?,
        })
    }
}

impl TryFrom<ExchangeConfig> for CoinbaseConfig {
    type Error = AnyhowError;
    fn try_from(c: ExchangeConfig) -> Result<Self> {
        Ok(Self {
            start_date: c.start_date()?,
            // fixme: decide which one to use by check if c.symbols or c.assets is present
            symbols: c
                .assets
                .ok_or_else(|| anyhow!("missing assets in binance config"))?
                .clone(),
        })
    }
}

#[derive(StructOpt)]
pub enum PortfolioAction {
    Balances,
    Sync,
    RevenueReport {
        #[structopt(long)]
        asset: Option<String>,
    },
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum Args {
    Portfolio {
        /// Action to run
        #[structopt(subcommand)]
        action: PortfolioAction,
        /// Configuration file
        #[structopt(short, long, parse(try_from_os_str = read_config_file))]
        config: Config,
        /// Operations file in JSON format
        #[structopt(long = "custom-ops", parse(try_from_os_str = read_file))]
        ops_file: Option<File>,
    },
}
