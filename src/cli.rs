use std::{
    convert::{TryFrom, TryInto},
    ffi::{OsStr, OsString},
    fs::{read_to_string, File},
    path::PathBuf,
};

use anyhow::{anyhow, Result};
use chrono::NaiveDate;
use serde::Deserialize;
use structopt::{self, StructOpt};
use toml;

use crate::errors::Error;

impl TryFrom<&str> for PortfolioAction {
    type Error = anyhow::Error;

    fn try_from(s: &str) -> Result<PortfolioAction> {
        match s {
            "balances" => Ok(PortfolioAction::Balances),
            "sync" => Ok(PortfolioAction::Sync),
            _ => Err(anyhow!("Invalid action".to_string()).context(Error::Cli))
        }
    }
}

fn validate_portfolio_action(action: &str) -> Result<PortfolioAction> {
    action.try_into()
}

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

#[derive(Clone, Deserialize)]
pub struct ExchangeConfig {
    // all the symbols to work with, only symbols included here will
    // appear in the report.
    pub symbols: Vec<String>,
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
        let contents = read_to_string(file_path)
            .map_err(|e| anyhow!("couldn't read config file: {}", e).context(Error::Cli))?;
        toml::from_str(contents.as_str())
            .map_err(|e| anyhow!("couldn't parse config file: {}", e).context(Error::Cli))
    }
}

pub enum PortfolioAction {
    Balances,
    Sync,
    RevenueReport,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum Args {
    Portfolio {
        /// Action to run
        #[structopt(parse(try_from_str = validate_portfolio_action))]
        action: PortfolioAction,
        /// Configuration file
        #[structopt(short, long, parse(try_from_os_str = read_config_file))]
        config: Config,
        /// Operations file in JSON format
        #[structopt(long = "custom-ops", parse(try_from_os_str = read_file))]
        ops_file: Option<File>,
    },
}
