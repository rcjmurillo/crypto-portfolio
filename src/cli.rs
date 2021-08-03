use std::{
    convert::{TryFrom, TryInto},
    ffi::{OsStr, OsString},
    fs::{read_to_string, File},
    path::PathBuf,
};

use serde::Deserialize;
use structopt::{self, StructOpt};
use toml;

use crate::result::Result;

pub enum PorfolioAction {
    Balances,
}

impl TryFrom<&str> for PorfolioAction {
    type Error = &'static str;

    fn try_from(s: &str) -> std::result::Result<Self, Self::Error> {
        if s == "balances" {
            Ok(PorfolioAction::Balances)
        } else {
            Err("Invalid action")
        }
    }
}

fn validate_porfolio_action(action: &str) -> Result<PorfolioAction> {
    Ok(action.try_into()?)
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

#[derive(Deserialize)]
pub struct ExchangeConfig {
    // How far back to look for transactions
    lookback_date: String,
}

#[derive(Deserialize)]
pub struct Config {
    // all the symbols to work with, only symbols included here will
    // appear in the report.
    pub symbols: Vec<String>,
    pub binance: Option<ExchangeConfig>,
    pub binance_us: Option<ExchangeConfig>,
}

impl Config {
    pub fn from_file_path(file_path: PathBuf) -> Result<Self> {
        let contents = read_to_string(file_path)?;
        Ok(toml::from_str(contents.as_str())?)
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub enum Args {
    Portfolio {
        /// Action to run, one of: balances d
        #[structopt(parse(try_from_str = validate_porfolio_action))]
        action: PorfolioAction,
        /// Configuration file
        #[structopt(short, long, parse(try_from_os_str = read_config_file))]
        config: Config,
        /// Operations file in JSON format
        #[structopt(long, parse(try_from_os_str = read_file))]
        ops_file: Option<File>,
    },
}
