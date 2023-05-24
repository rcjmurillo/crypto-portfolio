use std::fs::File;
use structopt::{self, StructOpt};

use crate::config::{read_config_file, read_file, Config};

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
