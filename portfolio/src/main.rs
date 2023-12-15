mod cli;
mod config;
mod data_syncer;
mod errors;
mod reports;

use structopt::StructOpt;

use cli::{Action, Args};

#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let args = Args::from_args();

    let Args { action, config } = args;

    match action {
        Action::Sync => data_syncer::sync_sources(config).await?,
        Action::Balances => reports::asset_balances(config).await?,
        Action::RevenueReport { asset } => reports::revenue_report(config, asset).await?,
    }

    Ok(())
}
