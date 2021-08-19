mod cli;
mod custom_ops;
mod errors;
mod operations;
mod reports;
mod result;

use std::sync::Arc;

use structopt::{self, StructOpt};

use crate::{
    cli::Args,
    custom_ops::FileDataFetcher,
    operations::{fetch_pipeline, BalanceTracker},
    result::Result,
};

#[tokio::main]
pub async fn main() -> Result<()> {
    let args = Args::from_args();

    let Args::Portfolio {
        config,
        action: _,
        ops_file,
    } = args;

    let config = Arc::new(config);

    let mut coin_tracker = BalanceTracker::new();

    let file_data_fetcher = match ops_file {
        Some(ops_file) => match FileDataFetcher::from_file(ops_file) {
            Ok(fetcher) => Some(Arc::new(fetcher)),
            Err(err) => {
                println!("could not process file: {}", err);
                None
            }
        },
        None => None,
    };

    let mut s = fetch_pipeline(
        file_data_fetcher.as_ref().and_then(|f| Some(Arc::clone(f))),
        Arc::clone(&config),
    )
    .await;

    while let Some(op) = s.recv().await {
        coin_tracker.track_operation(op);
    }

    reports::asset_balances(&coin_tracker, file_data_fetcher, &config).await?;
    println!();

    Ok(())
}
