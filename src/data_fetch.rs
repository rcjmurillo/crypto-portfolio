use std::collections::HashMap;
use std::convert::From;
use std::iter::{Extend, IntoIterator};
use std::vec::Vec;

use crate::binance::{BinanceFetcher, BinanceRegion};
use crate::private::all_symbols;
#[cfg(feature = "private_ops")]
use crate::private::PrivateOps;
use crate::result::Result;
use crate::tracker::{IntoOperations, Operation};

use async_stream::stream;

use async_trait::async_trait;
use futures::future::{join_all, Future};
use serde::Deserialize;
use tokio::time::{sleep, Duration};
use tokio_stream::StreamExt;

#[async_trait]
pub trait DataFetcher {
    async fn symbol_price_at(&self, symbol: &str, time: u64) -> f64;
}

#[async_trait]
pub trait ExchangeClient {
    async fn trades(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>>;
    async fn margin_trades(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>>;
    async fn loans(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>>;
    async fn repays(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>>;
    async fn deposits(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>>;
    async fn withdraws(&self, symbols: &[String]) -> Result<Vec<Box<dyn IntoOperations>>>;
}

pub enum TradeSide {
    Buy,
    Sell,
}

pub struct Trade {
    pub symbol: String,
    pub base_asset: String,
    pub quote_asset: String,
    pub price: f64,
    pub cost: f64,
    pub amount: f64,
    pub fee: f64,
    pub fee_asset: String,
    pub time: u64,
    pub side: TradeSide,
}

#[async_trait]
impl IntoOperations for Trade {
    fn into_ops(&self) -> Vec<Operation> {
        let mut ops = Vec::new();
        // determines if the first operation is going to increase or to decrease
        // the balance, then the second operation does the opposit.
        let mut sign = if let TradeSide::Buy = self.side {
            1.0
        } else {
            -1.0
        };

        if self.base_asset == "" || self.quote_asset == "" {
            panic!(
                "found empty base or quote asset in into_ops for symbol {}",
                self.symbol
            );
        }

        ops.push(Operation {
            asset: self.base_asset.to_string(),
            amount: self.amount * sign,
            cost: self.cost * sign,
        });
        sign *= -1.0; // invert sign
        ops.push(Operation {
            asset: self.quote_asset.to_string(),
            amount: self.price * self.amount * sign,
            cost: self.cost * sign,
        });

        if self.fee_asset != "" {
            ops.push(Operation {
                asset: self.fee_asset.to_string(),
                amount: -self.fee,
                cost: 0.0,
            });
        }

        ops
    }
}

#[derive(Deserialize, Debug)]
pub struct Deposit {
    pub asset: String,
    pub amount: f64,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Withdraw {
    pub asset: String,
    pub amount: f64,
    pub time: u64,
    pub fee: f64,
}

#[async_trait]
impl IntoOperations for Withdraw {
    fn into_ops(&self) -> Vec<Operation> {
        vec![Operation {
            asset: self.asset.clone(),
            amount: -self.fee,
            cost: 0.0,
        }]
    }
}

impl From<&HashMap<String, String>> for Deposit {
    fn from(data: &HashMap<String, String>) -> Self {
        Deposit {
            asset: String::from("USD"),
            amount: data.get("amount").unwrap().parse::<f64>().unwrap(),
        }
    }
}

async fn await_in_chunks<T>(handles: Vec<T>, chunk_size: usize) -> Vec<Operation>
where
    T: Future<Output = Vec<Operation>>,
{
    let mut all_chunks = Vec::new();
    let mut chunks = Vec::new();
    let len = handles.len();
    for (i, handle) in handles.into_iter().enumerate() {
        chunks.push(handle);
        if chunks.len() == chunk_size || i == len - 1 {
            all_chunks.push(chunks);
            chunks = Vec::new();
        }
    }

    let mut ops = Vec::new();
    for (_, chunk) in all_chunks.into_iter().enumerate() {
        for result_ops in join_all(chunk).await {
            ops.extend(result_ops);
        }
        // wait a little bit to not overwelm the API
        sleep(Duration::from_millis(1000)).await;
    }
    ops
}

async fn ops_from_client<'a, T: ExchangeClient>(
    c: &'a mut T,
    symbols: &'a [String],
) -> Vec<Box<dyn IntoOperations>> {
    let mut all_ops = Vec::new();
    println!("> fetching trades...");
    all_ops.extend(c.trades(symbols).await.unwrap());
    println!("> fetching margin trades...");
    all_ops.extend(c.margin_trades(symbols).await.unwrap());
    println!("> fetching loans...");
    all_ops.extend(c.loans(symbols).await.unwrap());
    println!("> fetching repays...");
    all_ops.extend(c.repays(symbols).await.unwrap());
    println!("> fetching fiat deposits...");
    all_ops.extend(c.deposits(symbols).await.unwrap());
    println!("> fetching coins withdraws...");
    all_ops.extend(c.withdraws(symbols).await.unwrap());
    println!("> ALL DONE!!!");
    all_ops
}

pub fn fetch_pipeline<'a>() -> impl StreamExt<Item = Result<Operation>> + 'a {
    stream! {
        let mut binance_client = BinanceFetcher::new(BinanceRegion::Global);
        let mut binance_us_client = BinanceFetcher::new(BinanceRegion::Us);

        println!("========== binance ==========");
        let mut ops = ops_from_client(&mut binance_client, &all_symbols()[..]).await;

        #[cfg(feature="private_ops")]
        {
            println!("========== private ops ==========");
            let mut private_ops = PrivateOps::new();
            ops.extend(ops_from_client(&mut private_ops, &all_symbols()[..]).await);
        }

        println!("========== binance US ==========");
        ops.extend(ops_from_client(&mut binance_us_client, &all_symbols()[..]).await);

        println!("\nDONE getting operations...");
        for op in ops {
            for x in op.into_ops() {
                yield Ok(x);
            }
        }

        ()
    }
}
