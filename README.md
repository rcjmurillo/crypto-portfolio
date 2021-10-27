# Crypto portfolio command line program

A simple personal WIP crypto portfolio to learn more rust.

It can fetch operations from multiple exchanges to create a single report of the current balance of each crypto asset and also unrealized profits/losses.

To run with cargo:
```shell
$ cargo run -- portfolio balances --config /path/to/config.toml --custom-ops /path/to/custom_ops.json
```

## Config file format

```toml
[binance]
symbols = ["AAVEUSD", "ADABTC", "ADAEUR", "BATBTC", "BATETH", "BNBBTC"]
start_date = 2020-12-01

[binance_us]
symbols = ["AAVEUSD", "UNIUSDT", "VETUSDT", "ZILUSD", "BNBUSD"]
start_date = 2020-09-01

[coinbase]
symbols = [
    "BTC", "ETH", "BCH", "ZRX", "ETC", "BAT", "KNC", "OMG"
]
start_date = 2020-01-01

[coinbase_pro]
symbols = [
    "COMP-BTC", "MKR-BTC", "ZRX-EUR", "ZRX-BTC", "XLM-EUR", "XLM-BTC", 
    "BTC-USDC", "BAT-USDC", "BTC-EUR"
]
start_date = 2020-01-01
```

## Custom ops file format
An JSON object with the following optional fields:
- trades (see ops::api_model::Trade for the expected fields)
- deposits (see ops::api_model::Deposit for the expected fields)
- withdraws (see ops::api_model::Withdraw for the expected fields)

```json
{
    "trades": [
        ...
    ],
    "deposits": [
        ...
    ],
    "withdraws": [
        ...
    ],
}
```

## Sample output

TODO