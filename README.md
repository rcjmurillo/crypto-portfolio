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
An JSON object with the following optional fields, each field must contain a list of objects:
- trades: (see `ops::Trade` for the expected fields on each object).
- deposits: (see `ops::Deposit` for the expected fields on each object)
- withdraws: (see `ops::Withdraw` for the expected fields on each object)

```json
{
    "trades": [
        {"symbol": "ETHBTC", "base_asset": "ETH", "quote_asset": "BTC", "price": 0.03128, "amount": 1.5, "time": 1603562874572, "fee": 0.001, "fee_asset": "BTC", "side": "sell"}
    ],
    "deposits": [
        {"asset": "BTC", "amount": 0.04, "time": 1619053678020, "fee": 0.0, "is_fiat": false}
    ],
    "withdraws": [
        {"asset": "ETH", "amount": 3.1, "fee": 0.005, "time": 163532453209}
    ],
}
```

## Sample output

TODO