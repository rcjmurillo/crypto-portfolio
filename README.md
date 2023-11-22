# Crypto portfolio command line program

A simple personal WIP crypto portfolio to learn more rust.

It can fetch transactions from multiple exchanges to create a single report of the current balance of each crypto asset and also unrealized profits/losses.

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
- withdrawals: (see `ops::Withdraw` for the expected fields on each object)

```json
{
    "trades": [
        {"symbol": "ETHBTC", "base_asset": "ETH", "quote_asset": "BTC", "price": 0.03128, "amount": 1.5, "time": 1603562874572, "fee": 0.001, "fee_asset": "BTC", "side": "sell"}
    ],
    "deposits": [
        {"asset": "BTC", "amount": 0.04, "time": 1619053678020, "fee": 0.0, "is_fiat": false}
    ],
    "withdrawals": [
        {"asset": "ETH", "amount": 3.1, "fee": 0.005, "time": 163532453209}
    ],
}
```

## Sample output

```shell
+-------+---------------+------------+-----------+--------------+-------------------------+-----------------------+
| Asset |        Amount |  Price USD | Value USD | Position USD | Unrealized Position USD | Unrealized Position % |
+-------+---------------+------------+-----------+--------------+-------------------------+-----------------------+
| ETH   |             2 |  3857.6400 |   7715.28 |     -3490.76 |                 4224.52 |               121.02% |
+-------+---------------+------------+-----------+--------------+-------------------------+-----------------------+
| ADA   |     1530.1234 |     0.7777 |   1189.97 |      -659.66 |                 1735.42 |                38.01% |
+-------+---------------+------------+-----------+--------------+-------------------------+-----------------------+
| BTC   |        0.5777 |  100123.77 |  57841.50 |    -23758.69 |                34082.81 |               143.45% |
+-------+---------------+------------+-----------+--------------+-------------------------+-----------------------+


+-------------------------+----------+
| Unrealized USD position | 38837.64 |
+-------------------------+----------+
| Assets USD value        | 66746.75 |
+-------------------------+----------+
```