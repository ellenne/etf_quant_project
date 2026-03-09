# etl_universe.py

Loads the ETF universe from `data/etf_universe.csv` into the `etf_metadata` table.

## Usage

```bash
python scripts/etl_universe.py
```

## Prerequisites

- Database must exist: run `init_db.py` first
- CSV file must exist: `data/etf_universe.csv`

## CSV Format

Required columns:

| Column | Description |
|--------|-------------|
| `ticker` | Primary ticker (e.g. IWDA.L) |
| `source_ticker` | Yahoo Finance symbol (often same as ticker) |
| `isin` | 12-character ISIN |
| `name` | Full ETF name |
| `provider` | Issuer (e.g. iShares, Vanguard) |
| `asset_class` | Equity, Bond, Commodity |
| `category` | e.g. Global, US Large Cap |
| `sector` | e.g. Broad Market, Technology |
| `benchmark` | Index tracked |
| `currency` | Trading currency |
| `data_source` | e.g. yfinance |

Optional columns (filled with `NULL` if missing): `expense_ratio`, `inception_date`, `description`

## Behavior

- **Replaces** all rows in `etf_metadata` (full reload)
- Validates required columns before loading
- Prints: `Loaded N ETFs into etf_metadata`

## Example CSV Row

```csv
ticker,source_ticker,isin,name,provider,asset_class,category,sector,benchmark,currency,data_source
SPY,SPY,US78462F1030,State Street SPDR S&P 500 ETF Trust,State Street,Equity,US Large Cap,Broad Market,S&P 500,USD,yfinance
```

## Downstream

- `etl_prices.py` and `etl_holdings.py` read from `etf_metadata` to get the list of tickers to fetch
