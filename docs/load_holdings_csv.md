# load_holdings_csv.py

Loads ETF holdings from a manually prepared CSV file. Use this when you need **full holdings** beyond the top 10 returned by `etl_holdings.py`.

## Usage

```bash
python scripts/load_holdings_csv.py
```

## Prerequisites

- Database with `etf_holdings` table (run `init_db.py`)
- CSV file: `data/etf_holdings.csv`

## CSV Format

**Required columns:** `etf_ticker`, `holding_name`, `holding_ticker`, `weight`

**Optional columns:** `etf_isin`, `snapshot_date`, `holding_isin`, `asset_type`, `sector`, `country`, `shares`, `market_value`

If `snapshot_date` is missing, it defaults to today.

## Example CSV

```csv
etf_ticker,etf_isin,snapshot_date,holding_name,holding_ticker,holding_isin,asset_type,sector,country,shares,weight,market_value
SPY,US78462F1030,2025-03-09,NVIDIA Corp,NVDA,,Equity,Technology,United States,,7.31,
SPY,US78462F1030,2025-03-09,Apple Inc,AAPL,,Equity,Technology,United States,,6.63,
```

## Where to Get Data

| Source | URL / Notes |
|--------|-------------|
| Yahoo Finance | finance.yahoo.com/quote/SPY/holdings/ (top 10 only) |
| iShares | ishares.com → fund page → Holdings → "Detailed Holdings and Analytics" (CSV) |
| Vanguard | vanguard.com → fund page → Holdings |
| SPDR | ssga.com → fund page → Holdings |

## Behavior

- **Deletes** existing rows for each (etf_ticker, snapshot_date) pair in the CSV
- **Inserts** all rows from the CSV
- Missing optional columns are filled with `NULL`
- `data_source` is set to `manual_csv`

## Template

A template is provided at `data/etf_holdings_template.csv`.

## If File Is Missing

The script prints instructions and example rows, then exits without error.
