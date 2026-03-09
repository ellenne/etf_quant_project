# etl_prices.py

Downloads daily price history for each ETF from Yahoo Finance and loads it into `prices_daily`.

## Usage

```bash
python scripts/etl_prices.py
```

## Prerequisites

- Database with `etf_metadata` and `prices_daily` tables (run `init_db.py`)
- ETF universe loaded (run `etl_universe.py`)

## What It Does

1. **Fetches tickers** from `etf_metadata` (ticker, source_ticker)
2. **Downloads** daily OHLC from Yahoo Finance via `yf.download(source_ticker, start="2015-01-01")`
3. **Flattens** MultiIndex columns (yfinance returns tuples like `('Close','SPY')` for single tickers)
4. **Maps** to schema: date, ticker, close, volume
5. **Upserts**: Deletes existing rows for same ticker/date, then inserts new rows

## Output Schema

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Trading date |
| ticker | TEXT | ETF ticker (from etf_metadata) |
| close | DOUBLE | Closing price |
| volume | DOUBLE | Trading volume |

## Behavior

- Uses `source_ticker` for Yahoo Finance (e.g. IWDA.L, SPY)
- Stores `ticker` from etf_metadata for consistency
- Start date: 2015-01-01 (configurable in `download_prices()`)
- Failed tickers are reported at the end; script continues

## Yahoo Finance Ticker Format

Use exchange suffixes for non-US listings:

- `.AS` – Euronext Amsterdam
- `.DE` – Xetra (Frankfurt)
- `.L` – London
- `.PA` – Paris

Example: `VWRA.L` for Vanguard FTSE All-World on London.

## Downstream

- `build_features.py` reads `prices_daily` to compute returns, volatility, momentum
