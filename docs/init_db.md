# init_db.py

Creates the DuckDB database file and all tables used by the ETF Quant pipeline.

## Usage

```bash
python scripts/init_db.py
```

## What It Does

- Creates `db/etf_data.duckdb` if it does not exist
- Creates tables with `CREATE TABLE IF NOT EXISTS` (safe to re-run)

## Tables Created

| Table | Purpose |
|-------|---------|
| `etf_metadata` | ETF info: isin, ticker, source_ticker, name, provider, asset_class, category, sector, benchmark, currency, etc. |
| `prices_daily` | Daily OHLC: date, ticker, close, volume |
| `etf_holdings` | Holdings snapshots: snapshot_date, etf_ticker, holding_ticker, weight, holding_isin, etc. |
| `etf_features` | Computed features: ret_1w, ret_1m, vol_1m, momentum_12_1, price_vs_200dma, etc. |
| `strategy_signals` | Strategy output: date, ticker, score, rank, weight |

## When to Run

- **First time setup**: Run before any ETL
- **Reset**: Re-running is idempotent; tables are not dropped, only created if missing

## Dependencies

- `duckdb`
