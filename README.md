# ETF Quant Project

A quantitative ETF analysis pipeline: load metadata, prices, holdings, compute features, and run strategy signals. Data is stored in DuckDB.

## Project Structure

```
etf_quant_project/
├── data/
│   ├── etf_universe.csv      # ETF universe (tickers, metadata)
│   ├── etf_holdings.csv      # Optional: full holdings (manual)
│   └── etf_holdings_template.csv
├── db/
│   └── etf_data.duckdb       # DuckDB database
├── docs/                     # Documentation
│   ├── README.md             # Docs index
│   ├── etl_holdings.md
│   ├── etl_prices.md
│   ├── etl_universe.md
│   ├── load_holdings_csv.md
│   ├── init_db.md
│   ├── build_features.md
│   └── ...
├── scripts/
│   ├── init_db.py            # Create database & tables
│   ├── etl_universe.py       # Load ETF metadata from CSV
│   ├── etl_prices.py         # Download price history (Yahoo Finance)
│   ├── etl_holdings.py       # Download top 10 holdings (Yahoo Finance)
│   ├── load_holdings_csv.py  # Load full holdings from manual CSV
│   ├── build_features.py     # Compute features from prices
│   └── ...
└── requirements.txt
```

## Quick Start

```bash
# 1. Create virtual environment and install dependencies
python -m venv venv
venv\Scripts\activate   # Windows
pip install -r requirements.txt

# 2. Initialize database
python scripts/init_db.py

# 3. Load ETF universe (from data/etf_universe.csv)
python scripts/etl_universe.py

# 4. Download prices
python scripts/etl_prices.py

# 5. Download top 10 holdings per ETF
python scripts/etl_holdings.py

# 6. Build features (for strategy)
python scripts/build_features.py
```

## Typical Workflow

| Step | Script | Purpose |
|------|--------|---------|
| 1 | `init_db.py` | Create `etf_data.duckdb` and all tables |
| 2 | `etl_universe.py` | Load ETFs from `data/etf_universe.csv` → `etf_metadata` |
| 3 | `etl_prices.py` | Fetch daily OHLC from Yahoo Finance → `prices_daily` |
| 4 | `etl_holdings.py` | Fetch top 10 holdings per ETF → `etf_holdings` |
| 5 | `build_features.py` | Compute returns, volatility, momentum → `etf_features` |

For **full holdings** (beyond top 10), use `load_holdings_csv.py` with data copied from provider websites.

## Database Tables

| Table | Description |
|-------|-------------|
| `etf_metadata` | ETF info (ticker, ISIN, name, provider, asset class, etc.) |
| `prices_daily` | Daily OHLC (date, ticker, close, volume) |
| `etf_holdings` | Holdings snapshots (etf_ticker, holding_ticker, weight, etc.) |
| `etf_features` | Computed features (returns, volatility, momentum) |
| `strategy_signals` | Strategy output (score, rank, weight) |

## Documentation

See the [docs/](docs/README.md) folder for detailed documentation of each script.

## Requirements

- Python 3.10+
- DuckDB, pandas, yfinance, requests, numpy
