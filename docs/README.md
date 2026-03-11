# Documentation Index

Detailed documentation for each script in the ETF Quant project.

## ETL & Data Loading

| Script | Doc | Description |
|--------|-----|-------------|
| `init_db.py` | [init_db.md](init_db.md) | Create database and tables |
| `etl_universe.py` | [etl_universe.md](etl_universe.md) | Load ETF metadata from CSV |
| `etl_prices.py` | [etl_prices.md](etl_prices.md) | Download price history from Yahoo Finance |
| `etl_holdings.py` | [etl_holdings.md](etl_holdings.md) | Download top 10 holdings from Yahoo Finance |
| `load_holdings_csv.py` | [load_holdings_csv.md](load_holdings_csv.md) | Load full holdings from manual CSV |

## Features & Strategy

| Script | Doc | Description |
|--------|-----|-------------|
| `build_features.py` | [build_features.md](build_features.md) | Compute features from price data |
| `generate_signals.py` | [generate_signals.md](generate_signals.md) | Generate strategy signals (top 5 ETFs) |
| `generate_historical_signals.py` | [generate_historical_signals.md](generate_historical_signals.md) | Generate historical monthly signals for backtest |
| `backtest_strategy.py` | [backtest_strategy.md](backtest_strategy.md) | Backtest monthly rotation strategy |

## Analysis & Research

| Script | Doc | Description |
|--------|-----|-------------|
| `analyze_backtest_results.py` | [analyze_backtest_results.md](analyze_backtest_results.md) | Analyze backtest results, benchmarks, signals, sectors |
| `research_strategy_variants.py` | [research_strategy_variants.md](research_strategy_variants.md) | Compare multiple strategy variants |

## Utilities & Testing

| Script | Doc | Description |
|--------|-----|-------------|
| `load_etf_metadata.py` | [load_etf_metadata.md](load_etf_metadata.md) | Insert single ETF (dev/test) |
| `test_db.py` | [test_db.md](test_db.md) | List tables in database |
| `check_prices.py` | [check_prices.md](check_prices.md) | Inspect prices_daily |
| `test_yahoo_one.py` | [test_yahoo_one.md](test_yahoo_one.md) | Test yfinance download |
