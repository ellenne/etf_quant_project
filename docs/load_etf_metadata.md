# load_etf_metadata.py

A simple utility to insert a single ETF into `etf_metadata`. Useful for development and testing.

## Usage

```bash
python scripts/load_etf_metadata.py
```

## What It Does

- Inserts one hardcoded ETF (iShares Core MSCI World) into `etf_metadata`
- Does **not** validate schema or check for duplicates
- Intended for quick tests, not production ETL

## Default Data

```python
{
    "isin": "IE00B4L5Y983",
    "ticker": "IWDA",
    "name": "iShares Core MSCI World",
    "provider": "BlackRock",
    "expense_ratio": 0.20,
    "asset_class": "Equity",
    "category": "Global",
    "benchmark": "MSCI World",
    "inception_date": "2009-09-25",
    "description": "Tracks developed market equities"
}
```

## Note

For production, use `etl_universe.py` to load the full universe from `data/etf_universe.csv`. This script may not match the current `etf_metadata` schema (e.g. `source_ticker`).
