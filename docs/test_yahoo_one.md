# test_yahoo_one.py

A minimal script to test a single Yahoo Finance download. Useful for debugging yfinance or ticker format issues.

## Usage

```bash
python scripts/test_yahoo_one.py
```

## What It Does

- Downloads IWDA.AS from Yahoo Finance (start=2020-01-01)
- Prints the first 5 rows and column names

## Use Case

- Verify yfinance is installed and working
- Test ticker format (e.g. IWDA.AS vs IWDA.L)
- Inspect raw DataFrame structure before ETL
