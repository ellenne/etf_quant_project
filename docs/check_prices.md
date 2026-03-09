# check_prices.py

Inspects the `prices_daily` table: row count and a sample of rows.

## Usage

```bash
python scripts/check_prices.py
```

## Output

```
Row count:
[(12345,)]

Sample:
(date, ticker, close, volume)
(date, ticker, close, volume)
...
```

## Use Case

- Verify `etl_prices.py` ran successfully
- Quick sanity check on price data
