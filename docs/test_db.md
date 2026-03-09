# test_db.py

Lists all tables in the DuckDB database. A simple connectivity and schema check.

## Usage

```bash
python scripts/test_db.py
```

## Output

```
Tables in DB:
('etf_metadata',)
('prices_daily',)
('etf_holdings',)
('etf_features',)
('strategy_signals',)
```

## Use Case

- Verify database exists and is readable
- Confirm tables were created by `init_db.py`
