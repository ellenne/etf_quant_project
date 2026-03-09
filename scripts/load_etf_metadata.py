import duckdb
import pandas as pd

con = duckdb.connect("db/etf_data.duckdb")

data = [
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
]

df = pd.DataFrame(data)

con.execute("INSERT INTO etf_metadata SELECT * FROM df")

print("ETF inserted")
