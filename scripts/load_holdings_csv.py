"""
Load ETF holdings from a CSV file (manual copy/paste from provider websites).
Use this when you need full holdings beyond yfinance's top 10.

CSV format (header required):
  etf_ticker,etf_isin,snapshot_date,holding_name,holding_ticker,holding_isin,asset_type,sector,country,shares,weight,market_value

Minimal required columns: etf_ticker, holding_name, holding_ticker, weight
Other columns can be empty. snapshot_date defaults to today if missing.
"""
from __future__ import annotations

from datetime import date
from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = "db/etf_data.duckdb"
CSV_PATH = "data/etf_holdings.csv"


def main() -> None:
    csv_file = Path(CSV_PATH)
    if not csv_file.exists():
        print(f"Holdings file not found: {CSV_PATH}")
        print("\nCreate a CSV with columns (minimal: etf_ticker, holding_name, holding_ticker, weight):")
        print("  etf_ticker,etf_isin,snapshot_date,holding_name,holding_ticker,holding_isin,asset_type,sector,country,shares,weight,market_value")
        print("\nExample rows:")
        print("  SPY,US78462F1030,2025-03-09,NVIDIA Corp,NVDA,,Equity,Technology,United States,,7.31,,")
        print("  SPY,US78462F1030,2025-03-09,Apple Inc,AAPL,,Equity,Technology,United States,,6.63,,")
        print("\nCopy from: finance.yahoo.com/quote/SPY/holdings/ or provider sites (iShares, Vanguard, SPDR)")
        return

    df = pd.read_csv(csv_file)

    required = ["etf_ticker", "holding_name", "holding_ticker", "weight"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV must have columns: {required}. Missing: {missing}")

    df = df.copy()

    # Fill defaults
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = date.today()
    else:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    for col in ["etf_isin", "holding_isin", "asset_type", "sector", "country", "shares", "market_value", "data_source"]:
        if col not in df.columns:
            df[col] = None

    df["data_source"] = df["data_source"].fillna("manual_csv")

    con = duckdb.connect(DB_PATH)

    # Delete existing rows for these ETF/snapshot_date pairs
    for (etf, snap) in df[["etf_ticker", "snapshot_date"]].drop_duplicates().itertuples(index=False):
        con.execute(
            "DELETE FROM etf_holdings WHERE etf_ticker = ? AND snapshot_date = ?",
            [etf, snap],
        )

    # Ensure all table columns exist (fill missing with None)
    cols = [
        "snapshot_date", "etf_ticker", "etf_isin", "holding_name", "holding_ticker",
        "holding_isin", "asset_type", "sector", "country", "shares", "weight",
        "market_value", "data_source",
    ]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols]

    con.register("holdings_df", df)
    con.execute("""
        INSERT INTO etf_holdings (
            snapshot_date, etf_ticker, etf_isin, holding_name, holding_ticker,
            holding_isin, asset_type, sector, country, shares, weight,
            market_value, data_source
        )
        SELECT snapshot_date, etf_ticker, etf_isin, holding_name, holding_ticker,
               holding_isin, asset_type, sector, country, shares, weight,
               market_value, data_source
        FROM holdings_df
    """)

    row_count = len(df)
    print(f"Loaded {row_count} holdings from {CSV_PATH}")


if __name__ == "__main__":
    main()
