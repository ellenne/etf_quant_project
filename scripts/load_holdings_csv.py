"""
Load ETF holdings from a CSV file (manual copy/paste from provider websites).
Use this when you need full holdings beyond yfinance's top 10.

CSV format (header required):
  etf_ticker,etf_isin,snapshot_date,holding_name,holding_ticker,holding_isin,asset_type,sector,country,shares,weight,market_value

Minimal required columns: etf_ticker, holding_name, holding_ticker, weight
Other columns can be empty. snapshot_date defaults to today if missing.
Weight can be "1.96%" or 1.96 (percent sign stripped automatically).
"""
from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
import duckdb
import pandas as pd

# Paths relative to project root (parent of scripts/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "etf_data.duckdb"
DEFAULT_CSV = PROJECT_ROOT / "data" / "etf_holdings.csv"
FALLBACK_CSV = PROJECT_ROOT / "data" / "etf_holdings_template.csv"


def main() -> None:
    parser = argparse.ArgumentParser(description="Load ETF holdings from CSV")
    parser.add_argument(
        "--csv",
        type=Path,
        default=None,
        help="Path to CSV file (default: data/etf_holdings.csv, fallback: data/etf_holdings_template.csv)",
    )
    args = parser.parse_args()

    csv_file = args.csv
    if csv_file is None:
        csv_file = DEFAULT_CSV if DEFAULT_CSV.exists() else FALLBACK_CSV
    else:
        csv_file = Path(csv_file)
        if not csv_file.is_absolute():
            csv_file = PROJECT_ROOT / csv_file

    if not csv_file.exists():
        print(f"Holdings file not found: {csv_file}")
        print("\nCreate a CSV with columns (minimal: etf_ticker, holding_name, holding_ticker, weight):")
        print("  etf_ticker,etf_isin,snapshot_date,holding_name,holding_ticker,holding_isin,asset_type,sector,country,shares,weight,market_value")
        print("\nExample rows:")
        print("  SPY,US78462F1030,2025-03-09,NVIDIA Corp,NVDA,,Equity,Technology,United States,,7.31,,")
        print("  SPY,US78462F1030,2025-03-09,Apple Inc,AAPL,,Equity,Technology,United States,,6.63,,")
        print("\nCopy from: finance.yahoo.com/quote/SPY/holdings/ or provider sites (iShares, Vanguard, SPDR)")
        return

    print(f"Loading from: {csv_file}")
    df = pd.read_csv(csv_file)

    required = ["etf_ticker", "holding_name", "weight"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"CSV must have columns: {required}. Missing: {missing}")

    df = df.copy()

    # Parse weight: strip "%" and convert to numeric (e.g. "1.96%" -> 1.96)
    df["weight"] = df["weight"].astype(str).str.replace("%", "", regex=False).str.strip()
    df["weight"] = pd.to_numeric(df["weight"], errors="coerce")

    # holding_ticker optional (bonds often have none)
    if "holding_ticker" not in df.columns:
        df["holding_ticker"] = None
    else:
        df["holding_ticker"] = df["holding_ticker"].fillna("").astype(str)

    # Fill defaults
    if "snapshot_date" not in df.columns:
        df["snapshot_date"] = date.today()
    else:
        df["snapshot_date"] = pd.to_datetime(df["snapshot_date"]).dt.date

    for col in ["etf_isin", "holding_isin", "asset_type", "sector", "country", "shares", "market_value", "data_source"]:
        if col not in df.columns:
            df[col] = None

    df["data_source"] = df["data_source"].fillna("manual_csv")

    con = duckdb.connect(str(DB_PATH))

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
    print(f"Loaded {row_count} holdings from {csv_file}")


if __name__ == "__main__":
    main()
