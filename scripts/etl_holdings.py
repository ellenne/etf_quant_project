"""
ETL for ETF holdings. Fetches top 10 holdings from Yahoo Finance via yfinance.

WHY ONLY TOP 10?
  Yahoo Finance's API (used by yfinance) only exposes the top 10 holdings.
  There is no way to get full holdings programmatically from yfinance.

FOR FULL HOLDINGS:
  Use load_holdings_csv.py with data copied from provider websites:
  - iShares: ishares.com → fund page → Holdings → Detailed Holdings (CSV)
  - Vanguard: vanguard.com → fund page → Holdings
  - SPDR: ssga.com → fund page → Holdings
"""
from __future__ import annotations

import argparse
from datetime import date
import duckdb
import pandas as pd
import requests
import yfinance as yf

DB_PATH = "db/etf_data.duckdb"

# Cache for symbol -> ISIN lookups (same stock appears in multiple ETFs)
_ISIN_CACHE: dict[str, str | None] = {}


def get_symbol_for_isin(isin: str) -> str | None:
    """Look up Yahoo Finance symbol from ISIN using Yahoo search API."""
    url = "https://query1.finance.yahoo.com/v1/finance/search"
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.109 Safari/537.36",
    }
    params = {
        "q": isin,
        "quotesCount": 1,
        "newsCount": 0,
        "listsCount": 0,
        "quotesQueryId": "tss_match_phrase_query",
    }
    resp = requests.get(url=url, headers=headers, params=params)
    data = resp.json()
    if "quotes" in data and len(data["quotes"]) > 0:
        return data["quotes"][0]["symbol"]
    return None


def get_isin_for_symbol(symbol: str) -> str | None:
    """Look up ISIN for a holding symbol. Uses yfinance (symbol -> ISIN). Cached."""
    if not symbol or not isinstance(symbol, str):
        return None
    symbol = str(symbol).strip()
    if symbol in _ISIN_CACHE:
        return _ISIN_CACHE[symbol]
    try:
        t = yf.Ticker(symbol)
        isin = t.isin
        result = isin if (isin and isin != "-") else None
        _ISIN_CACHE[symbol] = result
        return result
    except Exception:  # HTTP 404, network errors, etc. - skip ISIN for this symbol
        _ISIN_CACHE[symbol] = None
        return None


def fetch_etfs(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str, str]]:
    """Returns (ticker, source_ticker, isin) for each ETF."""
    rows = con.execute("""
        SELECT ticker, source_ticker, isin
        FROM etf_metadata
        WHERE source_ticker IS NOT NULL
        ORDER BY ticker
    """).fetchall()
    return rows


def download_holdings(
    etf_ticker: str,
    yf_ticker: str,
    etf_isin: str,
    snapshot_date: date,
) -> pd.DataFrame:
    """
    Fetch top 10 holdings from Yahoo Finance.
    Returns DataFrame with columns matching etf_holdings table.
    """
    t = yf.Ticker(yf_ticker)
    try:
        fd = t.funds_data
        if fd is None:
            return pd.DataFrame()
        top = fd.top_holdings
        if top is None or top.empty:
            return pd.DataFrame()
    except Exception:
        return pd.DataFrame()

    # top_holdings has: Symbol (index), Name, Holding Percent
    top = top.reset_index()
    top.columns = [c.strip().lower().replace(" ", "_") for c in top.columns]

    # Map to etf_holdings schema (yfinance: Symbol, Name, Holding Percent)
    holding_tickers = top["symbol"].tolist()
    holding_isins = [get_isin_for_symbol(s) for s in holding_tickers]

    result = pd.DataFrame({
        "snapshot_date": snapshot_date,
        "etf_ticker": etf_ticker,
        "etf_isin": etf_isin,
        "holding_name": top["name"],
        "holding_ticker": holding_tickers,
        "holding_isin": holding_isins,
        "asset_type": None,
        "sector": None,
        "country": None,
        "shares": None,
        "weight": top["holding_percent"],
        "market_value": None,
        "data_source": "yfinance",
    })

    return result


def load_holdings(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    # Delete existing rows for this ETF on this snapshot date
    con.execute("""
        DELETE FROM etf_holdings
        WHERE etf_ticker = ?
          AND snapshot_date = ?
    """, [df["etf_ticker"].iloc[0], df["snapshot_date"].iloc[0]])

    con.register("holdings_df", df)
    con.execute("""
        INSERT INTO etf_holdings (
            snapshot_date, etf_ticker, etf_isin, holding_name, holding_ticker,
            holding_isin, asset_type, sector, country, shares, weight,
            market_value, data_source
        )
        SELECT
            snapshot_date, etf_ticker, etf_isin, holding_name, holding_ticker,
            holding_isin, asset_type, sector, country, shares, weight,
            market_value, data_source
        FROM holdings_df
    """)
    return len(df)


def main() -> None:
    parser = argparse.ArgumentParser(description="ETL ETF holdings from Yahoo Finance (top 10)")
    parser.add_argument(
        "--delete-today",
        action="store_true",
        help="Delete today's holdings snapshot before fetching (use to replace top-10 with fresh data)",
    )
    args = parser.parse_args()

    con = duckdb.connect(DB_PATH)
    etfs = fetch_etfs(con)
    snapshot_date = date.today()

    if not etfs:
        print("No ETFs found in etf_metadata. Run etl_universe.py first.")
        return

    if args.delete_today:
        deleted = con.execute(
            "SELECT COUNT(*) FROM etf_holdings WHERE snapshot_date = ?",
            [snapshot_date],
        ).fetchone()[0]
        con.execute("DELETE FROM etf_holdings WHERE snapshot_date = ?", [snapshot_date])
        print(f"Deleted {deleted} existing rows for {snapshot_date}\n")

    total_rows = 0
    failed: list[str] = []

    for ticker, source_ticker, isin in etfs:
        try:
            print(f"Fetching holdings for {ticker}...")
            df = download_holdings(ticker, source_ticker, isin, snapshot_date)
            inserted = load_holdings(con, df)
            total_rows += inserted
            print(f"  -> inserted {inserted} rows (top 10 holdings)")
        except Exception as exc:
            failed.append(ticker)
            print(f"  -> failed: {ticker} | {exc}")

    final_count = con.execute("SELECT COUNT(*) FROM etf_holdings").fetchone()[0]
    print("\nDone.")
    print(f"Total inserted this run: {total_rows}")
    print(f"Total rows in etf_holdings: {final_count}")

    if failed:
        print("Failed tickers:")
        for t in failed:
            print(f" - {t}")

    print(f"\nSnapshot date: {snapshot_date}")
    print("Note: yfinance returns only top 10 holdings per ETF.")
    print("For ALL holdings: use load_holdings_csv.py with data from provider websites.")


if __name__ == "__main__":
    main()
