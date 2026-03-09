from __future__ import annotations

import duckdb
import pandas as pd
import yfinance as yf

DB_PATH = "db/etf_data.duckdb"


def fetch_tickers(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    rows = con.execute("""
        SELECT DISTINCT ticker, source_ticker
        FROM etf_metadata
        WHERE source_ticker IS NOT NULL
        ORDER BY ticker
    """).fetchall()
    return rows

def flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flatten MultiIndex columns returned by yfinance.
    Keeps only the first level, e.g. ('Close', 'IWDA.AS') -> 'Close'
    """

    if isinstance(df.columns, pd.MultiIndex):
        flattened = []
        for col in df.columns:
            if isinstance(col, tuple):
                flattened.append(str(col[0]))
            else:
                flattened.append(str(col))
        df.columns = flattened
    else:
        df.columns = [str(c) for c in df.columns]
    return df

def download_prices(ticker: str, start: str = "2015-01-01") -> pd.DataFrame:
    df = yf.download(
        ticker,
        start=start,
        auto_adjust=False,
        progress=False,
    )

    if df.empty:
        return pd.DataFrame()

    print("RAW COLUMNS:", df.columns)
    df = flatten_columns(df)
    print("FLATTENED COLUMNS:", df.columns)
    df = df.reset_index()
    print("AFTER RESET_INDEX:", df.columns)

    # normalize column names
    df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    if "date" not in df.columns:
        raise ValueError(
            f"'date' column not found for ticker {ticker}. Columns: {df.columns.tolist()}"
        )

    if "close" not in df.columns:
        raise ValueError(
            f"'close' column not found for ticker {ticker}. Columns: {df.columns.tolist()}"
        )

    result = pd.DataFrame({
        "date": pd.to_datetime(df["date"]).dt.date,
        "ticker": ticker,
        "close": pd.to_numeric(df["close"], errors="coerce"),
        "volume": pd.to_numeric(df["volume"], errors="coerce") if "volume" in df.columns else None,
    })

    result = result.dropna(subset=["date", "close"])
    return result


def load_prices(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    con.register("prices_df", df)

    con.execute("""
        DELETE FROM prices_daily
        USING prices_df
        WHERE prices_daily.ticker = prices_df.ticker
          AND prices_daily.date = prices_df.date
    """)

    con.execute("""
        INSERT INTO prices_daily (date, ticker, close, volume)
        SELECT date, ticker, close, volume
        FROM prices_df
    """)

    return len(df)


def main() -> None:
    con = duckdb.connect(DB_PATH)
    tickers = fetch_tickers(con)

    if not tickers:
        print("No tickers found in etf_metadata. Run etl_universe.py first.")
        return

    total_rows = 0
    failed: list[str] = []

    for ticker, source_ticker in tickers:
        try:
            print(f"Downloading {ticker} using source ticker {source_ticker}...")
            df = download_prices(source_ticker)
            if not df.empty:
                df["ticker"] = ticker
            inserted = load_prices(con, df)
            total_rows += inserted
            print(f"  -> inserted {inserted} rows")
        except Exception as exc:
            failed.append((ticker, source_ticker))
            print(f"  -> failed: {ticker} ({source_ticker}) | {exc}")

    final_count = con.execute("SELECT COUNT(*) FROM prices_daily").fetchone()[0]
    print("\nDone.")
    print(f"Total inserted this run: {total_rows}")
    print(f"Total rows in prices_daily: {final_count}")

    if failed:
        print("Failed tickers:")
        for t in failed:
            print(f" - {t}")


if __name__ == "__main__":
    main()