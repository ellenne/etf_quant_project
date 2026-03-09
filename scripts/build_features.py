from __future__ import annotations

import math
import duckdb
import numpy as np
import pandas as pd

DB_PATH = "db/etf_data.duckdb"
TRADING_DAYS_YEAR = 252


def load_prices(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    query = """
        SELECT
            date,
            ticker,
            close,
            volume
        FROM prices_daily
        ORDER BY ticker, date
    """
    df = con.execute(query).df()

    if df.empty:
        raise ValueError("prices_daily is empty. Run etl_prices.py first.")

    df["date"] = pd.to_datetime(df["date"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["date", "ticker", "close"])

    return df


def compute_features_for_one_ticker(df_ticker: pd.DataFrame) -> pd.DataFrame:
    df = df_ticker.sort_values("date").copy()

    # Daily returns
    df["daily_ret"] = df["close"].pct_change()

    # Simple trailing returns
    df["ret_1w"] = df["close"] / df["close"].shift(5) - 1
    df["ret_1m"] = df["close"] / df["close"].shift(21) - 1
    df["ret_3m"] = df["close"] / df["close"].shift(63) - 1
    df["ret_6m"] = df["close"] / df["close"].shift(126) - 1
    df["ret_12m"] = df["close"] / df["close"].shift(252) - 1

    # Classic 12_1 momentum:
    # return from t-252 to t-21 (exclude most recent month)
    df["momentum_12_1"] = df["close"].shift(21) / df["close"].shift(252) - 1

    # Rolling annualized volatility
    df["vol_1m"] = df["daily_ret"].rolling(21).std() * math.sqrt(TRADING_DAYS_YEAR)
    df["vol_3m"] = df["daily_ret"].rolling(63).std() * math.sqrt(TRADING_DAYS_YEAR)
    df["vol_12m"] = df["daily_ret"].rolling(252).std() * math.sqrt(TRADING_DAYS_YEAR)

    # 200-day moving average trend filter
    rolling_200dma = df["close"].rolling(200).mean()
    df["price_vs_200dma"] = df["close"] / rolling_200dma - 1

    # All-time-high drawdown
    cummax_close = df["close"].cummax()
    df["ath_drawdown"] = df["close"] / cummax_close - 1

    # Rolling 1-year max drawdown
    rolling_max_1y = df["close"].rolling(252).max()
    current_dd_1y = df["close"] / rolling_max_1y - 1
    df["max_dd_1y"] = current_dd_1y.rolling(252).min()

    return df


def compute_all_features(prices: pd.DataFrame) -> pd.DataFrame:
    frames = []

    for ticker, grp in prices.groupby("ticker", sort=True):
        features = compute_features_for_one_ticker(grp)
        frames.append(features)

    out = pd.concat(frames, ignore_index=True)

    keep_cols = [
        "date",
        "ticker",
        "ret_1w",
        "ret_1m",
        "ret_3m",
        "ret_6m",
        "ret_12m",
        "momentum_12_1",
        "vol_1m",
        "vol_3m",
        "vol_12m",
        "max_dd_1y",
        "price_vs_200dma",
        "ath_drawdown",
    ]

    out = out[keep_cols].copy()

    # optional: remove rows where everything is null
    metric_cols = [c for c in keep_cols if c not in ["date", "ticker"]]
    out = out.dropna(subset=metric_cols, how="all")

    return out


def load_features(con: duckdb.DuckDBPyConnection, features: pd.DataFrame) -> None:
    # easiest early-stage approach: rebuild the whole table
    con.execute("DELETE FROM etf_features")

    con.register("features_df", features)
    con.execute("""
        INSERT INTO etf_features (
            date,
            ticker,
            ret_1w,
            ret_1m,
            ret_3m,
            ret_6m,
            ret_12m,
            momentum_12_1,
            vol_1m,
            vol_3m,
            vol_12m,
            max_dd_1y,
            price_vs_200dma,
            ath_drawdown
        )
        SELECT
            date,
            ticker,
            ret_1w,
            ret_1m,
            ret_3m,
            ret_6m,
            ret_12m,
            momentum_12_1,
            vol_1m,
            vol_3m,
            vol_12m,
            max_dd_1y,
            price_vs_200dma,
            ath_drawdown
        FROM features_df
    """)


def main() -> None:
    con = duckdb.connect(DB_PATH)

    prices = load_prices(con)
    print(f"Loaded {len(prices):,} price rows from prices_daily")

    features = compute_all_features(prices)
    print(f"Computed {len(features):,} feature rows")

    load_features(con, features)

    cnt = con.execute("SELECT COUNT(*) FROM etf_features").fetchone()[0]
    print(f"Loaded {cnt:,} rows into etf_features")

    sample = con.execute("""
        SELECT *
        FROM etf_features
        ORDER BY ticker, date DESC
        LIMIT 10
    """).fetchdf()

    print("\nSample rows:")
    print(sample.to_string(index=False))


if __name__ == "__main__":
    main()
