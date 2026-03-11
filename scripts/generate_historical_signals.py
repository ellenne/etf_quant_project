"""
Generate historical monthly ETF signals for all month-end dates in etf_features.

Loads into strategy_signals. Required for meaningful backtests.

Future: Add historical_signal_diagnostics table (signal_date, n_raw, n_valid,
n_trend, n_selected) for debugging and research.

Future: Explicit minimum-history rule (e.g. 252 or 273 trading days) for young
ETFs. Not needed now — feature nulls already exclude them.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "etf_data.duckdb"

# Scoring weights (must sum to 1.0)
WEIGHT_MOMENTUM = 0.35
WEIGHT_RET_3M = 0.20
WEIGHT_RET_1M = 0.15
WEIGHT_VOL = -0.15  # negative: lower vol is better
WEIGHT_DRAWDOWN = -0.15  # negative: smaller drawdown is better

TOP_N = 5
EQUAL_WEIGHT = 1.0 / TOP_N

REQUIRED_COLS = [
    "momentum_12_1",
    "ret_3m",
    "ret_1m",
    "vol_3m",
    "max_dd_1y",
    "price_vs_200dma",
]


def get_signal_dates(con: duckdb.DuckDBPyConnection) -> list[pd.Timestamp]:
    """
    Get last date of each calendar month in etf_features.
    """
    df = con.execute(
        """
        SELECT DISTINCT date
        FROM etf_features
        ORDER BY date
        """
    ).fetchdf()

    if df.empty:
        return []

    df["date"] = pd.to_datetime(df["date"])
    last_per_month = (
        df.groupby([df["date"].dt.year, df["date"].dt.month])["date"]
        .max()
        .tolist()
    )
    return sorted(last_per_month)


def load_features_for_date(
    con: duckdb.DuckDBPyConnection,
    signal_date: pd.Timestamp,
) -> pd.DataFrame:
    """Load all feature rows for the given date."""
    df = con.execute(
        """
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
        FROM etf_features
        WHERE date = ?
        """,
        [signal_date.date()],
    ).fetchdf()

    return df


def filter_valid_features(df: pd.DataFrame) -> pd.DataFrame:
    """Exclude ETFs with missing required features."""
    return df.dropna(subset=REQUIRED_COLS).copy()


def apply_trend_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only ETFs where price_vs_200dma > 0 (above 200-day MA)."""
    return df[df["price_vs_200dma"] > 0].copy()


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute cross-sectional score using percentile ranks.
    Higher momentum and returns are better; lower vol and drawdown are better.
    """
    out = df.copy()

    out["rank_mom"] = out["momentum_12_1"].rank(pct=True, method="average")
    out["rank_ret3m"] = out["ret_3m"].rank(pct=True, method="average")
    out["rank_ret1m"] = out["ret_1m"].rank(pct=True, method="average")
    out["rank_vol"] = out["vol_3m"].rank(pct=True, method="average")
    out["rank_dd"] = out["max_dd_1y"].abs().rank(pct=True, method="average")

    out["score"] = (
        WEIGHT_MOMENTUM * out["rank_mom"]
        + WEIGHT_RET_3M * out["rank_ret3m"]
        + WEIGHT_RET_1M * out["rank_ret1m"]
        + WEIGHT_VOL * out["rank_vol"]
        + WEIGHT_DRAWDOWN * out["rank_dd"]
    )

    return out


def select_top_and_assign_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Select top N ETFs by score, assign equal weight 0.20 each."""
    df = df.sort_values(["score", "ticker"], ascending=[False, True]).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    df["weight"] = 0.0

    top = df.head(TOP_N)
    top = top.copy()
    top["weight"] = EQUAL_WEIGHT

    return top[["date", "ticker", "score", "rank", "weight"]]


def process_signal_date(
    con: duckdb.DuckDBPyConnection,
    signal_date: pd.Timestamp,
) -> tuple[int, int, int, int, Optional[pd.DataFrame]]:
    """
    Process one signal date. Returns (n_raw, n_valid, n_trend, n_selected, signals_df).
    signals_df is None if month is skipped (no valid ETFs after filtering).
    """
    df = load_features_for_date(con, signal_date)
    n_raw = len(df)

    if df.empty:
        return 0, 0, 0, 0, None

    df = filter_valid_features(df)
    n_valid = len(df)

    if df.empty:
        return n_raw, 0, 0, 0, None

    df = apply_trend_filter(df)
    n_trend = len(df)

    if df.empty:
        return n_raw, n_valid, 0, 0, None

    df = compute_scores(df)
    signals = select_top_and_assign_weights(df)
    n_selected = len(signals)

    return n_raw, n_valid, n_trend, n_selected, signals


def get_per_month_diagnostics(
    con: duckdb.DuckDBPyConnection,
    signal_dates: list[pd.Timestamp],
) -> tuple[pd.DataFrame, list[pd.DataFrame], list[pd.Timestamp]]:
    """
    Process all signal dates. Returns:
      - diagnostics_df: signal_date, n_raw, n_valid, n_trend, n_selected per month
      - all_signals: list of signal DataFrames for months with valid data
      - processed_dates: dates corresponding to all_signals
    """
    rows = []
    all_signals: list[pd.DataFrame] = []
    processed_dates: list[pd.Timestamp] = []

    for d in signal_dates:
        n_raw, n_valid, n_trend, n_selected, signals_df = process_signal_date(con, d)
        rows.append(
            {
                "signal_date": d,
                "n_raw": n_raw,
                "n_valid": n_valid,
                "n_trend": n_trend,
                "n_selected": n_selected,
            }
        )
        if signals_df is not None and not signals_df.empty:
            all_signals.append(signals_df)
            processed_dates.append(d)

    return pd.DataFrame(rows), all_signals, processed_dates


def main() -> None:
    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
    except Exception as e:
        print(f"Error: Cannot connect to database at {DB_PATH}")
        print(f"  {e}")
        return

    try:
        run_historical_signal_generation(con)
    finally:
        con.close()


def run_historical_signal_generation(con: duckdb.DuckDBPyConnection) -> None:
    """Generate historical signals for all month-ends and load into strategy_signals."""
    signal_dates = get_signal_dates(con)

    if not signal_dates:
        print("Error: etf_features is empty. Run build_features.py first.")
        return

    print(f"Generating historical signals for {len(signal_dates)} month-ends")
    print(f"Date range: {signal_dates[0].date()} to {signal_dates[-1].date()}\n")

    # Delete all existing rows
    deleted = con.execute("SELECT COUNT(*) FROM strategy_signals").fetchone()[0]
    con.execute("DELETE FROM strategy_signals")
    print(f"Deleted {deleted} existing rows from strategy_signals\n")

    diagnostics_df, all_signals, processed_dates = get_per_month_diagnostics(
        con, signal_dates
    )

    if not all_signals:
        print("Error: No months had valid ETFs after filtering.")
        return

    # Insert all at once
    combined = pd.concat(all_signals, ignore_index=True)
    combined["date"] = combined["date"].dt.date
    con.register("signals_df", combined)
    con.execute(
        """
        INSERT INTO strategy_signals (date, ticker, score, rank, weight)
        SELECT date, ticker, score, rank, weight
        FROM signals_df
        """
    )

    total_rows = len(combined)

    # Summary (compute from diagnostics)
    processed = diagnostics_df[diagnostics_df["n_selected"] > 0]
    months_processed = len(processed)
    avg_trend = processed["n_trend"].mean()
    avg_selected = processed["n_selected"].mean()

    print("=" * 60)
    print("HISTORICAL SIGNAL GENERATION SUMMARY")
    print("=" * 60)
    print(f"First signal date:      {processed_dates[0].date()}")
    print(f"Last signal date:      {processed_dates[-1].date()}")
    print(f"Rebalance dates:       {months_processed}")
    print(f"Total rows inserted:   {total_rows}")
    print(f"Avg ETFs considered:   {avg_trend:.1f} (after filtering)")
    print(f"Avg ETFs selected:     {avg_selected:.1f}")

    # Sample: first 3 and last 3 rebalance dates with selected tickers
    print("\nSample rebalance dates (first 3 and last 3):")
    print("-" * 50)

    if len(all_signals) <= 6:
        for i in range(len(all_signals)):
            tickers = all_signals[i]["ticker"].tolist()
            print(f"  {processed_dates[i].date()}: {', '.join(tickers)}")
    else:
        for i in range(3):
            tickers = all_signals[i]["ticker"].tolist()
            print(f"  {processed_dates[i].date()}: {', '.join(tickers)}")
        print("  ...")
        for i in range(len(all_signals) - 3, len(all_signals)):
            tickers = all_signals[i]["ticker"].tolist()
            print(f"  {processed_dates[i].date()}: {', '.join(tickers)}")

    print("\nDone.")


if __name__ == "__main__":
    main()
