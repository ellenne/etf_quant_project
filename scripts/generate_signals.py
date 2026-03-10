"""
Generate strategy signals from ETF features.

Reads the latest date from etf_features, applies filters and scoring,
and writes top 5 ETFs (equal-weighted) to strategy_signals.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

# Paths relative to project root
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


def get_latest_date(con: duckdb.DuckDBPyConnection) -> Optional[pd.Timestamp]:
    """Get the most recent date in etf_features."""
    result = con.execute(
        "SELECT MAX(date) AS max_date FROM etf_features"
    ).fetchone()[0]
    return pd.Timestamp(result) if result is not None else None


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
    """
    Exclude ETFs with missing required features.
    Required: momentum_12_1, vol_3m, max_dd_1y must not be null.
    """
    required = ["momentum_12_1", "ret_3m", "ret_1m", "vol_3m", "max_dd_1y", "price_vs_200dma"]
    return df.dropna(subset=required).copy()


def apply_trend_filter(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only ETFs where price_vs_200dma > 0 (above 200-day MA)."""
    return df[df["price_vs_200dma"] > 0].copy()


def compute_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute ranking score using percentile ranks.

    Higher momentum and returns are better.
    Lower volatility and smaller drawdown are better.
    """
    out = df.copy()

    # Percentile rank [0, 1]: higher value = higher rank
    out["rank_mom"] = out["momentum_12_1"].rank(pct=True, method="average")
    out["rank_ret3m"] = out["ret_3m"].rank(pct=True, method="average")
    out["rank_ret1m"] = out["ret_1m"].rank(pct=True, method="average")
    out["rank_vol"] = out["vol_3m"].rank(pct=True, method="average")
    out["rank_dd_bad"] = out["max_dd_1y"].abs().rank(pct=True, method="average")

    out["score"] = (
        WEIGHT_MOMENTUM * out["rank_mom"]
        + WEIGHT_RET_3M * out["rank_ret3m"]
        + WEIGHT_RET_1M * out["rank_ret1m"]
        + WEIGHT_VOL * out["rank_vol"]
        + WEIGHT_DRAWDOWN * out["rank_dd_bad"]
    )

    return out


def select_top_and_assign_weights(df: pd.DataFrame) -> pd.DataFrame:
    """
    Select top N ETFs by score, assign equal weight.
    Others get weight 0 and are excluded from output.
    """
    df = df.sort_values("score", ascending=False).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)
    df["weight"] = 0.0

    top = df.head(TOP_N)
    top = top.copy()
    top["weight"] = EQUAL_WEIGHT

    return top[["date", "ticker", "score", "rank", "weight"]]


def delete_existing_signals(
    con: duckdb.DuckDBPyConnection,
    signal_date: pd.Timestamp,
) -> int:
    """Delete existing strategy_signals for the given date. Returns deleted count."""
    deleted = con.execute(
        "SELECT COUNT(*) FROM strategy_signals WHERE date = ?",
        [signal_date.date()],
    ).fetchone()[0]

    con.execute("DELETE FROM strategy_signals WHERE date = ?", [signal_date.date()])
    return deleted


def insert_signals(
    con: duckdb.DuckDBPyConnection,
    signals: pd.DataFrame,
) -> None:
    """Insert signal rows into strategy_signals."""
    if signals.empty:
        return

    con.register("signals_df", signals)
    con.execute(
        """
        INSERT INTO strategy_signals (date, ticker, score, rank, weight)
        SELECT date, ticker, score, rank, weight
        FROM signals_df
        """
    )


def main() -> None:
    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
    except Exception as e:
        print(f"Error: Cannot connect to database at {DB_PATH}")
        print(f"  {e}")
        return

    try:
        run_signal_generation(con)
    finally:
        con.close()


def run_signal_generation(con: duckdb.DuckDBPyConnection) -> None:
    """Core logic: load features, filter, score, and write signals."""
    # 1. Get latest date
    latest_date = get_latest_date(con)
    if latest_date is None:
        print("Error: etf_features is empty. Run build_features.py first.")
        return

    print(f"Latest feature date: {latest_date.date()}")

    # 2. Load features for that date
    df = load_features_for_date(con, latest_date)
    n_raw = len(df)

    if df.empty:
        print(f"Error: No feature rows for {latest_date.date()}")
        return

    print(f"ETFs with features on this date: {n_raw}")

    # 3. Filter: valid features
    df = filter_valid_features(df)
    n_valid = len(df)
    print(f"After excluding null momentum/vol/drawdown: {n_valid}")

    if df.empty:
        print("Error: No ETFs passed the validity filter.")
        return

    # 4. Filter: trend (price above 200d MA)
    df = apply_trend_filter(df)
    n_trend = len(df)
    print(f"After trend filter (price_vs_200dma > 0): {n_trend}")

    if df.empty:
        print("Error: No ETFs passed the trend filter.")
        return

    # 5. Compute scores
    df = compute_scores(df)

    # 6. Select top N and assign weights
    signals = select_top_and_assign_weights(df)
    n_selected = len(signals)

    # 7. Delete existing rows for this date
    deleted = delete_existing_signals(con, latest_date)
    if deleted > 0:
        print(f"Deleted {deleted} existing signal rows for {latest_date.date()}")

    # 8. Insert new signals
    insert_signals(con, signals)
    print(f"Inserted {n_selected} signal rows")

    # 9. Summary
    print("\n" + "=" * 60)
    print("SIGNAL GENERATION SUMMARY")
    print("=" * 60)
    print(f"Latest signal date:    {latest_date.date()}")
    print(f"ETFs considered:      {n_trend}")
    print(f"ETFs selected:        {n_selected}")

    # 10. Top 10 ranked (by score) for display
    top10 = df.sort_values("score", ascending=False).head(10)
    print("\nTop 10 ranked ETFs (by score):")
    print("-" * 50)
    for i, row in enumerate(top10.itertuples(), 1):
        sel = " *" if i <= TOP_N else ""
        print(f"  {i:2}. {row.ticker:8}  score={row.score:.4f}{sel}")

    print("\nDone.")


if __name__ == "__main__":
    main()
