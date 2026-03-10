"""
Backtest the monthly ETF rotation strategy.

Uses strategy_signals for rebalance dates and weights, prices_daily for NAV.
Applies 10 bps transaction cost per traded weight. Outputs metrics and
saves results to backtest_daily, backtest_benchmarks, and backtest_summary.

IMPORTANT: Requires historical signals. Run generate_signals.py --historical first.
Otherwise strategy_signals has only the latest date and the backtest is not meaningful.

SQL schemas (tables created automatically if not exist):

  backtest_daily:     date DATE, nav DOUBLE
  backtest_benchmarks: date DATE, ticker TEXT, nav DOUBLE
  backtest_summary:   metric TEXT, value DOUBLE

Future improvements (not critical for MVP):
  A. Run metadata: run_id, run_timestamp, strategy_name (avoid wiping previous results)
  B. Benchmark metrics in DB: CAGR, vol, max_dd per benchmark (not just printed)
  C. Signal coverage diagnostics: avg ETFs per rebalance, count of rebalances with <5 ETFs
"""
from __future__ import annotations

import bisect
import math
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "etf_data.duckdb"

# Transaction cost: 10 basis points per traded weight change
COST_BPS = 10
COST_PER_WEIGHT = COST_BPS / 10_000  # 0.001

# Benchmarks for buy-and-hold comparison
BENCHMARK_TICKERS = ["SPY", "VWCE.DE", "IWDA.AS"]

# SQL schemas for backtest tables (create if not exist)
SCHEMA_BACKTEST_DAILY = """
CREATE TABLE IF NOT EXISTS backtest_daily (
    date DATE,
    nav DOUBLE
)
"""
SCHEMA_BACKTEST_BENCHMARKS = """
CREATE TABLE IF NOT EXISTS backtest_benchmarks (
    date DATE,
    ticker TEXT,
    nav DOUBLE
)
"""
SCHEMA_BACKTEST_SUMMARY = """
CREATE TABLE IF NOT EXISTS backtest_summary (
    metric TEXT,
    value DOUBLE
)
"""


def ensure_backtest_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Create backtest tables if they do not exist."""
    con.execute(SCHEMA_BACKTEST_DAILY)
    con.execute(SCHEMA_BACKTEST_BENCHMARKS)
    con.execute(SCHEMA_BACKTEST_SUMMARY)


def load_signals(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load strategy signals ordered by date."""
    df = con.execute(
        """
        SELECT date, ticker, score, rank, weight
        FROM strategy_signals
        WHERE weight > 0
        ORDER BY date
        """
    ).fetchdf()

    if df.empty:
        raise ValueError("strategy_signals is empty. Run generate_signals.py first.")

    df["date"] = pd.to_datetime(df["date"])
    return df


def load_prices(
    con: duckdb.DuckDBPyConnection,
    tickers: set[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Load daily close prices for given tickers and date range."""
    ticker_list = list(tickers)
    placeholders = ",".join(["?"] * len(ticker_list))
    params = [start_date.date(), end_date.date()] + ticker_list

    df = con.execute(
        f"""
        SELECT date, ticker, close
        FROM prices_daily
        WHERE date >= ? AND date <= ?
          AND ticker IN ({placeholders})
        ORDER BY date
        """,
        params,
    ).fetchdf()

    df["date"] = pd.to_datetime(df["date"])
    return df


def get_rebalance_dates(signals: pd.DataFrame) -> list[pd.Timestamp]:
    """Return sorted list of unique rebalance dates."""
    return sorted(signals["date"].unique().tolist())


def get_target_weights_for_date(
    signals: pd.DataFrame,
    signal_date: pd.Timestamp,
    prices: pd.DataFrame,
    execution_date: pd.Timestamp | None = None,
) -> dict[str, float]:
    """
    Get target weights for a rebalance.
    Uses signal_date for ticker selection; execution_date for price lookup
    (when signal date is not a trading day, we execute on the next).
    Skips ETFs with no valid price on execution date. Equal weight the rest.
    """
    day_signals = signals[signals["date"] == signal_date]
    if day_signals.empty:
        return {}

    price_date = execution_date if execution_date is not None else signal_date
    day_prices = prices[
        (prices["date"] == price_date) & (prices["close"].notna()) & (prices["close"] > 0)
    ]
    valid_tickers = set(day_prices["ticker"].unique())

    tickers = [t for t in day_signals["ticker"].tolist() if t in valid_tickers]
    if not tickers:
        return {}

    w = 1.0 / len(tickers)
    return {t: w for t in tickers}


def compute_turnover(
    old_weights: dict[str, float],
    new_weights: dict[str, float],
) -> float:
    """One-way turnover: 0.5 * sum of absolute weight changes."""
    all_tickers = set(old_weights) | set(new_weights)
    return 0.5 * sum(
        abs(new_weights.get(t, 0.0) - old_weights.get(t, 0.0))
        for t in all_tickers
    )


def map_rebalance_to_trading_dates(
    rebalance_dates: list[pd.Timestamp],
    trading_dates: list[pd.Timestamp],
) -> dict[pd.Timestamp, pd.Timestamp]:
    """
    Map each signal/rebalance date to the next available trading date.
    Returns effective_trading_date -> signal_date.

    When signal dates are month-end and the market is closed, we execute
    on the next trading day. If multiple signal dates map to the same
    trading date, we use the latest signal.
    """
    if not trading_dates:
        return {}
    trading_sorted = sorted(trading_dates)

    effective_to_signal: dict[pd.Timestamp, pd.Timestamp] = {}
    for r in rebalance_dates:
        idx = bisect.bisect_left(trading_sorted, r)
        if idx >= len(trading_sorted):
            continue
        next_trading = trading_sorted[idx]
        if next_trading not in effective_to_signal or r > effective_to_signal[next_trading]:
            effective_to_signal[next_trading] = r

    return effective_to_signal


def run_backtest(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    rebalance_dates: list[pd.Timestamp],
) -> tuple[pd.DataFrame, float, int]:
    """
    Run backtest. Returns (daily_nav_df, total_turnover, n_rebalances).

    Initial entry: Option A — no cost to enter the initial portfolio at NAV = 1.0.
    Transaction costs apply only at subsequent rebalances.
    (Option B would charge: nav *= 1 - COST_PER_WEIGHT * sum(weights) after init.)
    """
    # Build price pivot: date x ticker
    price_pivot = prices.pivot(index="date", columns="ticker", values="close")
    price_pivot = price_pivot.ffill()  # forward-fill missing prices
    # Note: ffill can create artificial flat returns when one ETF is closed and another
    # is open. For later: compute returns per ticker first, then join by date; use 0
    # return only when truly appropriate.

    all_dates = sorted(price_pivot.index.unique())
    if not all_dates:
        raise ValueError("No price data in date range.")

    # Map signal dates to next available trading date (handles month-end when market closed)
    effective_to_signal = map_rebalance_to_trading_dates(rebalance_dates, all_dates)
    if not effective_to_signal:
        raise ValueError("No rebalance date maps to a trading date.")

    first_effective = min(effective_to_signal.keys())
    start_idx = all_dates.index(first_effective) if first_effective in all_dates else 0
    trading_dates = all_dates[start_idx:]

    # Initial weights at first rebalance (use execution date for price lookup).
    # Option A: no initial entry cost — we start fully invested at NAV = 1.0.
    first_signal = effective_to_signal[first_effective]
    weights = get_target_weights_for_date(
        signals, first_signal, prices, execution_date=first_effective
    )
    if not weights:
        raise ValueError(
            f"No valid prices for any signal ETF on {first_effective.date()} "
            f"(signal date {first_signal.date()})"
        )

    nav_series = []
    nav = 1.0
    total_turnover = 0.0
    n_rebalances = 0

    prev_date = None

    for i, d in enumerate(trading_dates):
        # Rebalance? (d is effective trading date)
        if d in effective_to_signal:
            signal_date = effective_to_signal[d]
            new_weights = get_target_weights_for_date(
                signals, signal_date, prices, execution_date=d
            )
            if new_weights:
                turnover = compute_turnover(weights, new_weights)
                total_turnover += turnover
                n_rebalances += 1
                cost = COST_PER_WEIGHT * turnover
                nav *= 1 - cost
                weights = new_weights

        # Daily return
        row = price_pivot.loc[d]
        ret = 0.0
        for ticker, w in weights.items():
            p_today = row.get(ticker)
            if pd.isna(p_today) or p_today <= 0:
                continue
            if prev_date is not None and ticker in price_pivot.columns:
                p_yesterday = price_pivot.loc[prev_date, ticker]
                if pd.notna(p_yesterday) and p_yesterday > 0:
                    ret += w * (p_today / p_yesterday - 1)
            else:
                # First day for this ticker in our series
                ret += w * 0.0  # no prior price, assume flat

        nav *= 1 + ret
        nav_series.append({"date": d, "nav": nav})
        prev_date = d

    df = pd.DataFrame(nav_series)
    return df, total_turnover, n_rebalances


def compute_benchmark_nav(
    prices: pd.DataFrame,
    ticker: str,
    start_date: pd.Timestamp,
) -> pd.DataFrame:
    """Compute buy-and-hold NAV for a benchmark ticker."""
    t_prices = prices[prices["ticker"] == ticker].sort_values("date")
    if t_prices.empty:
        return pd.DataFrame()

    t_prices = t_prices[t_prices["date"] >= start_date]
    if t_prices.empty:
        return pd.DataFrame()

    t_prices = t_prices.copy()
    t_prices["nav"] = t_prices["close"] / t_prices["close"].iloc[0]
    return t_prices[["date", "nav"]].copy()


def compute_cagr_from_nav_series(df: pd.DataFrame) -> tuple[float, float] | None:
    """Compute total return and CAGR from a NAV series using its own date range."""
    if df.empty or len(df) < 2:
        return None
    total_return = df["nav"].iloc[-1] / df["nav"].iloc[0] - 1
    n_days = (df["date"].iloc[-1] - df["date"].iloc[0]).days
    n_years = n_days / 365.25 if n_days > 0 else 0
    if n_years <= 0:
        return (total_return, 0.0)
    cagr = (1 + total_return) ** (1 / n_years) - 1 if total_return > -1 else -1.0
    return (total_return, cagr)


def compute_metrics(
    nav_df: pd.DataFrame,
    total_turnover: float,
    n_rebalances: int,
) -> dict[str, float]:
    """Compute CAGR, vol, Sharpe, max DD, turnover."""
    if len(nav_df) < 2:
        return {}

    nav = nav_df["nav"].values
    start_date = nav_df["date"].iloc[0]
    end_date = nav_df["date"].iloc[-1]
    n_days = (end_date - start_date).days
    n_years = n_days / 365.25 if n_days > 0 else 0

    if n_years <= 0:
        return {}

    # CAGR
    total_return = nav[-1] / nav[0] - 1
    cagr = (1 + total_return) ** (1 / n_years) - 1 if total_return > -1 else -1.0

    # Daily returns
    daily_rets = pd.Series(nav).pct_change().dropna()

    # Annualized volatility
    vol_ann = daily_rets.std() * math.sqrt(252) if len(daily_rets) > 0 else 0.0

    # Sharpe (rf=0)
    sharpe = (daily_rets.mean() * 252) / vol_ann if vol_ann > 0 else 0.0

    # Max drawdown
    cummax = pd.Series(nav).cummax()
    drawdowns = (pd.Series(nav) - cummax) / cummax
    max_dd = drawdowns.min()

    # Annualized turnover
    turnover_ann = (total_turnover / n_rebalances) * 12 if n_rebalances > 0 else 0.0

    return {
        "cagr": cagr,
        "vol_ann": vol_ann,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "turnover_ann": turnover_ann,
        "n_years": n_years,
        "total_return": total_return,
    }


def save_results(
    con: duckdb.DuckDBPyConnection,
    nav_df: pd.DataFrame,
    benchmark_dfs: dict[str, pd.DataFrame],
    metrics: dict[str, float],
) -> None:
    """Save backtest results to database."""
    # Clear existing
    con.execute("DELETE FROM backtest_daily")
    con.execute("DELETE FROM backtest_benchmarks")
    con.execute("DELETE FROM backtest_summary")

    # Strategy NAV
    if not nav_df.empty:
        nav_df_out = nav_df[["date", "nav"]].copy()
        nav_df_out["date"] = nav_df_out["date"].dt.date
        con.register("nav_df", nav_df_out)
        con.execute(
            "INSERT INTO backtest_daily (date, nav) SELECT date, nav FROM nav_df"
        )

    # Benchmarks
    for ticker, bdf in benchmark_dfs.items():
        if bdf.empty:
            continue
        bdf_out = bdf[["date", "nav"]].copy()
        bdf_out["date"] = bdf_out["date"].dt.date
        bdf_out["ticker"] = ticker
        con.register("bench_df", bdf_out)
        con.execute(
            """
            INSERT INTO backtest_benchmarks (date, ticker, nav)
            SELECT date, ticker, nav FROM bench_df
            """
        )

    # Summary metrics
    summary_rows = [{"metric": k, "value": v} for k, v in metrics.items()]
    if summary_rows:
        summary_df = pd.DataFrame(summary_rows)
        con.register("summary_df", summary_df)
        con.execute(
            "INSERT INTO backtest_summary (metric, value) SELECT metric, value FROM summary_df"
        )


def main() -> None:
    try:
        con = duckdb.connect(str(DB_PATH), read_only=False)
    except Exception as e:
        print(f"Error: Cannot connect to database at {DB_PATH}")
        print(f"  {e}")
        return

    try:
        run_backtest_main(con)
    finally:
        con.close()


def run_backtest_main(con: duckdb.DuckDBPyConnection) -> None:
    """Core backtest logic."""
    ensure_backtest_tables(con)

    # 1. Load signals
    signals = load_signals(con)
    rebalance_dates = get_rebalance_dates(signals)
    print(f"Loaded {len(signals)} signal rows across {len(rebalance_dates)} rebalance dates")
    print(f"Date range: {rebalance_dates[0].date()} to {rebalance_dates[-1].date()}")

    # 2. Collect all tickers we need (signals + benchmarks)
    signal_tickers = set(signals["ticker"].unique())
    all_tickers = signal_tickers | set(BENCHMARK_TICKERS)

    # 3. Load prices
    start_date = rebalance_dates[0]
    end_date = con.execute("SELECT MAX(date) FROM prices_daily").fetchone()[0]
    end_date = pd.Timestamp(end_date) if end_date else rebalance_dates[-1]

    prices = load_prices(con, all_tickers, start_date, end_date)
    if prices.empty:
        print("Error: No price data for signal tickers in date range.")
        return

    print(f"Loaded {len(prices):,} price rows")

    # 4. Run backtest
    nav_df, total_turnover, n_rebalances = run_backtest(signals, prices, rebalance_dates)

    if nav_df.empty:
        print("Error: Backtest produced no NAV series.")
        return

    # 5. Compute metrics
    metrics = compute_metrics(nav_df, total_turnover, n_rebalances)

    # 6. Benchmark NAVs
    benchmark_dfs = {}
    for ticker in BENCHMARK_TICKERS:
        bdf = compute_benchmark_nav(prices, ticker, start_date)
        benchmark_dfs[ticker] = bdf
        if bdf.empty:
            print(f"  Benchmark {ticker}: no price data (skipped)")
        else:
            result = compute_cagr_from_nav_series(bdf)
            if result:
                b_ret, b_cagr = result
                print(f"  Benchmark {ticker}: total return {b_ret:.2%}, CAGR {b_cagr:.2%}")
            else:
                print(f"  Benchmark {ticker}: insufficient data")

    # 7. Save to DB
    save_results(con, nav_df, benchmark_dfs, metrics)

    # 8. Print summary
    print("\n" + "=" * 60)
    print("BACKTEST SUMMARY")
    print("=" * 60)
    print(f"Start date:           {nav_df['date'].iloc[0].date()}")
    print(f"End date:             {nav_df['date'].iloc[-1].date()}")
    print(f"Years:                {metrics.get('n_years', 0):.2f}")
    print(f"Total return:         {metrics.get('total_return', 0):.2%}")
    vol = metrics.get("vol_ann", 0)
    vol_str = f"{vol:.2%}" if pd.notna(vol) and vol != 0 else "N/A"
    print(f"CAGR:                 {metrics.get('cagr', 0):.2%}")
    print(f"Ann. volatility:      {vol_str}")
    print(f"Sharpe ratio (rf=0):  {metrics.get('sharpe', 0):.2f}")
    print(f"Max drawdown:         {metrics.get('max_dd', 0):.2%}")
    print(f"Ann. turnover:        {metrics.get('turnover_ann', 0):.2f}")
    print("\nResults saved to: backtest_daily, backtest_benchmarks, backtest_summary")
    print("Done.")


if __name__ == "__main__":
    main()
