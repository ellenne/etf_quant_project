"""
Analyze ETF strategy backtest results.

Reads backtest_daily, backtest_benchmarks, strategy_signals, etf_metadata.
Produces diagnostics and saves to strategy_diagnostics, benchmark_comparison,
signal_diagnostics, sector_diagnostics.
"""
from __future__ import annotations

import math
from pathlib import Path
from typing import Optional

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DB_PATH = PROJECT_ROOT / "db" / "etf_data.duckdb"

TRADING_DAYS_YEAR = 252
ROLLING_WINDOW = 252


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Check if a table exists."""
    result = con.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_name = ?
        """,
        [name],
    ).fetchone()[0]
    return result > 0


def table_empty(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """Check if a table is empty."""
    result = con.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
    return result == 0


def ensure_table(con: duckdb.DuckDBPyConnection, name: str, ddl: str) -> None:
    """Create table if not exists."""
    con.execute(ddl)


def load_strategy_nav(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load strategy NAV from backtest_daily."""
    if not table_exists(con, "backtest_daily"):
        raise ValueError("Table backtest_daily does not exist. Run backtest_strategy.py first.")
    if table_empty(con, "backtest_daily"):
        raise ValueError("backtest_daily is empty. Run backtest_strategy.py first.")

    df = con.execute("SELECT date, nav FROM backtest_daily ORDER BY date").fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_benchmark_navs(con: duckdb.DuckDBPyConnection) -> dict[str, pd.DataFrame]:
    """Load benchmark NAVs from backtest_benchmarks."""
    if not table_exists(con, "backtest_benchmarks"):
        return {}

    df = con.execute(
        "SELECT date, ticker, nav FROM backtest_benchmarks ORDER BY date"
    ).fetchdf()
    if df.empty:
        return {}

    df["date"] = pd.to_datetime(df["date"])
    return {t: g[["date", "nav"]].copy() for t, g in df.groupby("ticker")}


def load_strategy_signals(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load strategy signals (weight > 0 only)."""
    if not table_exists(con, "strategy_signals"):
        raise ValueError("Table strategy_signals does not exist.")
    if table_empty(con, "strategy_signals"):
        raise ValueError("strategy_signals is empty. Run generate_historical_signals.py first.")

    df = con.execute(
        """
        SELECT date, ticker, score, rank, weight
        FROM strategy_signals
        WHERE weight > 0
        ORDER BY date
        """
    ).fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_etf_metadata(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load ETF metadata."""
    if not table_exists(con, "etf_metadata"):
        return pd.DataFrame()

    df = con.execute(
        "SELECT ticker, name, sector, asset_class, category FROM etf_metadata"
    ).fetchdf()
    return df


def compute_strategy_diagnostics(nav_df: pd.DataFrame) -> pd.DataFrame:
    """Compute cumulative return, daily returns, rolling 12m return, vol, max dd."""
    df = nav_df.sort_values("date").copy()
    df["daily_ret"] = df["nav"].pct_change()
    df["cum_ret"] = df["nav"] / df["nav"].iloc[0] - 1
    df["rolling_12m_ret"] = df["nav"] / df["nav"].shift(ROLLING_WINDOW) - 1
    df["rolling_12m_vol"] = df["daily_ret"].rolling(ROLLING_WINDOW).std() * math.sqrt(
        TRADING_DAYS_YEAR
    )
    cummax = df["nav"].cummax()
    df["rolling_max_dd"] = (df["nav"] - cummax) / cummax
    return df


def compute_series_metrics(nav_df: pd.DataFrame) -> dict[str, float]:
    """Compute total return, CAGR, vol, Sharpe, max dd for a NAV series."""
    if len(nav_df) < 2:
        return {}

    nav = nav_df["nav"].values
    start = nav_df["date"].iloc[0]
    end = nav_df["date"].iloc[-1]
    n_days = (end - start).days
    n_years = n_days / 365.25 if n_days > 0 else 0

    if n_years <= 0:
        return {}

    total_return = nav[-1] / nav[0] - 1
    cagr = (1 + total_return) ** (1 / n_years) - 1 if total_return > -1 else -1.0

    daily_rets = pd.Series(nav).pct_change().dropna()
    vol_ann = daily_rets.std() * math.sqrt(252) if len(daily_rets) > 0 else 0.0
    sharpe = (daily_rets.mean() * 252) / vol_ann if vol_ann > 0 else 0.0

    cummax = pd.Series(nav).cummax()
    drawdowns = (pd.Series(nav) - cummax) / cummax
    max_dd = drawdowns.min()

    return {
        "total_return": total_return,
        "cagr": cagr,
        "vol_ann": vol_ann,
        "sharpe": sharpe,
        "max_dd": max_dd,
    }


def align_benchmarks(
    strategy_nav: pd.DataFrame,
    benchmark_dfs: dict[str, pd.DataFrame],
) -> dict[str, pd.DataFrame]:
    """Align strategy and benchmarks to common dates."""
    strategy_dates = set(strategy_nav["date"])
    result = {"Strategy": strategy_nav.copy()}

    for ticker, bdf in benchmark_dfs.items():
        if bdf.empty:
            continue
        common = bdf[bdf["date"].isin(strategy_dates)].copy()
        if len(common) < 2:
            continue
        result[ticker] = common.sort_values("date")

    return result


def compute_signal_diagnostics(signals: pd.DataFrame) -> tuple[pd.DataFrame, float, int]:
    """
    Per-ETF: n_selections, avg_rank, first_date, last_date.
    Returns (diagnostics_df, avg_etfs_per_month, months_with_fewer_than_5).
    """
    if signals.empty:
        return pd.DataFrame(), 0.0, 0

    agg = signals.groupby("ticker").agg(
        n_selections=("ticker", "count"),
        avg_rank=("rank", "mean"),
        first_date=("date", "min"),
        last_date=("date", "max"),
    ).reset_index()

    etfs_per_month = signals.groupby("date").size()
    avg_etfs = float(etfs_per_month.mean())
    months_under_5 = int((etfs_per_month < 5).sum())

    return agg, avg_etfs, months_under_5


def compute_sector_diagnostics(
    signals: pd.DataFrame,
    metadata: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """
    Sector diagnostics. Returns (sector_counts, sector_over_time, latest_sectors).
    """
    if signals.empty or metadata.empty or "sector" not in metadata.columns:
        return pd.DataFrame(), pd.DataFrame(), []

    merged = signals.merge(metadata[["ticker", "sector"]], on="ticker", how="left")
    merged["sector"] = merged["sector"].fillna("Unknown")

    sector_counts = merged.groupby("sector").size().reset_index(name="n_selections")
    sector_counts = sector_counts.sort_values("n_selections", ascending=False)

    sector_over_time = (
        merged.groupby(["date", "sector"]).size().reset_index(name="n_selected")
    )

    latest_date = signals["date"].max()
    latest = merged[merged["date"] == latest_date]["sector"].unique().tolist()
    latest_sectors = [s for s in latest if s and s != "Unknown"]

    return sector_counts, sector_over_time, latest_sectors


def save_strategy_diagnostics(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
) -> None:
    """Save strategy diagnostics to DB."""
    con.execute("DROP TABLE IF EXISTS strategy_diagnostics")
    con.execute(
        """
        CREATE TABLE strategy_diagnostics (
            date DATE,
            nav DOUBLE,
            daily_ret DOUBLE,
            cum_ret DOUBLE,
            rolling_12m_ret DOUBLE,
            rolling_12m_vol DOUBLE,
            rolling_max_dd DOUBLE
        )
        """
    )
    if not df.empty:
        out = df.copy()
        out["date"] = out["date"].dt.date
        con.register("diag_df", out)
        con.execute(
            """
            INSERT INTO strategy_diagnostics
            SELECT date, nav, daily_ret, cum_ret, rolling_12m_ret, rolling_12m_vol, rolling_max_dd
            FROM diag_df
            """
        )


def save_benchmark_comparison(
    con: duckdb.DuckDBPyConnection,
    comparison: dict[str, dict],
) -> None:
    """Save benchmark comparison to DB."""
    con.execute("DROP TABLE IF EXISTS benchmark_comparison")
    con.execute(
        """
        CREATE TABLE benchmark_comparison (
            ticker TEXT,
            total_return DOUBLE,
            cagr DOUBLE,
            vol_ann DOUBLE,
            sharpe DOUBLE,
            max_dd DOUBLE
        )
        """
    )
    if comparison:
        rows = [
            {
                "ticker": t,
                "total_return": m["total_return"],
                "cagr": m["cagr"],
                "vol_ann": m["vol_ann"],
                "sharpe": m["sharpe"],
                "max_dd": m["max_dd"],
            }
            for t, m in comparison.items()
        ]
        df = pd.DataFrame(rows)
        con.register("comp_df", df)
        con.execute(
            """
            INSERT INTO benchmark_comparison
            SELECT ticker, total_return, cagr, vol_ann, sharpe, max_dd
            FROM comp_df
            """
        )


def save_signal_diagnostics(
    con: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    avg_etfs: float,
    months_under_5: int,
) -> None:
    """Save signal diagnostics to DB."""
    con.execute("DROP TABLE IF EXISTS signal_diagnostics")
    con.execute(
        """
        CREATE TABLE signal_diagnostics (
            ticker TEXT,
            n_selections INTEGER,
            avg_rank DOUBLE,
            first_date DATE,
            last_date DATE
        )
        """
    )
    if not df.empty:
        out = df.copy()
        out["first_date"] = out["first_date"].dt.date
        out["last_date"] = out["last_date"].dt.date
        con.register("sig_df", out)
        con.execute(
            """
            INSERT INTO signal_diagnostics
            SELECT ticker, n_selections, avg_rank, first_date, last_date
            FROM sig_df
            """
        )

    con.execute("DROP TABLE IF EXISTS signal_summary")
    con.execute(
        """
        CREATE TABLE signal_summary (
            metric TEXT,
            value DOUBLE
        )
        """
    )
    con.execute(
        "INSERT INTO signal_summary VALUES ('avg_etfs_per_month', ?), ('months_with_fewer_than_5', ?)",
        [avg_etfs, float(months_under_5)],
    )


def save_sector_diagnostics(
    con: duckdb.DuckDBPyConnection,
    sector_counts: pd.DataFrame,
    sector_over_time: pd.DataFrame,
) -> None:
    """Save sector diagnostics to DB."""
    con.execute("DROP TABLE IF EXISTS sector_diagnostics")
    con.execute(
        """
        CREATE TABLE sector_diagnostics (
            sector TEXT,
            n_selections INTEGER
        )
        """
    )
    if not sector_counts.empty:
        con.register("sec_df", sector_counts)
        con.execute(
            "INSERT INTO sector_diagnostics SELECT sector, n_selections FROM sec_df"
        )

    con.execute("DROP TABLE IF EXISTS sector_over_time")
    con.execute(
        """
        CREATE TABLE sector_over_time (
            signal_date DATE,
            sector TEXT,
            n_selected INTEGER
        )
        """
    )
    if not sector_over_time.empty:
        out = sector_over_time.copy()
        out["date"] = out["date"].dt.date
        out = out.rename(columns={"date": "signal_date"})
        con.register("sot_df", out)
        con.execute(
            """
            INSERT INTO sector_over_time
            SELECT signal_date, sector, n_selected
            FROM sot_df
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
        run_analysis(con)
    finally:
        con.close()


def run_analysis(con: duckdb.DuckDBPyConnection) -> None:
    """Run full analysis and save results."""
    print("=" * 60)
    print("BACKTEST RESULTS ANALYSIS")
    print("=" * 60)

    try:
        strategy_nav = load_strategy_nav(con)
    except ValueError as e:
        print(f"Error: {e}")
        return

    benchmark_dfs = load_benchmark_navs(con)
    try:
        signals = load_strategy_signals(con)
    except ValueError as e:
        print(f"Error: {e}")
        return

    metadata = load_etf_metadata(con)

    # A. Strategy diagnostics
    print("\n--- Strategy Performance ---")
    strategy_diag = compute_strategy_diagnostics(strategy_nav)
    print(f"Date range: {strategy_diag['date'].min().date()} to {strategy_diag['date'].max().date()}")
    print(f"Final NAV: {strategy_diag['nav'].iloc[-1]:.4f}")
    print(f"Cumulative return: {strategy_diag['cum_ret'].iloc[-1]:.2%}")

    metrics = compute_series_metrics(strategy_nav)
    if metrics:
        print(f"CAGR: {metrics['cagr']:.2%}")
        print(f"Ann. vol: {metrics['vol_ann']:.2%}")
        print(f"Sharpe (rf=0): {metrics['sharpe']:.2f}")
        print(f"Max drawdown: {metrics['max_dd']:.2%}")

    # B. Benchmark comparison
    print("\n--- Benchmark Comparison ---")
    aligned = align_benchmarks(strategy_nav, benchmark_dfs)
    comparison = {}
    for ticker, nav_df in aligned.items():
        m = compute_series_metrics(nav_df)
        if m:
            comparison[ticker] = m
            print(f"{ticker:12}  Total: {m['total_return']:>8.2%}  CAGR: {m['cagr']:>8.2%}  Vol: {m['vol_ann']:>8.2%}  Sharpe: {m['sharpe']:>6.2f}  MaxDD: {m['max_dd']:>8.2%}")

    # C. Signal diagnostics
    print("\n--- Signal Diagnostics ---")
    sig_diag, avg_etfs, months_under_5 = compute_signal_diagnostics(signals)
    print(f"Avg ETFs selected per month: {avg_etfs:.1f}")
    print(f"Months with fewer than 5 ETFs: {months_under_5}")

    if not sig_diag.empty:
        top = sig_diag.nlargest(10, "n_selections")
        print("\nTop 10 by selection count:")
        for _, row in top.iterrows():
            print(f"  {row['ticker']:8}  n={row['n_selections']:3}  avg_rank={row['avg_rank']:.1f}  {row['first_date'].date()} to {row['last_date'].date()}")

    # D. Sector diagnostics
    print("\n--- Sector Diagnostics ---")
    sector_counts, sector_over_time, latest_sectors = compute_sector_diagnostics(
        signals, metadata
    )

    if not sector_counts.empty:
        print("Sector selection frequency:")
        for _, row in sector_counts.iterrows():
            print(f"  {row['sector']:25}  {row['n_selections']:4}")
        print(f"\nLatest selected sectors: {', '.join(latest_sectors) if latest_sectors else 'N/A'}")
    else:
        print("No sector data (etf_metadata may be empty or missing sector).")

    # E. Save to DB
    print("\n--- Saving to Database ---")
    save_strategy_diagnostics(con, strategy_diag)
    save_benchmark_comparison(con, comparison)
    save_signal_diagnostics(con, sig_diag, avg_etfs, months_under_5)
    save_sector_diagnostics(con, sector_counts, sector_over_time)
    print("Saved: strategy_diagnostics, benchmark_comparison, signal_diagnostics, signal_summary, sector_diagnostics, sector_over_time")

    print("\nDone.")


if __name__ == "__main__":
    main()
