"""
Deeper analysis of winning regime-filter strategies.

Recreates and backtests: baseline_top5, spy_200dma_defensive_top5,
spy_250dma_defensive_top5, defensive_shy_only. Computes NAV, drawdowns,
monthly returns, regime diagnostics, allocation diagnostics.
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

COST_PER_WEIGHT = 0.001  # 10 bps
REQUIRED_COLS = [
    "momentum_12_1",
    "ret_3m",
    "ret_1m",
    "vol_3m",
    "max_dd_1y",
    "price_vs_200dma",
]

WEIGHT_MOMENTUM = 0.35
WEIGHT_RET_3M = 0.20
WEIGHT_RET_1M = 0.15
WEIGHT_VOL = -0.15
WEIGHT_DRAWDOWN = -0.15

WINNING_STRATEGIES = [
    ("baseline_top5_equal_weight", "baseline", None, None, 5),
    ("spy_200dma_defensive_top5", "regime", "SPY", 200, 5),
    ("spy_250dma_defensive_top5", "regime", "SPY", 250, 5),
    ("defensive_shy_only", "regime", "SPY", 200, 5),
]
DEFENSIVE_BASKETS = {
    "spy_200dma_defensive_top5": ["GLD", "SHY", "TLT", "XLU"],
    "spy_250dma_defensive_top5": ["GLD", "SHY", "TLT", "XLU"],
    "defensive_shy_only": ["SHY"],
}


def get_signal_dates(con: duckdb.DuckDBPyConnection) -> list[pd.Timestamp]:
    """Last date of each calendar month in etf_features."""
    df = con.execute(
        "SELECT DISTINCT date FROM etf_features ORDER BY date"
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


def load_features(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Load all features from etf_features."""
    df = con.execute(
        """
        SELECT date, ticker, momentum_12_1, ret_1m, ret_3m, vol_3m, max_dd_1y, price_vs_200dma
        FROM etf_features
        ORDER BY date
        """
    ).fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    return df


def load_prices(
    con: duckdb.DuckDBPyConnection,
    tickers: set[str],
    start_date: pd.Timestamp,
    end_date: pd.Timestamp,
) -> pd.DataFrame:
    """Load daily close prices for tickers in date range."""
    ticker_list = list(tickers)
    placeholders = ",".join(["?"] * len(ticker_list))
    params = [start_date.date(), end_date.date()] + ticker_list
    df = con.execute(
        f"""
        SELECT date, ticker, close
        FROM prices_daily
        WHERE date >= ? AND date <= ? AND ticker IN ({placeholders})
        ORDER BY date
        """,
        params,
    ).fetchdf()
    df["date"] = pd.to_datetime(df["date"])
    return df


def compute_price_vs_ma(
    prices: pd.DataFrame,
    ticker: str,
    ma_days: int,
) -> pd.DataFrame:
    """Compute price / MA - 1 for a ticker."""
    t_prices = prices[prices["ticker"] == ticker].sort_values("date")
    if t_prices.empty or len(t_prices) < ma_days:
        return pd.DataFrame()
    t_prices = t_prices.copy()
    t_prices["ma"] = t_prices["close"].rolling(ma_days).mean()
    t_prices["price_vs_ma"] = t_prices["close"] / t_prices["ma"] - 1
    return t_prices[["date", "price_vs_ma"]].dropna()


def build_regime_map(
    features: pd.DataFrame,
    prices: pd.DataFrame,
    regime_ticker: str,
    regime_ma_days: int,
    signal_dates: list[pd.Timestamp],
) -> dict[pd.Timestamp, bool]:
    """signal_date -> True if risk-on, False if defensive."""
    result: dict[pd.Timestamp, bool] = {}

    if regime_ma_days == 200 and regime_ticker in features["ticker"].unique():
        for d in signal_dates:
            row = features[
                (features["date"] == d) & (features["ticker"] == regime_ticker)
            ]
            if row.empty:
                result[d] = True
                continue
            pv = row["price_vs_200dma"].iloc[0]
            result[d] = pd.notna(pv) and pv > 0
        return result

    pv_ma = compute_price_vs_ma(prices, regime_ticker, regime_ma_days)
    if pv_ma.empty:
        for d in signal_dates:
            result[d] = True
        return result

    pv_by_date = pv_ma.set_index("date")["price_vs_ma"]
    for d in signal_dates:
        if d in pv_by_date.index:
            result[d] = pv_by_date.loc[d] > 0
        else:
            before = pv_by_date.index[pv_by_date.index <= d]
            if len(before) > 0:
                result[d] = pv_by_date.loc[before[-1]] > 0
            else:
                after = pv_by_date.index[pv_by_date.index >= d]
                result[d] = pv_by_date.loc[after[0]] > 0 if len(after) > 0 else True

    return result


def map_rebalance_to_trading_dates(
    rebalance_dates: list[pd.Timestamp],
    trading_dates: list[pd.Timestamp],
) -> dict[pd.Timestamp, pd.Timestamp]:
    """Map signal dates to next available trading date."""
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


def compute_turnover(old_weights: dict[str, float], new_weights: dict[str, float]) -> float:
    """One-way turnover: 0.5 * sum of absolute weight changes."""
    all_tickers = set(old_weights) | set(new_weights)
    return 0.5 * sum(abs(new_weights.get(t, 0.0) - old_weights.get(t, 0.0)) for t in all_tickers)


def baseline_score(df: pd.DataFrame) -> pd.DataFrame:
    """Compute baseline composite score."""
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


def generate_signals_baseline(
    features: pd.DataFrame,
    signal_dates: list[pd.Timestamp],
    prices: pd.DataFrame,
    top_n: int,
) -> pd.DataFrame:
    """Baseline: composite score, top N, equal weight."""
    rows = []
    for d in signal_dates:
        df = features[features["date"] == d].dropna(subset=REQUIRED_COLS)
        df = df[df["price_vs_200dma"] > 0]
        if df.empty:
            continue
        df = baseline_score(df)
        top = df.sort_values(["score", "ticker"], ascending=[False, True]).head(top_n)
        exec_dates = prices[prices["date"] >= d]["date"].unique()
        exec_date = min(exec_dates) if len(exec_dates) else d
        day_prices = prices[
            (prices["date"] == exec_date) & (prices["close"].notna()) & (prices["close"] > 0)
        ]
        valid = set(day_prices["ticker"])
        top = top[top["ticker"].isin(valid)]
        if top.empty:
            continue
        w = 1.0 / len(top)
        for _, r in top.iterrows():
            rows.append({"date": d, "ticker": r["ticker"], "weight": w})
    return pd.DataFrame(rows)


def generate_signals_regime(
    features: pd.DataFrame,
    prices: pd.DataFrame,
    signal_dates: list[pd.Timestamp],
    regime_map: dict[pd.Timestamp, bool],
    defensive_basket: list[str],
    top_n: int,
) -> pd.DataFrame:
    """Generate signals for a regime filter variant."""
    rows = []
    for d in signal_dates:
        risk_on = regime_map.get(d, True)

        if risk_on:
            df = features[features["date"] == d].dropna(subset=REQUIRED_COLS)
            df = df[df["price_vs_200dma"] > 0]
        else:
            df = features[(features["date"] == d) & (features["ticker"].isin(defensive_basket))]
            df = df.dropna(subset=REQUIRED_COLS)
            df = df[df["price_vs_200dma"] > 0]

        if df.empty:
            continue

        df = baseline_score(df)
        top = df.sort_values(["score", "ticker"], ascending=[False, True]).head(top_n)

        exec_dates = prices[prices["date"] >= d]["date"].unique()
        exec_date = min(exec_dates) if len(exec_dates) else d
        day_prices = prices[
            (prices["date"] == exec_date) & (prices["close"].notna()) & (prices["close"] > 0)
        ]
        valid = set(day_prices["ticker"])
        top = top[top["ticker"].isin(valid)]

        if top.empty:
            continue

        w = 1.0 / len(top)
        for _, r in top.iterrows():
            rows.append({"date": d, "ticker": r["ticker"], "weight": w})

    return pd.DataFrame(rows)


def get_weights_for_date(
    signals: pd.DataFrame,
    signal_date: pd.Timestamp,
    prices: pd.DataFrame,
    execution_date: pd.Timestamp,
) -> dict[str, float]:
    """Get target weights for a rebalance."""
    day_signals = signals[signals["date"] == signal_date]
    if day_signals.empty:
        return {}

    day_prices = prices[
        (prices["date"] == execution_date) & (prices["close"].notna()) & (prices["close"] > 0)
    ]
    valid = set(day_prices["ticker"].unique())

    result = {}
    for _, r in day_signals.iterrows():
        if r["ticker"] in valid:
            result[r["ticker"]] = r["weight"]
    if not result:
        return {}
    total = sum(result.values())
    return {t: w / total for t, w in result.items()}


def run_backtest(
    signals: pd.DataFrame,
    prices: pd.DataFrame,
    signal_dates: list[pd.Timestamp],
) -> pd.DataFrame:
    """Backtest a strategy. Returns daily NAV DataFrame (date, nav)."""
    if signals.empty:
        return pd.DataFrame()

    price_pivot = prices.pivot(index="date", columns="ticker", values="close").ffill()
    all_dates = sorted(price_pivot.index.unique())
    rebalance_dates = sorted(signals["date"].unique().tolist())

    effective_to_signal = map_rebalance_to_trading_dates(rebalance_dates, all_dates)
    if not effective_to_signal:
        return pd.DataFrame()

    first_effective = min(effective_to_signal.keys())
    start_idx = all_dates.index(first_effective) if first_effective in all_dates else 0
    trading_dates = all_dates[start_idx:]

    first_signal = effective_to_signal[first_effective]
    weights = get_weights_for_date(signals, first_signal, prices, first_effective)
    if not weights:
        return pd.DataFrame()

    nav_series = []
    nav = 1.0
    prev_date = None

    for d in trading_dates:
        if d in effective_to_signal:
            signal_date = effective_to_signal[d]
            new_weights = get_weights_for_date(signals, signal_date, prices, d)
            if new_weights:
                turnover = compute_turnover(weights, new_weights)
                nav *= 1 - COST_PER_WEIGHT * turnover
                weights = new_weights

        ret = 0.0
        row = price_pivot.loc[d]
        for ticker, w in weights.items():
            p_today = row.get(ticker)
            if pd.isna(p_today) or p_today <= 0:
                continue
            if prev_date is not None:
                p_yesterday = price_pivot.loc[prev_date, ticker]
                if pd.notna(p_yesterday) and p_yesterday > 0:
                    ret += w * (p_today / p_yesterday - 1)

        nav *= 1 + ret
        nav_series.append({"date": d, "nav": nav})
        prev_date = d

    return pd.DataFrame(nav_series)


def compute_drawdown_series(nav_df: pd.DataFrame) -> pd.DataFrame:
    """Compute drawdown series from NAV."""
    if nav_df.empty or len(nav_df) < 2:
        return pd.DataFrame()
    df = nav_df.copy()
    df["cummax"] = df["nav"].cummax()
    df["drawdown"] = (df["nav"] - df["cummax"]) / df["cummax"]
    return df[["date", "drawdown"]]


def compute_monthly_returns(nav_df: pd.DataFrame) -> pd.DataFrame:
    """Compute monthly returns from daily NAV."""
    if nav_df.empty or len(nav_df) < 2:
        return pd.DataFrame()
    df = nav_df.set_index("date").resample("ME").last().dropna()
    df["monthly_ret"] = df["nav"].pct_change()
    df = df.reset_index()
    return df[["date", "monthly_ret"]].dropna()


def compute_metrics(nav_df: pd.DataFrame) -> dict[str, float]:
    """Compute total return, CAGR, vol, Sharpe, max DD."""
    if len(nav_df) < 2:
        return {}

    nav = nav_df["nav"].values
    start = nav_df["date"].iloc[0]
    end = nav_df["date"].iloc[-1]
    n_years = (end - start).days / 365.25
    if n_years <= 0:
        return {}

    total_return = nav[-1] / nav[0] - 1
    cagr = (1 + total_return) ** (1 / n_years) - 1 if total_return > -1 else -1.0
    daily_rets = pd.Series(nav).pct_change().dropna()
    vol_ann = daily_rets.std() * math.sqrt(252) if len(daily_rets) > 0 else 0.0
    sharpe = (daily_rets.mean() * 252) / vol_ann if vol_ann > 0 else 0.0
    cummax = pd.Series(nav).cummax()
    max_dd = ((pd.Series(nav) - cummax) / cummax).min()

    return {
        "total_return": total_return,
        "cagr": cagr,
        "vol_ann": vol_ann,
        "sharpe": sharpe,
        "max_dd": max_dd,
    }


def compute_defensive_streak_durations(defensive_dates: list[pd.Timestamp]) -> list[int]:
    """Compute duration (in months) of each consecutive defensive streak."""
    if not defensive_dates:
        return []
    sorted_dates = sorted(defensive_dates)
    streaks = []
    current = 1
    for i in range(1, len(sorted_dates)):
        prev = sorted_dates[i - 1]
        curr = sorted_dates[i]
        days_diff = (curr - prev).days
        if days_diff <= 35:  # same or next month
            current += 1
        else:
            streaks.append(current)
            current = 1
    streaks.append(current)
    return streaks


def compute_allocation_diagnostics(signals: pd.DataFrame) -> pd.DataFrame:
    """Count how many times each ETF was selected per strategy."""
    if signals.empty:
        return pd.DataFrame()
    return signals.groupby("ticker").size().reset_index(name="n_selections")


def save_results(
    con: duckdb.DuckDBPyConnection,
    nav_comparison: pd.DataFrame,
    monthly_returns: pd.DataFrame,
    drawdowns: pd.DataFrame,
    regime_diagnostics: pd.DataFrame,
    allocation_diagnostics: pd.DataFrame,
) -> None:
    """Save all result tables to DuckDB."""
    for name, df, ddl in [
        ("winner_nav_comparison", nav_comparison, "date DATE, strategy TEXT, nav DOUBLE"),
        ("winner_monthly_returns", monthly_returns, "date DATE, strategy TEXT, monthly_ret DOUBLE"),
        ("winner_drawdowns", drawdowns, "date DATE, strategy TEXT, drawdown DOUBLE"),
        ("winner_regime_diagnostics", regime_diagnostics, "strategy TEXT, metric TEXT, value TEXT"),
        ("winner_allocation_diagnostics", allocation_diagnostics, "strategy TEXT, ticker TEXT, n_selections INTEGER"),
    ]:
        con.execute(f"DROP TABLE IF EXISTS {name}")
        con.execute(f"CREATE TABLE {name} ({ddl})")
        if not df.empty:
            df_out = df.copy()
            for col in df_out.columns:
                if pd.api.types.is_datetime64_any_dtype(df_out[col]):
                    df_out[col] = df_out[col].dt.date
            con.register("df_out", df_out)
            cols = ", ".join(df_out.columns)
            con.execute(f"INSERT INTO {name} SELECT {cols} FROM df_out")


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
    """Run full winner analysis."""
    print("=" * 60)
    print("REGIME FILTER WINNERS ANALYSIS")
    print("=" * 60)

    features = load_features(con)
    if features.empty:
        print("Error: etf_features is empty. Run build_features.py first.")
        return

    signal_dates = get_signal_dates(con)
    if not signal_dates:
        print("Error: No signal dates in etf_features.")
        return

    all_tickers = set(features["ticker"].unique()) | {"SPY", "GLD", "SHY", "TLT", "XLU"}
    start_date = signal_dates[0]
    end_date = con.execute("SELECT MAX(date) FROM prices_daily").fetchone()[0]
    end_date = pd.Timestamp(end_date) if end_date else signal_dates[-1]

    prices = load_prices(con, all_tickers, start_date, end_date)
    if prices.empty:
        print("Error: No price data.")
        return

    nav_comparison_rows = []
    monthly_returns_rows = []
    drawdowns_rows = []
    regime_diag_rows = []
    allocation_diag_rows = []

    print("\n--- Strategy Comparison ---\n")

    for name, strategy_type, regime_ticker, regime_ma, top_n in WINNING_STRATEGIES:
        if strategy_type == "baseline":
            signals = generate_signals_baseline(features, signal_dates, prices, top_n)
            regime_map = None
        else:
            regime_map = build_regime_map(
                features, prices, regime_ticker or "SPY", regime_ma or 200, signal_dates
            )
            defensive_basket = DEFENSIVE_BASKETS.get(name, ["GLD", "SHY", "TLT", "XLU"])
            signals = generate_signals_regime(
                features, prices, signal_dates, regime_map, defensive_basket, top_n
            )

        nav_df = run_backtest(signals, prices, signal_dates)
        if nav_df.empty:
            print(f"  {name}: no valid NAV")
            continue

        metrics = compute_metrics(nav_df)
        print(f"  {name}:")
        print(f"    Total return: {metrics['total_return']:.2%}  CAGR: {metrics['cagr']:.2%}")
        print(f"    Vol: {metrics['vol_ann']:.2%}  Sharpe: {metrics['sharpe']:.2f}  MaxDD: {metrics['max_dd']:.2%}")

        nav_df["strategy"] = name
        nav_comparison_rows.append(nav_df[["date", "strategy", "nav"]])

        monthly = compute_monthly_returns(nav_df)
        monthly["strategy"] = name
        monthly_returns_rows.append(monthly[["date", "strategy", "monthly_ret"]])

        dd_df = compute_drawdown_series(nav_df)
        dd_df["strategy"] = name
        drawdowns_rows.append(dd_df[["date", "strategy", "drawdown"]])

        alloc = compute_allocation_diagnostics(signals)
        alloc["strategy"] = name
        allocation_diag_rows.append(alloc[["strategy", "ticker", "n_selections"]])

        if regime_map is not None:
            defensive_dates = sorted([d for d in signal_dates if not regime_map.get(d, True)])
            n_defensive = len(defensive_dates)
            pct_defensive = 100 * n_defensive / len(signal_dates) if signal_dates else 0
            streaks = compute_defensive_streak_durations(defensive_dates)
            avg_streak = sum(streaks) / len(streaks) if streaks else 0

            regime_diag_rows.append({"strategy": name, "metric": "months_defensive", "value": str(n_defensive)})
            regime_diag_rows.append({"strategy": name, "metric": "pct_defensive", "value": f"{pct_defensive:.1f}%"})
            regime_diag_rows.append({"strategy": name, "metric": "avg_defensive_streak_months", "value": f"{avg_streak:.1f}"})

            first_10 = defensive_dates[:10]
            last_10 = defensive_dates[-10:] if len(defensive_dates) >= 10 else defensive_dates
            regime_diag_rows.append({"strategy": name, "metric": "first_10_defensive_dates", "value": ",".join(str(d.date()) for d in first_10)})
            regime_diag_rows.append({"strategy": name, "metric": "last_10_defensive_dates", "value": ",".join(str(d.date()) for d in last_10)})

            print(f"    Regime: {n_defensive} months defensive ({pct_defensive:.1f}%), avg streak {avg_streak:.1f} months")

    nav_comparison = pd.concat(nav_comparison_rows, ignore_index=True) if nav_comparison_rows else pd.DataFrame()
    monthly_returns = pd.concat(monthly_returns_rows, ignore_index=True) if monthly_returns_rows else pd.DataFrame()
    drawdowns = pd.concat(drawdowns_rows, ignore_index=True) if drawdowns_rows else pd.DataFrame()
    regime_diagnostics = pd.DataFrame(regime_diag_rows) if regime_diag_rows else pd.DataFrame()
    allocation_diagnostics = pd.concat(allocation_diag_rows, ignore_index=True) if allocation_diag_rows else pd.DataFrame()

    print("\n--- Regime Diagnostics (first 10 / last 10 defensive dates) ---\n")
    for name, strategy_type, _, _, _ in WINNING_STRATEGIES:
        if strategy_type != "regime":
            continue
        rows = [r for r in regime_diag_rows if r["strategy"] == name]
        for r in rows:
            if "first_10" in r["metric"] or "last_10" in r["metric"]:
                print(f"  {name} {r['metric']}: {r['value'][:80]}{'...' if len(r['value']) > 80 else ''}")

    print("\n--- Allocation Diagnostics (top ETFs per strategy) ---\n")
    for name, strategy_type, regime_ticker, regime_ma, top_n in WINNING_STRATEGIES:
        alloc = allocation_diagnostics[allocation_diagnostics["strategy"] == name]
        if alloc.empty:
            continue
        top = alloc.nlargest(10, "n_selections")
        print(f"  {name}: {', '.join(f'{r.ticker}({r.n_selections})' for _, r in top.iterrows())}")

    save_results(
        con,
        nav_comparison,
        monthly_returns,
        drawdowns,
        regime_diagnostics,
        allocation_diagnostics,
    )

    print("\nResults saved to: winner_nav_comparison, winner_monthly_returns, winner_drawdowns,")
    print("  winner_regime_diagnostics, winner_allocation_diagnostics")
    print("Done.")


if __name__ == "__main__":
    main()
