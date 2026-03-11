"""
Research script: test robustness of regime filter strategy variants.

Variants: different regime indicators (SPY/IWDA, 150/200/250 DMA), defensive baskets,
and top-N in normal regime.
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

# Variant configs: (name, regime_ticker, regime_ma_days, defensive_basket, top_n_normal)
# regime_ma_days: 0 means use price_vs_200dma from etf_features
VARIANT_CONFIGS = [
    ("spy_200dma_defensive_top5", "SPY", 200, ["GLD", "SHY", "TLT", "XLU"], 5),
    ("spy_150dma_defensive_top5", "SPY", 150, ["GLD", "SHY", "TLT", "XLU"], 5),
    ("spy_250dma_defensive_top5", "SPY", 250, ["GLD", "SHY", "TLT", "XLU"], 5),
    ("iwda_200dma_defensive_top5", "IWDA.L", 200, ["GLD", "SHY", "TLT", "XLU"], 5),
    ("spy_200dma_defensive_top3", "SPY", 200, ["GLD", "SHY", "TLT", "XLU"], 3),
    ("defensive_gld_shy", "SPY", 200, ["GLD", "SHY"], 5),
    ("defensive_gld_shy_tlt", "SPY", 200, ["GLD", "SHY", "TLT"], 5),
    ("defensive_gld_only", "SPY", 200, ["GLD"], 5),
    ("defensive_shy_only", "SPY", 200, ["SHY"], 5),
]


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
    """Compute price / MA - 1 for a ticker. Returns DataFrame with date, price_vs_ma."""
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
    """
    Build map: signal_date -> True if risk-on (price above MA), False if defensive.
    For ma_days=200, use price_vs_200dma from etf_features if available.
    """
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
                nearest = before[-1]
                result[d] = pv_by_date.loc[nearest] > 0
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


def generate_signals_regime_variant(
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
) -> tuple[pd.DataFrame, float, int]:
    """Backtest a strategy. Returns (nav_df, total_turnover, n_rebalances)."""
    if signals.empty:
        return pd.DataFrame(), 0.0, 0

    price_pivot = prices.pivot(index="date", columns="ticker", values="close").ffill()
    all_dates = sorted(price_pivot.index.unique())
    rebalance_dates = sorted(signals["date"].unique().tolist())

    effective_to_signal = map_rebalance_to_trading_dates(rebalance_dates, all_dates)
    if not effective_to_signal:
        return pd.DataFrame(), 0.0, 0

    first_effective = min(effective_to_signal.keys())
    start_idx = all_dates.index(first_effective) if first_effective in all_dates else 0
    trading_dates = all_dates[start_idx:]

    first_signal = effective_to_signal[first_effective]
    weights = get_weights_for_date(signals, first_signal, prices, first_effective)
    if not weights:
        return pd.DataFrame(), 0.0, 0

    nav_series = []
    nav = 1.0
    total_turnover = 0.0
    n_rebalances = 0
    prev_date = None

    for d in trading_dates:
        if d in effective_to_signal:
            signal_date = effective_to_signal[d]
            new_weights = get_weights_for_date(signals, signal_date, prices, d)
            if new_weights:
                turnover = compute_turnover(weights, new_weights)
                total_turnover += turnover
                n_rebalances += 1
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

    return pd.DataFrame(nav_series), total_turnover, n_rebalances


def compute_metrics(
    nav_df: pd.DataFrame,
    total_turnover: float,
    n_rebalances: int,
) -> dict[str, float]:
    """Compute total return, CAGR, vol, Sharpe, max DD, turnover."""
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
    turnover_ann = (total_turnover / n_rebalances) * 12 if n_rebalances > 0 else 0.0

    return {
        "total_return": total_return,
        "cagr": cagr,
        "vol_ann": vol_ann,
        "sharpe": sharpe,
        "max_dd": max_dd,
        "turnover_ann": turnover_ann,
    }


def save_results(con: duckdb.DuckDBPyConnection, results: list[dict]) -> None:
    """Save variant summary to regime_filter_variant_summary."""
    con.execute("DROP TABLE IF EXISTS regime_filter_variant_summary")
    con.execute(
        """
        CREATE TABLE regime_filter_variant_summary (
            variant TEXT,
            total_return DOUBLE,
            cagr DOUBLE,
            vol_ann DOUBLE,
            sharpe DOUBLE,
            max_dd DOUBLE,
            turnover_ann DOUBLE
        )
        """
    )
    if results:
        df = pd.DataFrame(results)
        con.register("res_df", df)
        con.execute(
            """
            INSERT INTO regime_filter_variant_summary
            SELECT variant, total_return, cagr, vol_ann, sharpe, max_dd, turnover_ann
            FROM res_df
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
        run_research(con)
    finally:
        con.close()


def run_research(con: duckdb.DuckDBPyConnection) -> None:
    """Run regime filter variant research."""
    print("=" * 60)
    print("REGIME FILTER VARIANT RESEARCH")
    print("=" * 60)

    features = load_features(con)
    if features.empty:
        print("Error: etf_features is empty. Run build_features.py first.")
        return

    signal_dates = get_signal_dates(con)
    if not signal_dates:
        print("Error: No signal dates in etf_features.")
        return

    all_tickers = set(features["ticker"].unique())
    regime_tickers = {c[1] for c in VARIANT_CONFIGS}
    all_tickers |= regime_tickers
    all_tickers |= {"GLD", "SHY", "TLT", "XLU"}

    start_date = signal_dates[0]
    end_date = con.execute("SELECT MAX(date) FROM prices_daily").fetchone()[0]
    end_date = pd.Timestamp(end_date) if end_date else signal_dates[-1]

    prices = load_prices(con, all_tickers, start_date, end_date)
    if prices.empty:
        print("Error: No price data for signal tickers.")
        return

    results = []
    for name, regime_ticker, regime_ma, defensive_basket, top_n in VARIANT_CONFIGS:
        regime_map = build_regime_map(
            features, prices, regime_ticker, regime_ma, signal_dates
        )
        signals = generate_signals_regime_variant(
            features, prices, signal_dates, regime_map, defensive_basket, top_n
        )
        nav_df, turnover, n_reb = run_backtest(signals, prices, signal_dates)
        m = compute_metrics(nav_df, turnover, n_reb)
        if m:
            m["variant"] = name
            results.append(m)
            print(f"  {name}: Sharpe={m['sharpe']:.2f}  CAGR={m['cagr']:.2%}  MaxDD={m['max_dd']:.2%}")

    if not results:
        print("Error: No variant produced valid results.")
        return

    save_results(con, results)

    print("\n--- Ranked by Sharpe ---")
    by_sharpe = sorted(results, key=lambda x: x["sharpe"], reverse=True)
    for i, r in enumerate(by_sharpe, 1):
        print(f"  {i}. {r['variant']:35}  Sharpe={r['sharpe']:.2f}  CAGR={r['cagr']:.2%}  MaxDD={r['max_dd']:.2%}")

    print("\n--- Ranked by CAGR ---")
    by_cagr = sorted(results, key=lambda x: x["cagr"], reverse=True)
    for i, r in enumerate(by_cagr, 1):
        print(f"  {i}. {r['variant']:35}  CAGR={r['cagr']:.2%}  Sharpe={r['sharpe']:.2f}  MaxDD={r['max_dd']:.2%}")

    print("\n--- Ranked by Max Drawdown (best = least negative) ---")
    by_dd = sorted(results, key=lambda x: x["max_dd"], reverse=True)
    for i, r in enumerate(by_dd, 1):
        print(f"  {i}. {r['variant']:35}  MaxDD={r['max_dd']:.2%}  Sharpe={r['sharpe']:.2f}  CAGR={r['cagr']:.2%}")

    print("\nResults saved to regime_filter_variant_summary")
    print("Done.")


if __name__ == "__main__":
    main()
