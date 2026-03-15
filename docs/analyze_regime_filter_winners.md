# analyze_regime_filter_winners.py

Deeper analysis of winning regime-filter strategies. Recreates and backtests four strategies in memory, computes NAV, drawdowns, monthly returns, regime diagnostics, and allocation diagnostics.

## Usage

```bash
python scripts/analyze_regime_filter_winners.py
```

## Prerequisites

- Database with `etf_features`, `prices_daily`
- Feature data loaded (run `build_features.py` first)
- Price data loaded (run `etl_prices.py`)

## Strategies Analyzed

| Strategy | Type | Regime | Defensive basket |
|----------|------|--------|------------------|
| `baseline_top5_equal_weight` | Baseline | — | — |
| `spy_200dma_defensive_top5` | Regime | SPY 200DMA | GLD, SHY, TLT, XLU |
| `spy_250dma_defensive_top5` | Regime | SPY 250DMA | GLD, SHY, TLT, XLU |
| `defensive_shy_only` | Regime | SPY 200DMA | SHY |

## Outputs

### Comparison Summary

For each strategy: total return, CAGR, annualized volatility, Sharpe ratio, max drawdown.

### Regime Diagnostics (regime-filter strategies only)

- Months in defensive mode
- Percentage of months in defensive mode
- Average duration of a defensive streak (months)
- First 10 defensive regime dates
- Last 10 defensive regime dates

### Allocation Diagnostics

- Per strategy: count of how many times each ETF was selected
- For regime strategies: count of defensive ETF usage

## Output Tables

| Table | Columns | Description |
|-------|---------|-------------|
| `winner_nav_comparison` | date, strategy, nav | Daily NAV per strategy |
| `winner_monthly_returns` | date, strategy, monthly_ret | Monthly returns per strategy |
| `winner_drawdowns` | date, strategy, drawdown | Drawdown series per strategy |
| `winner_regime_diagnostics` | strategy, metric, value | Regime stats (months defensive, pct, streaks, dates) |
| `winner_allocation_diagnostics` | strategy, ticker, n_selections | ETF selection counts per strategy |

## Console Output

- Strategy comparison (total return, CAGR, vol, Sharpe, max DD)
- Regime diagnostics (defensive months, pct, avg streak, first/last 10 dates)
- Allocation diagnostics (top ETFs per strategy by selection count)
- Confirmation of saved tables

## Behavior

- Monthly signal dates from `etf_features`
- Signal dates mapped to next available trading date
- Transaction cost: 10 bps on one-way turnover
- Missing prices handled by skipping invalid ETFs

## Dependencies

- DuckDB
- pandas
