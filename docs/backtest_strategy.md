# backtest_strategy.py

Backtests the monthly ETF rotation strategy. Uses `strategy_signals` for rebalance dates and weights, `prices_daily` for NAV. Applies 10 bps transaction cost per traded weight. Saves results to `backtest_daily`, `backtest_benchmarks`, and `backtest_summary`.

## Usage

```bash
python scripts/backtest_strategy.py
```

## Prerequisites

- Database with `strategy_signals`, `prices_daily`, and backtest tables
- Signals generated (run `generate_signals.py` first)
- Price data loaded (run `etl_prices.py`)

## Pipeline

1. **Load signals** — Reads `strategy_signals` (weight > 0), gets rebalance dates
2. **Load prices** — Fetches `prices_daily` for signal tickers + benchmarks
3. **Run backtest** — For each day: rebalance when signal date, compute portfolio return, compound NAV
4. **Transaction costs** — 10 bps per traded weight change at each rebalance
5. **Benchmarks** — Buy-and-hold NAV for SPY, VWCE.DE, IWDA.AS (skipped if no data)
6. **Metrics** — CAGR, volatility, Sharpe, max drawdown, turnover
7. **Save** — Writes to backtest tables

## Transaction Costs

- **10 basis points** (0.001) per traded weight change
- At rebalance: cost = 0.001 × Σ|Δweight|
- Example: going from 0 to 0.20 in one ETF → cost = 0.0002 (2 bps of portfolio)

## Output Tables

| Table | Columns | Description |
|-------|---------|-------------|
| `backtest_daily` | date, nav | Daily strategy NAV (normalized to 1 at start) |
| `backtest_benchmarks` | date, ticker, nav | Buy-and-hold NAV for SPY, VWCE.DE, IWDA.AS |
| `backtest_summary` | metric, value | CAGR, vol_ann, sharpe, max_dd, turnover_ann, etc. |

## Metrics

| Metric | Description |
|--------|-------------|
| CAGR | Compound annual growth rate |
| Ann. volatility | Annualized daily return volatility |
| Sharpe ratio | (Ann. return - rf) / Ann. vol (rf = 0) |
| Max drawdown | Largest peak-to-trough decline |
| Ann. turnover | Average turnover per rebalance × 12 |

## Behavior

- **Rebalance dates** — Uses dates from `strategy_signals` (typically monthly)
- **Top 5, equal weight** — Holds selected ETFs at 0.20 each
- **Missing prices** — Skips ETFs with no valid price on rebalance date; equal-weights the rest
- **Forward-fill** — Missing prices within a holding period are forward-filled
- **Benchmarks** — SPY, VWCE.DE, IWDA.AS; skipped if no price data in `prices_daily`

## Configuration

Constants in the script:

| Constant | Default | Description |
|----------|---------|-------------|
| `COST_BPS` | 10 | Transaction cost in basis points per traded weight |
| `BENCHMARK_TICKERS` | SPY, VWCE.DE, IWDA.AS | Buy-and-hold comparison tickers |

## Console Output

- Signal and rebalance date range
- Price row count
- Benchmark total return and CAGR (or "no price data (skipped)")
- Summary: start/end date, years, total return, CAGR, vol, Sharpe, max DD, turnover
- Confirmation that results were saved

## Error Handling

- Exits if database cannot be opened (e.g. locked by another process)
- Exits if `strategy_signals` is empty
- Exits if no valid prices for any signal ETF on first rebalance date
- Exits if backtest produces no NAV series

## Dependencies

- DuckDB
- pandas
