# analyze_backtest_results.py

Research and diagnostic analysis of ETF strategy backtest results. Reads `backtest_daily`, `backtest_benchmarks`, `strategy_signals`, `etf_metadata` and produces diagnostics saved to multiple tables.

## Usage

```bash
python scripts/analyze_backtest_results.py
```

## Prerequisites

- Database with `backtest_daily`, `backtest_benchmarks`, `strategy_signals`
- Backtest run (run `backtest_strategy.py` first)
- Historical signals (run `generate_historical_signals.py` or `generate_signals.py --historical`)
- `etf_metadata` optional (for sector diagnostics)

## Outputs

### A. Strategy Performance Diagnostics

- Cumulative return series
- Daily returns
- Rolling 12-month return
- Rolling 12-month volatility
- Rolling max drawdown

Saved to `strategy_diagnostics`.

### B. Benchmark Comparison

- Strategy vs each benchmark (SPY, VWCE.DE, IWDA.AS) on common dates
- Total return, CAGR, annualized vol, Sharpe, max drawdown per series

Saved to `benchmark_comparison`.

### C. Signal Diagnostics

- Per ETF: selection count, average rank, first/last selection date
- Average ETFs selected per month
- Months with fewer than 5 ETFs

Saved to `signal_diagnostics` and `signal_summary`.

### D. Sector Diagnostics

- Sector selection frequency (requires `etf_metadata` with sector)
- Sector selection over time
- Latest selected sectors

Saved to `sector_diagnostics` and `sector_over_time`.

## Output Tables

| Table | Columns | Description |
|-------|---------|-------------|
| `strategy_diagnostics` | date, nav, daily_ret, cum_ret, rolling_12m_ret, rolling_12m_vol, rolling_max_dd | Strategy time series |
| `benchmark_comparison` | ticker, total_return, cagr, vol_ann, sharpe, max_dd | Per-series metrics |
| `signal_diagnostics` | ticker, n_selections, avg_rank, first_date, last_date | Per-ETF stats |
| `signal_summary` | metric, value | avg_etfs_per_month, months_with_fewer_than_5 |
| `sector_diagnostics` | sector, n_selections | Sector frequency |
| `sector_over_time` | signal_date, sector, n_selected | Sector by month |

## Console Output

- Strategy performance summary (date range, final NAV, CAGR, vol, Sharpe, max DD)
- Benchmark comparison table
- Signal diagnostics (avg ETFs, months under 5, top 10 by selection count)
- Sector selection frequency and latest sectors
- Confirmation of saved tables

## Error Handling

- Exits if database cannot be opened
- Exits if `backtest_daily` does not exist or is empty
- Exits if `strategy_signals` does not exist or is empty
- Sector diagnostics skipped if `etf_metadata` is empty or missing sector column

## Dependencies

- DuckDB
- pandas
