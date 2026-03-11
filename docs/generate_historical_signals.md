# generate_historical_signals.py

Generates historical monthly ETF signals for all month-end dates in `etf_features` and loads them into `strategy_signals`. Required for meaningful backtests.

## Usage

```bash
python scripts/generate_historical_signals.py
```

## Prerequisites

- Database with `etf_features` and `strategy_signals` tables
- Feature data loaded (run `build_features.py` first)

## Pipeline

1. **Signal dates** — Last available date per calendar month from `etf_features`
2. **For each month** — Load features, filter valid (non-null required cols), apply trend filter
3. **Scoring** — Composite score: 0.35×momentum + 0.20×ret_3m + 0.15×ret_1m - 0.15×vol - 0.15×|max_dd|
4. **Selection** — Top 5 by score, equal weight (0.20 each)
5. **Write** — Delete all existing `strategy_signals`, insert all historical signals

## Required Features

Non-null required: `momentum_12_1`, `ret_3m`, `ret_1m`, `vol_3m`, `max_dd_1y`, `price_vs_200dma`

Trend filter: `price_vs_200dma > 0`

## Output

Writes to `strategy_signals`:

| Column | Type | Description |
|--------|------|-------------|
| date | DATE | Signal date |
| ticker | TEXT | ETF ticker |
| score | DOUBLE | Composite score |
| rank | INTEGER | Rank (1 = best) |
| weight | DOUBLE | 0.20 for selected ETFs |

## Console Output

- First/last signal date
- Rebalance dates count
- Total rows inserted
- Avg ETFs considered (after filtering)
- Avg ETFs selected
- Sample of first 3 and last 3 rebalance dates with tickers

## Diagnostics

`get_per_month_diagnostics()` returns a DataFrame with `signal_date`, `n_raw`, `n_valid`, `n_trend`, `n_selected` per month. Used internally for summary stats.

## Error Handling

- Exits if database cannot be opened
- Exits if `etf_features` is empty
- Skips months with no valid ETFs after filtering

## Future

- `historical_signal_diagnostics` table for persistence
- Explicit minimum-history rule (e.g. 252 trading days)

## Dependencies

- DuckDB
- pandas
