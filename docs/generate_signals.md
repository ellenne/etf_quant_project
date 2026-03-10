# generate_signals.py

Generates strategy signals from ETF features. Reads the latest date from `etf_features`, applies filters and scoring, and writes the top 5 ETFs (equal-weighted) to `strategy_signals`.

## Usage

```bash
python scripts/generate_signals.py
```

## Prerequisites

- Database with `etf_features` and `strategy_signals` tables
- Feature data loaded (run `build_features.py` first)

## Pipeline

1. **Latest date** — Uses `MAX(date)` from `etf_features`
2. **Load features** — Fetches all rows for that date
3. **Validity filter** — Excludes ETFs with null `momentum_12_1`, `vol_3m`, or `max_dd_1y`
4. **Trend filter** — Keeps only ETFs where `price_vs_200dma > 0` (price above 200-day MA)
5. **Scoring** — Computes a composite score from percentile ranks
6. **Selection** — Top 5 by score, equal weight (0.20 each)
7. **Write** — Deletes existing signals for that date, inserts new rows

## Scoring Formula

Uses percentile ranks (0–1) so scores are comparable across dates:

```
score =
  0.35 × rank(momentum_12_1)
+ 0.20 × rank(ret_3m)
+ 0.15 × rank(ret_1m)
- 0.15 × rank(vol_3m)
- 0.15 × rank(|max_dd_1y|)
```

| Component | Weight | Direction |
|-----------|--------|-----------|
| Momentum (12-1) | 0.35 | Higher is better |
| 3-month return | 0.20 | Higher is better |
| 1-month return | 0.15 | Higher is better |
| 3-month volatility | -0.15 | Lower is better |
| Max drawdown (abs) | -0.15 | Smaller is better |

## Output

Writes to `strategy_signals`:

| Column | Type | Description |
|--------|------|-------------|
| `date` | DATE | Signal date |
| `ticker` | TEXT | ETF ticker |
| `score` | DOUBLE | Composite ranking score |
| `rank` | INTEGER | Rank (1 = best) |
| `weight` | DOUBLE | Portfolio weight (0.20 for top 5, 0 for others) |

Only the top 5 ETFs are inserted; all others are excluded.

## Console Output

- Latest feature date
- ETF counts at each filter stage
- Summary: date, considered, selected
- Top 10 ranked ETFs with scores (`*` marks selected)

## Configuration

Constants in the script (modify for tuning):

| Constant | Default | Description |
|----------|---------|-------------|
| `TOP_N` | 5 | Number of ETFs to select |
| `WEIGHT_MOMENTUM` | 0.35 | Score weight for momentum |
| `WEIGHT_RET_3M` | 0.20 | Score weight for 3m return |
| `WEIGHT_RET_1M` | 0.15 | Score weight for 1m return |
| `WEIGHT_VOL` | -0.15 | Score weight for volatility |
| `WEIGHT_DRAWDOWN` | -0.15 | Score weight for drawdown |

## Error Handling

- Exits with message if database cannot be opened (e.g. locked by another process)
- Exits if `etf_features` is empty
- Exits if no ETFs pass the validity or trend filters

## Dependencies

- DuckDB
- pandas
