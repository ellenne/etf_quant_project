# research_strategy_variants.py

Research script that tests multiple ETF strategy variants and compares backtest performance. Generates signals in memory, backtests each variant, and saves a ranked comparison.

## Usage

```bash
python scripts/research_strategy_variants.py
```

## Prerequisites

- Database with `etf_features`, `prices_daily`
- Feature data loaded (run `build_features.py` first)
- Price data loaded (run `etl_prices.py`)

## Variants

| Variant | Description |
|---------|-------------|
| `baseline_top5_equal_weight` | Composite score, top 5, equal weight |
| `top3_equal_weight` | Composite score, top 3, equal weight |
| `top5_momentum_only` | Rank only on momentum_12_1, trend filter, top 5 equal weight |
| `top5_inverse_vol_weight` | Composite score, top 5, weights ∝ 1/vol_3m |
| `top5_with_spy_regime_filter` | If SPY below 200DMA → GLD, SHY, TLT, XLU only; else baseline top 5 |

## Composite Score

```
0.35 × rank(momentum_12_1)
+ 0.20 × rank(ret_3m)
+ 0.15 × rank(ret_1m)
- 0.15 × rank(vol_3m)
- 0.15 × rank(|max_dd_1y|)
```

## SPY Regime Filter

- **Risk-on** (SPY above 200DMA): Use baseline top 5 from full universe
- **Defensive** (SPY below 200DMA): Select only from GLD, SHY, TLT, XLU (top by composite score, equal weight)

## Pipeline

1. Load features and prices
2. Get monthly signal dates (last date per month)
3. For each variant: generate signals in memory (date, ticker, weight)
4. Backtest each variant (10 bps cost, one-way turnover)
5. Compute metrics: total return, CAGR, vol, Sharpe, max DD, turnover
6. Save to `strategy_variant_summary`
7. Print ranked by Sharpe and by CAGR

## Output Table

`strategy_variant_summary`:

| Column | Type | Description |
|--------|------|-------------|
| variant | TEXT | Variant name |
| total_return | DOUBLE | Total return |
| cagr | DOUBLE | Compound annual growth rate |
| vol_ann | DOUBLE | Annualized volatility |
| sharpe | DOUBLE | Sharpe ratio (rf=0) |
| max_dd | DOUBLE | Max drawdown |
| turnover_ann | DOUBLE | Annualized turnover |

## Console Output

- Per-variant metrics during run
- Ranked by Sharpe (best first)
- Ranked by CAGR (best first)
- Confirmation that results were saved

## Behavior

- Monthly signal dates from `etf_features`
- Signal dates mapped to next available trading date
- Valid non-null features required before ranking
- Inverse-vol: weights ∝ 1/vol_3m (vol clipped at 1e-6)
- Tie-break: sort by ticker for deterministic results

## Dependencies

- DuckDB
- pandas
