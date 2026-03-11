# research_regime_filter_variants.py

Research script that tests robustness of regime filter strategy variants. Compares different regime indicators (SPY/IWDA, 150/200/250 DMA), defensive baskets, and top-N in normal regime.

## Usage

```bash
python scripts/research_regime_filter_variants.py
```

## Prerequisites

- Database with `etf_features`, `prices_daily`
- Feature data loaded (run `build_features.py` first)
- Price data loaded (run `etl_prices.py`)

## Variants

| Variant | Regime indicator | Defensive basket | Top N (normal) |
|---------|------------------|------------------|----------------|
| `spy_200dma_defensive_top5` | SPY 200DMA | GLD, SHY, TLT, XLU | 5 |
| `spy_150dma_defensive_top5` | SPY 150DMA | GLD, SHY, TLT, XLU | 5 |
| `spy_250dma_defensive_top5` | SPY 250DMA | GLD, SHY, TLT, XLU | 5 |
| `iwda_200dma_defensive_top5` | IWDA.L 200DMA | GLD, SHY, TLT, XLU | 5 |
| `spy_200dma_defensive_top3` | SPY 200DMA | GLD, SHY, TLT, XLU | 3 |
| `defensive_gld_shy` | SPY 200DMA | GLD, SHY | 5 |
| `defensive_gld_shy_tlt` | SPY 200DMA | GLD, SHY, TLT | 5 |
| `defensive_gld_only` | SPY 200DMA | GLD | 5 |
| `defensive_shy_only` | SPY 200DMA | SHY | 5 |

## Regime Logic

- **Risk-on** (regime ticker above MA): Baseline top-N from full universe, trend filter (`price_vs_200dma > 0`)
- **Defensive** (regime ticker below MA): Select only from defensive basket, trend filter still applies, equal weight

## Regime Indicator Sources

- **200DMA**: Uses `price_vs_200dma` from `etf_features` when the regime ticker exists in features
- **150DMA / 250DMA**: Computed from `prices_daily` (rolling MA)
- **IWDA.L**: Uses features if available; otherwise computed from prices

## Pipeline

1. Load features and prices
2. Get monthly signal dates (last date per month)
3. For each variant: build regime map, generate signals in memory
4. Backtest each variant (10 bps cost, one-way turnover)
5. Compute metrics: total return, CAGR, vol, Sharpe, max DD, turnover
6. Save to `regime_filter_variant_summary`
7. Print ranked by Sharpe, CAGR, and max drawdown

## Output Table

`regime_filter_variant_summary`:

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
- Ranked by max drawdown (best = least negative)
- Confirmation that results were saved

## Behavior

- Monthly signal dates from `etf_features`
- Signal dates mapped to next available trading date
- Valid non-null features required before ranking
- Defensive ETFs must pass trend filter (`price_vs_200dma > 0`)
- Tie-break: sort by ticker for deterministic results

## Dependencies

- DuckDB
- pandas
