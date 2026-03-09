# build_features.py

Computes quantitative features from price history and loads them into `etf_features`. Used for strategy signals and backtesting.

## Usage

```bash
python scripts/build_features.py
```

## Prerequisites

- Database with `prices_daily` and `etf_features` tables
- Price data loaded (run `etl_prices.py`)

## Features Computed

| Feature | Description |
|---------|-------------|
| `ret_1w` | 5-day return |
| `ret_1m` | 21-day return |
| `ret_3m` | 63-day return |
| `ret_6m` | 126-day return |
| `ret_12m` | 252-day return |
| `momentum_12_1` | 12-1 momentum (return from t-252 to t-21, excluding last month) |
| `vol_1m` | 21-day rolling annualized volatility |
| `vol_3m` | 63-day rolling annualized volatility |
| `vol_12m` | 252-day rolling annualized volatility |
| `max_dd_1y` | Rolling 1-year maximum drawdown |
| `price_vs_200dma` | Price / 200-day MA - 1 (trend filter) |
| `ath_drawdown` | Current price / all-time high - 1 |

## Behavior

- **Rebuilds** `etf_features` entirely (DELETE + INSERT)
- Computes per-ticker; uses 252 trading days per year for volatility
- Drops rows where all feature columns are null
- Prints sample rows at the end

## Downstream

- `strategy_signals` table is intended for strategy output (scores, ranks, weights)
- Features can be used for screening, ranking, or allocation logic
