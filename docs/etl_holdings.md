# ETL Holdings Documentation

This document describes how `scripts/etl_holdings.py` works and the snapshot date substitution workflow.

---

## Overview

`etl_holdings.py` fetches the **top 10 holdings** of each ETF in your universe from Yahoo Finance and loads them into the `etf_holdings` table. It also enriches each holding with its ISIN (International Securities Identification Number) when available.

---

## How It Works

### 1. Prerequisites

- **Database**: `db/etf_data.duckdb` must exist with `etf_metadata` and `etf_holdings` tables.
- **ETF universe**: Run `etl_universe.py` first to populate `etf_metadata` from `data/etf_universe.csv`.

### 2. Execution Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           etl_holdings.py main()                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  1. fetch_etfs(con)                                                          │
│     → SELECT ticker, source_ticker, isin FROM etf_metadata                    │
│     → Returns list of (ticker, source_ticker, isin) for each ETF              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  2. snapshot_date = date.today()                                              │
│     → All holdings in this run share the same snapshot date                    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                    ┌─────────────────┴─────────────────┐
                    │  --delete-today flag set?         │
                    └─────────────────┬─────────────────┘
                          Yes         │         No
                    ┌─────────────────┴─────────────────┐
                    ▼                                   │
┌───────────────────────────────────┐                  │
│  3a. DELETE FROM etf_holdings     │                  │
│      WHERE snapshot_date = today   │                  │
│  (removes existing today's data)   │                  │
└───────────────────────────────────┘                  │
                    │                                   │
                    └─────────────────┬─────────────────┘
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  4. For each ETF:                                                             │
│     download_holdings() → load_holdings()                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3. Per-ETF Processing: `download_holdings()`

For each ETF:

1. **Fetch top 10** via `yf.Ticker(yf_ticker).funds_data.top_holdings`
   - Returns: Symbol, Name, Holding Percent
   - Bond/commodity ETFs (e.g. GLD, TLT, SHY) may return 0 rows

2. **Enrich with ISIN** for each holding:
   - `get_isin_for_symbol(symbol)` uses `yf.Ticker(symbol).isin`
   - Results are cached in `_ISIN_CACHE` (same stock in multiple ETFs → one lookup)
   - Invalid symbols or API errors → `holding_isin` stays `NULL`

3. **Map to schema**:
   - `snapshot_date`, `etf_ticker`, `etf_isin`, `holding_name`, `holding_ticker`, `holding_isin`, `weight`, etc.

### 4. Per-ETF Loading: `load_holdings()`

For each ETF’s DataFrame:

1. **Delete** existing rows for that ETF and snapshot date:
   ```sql
   DELETE FROM etf_holdings
   WHERE etf_ticker = ? AND snapshot_date = ?
   ```

2. **Insert** the new rows from the DataFrame.

This keeps each ETF’s holdings for a given date idempotent: re-running replaces the previous data for that ETF/date.

---

## Snapshot Date Substitution

### What Is a Snapshot Date?

`snapshot_date` is the date when the holdings data was captured. All holdings in a single run share the same snapshot date (today).

### Why Substitute (Replace) a Snapshot?

You may want to replace today’s snapshot when:

- You first loaded top-10 data and later want to refresh it.
- You ran the script multiple times and want a clean re-run.
- You want to overwrite partial or incorrect data for today.

### How Substitution Works

| Step | Action |
|------|--------|
| 1 | Run `python scripts/etl_holdings.py --delete-today` |
| 2 | Script deletes all rows where `snapshot_date = today` |
| 3 | Script fetches fresh top-10 holdings for each ETF |
| 4 | Script inserts new rows with `snapshot_date = today` |

**Without `--delete-today`:**

- Existing rows for today are **not** deleted globally.
- Per-ETF, `load_holdings()` still deletes and re-inserts for that ETF and today.
- So each ETF’s data for today is replaced, but no global “wipe” of today’s data happens first.

**With `--delete-today`:**

- First, **all** rows for today are deleted in one pass.
- Then each ETF is fetched and loaded.
- Use this when you want a clean slate for today before re-running.

### Example Workflow

```bash
# Initial run: insert today's top 10 holdings
python scripts/etl_holdings.py

# Later: you want to replace today's data (e.g. after fixing a bug or refreshing)
python scripts/etl_holdings.py --delete-today
```

Output with `--delete-today`:

```
Deleted 261 existing rows for 2026-03-09

Fetching holdings for SPY...
  -> inserted 10 rows (top 10 holdings)
...
```

---

## Helper Functions

### `get_symbol_for_isin(isin)`

- **Purpose**: Resolve a Yahoo Finance symbol from an ISIN.
- **Source**: Yahoo Finance search API (`query1.finance.yahoo.com/v1/finance/search`).
- **Use case**: When you have an ISIN and need the ticker for further lookups.

### `get_isin_for_symbol(symbol)`

- **Purpose**: Resolve an ISIN from a Yahoo Finance symbol.
- **Source**: `yf.Ticker(symbol).isin` (yfinance, backed by Business Insider).
- **Caching**: Results stored in `_ISIN_CACHE` to avoid repeated lookups (e.g. AAPL in SPY, QQQ, IWD, etc.).
- **Fallback**: Returns `None` on invalid symbol, HTTP 404, or when yfinance returns `"-"`.

---

## Limitations

| Limitation | Details |
|------------|---------|
| **Top 10 only** | Yahoo Finance exposes only the top 10 holdings. Full holdings require `load_holdings_csv.py` with manual data. |
| **Bond/commodity ETFs** | GLD, TLT, SHY, etc. often return 0 holdings (no equity-style top 10). |
| **ISIN coverage** | ~50% of holdings get `holding_isin` populated; the rest stay `NULL` (invalid symbols, API limits). |

---

## Related Scripts

| Script | Purpose |
|--------|---------|
| `etl_universe.py` | Load ETF metadata from CSV into `etf_metadata` |
| `load_holdings_csv.py` | Load full holdings from a manual CSV (provider websites) |
| `init_db.py` | Create database and tables |
