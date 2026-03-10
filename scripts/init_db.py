import duckdb

# create database file
con = duckdb.connect("db/etf_data.duckdb")

# ETF metadata table
con.execute("""
CREATE TABLE IF NOT EXISTS etf_metadata (
    isin TEXT,
    ticker TEXT,
    source_ticker TEXT,
    name TEXT,
    provider TEXT,
    expense_ratio DOUBLE,
    asset_class TEXT,
    category TEXT,
    sector TEXT,
    benchmark TEXT,
    currency TEXT,
    data_source TEXT,
    inception_date DATE,
    description TEXT
)
""")

# price history
con.execute("""
CREATE TABLE IF NOT EXISTS prices_daily (
    date DATE,
    ticker TEXT,
    close DOUBLE,
    volume DOUBLE
)
""")

# ETF holdings
con.execute("""
CREATE TABLE IF NOT EXISTS etf_holdings (
    snapshot_date DATE,
    etf_ticker TEXT,
    etf_isin TEXT,
    holding_name TEXT,
    holding_ticker TEXT,
    holding_isin TEXT,
    asset_type TEXT,
    sector TEXT,
    country TEXT,
    shares DOUBLE,
    weight DOUBLE,
    market_value DOUBLE,
    data_source TEXT
)
""")

# features used by strategy
con.execute("""
CREATE TABLE IF NOT EXISTS etf_features (
    date DATE,
    ticker TEXT,
    ret_1w DOUBLE,
    ret_1m DOUBLE,
    ret_3m DOUBLE,
    ret_6m DOUBLE,
    ret_12m DOUBLE,
    momentum_12_1 DOUBLE,
    vol_1m DOUBLE,
    vol_3m DOUBLE,
    vol_12m DOUBLE,
    max_dd_1y DOUBLE,
    price_vs_200dma DOUBLE,
    ath_drawdown DOUBLE
)
""")

# strategy signals
con.execute("""
CREATE TABLE IF NOT EXISTS strategy_signals (
    date DATE,
    ticker TEXT,
    score DOUBLE,
    rank INTEGER,
    weight DOUBLE
)
""")

# asset_master
con.execute("""
CREATE TABLE IF NOT EXISTS asset_master (
    asset_id TEXT,
    canonical_name TEXT,
    canonical_ticker TEXT,
    isin TEXT,
    cusip TEXT,
    sedol TEXT,
    asset_type TEXT,
    sector TEXT,
    industry TEXT,
    country TEXT,
    currency TEXT,
    issuer TEXT
)
""")

# asset_alias
con.execute("""
CREATE TABLE IF NOT EXISTS asset_alias (
    alias_name TEXT,
    alias_ticker TEXT,
    alias_isin TEXT,
    asset_id TEXT,
    source TEXT
)
""")

# backtest results (populated by backtest_strategy.py)
con.execute("""
CREATE TABLE IF NOT EXISTS backtest_daily (
    date DATE,
    nav DOUBLE
)
""")
con.execute("""
CREATE TABLE IF NOT EXISTS backtest_benchmarks (
    date DATE,
    ticker TEXT,
    nav DOUBLE
)
""")
con.execute("""
CREATE TABLE IF NOT EXISTS backtest_summary (
    metric TEXT,
    value DOUBLE
)
""")

print("Database initialized successfully")
