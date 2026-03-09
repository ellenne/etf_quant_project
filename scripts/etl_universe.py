from pathlib import Path
import duckdb
import pandas as pd

DB_PATH = "db/etf_data.duckdb"
CSV_PATH = "data/etf_universe.csv"


def main() -> None:
    csv_file = Path(CSV_PATH)
    if not csv_file.exists():
        raise FileNotFoundError(f"Universe file not found: {CSV_PATH}")

    df = pd.read_csv(csv_file)

    required_cols = [
        "ticker",
        "source_ticker",
        "isin",
        "name",
        "provider",
        "asset_class",
        "category",
        "sector",
        "benchmark",
        "currency",
        "data_source",
    ]

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns in CSV: {missing}")

    df = df.copy()
    df["expense_ratio"] = None
    df["inception_date"] = None
    df["description"] = None

    con = duckdb.connect(DB_PATH)

    con.execute("DELETE FROM etf_metadata")

    con.register("universe_df", df)
    con.execute("""
        INSERT INTO etf_metadata (
            isin,
            ticker,
            source_ticker,
            name,
            provider,
            expense_ratio,
            asset_class,
            category,
            sector,
            benchmark,
            currency,
            data_source,
            inception_date,
            description
        )
        SELECT
            isin,
            ticker,
            source_ticker,
            name,
            provider,
            expense_ratio,
            asset_class,
            category,
            sector,
            benchmark,
            currency,
            data_source,
            inception_date,
            description
        FROM universe_df
    """)

    row_count = con.execute("SELECT COUNT(*) FROM etf_metadata").fetchone()[0]
    print(f"Loaded {row_count} ETFs into etf_metadata")


if __name__ == "__main__":
    main()
