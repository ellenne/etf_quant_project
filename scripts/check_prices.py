import duckdb

con = duckdb.connect("db/etf_data.duckdb")

print("Row count:")
print(con.execute("SELECT COUNT(*) FROM prices_daily").fetchall())

print("\nSample:")
rows = con.execute("""
    SELECT *
    FROM prices_daily
    ORDER BY ticker, date
    LIMIT 20
""").fetchall()

for row in rows:
    print(row)