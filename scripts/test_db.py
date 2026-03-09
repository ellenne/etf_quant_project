import duckdb

con = duckdb.connect("db/etf_data.duckdb")

tables = con.execute("SHOW TABLES").fetchall()

print("Tables in DB:")
for t in tables:
    print(t)
