import duckdb

PARQUET_PATH = "./output/municipales_2026_bureaux_vote_france.parquet"
DB_PATH = "municipales.duckdb"

con = duckdb.connect(DB_PATH)

con.execute("DROP TABLE IF EXISTS municipales_2026")

con.execute(f"""
CREATE TABLE municipales_2026 AS
SELECT *
FROM read_parquet('{PARQUET_PATH}')
""")

print("Base créée : municipales.duckdb")
print(con.execute("SELECT COUNT(*) FROM municipales_2026").fetchdf())
print(con.execute("DESCRIBE municipales_2026").fetchdf())