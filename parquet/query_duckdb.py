import os
import duckdb

bucket = os.environ["S3_BUCKET"]
key_id = os.environ["CELLAR_ADDON_KEY_ID"]
secret = os.environ["CELLAR_ADDON_KEY_SECRET"]
endpoint = os.environ["CELLAR_ADDON_HOST"]

parquet_path = "elections/municipales_2026/municipales_2026_bureaux_vote_france.parquet"

con = duckdb.connect("municipales.duckdb")

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")

con.execute(f"""
SET s3_region='default';
SET s3_access_key_id='{key_id}';
SET s3_secret_access_key='{secret}';
SET s3_endpoint='{endpoint}';
SET s3_use_ssl=true;
SET s3_url_style='path';
""")

query = f"""
SELECT tour, COUNT(*) AS nb_lignes
FROM read_parquet('s3://{bucket}/{parquet_path}')
GROUP BY tour
ORDER BY tour
"""

df = con.execute(query).fetchdf()
print(df)

print(con.execute("""
SELECT *
FROM read_parquet('s3://matthias/elections/municipales_2026/municipales_2026_bureaux_vote_france.parquet')
LIMIT 10
""").fetchdf())