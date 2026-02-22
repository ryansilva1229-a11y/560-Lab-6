import duckdb
import pandas as pd

conn = duckdb.connect('Data/mydb.duckdb')
df = conn.execute("SELECT * FROM OIL_DATA LIMIT 5").fetchdf()
print(df)