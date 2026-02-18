# /// script
# dependencies = [
#   "duckdb",
# ]
# ///

import duckdb
import json
import os

db_path = os.environ["FF_DATABASE_PATH"]
input_tables = json.loads(os.environ["FF_INPUT_TABLES"])
output_table = os.environ["FF_OUTPUT_TABLE"]

conn = duckdb.connect(db_path)

# Read from upstream table
df = conn.execute(f"SELECT * FROM {input_tables[0]}").fetchdf()

# Enrich: add a score column
df["score"] = df["amount"] * 1.1

# Write output table
conn.execute(f"CREATE OR REPLACE TABLE {output_table} AS SELECT * FROM df")
conn.close()
