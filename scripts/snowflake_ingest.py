"""
Snowflake ingest script.
Reads the three gold parquet tables from /shared/data/golden/
and loads them into Snowflake using write_pandas.

Required environment variables:
    SNOWFLAKE_ACCOUNT   e.g. dxc27173.us-east-1
    SNOWFLAKE_USER      e.g. coco
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_DATABASE  e.g. AIRBNB_PIPELINE
    SNOWFLAKE_SCHEMA    e.g. GOLD
    SNOWFLAKE_WAREHOUSE e.g. AIRBNB_WH
"""

import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# ─────────────────────────────────────────
# 1. Config
# ─────────────────────────────────────────
GOLDEN_BASE = "/shared/data/golden"

TABLES = [
    "fact_neighborhood_month",
    "fact_host_summary",
    "fact_listing_summary",
]

def get_conn():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )

# ─────────────────────────────────────────
# 2. Ingest each gold table
# ─────────────────────────────────────────
conn = get_conn()

for table in TABLES:
    path = f"{GOLDEN_BASE}/{table}"
    print(f"Loading {path} → Snowflake table {table.upper()} ...")

    df = pd.read_parquet(path)
    df.columns = [c.upper() for c in df.columns]  # Snowflake prefers uppercase

    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table.upper(),
        auto_create_table=True,
        overwrite=True,
    )
    print(f"  {nrows} rows loaded ({nchunks} chunks), success={success}")

conn.close()
print("Snowflake ingest complete.")
