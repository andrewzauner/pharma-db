#!/usr/bin/env python
# load_ob_from_csv.py — Lightweight Orange Book CSV → SQL Server loader
# - Windows Auth to localhost
# - No explicit DDL; schema inferred from CSV
# - if_exists='replace' (drops & recreates each table)
# - Creates Dim/Fact schemas if missing

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# === Config ===
DATA_DIR = r"C:\Users\andre\Documents\code\PharmaDB\data"
DB_NAME  = "Pharma"
DRIVER   = "ODBC Driver 17 for SQL Server"  # or "ODBC Driver 18 for SQL Server"
SERVER   = "localhost"
REPLACE_TABLES = True  # True -> to_sql(if_exists="replace"), False -> "append"

# CSV -> (schema, table) mapping (no DDL; schema inferred)
CSV_MAP = {
    os.path.join(DATA_DIR, "ob_dim_product.csv"):       ("Dim",  "OB_Product"),
    os.path.join(DATA_DIR, "ob_dim_tecode.csv"):        ("Dim",  "OB_TECode"),
    os.path.join(DATA_DIR, "ob_dim_product_te.csv"):    ("Dim",  "OB_ProductTE"),
    os.path.join(DATA_DIR, "ob_fact_exclusivity.csv"):  ("Fact", "OB_Exclusivity"),
    os.path.join(DATA_DIR, "ob_fact_patent.csv"):       ("Fact", "OB_Patent"),
    os.path.join(DATA_DIR, "ob_dim_application.csv"):   ("Dim",  "OB_Application"),  # may be empty
}



# Drop OB-related FKs so replace can proceed
def drop_ob_foreign_keys(engine):
    fk_sql = text("""
    DECLARE @sql nvarchar(max) = N'';
    ;WITH fks AS (
      SELECT 'ALTER TABLE ' + QUOTENAME(ps.name) + '.' + QUOTENAME(pt.name) +
             ' DROP CONSTRAINT ' + QUOTENAME(fk.name) AS cmd
      FROM sys.foreign_keys fk
      JOIN sys.tables      pt ON pt.object_id = fk.parent_object_id   -- child
      JOIN sys.schemas     ps ON ps.schema_id   = pt.schema_id
      JOIN sys.tables      rt ON rt.object_id = fk.referenced_object_id -- parent
      JOIN sys.schemas     rs ON rs.schema_id   = rt.schema_id
      WHERE ps.name IN ('Dim','Fact') AND rs.name IN ('Dim','Fact')
        AND (rt.name IN ('OB_Product','OB_TECode','OB_ProductTE','OB_Exclusivity','OB_Patent','OB_Application')
             OR pt.name IN ('OB_Product','OB_TECode','OB_ProductTE','OB_Exclusivity','OB_Patent','OB_Application'))
    )
    SELECT @sql = STRING_AGG(cmd, ';' + CHAR(10)) FROM fks;
    IF @sql IS NOT NULL AND LEN(@sql) > 0 EXEC sp_executesql @sql;
    """)
    with engine.begin() as conn:
        conn.execute(fk_sql)



def get_engine():
    # trusted_connection=yes for Windows Auth
    conn_uri = (
        f"mssql+pyodbc://@{SERVER}/{DB_NAME}"
        f"?driver={DRIVER.replace(' ', '+')}"
        f"&trusted_connection=yes"
    )
    # fast_executemany speeds up inserts
    engine = create_engine(
        conn_uri,
        fast_executemany=True,
        pool_pre_ping=True,
    )
    return engine

def ensure_schemas(engine):
    with engine.begin() as conn:
        conn.execute(text("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='Dim')  EXEC('CREATE SCHEMA Dim');"))
        conn.execute(text("IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name='Fact') EXEC('CREATE SCHEMA Fact');"))

def load_csv(engine, csv_path, schema, table):
    if not os.path.exists(csv_path):
        print(f"SKIP {schema}.{table}: {csv_path} not found")
        return

    # Read as generic object to avoid pandas auto-coercing to float/NaN
    df = pd.read_csv(csv_path, dtype=object, keep_default_na=True)

    # Trim strings and turn blank/whitespace-only into None (NULL)
    for col in df.columns:
        # Trim strings
        df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
        # Blank -> None
        df[col] = df[col].apply(lambda x: None if (isinstance(x, str) and x == "") else x)

    mode = "replace" if REPLACE_TABLES else "append"
    print(f"Loading {os.path.basename(csv_path)} → {schema}.{table} (mode={mode}) ...")
    df.to_sql(table, con=engine, schema=schema, if_exists=mode, index=False)
    print(f"✓ Loaded {len(df):,} rows into {schema}.{table}")


def main():
    engine = get_engine()  # or your get_engine()
    ensure_schemas(engine)

    # NEW: drop FKs so replace can drop/recreate tables freely
    drop_ob_foreign_keys(engine)

    any_loaded = False
    for csv_path, (schema, table) in CSV_MAP.items():
        try:
            load_csv(engine, csv_path, schema, table)
            any_loaded = True
        except Exception as e:
            print(f"ERROR loading {schema}.{table}: {e}")
            raise

    if not any_loaded:
        print("No Orange Book CSVs found. Check DATA_DIR paths.")
        sys.exit(1)

    print("\n=== Orange Book Load Complete ===")

if __name__ == "__main__":
    main()
