#!/usr/bin/env python
# load_rxnorm_from_csv.py
# Standalone loader: read CSVs and load into SQL Server (no API calls)

import os
import sys
import argparse
import pandas as pd
import pyodbc

# Get the directory of this script, then go up one level to find data/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DEFAULT_CSV_DIR = os.path.join(SCRIPT_DIR, "data")

def conn_string(server: str, database: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;TrustServerCertificate=yes;"
    )

def get_conn(server: str, database: str, driver: str):
    return pyodbc.connect(conn_string(server, database, driver), autocommit=False)

def sql_type_for(col: str) -> str:
    c = col.lower()
    if col in ("IngredientRxCUI", "ProductRxCUI"):
        return "VARCHAR(20)"
    if col == "NDC11":
        return "VARCHAR(11)"
    if col in ("OrphanFlag", "SpecialtyFlag", "IsCurrent", "RefrigeratedFlag"):
        return "BIT"
    if col == "UnitsPerPack":
        return "DECIMAL(18,4)"
    if col in ("PackSize", "UoM", "GTIN", "RxTTY", "Strength", "Route"):
        return "NVARCHAR(120)"
    if col in ("BrandName", "GenericName", "PreferredName", "DosageForm"):
        return "NVARCHAR(300)"
    if col in ("AtcClass",):
        return "NVARCHAR(60)"
    return "NVARCHAR(400)"

def ensure_table(cur, schema: str, table: str, df: pd.DataFrame, replace: bool):
    """
    Ensure [schema].[table] exists. If replace=True, drop & create with columns inferred from df.
    """
    full_table = f"[{schema}].[{table}]"

    # Create schema if missing
    cur.execute(
        f"IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = N'{schema}') "
        f"EXEC(N'CREATE SCHEMA [{schema}]');"
    )

    if replace:
        cur.execute(
            f"IF OBJECT_ID(N'{full_table}', 'U') IS NOT NULL DROP TABLE {full_table};"
        )
        cols_sql = ", ".join(f"[{c}] {sql_type_for(c)} NULL" for c in df.columns)
        cur.execute(f"CREATE TABLE {full_table} ({cols_sql});")


def load_df(cur, full_table: str, df: pd.DataFrame):
    df = df.copy()
    # Map bit-ish columns to 0/1/NULL
    for bit_col in ("OrphanFlag","SpecialtyFlag","IsCurrent","RefrigeratedFlag"):
        if bit_col in df.columns:
            df[bit_col] = df[bit_col].apply(lambda x: None if pd.isna(x) else int(x))

    cols = list(df.columns)
    placeholders = ",".join(["?"] * len(cols))
    col_list = ",".join(f"[{c}]" for c in cols)
    sql = f"INSERT INTO {full_table} ({col_list}) VALUES ({placeholders})"

    rows = [tuple(None if (pd.isna(v) or v == "") else v for v in row) for _, row in df.iterrows()]
    cur.fast_executemany = True
    cur.executemany(sql, rows)

def add_indexes(cur, prd_full: str, pack_full: str):
    cur.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DimDrugProduct_Ingredient' "
                f"AND object_id=OBJECT_ID('{prd_full}')) "
                f"CREATE NONCLUSTERED INDEX IX_DimDrugProduct_Ingredient ON {prd_full} (IngredientRxCUI);")
    cur.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DimProductPack_Product' "
                f"AND object_id=OBJECT_ID('{pack_full}')) "
                f"CREATE NONCLUSTERED INDEX IX_DimProductPack_Product ON {pack_full} (ProductRxCUI, IsCurrent);")
    cur.execute(f"IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE name='IX_DimProductPack_Ingredient' "
                f"AND object_id=OBJECT_ID('{pack_full}')) "
                f"CREATE NONCLUSTERED INDEX IX_DimProductPack_Ingredient ON {pack_full} (IngredientRxCUI);")

def main():
    ap = argparse.ArgumentParser(description="Load RxNorm CSVs into SQL Server (no API calls).")
    ap.add_argument("--server",   default="localhost")
    ap.add_argument("--database", default="Pharma")
    ap.add_argument("--driver",   default="ODBC Driver 17 for SQL Server")
    ap.add_argument("--schema",   default="Dim")
    ap.add_argument("--csv-dir",  default=DEFAULT_CSV_DIR)
    ap.add_argument("--replace",  choices=["Y","N"], default="Y",
                    help="Y=drop & create table from CSV columns, N=append")
    args = ap.parse_args()

    # CSV paths
    ing_csv  = os.path.join(args.csv_dir, "dim_drug_ingredient.csv")
    prd_csv  = os.path.join(args.csv_dir, "dim_drug_product.csv")
    pack_csv = os.path.join(args.csv_dir, "dim_productpack_ndc.csv")
    for p in (ing_csv, prd_csv, pack_csv):
        if not os.path.exists(p):
            print(f"[ERROR] Missing CSV: {p}")
            sys.exit(1)

    # Read CSVs
    ing_df  = pd.read_csv(ing_csv, dtype=str).fillna(pd.NA)
    prd_df  = pd.read_csv(prd_csv, dtype=str).fillna(pd.NA)
    pack_df = pd.read_csv(pack_csv, dtype=str).fillna(pd.NA)

    schema   = args.schema
    ing_name = "DrugIngredient"
    prd_name = "DrugProduct"
    pack_name= "ProductPack"

    ing_tbl  = f"[{schema}].[{ing_name}]"
    prd_tbl  = f"[{schema}].[{prd_name}]"
    pack_tbl = f"[{schema}].[{pack_name}]"
    replace  = (args.replace == "Y")

    with get_conn(args.server, args.database, args.driver) as conn:
        cur = conn.cursor()

        # Create/replace as directed
        ensure_table(cur, schema, ing_name,  ing_df,  replace)
        ensure_table(cur, schema, prd_name,  prd_df,  replace)
        ensure_table(cur, schema, pack_name, pack_df, replace)

        if replace:
            conn.commit()  # commit DDL before data

        # Load in dependency-safe order
        load_df(cur, ing_tbl,  ing_df)
        load_df(cur, prd_tbl,  prd_df)
        load_df(cur, pack_tbl, pack_df)
        conn.commit()

        # Add indexes (best-effort; ignore duplicates if appending)
        try:
            add_indexes(cur, prd_tbl, pack_tbl)
            conn.commit()
        except Exception:
            conn.rollback()

    print("✔ CSVs loaded into SQL Server.")
    print(f"  Server   : {args.server}")
    print(f"  Database : {args.database}")
    print(f"  Schema   : {args.schema}")
    print(f"  Replace  : {args.replace}")
    print(f"  CSV dir  : {args.csv_dir}")

if __name__ == "__main__":
    main()
