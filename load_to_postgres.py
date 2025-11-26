#!/usr/bin/env python
"""
PostgreSQL loader for PharmaDB ETL pipeline.
Loads all CSV files from data/ directory into PostgreSQL raw schema.
"""
import os
import sys
import logging
import pandas as pd
from sqlalchemy import create_engine, text, inspect
from typing import Dict, Tuple, Optional

import config

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def get_postgres_engine():
    """Create SQLAlchemy engine for PostgreSQL."""
    db = config.DB_CONFIG
    conn_str = (
        f"postgresql://{db['user']}:{db['password']}@"
        f"{db['host']}:{db['port']}/{db['database']}"
    )
    engine = create_engine(
        conn_str,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 10}
    )
    return engine


def ensure_schema(engine, schema_name: str):
    """Ensure the target schema exists."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))


def infer_postgres_type(col_name: str, sample_values: pd.Series) -> str:
    """
    Infer PostgreSQL data type from column name and sample values.
    Returns appropriate PostgreSQL type string.
    """
    col_lower = col_name.lower()
    
    # Known patterns
    if "rxcui" in col_lower or "cui" in col_lower:
        return "VARCHAR(20)"
    if col_name == "NDC11":
        return "VARCHAR(11)"
    if "flag" in col_lower or col_lower.endswith("flag"):
        return "BOOLEAN"
    if "date" in col_lower:
        return "DATE"
    if "code" in col_lower and len(sample_values.dropna()) > 0:
        # Check if codes are numeric
        try:
            sample_values.dropna().astype(int)
            return "INTEGER"
        except (ValueError, TypeError):
            return "VARCHAR(50)"
    
    # Analyze sample values
    non_null = sample_values.dropna()
    if len(non_null) == 0:
        return "TEXT"
    
    # Try to infer from data
    sample = non_null.head(100)
    
    # Check if numeric
    try:
        pd.to_numeric(sample, errors="raise")
        # Check if integer
        if all(float(x).is_integer() if isinstance(x, str) else isinstance(x, (int, float)) and float(x).is_integer() 
               for x in sample.head(20)):
            return "INTEGER"
        return "NUMERIC"
    except (ValueError, TypeError):
        pass
    
    # Check if boolean-like
    str_vals = sample.astype(str).str.upper().str.strip()
    if all(v in ("Y", "N", "YES", "NO", "TRUE", "FALSE", "1", "0", "") for v in str_vals.head(20)):
        return "BOOLEAN"
    
    # Check max length for VARCHAR
    max_len = sample.astype(str).str.len().max()
    if max_len <= 50:
        return "VARCHAR(50)"
    elif max_len <= 200:
        return "VARCHAR(200)"
    elif max_len <= 500:
        return "VARCHAR(500)"
    else:
        return "TEXT"


def create_table_from_df(engine, schema: str, table: str, df: pd.DataFrame, replace: bool = True):
    """
    Create a PostgreSQL table from a DataFrame.
    If replace=True, drops existing table first.
    """
    full_table = f"{schema}.{table}"
    
    with engine.begin() as conn:
        if replace:
            conn.execute(text(f"DROP TABLE IF EXISTS {full_table} CASCADE"))
        
        # Build column definitions
        col_defs = []
        for col in df.columns:
            pg_type = infer_postgres_type(col, df[col])
            col_defs.append(f'"{col}" {pg_type}')
        
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS {full_table} (
            {', '.join(col_defs)}
        )
        """
        conn.execute(text(create_sql))
        logger.info(f"Created/verified table {full_table}")


def load_csv_to_postgres(
    csv_path: str,
    schema: str,
    table: str,
    engine,
    replace: bool = True
) -> int:
    """
    Load a CSV file into PostgreSQL.
    Returns number of rows loaded.
    """
    if not os.path.exists(csv_path):
        logger.warning(f"CSV not found: {csv_path}")
        return 0
    
    logger.info(f"Loading {os.path.basename(csv_path)} -> {schema}.{table}")
    
    # Read CSV as strings to preserve data
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=True, na_values=[""])
    
    if df.empty:
        logger.warning(f"CSV is empty: {csv_path}")
        return 0
    
    # Clean data: trim strings, convert empty strings to None
    for col in df.columns:
        df[col] = df[col].apply(
            lambda x: None if (pd.isna(x) or (isinstance(x, str) and x.strip() == "")) 
            else (x.strip() if isinstance(x, str) else x)
        )
    
    # Create table
    create_table_from_df(engine, schema, table, df, replace=replace)
    
    # Load data
    full_table = f"{schema}.{table}"
    
    # Convert boolean columns
    for col in df.columns:
        if "flag" in col.lower() or col.lower().endswith("flag"):
            df[col] = df[col].apply(
                lambda x: None if pd.isna(x) or x is None
                else (True if str(x).upper().strip() in ("Y", "YES", "TRUE", "1") 
                      else (False if str(x).upper().strip() in ("N", "NO", "FALSE", "0") else None))
            )
    
    # Use pandas to_sql for bulk insert
    df.to_sql(
        table,
        con=engine,
        schema=schema,
        if_exists="replace" if replace else "append",
        index=False,
        method="multi",
        chunksize=1000
    )
    
    row_count = len(df)
    logger.info(f"✓ Loaded {row_count:,} rows into {full_table}")
    return row_count


# CSV to table mappings for each data source
ORANGE_BOOK_MAP = {
    "ob_dim_application.csv": "ob_dim_application",
    "ob_dim_product.csv": "ob_dim_product",
    "ob_dim_tecode.csv": "ob_dim_tecode",
    "ob_dim_product_te.csv": "ob_dim_product_te",
    "ob_fact_exclusivity.csv": "ob_fact_exclusivity",
    "ob_fact_patent.csv": "ob_fact_patent",
}

PURPLE_BOOK_MAP = {
    "pb_dim_application.csv": "pb_dim_application",
    "pb_dim_product.csv": "pb_dim_product",
    "pb_dim_biosimilar.csv": "pb_dim_biosimilar",
    "pb_fact_exclusivity.csv": "pb_fact_exclusivity",
}

RXNORM_MAP = {
    "dim_drug_ingredient.csv": "rxnorm_dim_ingredient",
    "dim_drug_product.csv": "rxnorm_dim_product",
    "dim_productpack_ndc.csv": "rxnorm_dim_productpack",
}

ALL_MAPS = {**ORANGE_BOOK_MAP, **PURPLE_BOOK_MAP, **RXNORM_MAP}


def load_all_csvs(engine, schema: str, replace: bool = True, sources: Optional[list] = None) -> Dict[str, int]:
    """
    Load CSV files from data directory into PostgreSQL.
    
    Args:
        engine: SQLAlchemy engine
        schema: Target schema name
        replace: If True, replace tables; if False, append
        sources: Optional list of data sources to load ("orange-book", "purple-book", "rxnorm")
                 If None, loads all sources
    
    Returns dict mapping table names to row counts.
    """
    results = {}
    
    # Filter maps by source if specified
    maps_to_load = {}
    if sources:
        if "orange-book" in sources:
            maps_to_load.update(ORANGE_BOOK_MAP)
        if "purple-book" in sources:
            maps_to_load.update(PURPLE_BOOK_MAP)
        if "rxnorm" in sources:
            maps_to_load.update(RXNORM_MAP)
    else:
        maps_to_load = ALL_MAPS
    
    for csv_file, table_name in maps_to_load.items():
        csv_path = os.path.join(config.DATA_DIR, csv_file)
        try:
            count = load_csv_to_postgres(csv_path, schema, table_name, engine, replace=replace)
            results[table_name] = count
        except Exception as e:
            logger.error(f"Failed to load {csv_file}: {e}", exc_info=True)
            results[table_name] = -1
    
    return results


def main():
    """Main entry point for PostgreSQL loader."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Load PharmaDB CSVs into PostgreSQL")
    parser.add_argument("--replace", action="store_true", default=config.REPLACE_TABLES,
                        help="Replace existing tables (default: True)")
    parser.add_argument("--append", action="store_true",
                        help="Append to existing tables (overrides --replace)")
    parser.add_argument("--schema", default=config.DB_CONFIG["schema"],
                        help=f"Target schema (default: {config.DB_CONFIG['schema']})")
    parser.add_argument("--only", nargs="+", choices=["orange-book", "purple-book", "rxnorm"],
                        help="Load only specified data sources")
    args = parser.parse_args()
    
    replace = not args.append if args.append else args.replace
    
    try:
        engine = get_postgres_engine()
        logger.info(f"Connected to PostgreSQL: {config.DB_CONFIG['host']}/{config.DB_CONFIG['database']}")
        
        # Ensure schema exists
        ensure_schema(engine, args.schema)
        logger.info(f"Using schema: {args.schema}")
        
        # Load CSVs
        results = load_all_csvs(engine, args.schema, replace=replace, sources=args.only)
        
        # Summary
        print("\n=== PostgreSQL Load Summary ===")
        total_rows = 0
        for table, count in results.items():
            status = f"{count:,} rows" if count >= 0 else "FAILED"
            print(f"  {args.schema}.{table}: {status}")
            if count > 0:
                total_rows += count
        
        print(f"\nTotal rows loaded: {total_rows:,}")
        print(f"Mode: {'REPLACE' if replace else 'APPEND'}")
        
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()

