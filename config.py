"""
Configuration for PharmaDB ETL pipeline.
Database connection settings for PostgreSQL on Raspberry Pi.
"""
import os
from typing import Dict

# PostgreSQL connection settings
DB_CONFIG: Dict[str, str] = {
    "host": os.getenv("DB_HOST", "192.168.0.85"),
    "port": os.getenv("DB_PORT", "5432"),
    "database": os.getenv("DB_NAME", "asclepius"),
    "user": os.getenv("DB_USER", "asclepius_app"),
    "password": os.getenv("DB_PASSWORD", "admin"),
    "schema": "raw",  # Target schema for raw data
}

# Data directory (relative to script location)
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")

# ETL settings
REPLACE_TABLES = True  # If True, drop and recreate tables; if False, append


