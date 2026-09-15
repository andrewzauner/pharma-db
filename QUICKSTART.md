# Quick Start Guide

## First Time Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Set database credentials:**

   Copy the example config and set environment variables (config.py is
   gitignored so local credentials never get committed):
   ```bash
   cp config.py.example config.py

   # Windows PowerShell
   $env:DB_PASSWORD="your_password"
   
   # Linux/Mac
   export DB_PASSWORD=your_password
   ```

3. **Run the full pipeline:**
   ```bash
   python run_etl_pipeline.py
   ```

That's it! The pipeline will:
- Download Orange Book data from FDA
- Download Purple Book data from FDA  
- Query RxNorm API for drug data
- Load everything into PostgreSQL `raw` schema

## Verify Data

Connect to your PostgreSQL database:

```sql
-- Check what tables were created
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'raw'
ORDER BY table_name;

-- Check row counts
SELECT 
    schemaname,
    tablename,
    n_live_tup as row_count
FROM pg_stat_user_tables
WHERE schemaname = 'raw'
ORDER BY tablename;
```

## Common Tasks

**Re-run just the database load:**
```bash
python load_to_postgres.py
```

**Run only Orange Book:**
```bash
python orange_book_ingest.py
python load_to_postgres.py
```

**Append new data (don't replace):**
```bash
python run_etl_pipeline.py --append
```

## Troubleshooting

**Connection refused:**
- Check PostgreSQL is running: `ping 192.168.0.85`
- Verify credentials in `config.py`
- Check PostgreSQL `pg_hba.conf` allows remote connections

**No data loaded:**
- Check CSV files exist in `data/` directory
- Review logs for errors
- Verify ingest scripts completed successfully

**Missing dependencies:**
```bash
pip install -r requirements.txt
```


