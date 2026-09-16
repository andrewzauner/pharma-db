# PharmaDB ETL Pipeline

ETL tool for extracting pharmaceutical data from multiple sources and loading into PostgreSQL.

## Data Sources

1. **FDA Orange Book**: Approved drug products with therapeutic equivalence codes, patents, and exclusivity
2. **FDA Purple Book**: Licensed biological products and biosimilars
3. **RxNorm**: Standardized drug nomenclature and NDC codes

## Architecture

```
┌─────────────────┐
│  Data Sources   │
│  (FDA, RxNorm)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Ingest Scripts │
│  (Download/ETL)│
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  CSV Files      │
│  (data/)        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  PostgreSQL     │
│  raw schema     │
└─────────────────┘
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure Database Connection

Copy the example config, then set environment variables for the PostgreSQL
connection (config.py is gitignored so local credentials are never committed):

```bash
cp config.py.example config.py
export DB_HOST=192.168.0.85
export DB_PASSWORD=your_password
```

### 3. Run the Pipeline

#### Full Pipeline (Recommended)

Run all ingest steps and load to database:

```bash
python run_etl_pipeline.py
```

#### Individual Steps

**Orange Book only:**
```bash
python orange_book_ingest.py
python load_to_postgres.py --only orange-book
```

**Purple Book only:**
```bash
python purple_book_ingest.py
python load_to_postgres.py --only purple-book
```

**RxNorm only:**
```bash
python rxnorm_ingest.py
python load_to_postgres.py --only rxnorm
```

By default this queries RxNav for a small built-in seed list (adalimumab,
lipitor, acetaminophen, semaglutide). To look up specific drugs instead:
```bash
python rxnorm_ingest.py --names ibuprofen naproxen
```
Or to build real coverage of what's actually in the Orange Book, seed from
its most-common active ingredients (run `orange_book_ingest.py` first):
```bash
python rxnorm_ingest.py --from-orange-book 50
```
`run_etl_pipeline.py --rxnorm-names <names>` passes through to `--names`.

**Load existing CSVs only:**
```bash
python load_to_postgres.py
```

## Scripts

### Ingest Scripts

- `orange_book_ingest.py`: Downloads and processes FDA Orange Book data
- `purple_book_ingest.py`: Downloads and processes FDA Purple Book data
- `rxnorm_ingest.py`: Queries RxNorm API for drug data

### Load Scripts

- `load_to_postgres.py`: Loads all CSV files into PostgreSQL `raw` schema
- `load_ob_from_csv.py`: Legacy SQL Server loader (Orange Book)
- `load_rxnorm_from_csv.py`: Legacy SQL Server loader (RxNorm)

### Validation

- `validate_data.py`: Checks the processed CSVs in `data/` for empty tables,
  duplicate/collided columns (a common failure mode when an ingest script's
  column-alias map accidentally maps two different source columns to the
  same output name), dangling foreign keys between fact and dimension
  tables, and raw source files that were saved as the wrong file type (an
  HTML error page or an image instead of real data, which happens when an
  FDA download silently fails). Runs automatically between ingest and load
  in `run_etl_pipeline.py`; exits non-zero if it finds an error. Run it
  standalone with `python validate_data.py`.

### Orchestration

- `run_etl_pipeline.py`: Main orchestration script that runs all steps in
  order (ingest → validate → load). If validation reports errors, the load
  step is skipped by default to avoid pushing corrupted data to Postgres;
  pass `--force-load` to load anyway, or `--skip-validate` to skip the check.

### Configuration

- `config.py`: Database connection settings and configuration

## Output Tables

All tables are created in the `raw` schema:

### Orange Book Tables
- `ob_dim_application`: Application metadata
- `ob_dim_product`: Product information
- `ob_dim_tecode`: Therapeutic equivalence codes
- `ob_dim_product_te`: Product-TE code relationships
- `ob_fact_exclusivity`: Exclusivity records
- `ob_fact_patent`: Patent records

### Purple Book Tables
- `pb_dim_application`: BLA application metadata
- `pb_dim_product`: Biological product information
- `pb_dim_biosimilar`: Biosimilar product information
- `pb_fact_exclusivity`: Exclusivity records

### RxNorm Tables
- `rxnorm_dim_ingredient`: Drug ingredients
- `rxnorm_dim_product`: Drug products (SCD/SBD)
- `rxnorm_dim_productpack`: NDC codes and packaging

## Command Line Options

### run_etl_pipeline.py

```bash
# Run full pipeline
python run_etl_pipeline.py

# Skip ingest, only load existing CSVs
python run_etl_pipeline.py --skip-ingest

# Skip load, only run ingest
python run_etl_pipeline.py --skip-load

# Run only specific data sources
python run_etl_pipeline.py --only orange-book purple-book

# Append instead of replace
python run_etl_pipeline.py --append
```

### load_to_postgres.py

```bash
# Replace tables (default)
python load_to_postgres.py --replace

# Append to existing tables
python load_to_postgres.py --append

# Use different schema
python load_to_postgres.py --schema staging
```

## Data Directory Structure

```
PharmaDB/
├── data/                      # Output directory for CSVs
│   ├── orange_book_raw/       # Raw Orange Book files
│   ├── purple_book_raw/       # Raw Purple Book files
│   ├── ob_*.csv               # Orange Book processed CSVs
│   ├── pb_*.csv               # Purple Book processed CSVs
│   └── dim_*.csv               # RxNorm processed CSVs
├── orange_book_ingest.py
├── purple_book_ingest.py
├── rxnorm_ingest.py
├── load_to_postgres.py
├── run_etl_pipeline.py
├── config.py
└── requirements.txt
```

## Testing

```bash
python -m pytest tests/
```

`tests/` covers the column-mapping and parsing logic in the ingest scripts
(`orange_book_ingest.py`, `purple_book_ingest.py`, `rxnorm_ingest.py`) and
`validate_data.py`'s own checks, with no network access required — RxNorm
API calls are mocked with fixtures mirroring real RxNav response shapes.
Most of these tests are regression tests for real bugs found in this
pipeline (a source column silently not mapping to its output column,
producing a 100%-empty or garbage field); `.github/workflows/ci.yml` runs
them, plus `validate_data.py` against the committed data snapshot, on every
push and pull request.

## Notes

- The `raw` schema is intended to be transient - data is loaded as-is without transformation
- Tables are replaced by default (use `--append` to add to existing data)
- All paths are relative to the script directory
- The pipeline handles missing files gracefully (warns but continues)

## Troubleshooting

### Database Connection Issues

1. Verify PostgreSQL is running on the Pi: `ping 192.168.0.85`
2. Check credentials in `config.py` or environment variables
3. Ensure PostgreSQL allows remote connections (check `pg_hba.conf`)

### Missing Data

- Check that ingest scripts completed successfully
- Verify CSV files exist in `data/` directory
- Review logs for warnings or errors

### Encoding Issues

- Orange Book files may use UTF-16 encoding (handled automatically)
- Purple Book files may be CSV or XLSX (handled automatically)

## License

This is a data extraction and loading tool. Ensure compliance with FDA and RxNorm data usage terms.


