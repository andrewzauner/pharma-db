#!/usr/bin/env python
"""
Main orchestration script for PharmaDB ETL pipeline.
Runs all ingest processes and loads data into PostgreSQL raw schema.

Execution order:
1. Orange Book ingest (downloads and processes FDA Orange Book data)
2. Purple Book ingest (downloads and processes FDA Purple Book data)
3. RxNorm ingest (queries RxNorm API for drug data)
4. Load all CSVs to PostgreSQL raw schema
"""
import os
import sys
import subprocess
import logging
import argparse
from datetime import datetime
from pathlib import Path

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


def run_script(script_path: str, description: str, *args) -> bool:
    """
    Run a Python script and return True if successful.
    """
    script_full = os.path.join(os.path.dirname(__file__), script_path)
    
    if not os.path.exists(script_full):
        logger.error(f"Script not found: {script_full}")
        return False
    
    logger.info(f"Starting: {description}")
    logger.info(f"Running: python {script_path} {' '.join(args)}")
    
    try:
        result = subprocess.run(
            [sys.executable, script_full] + list(args),
            cwd=os.path.dirname(__file__),
            capture_output=False,  # Show output in real-time
            check=True
        )
        logger.info(f"✓ Completed: {description}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"✗ Failed: {description} (exit code: {e.returncode})")
        return False
    except Exception as e:
        logger.error(f"✗ Error running {description}: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Run complete PharmaDB ETL pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run full pipeline (ingest + load)
  python run_etl_pipeline.py

  # Run only ingest steps (no database load)
  python run_etl_pipeline.py --skip-load

  # Run only database load (skip ingest)
  python run_etl_pipeline.py --skip-ingest

  # Run specific data sources only
  python run_etl_pipeline.py --only orange-book purple-book

  # Append to existing tables instead of replacing
  python run_etl_pipeline.py --append
        """
    )
    
    parser.add_argument(
        "--skip-ingest",
        action="store_true",
        help="Skip all ingest steps, only load existing CSVs to database"
    )
    parser.add_argument(
        "--skip-load",
        action="store_true",
        help="Skip database load, only run ingest steps"
    )
    parser.add_argument(
        "--only",
        nargs="+",
        choices=["orange-book", "purple-book", "rxnorm"],
        help="Run only specified data sources"
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Append to existing tables instead of replacing (default: replace)"
    )
    parser.add_argument(
        "--rxnorm-names",
        nargs="+",
        help="Custom drug names for RxNorm ingest (default: uses seed names from script)"
    )
    parser.add_argument(
        "--skip-validate",
        action="store_true",
        help="Skip data quality validation between ingest and load"
    )
    parser.add_argument(
        "--force-load",
        action="store_true",
        help="Load to Postgres even if data quality validation reported errors"
    )

    args = parser.parse_args()
    
    start_time = datetime.now()
    logger.info("=" * 70)
    logger.info("PharmaDB ETL Pipeline Started")
    logger.info("=" * 70)
    
    # Track results
    results = {
        "orange_book": False,
        "purple_book": False,
        "rxnorm": False,
        "validate": False,
        "load": False
    }
    
    # Determine which steps to run
    run_orange_book = not args.skip_ingest and (not args.only or "orange-book" in args.only)
    run_purple_book = not args.skip_ingest and (not args.only or "purple-book" in args.only)
    run_rxnorm = not args.skip_ingest and (not args.only or "rxnorm" in args.only)
    run_load = not args.skip_load
    
    # Step 1: Orange Book Ingest
    if run_orange_book:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 1: Orange Book Ingest")
        logger.info("=" * 70)
        results["orange_book"] = run_script(
            "orange_book_ingest.py",
            "Orange Book data ingest"
        )
        if not results["orange_book"]:
            logger.warning("Orange Book ingest failed, but continuing...")
    
    # Step 2: Purple Book Ingest
    if run_purple_book:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 2: Purple Book Ingest")
        logger.info("=" * 70)
        results["purple_book"] = run_script(
            "purple_book_ingest.py",
            "Purple Book data ingest"
        )
        if not results["purple_book"]:
            logger.warning("Purple Book ingest failed, but continuing...")
    
    # Step 3: RxNorm Ingest
    if run_rxnorm:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 3: RxNorm Ingest")
        logger.info("=" * 70)
        # Note: RxNorm script uses seed names by default
        # If custom names provided, we'd need to modify rxnorm_ingest.py to accept args
        results["rxnorm"] = run_script(
            "rxnorm_ingest.py",
            "RxNorm data ingest"
        )
        if not results["rxnorm"]:
            logger.warning("RxNorm ingest failed, but continuing...")
    
    # Step 4: Validate data quality
    validation_passed = True
    if not args.skip_validate:
        logger.info("\n" + "=" * 70)
        logger.info("STEP 4: Validate Data Quality")
        logger.info("=" * 70)
        validation_passed = run_script("validate_data.py", "Data quality validation")
        results["validate"] = validation_passed
        if not validation_passed:
            logger.error("Data quality validation found errors (see report above).")
    else:
        results["validate"] = True

    # Step 5: Load to PostgreSQL
    if run_load:
        if not validation_passed and not args.force_load:
            logger.error("Skipping PostgreSQL load because validation failed. Fix the data or re-run with --force-load.")
        else:
            logger.info("\n" + "=" * 70)
            logger.info("STEP 5: Load to PostgreSQL")
            logger.info("=" * 70)
            load_args = ["--append"] if args.append else []
            results["load"] = run_script(
                "load_to_postgres.py",
                "PostgreSQL data load",
                *load_args
            )
            if not results["load"]:
                logger.error("PostgreSQL load failed!")
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info("\n" + "=" * 70)
    logger.info("ETL Pipeline Summary")
    logger.info("=" * 70)
    logger.info(f"Duration: {duration:.1f} seconds")
    logger.info("")
    
    for step, success in results.items():
        status = "✓ SUCCESS" if success else "✗ FAILED/SKIPPED"
        logger.info(f"  {step.replace('_', ' ').title()}: {status}")
    
    # Exit code
    all_success = all(results.values())
    if not all_success:
        logger.warning("\nSome steps failed. Check logs above for details.")
        sys.exit(1)
    else:
        logger.info("\n✓ All steps completed successfully!")
        sys.exit(0)


if __name__ == "__main__":
    main()


