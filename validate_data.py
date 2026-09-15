#!/usr/bin/env python
"""
Data quality validation for PharmaDB ETL outputs.

Run after ingest (and before/after loading to Postgres) to catch the class of
bugs that can silently corrupt this pipeline's output:

  - a raw source file that was actually saved as an HTML error page, an
    image, or a PDF instead of real data (a broken download)
  - two distinct source columns getting aliased onto the same output column
    name, which pandas silently turns into duplicate columns instead of an
    error
  - an output table that came out empty
  - fact rows whose foreign key doesn't exist in the corresponding dimension
    table (a sign the two were built from mismatched or truncated inputs)

This does not raise exceptions on findings; it prints a report and exits
non-zero only for errors (empty tables, duplicate columns, missing files,
broken raw downloads). Referential integrity gaps are reported as warnings
since some amount of orphaned FK is expected in FDA data (e.g. historical
patent rows referencing since-withdrawn applications).
"""
import os
import sys
import argparse

import pandas as pd

import config

DATA_DIR = config.DATA_DIR


class Report:
    def __init__(self):
        self.errors = []
        self.warnings = []

    def error(self, msg: str):
        self.errors.append(msg)

    def warn(self, msg: str):
        self.warnings.append(msg)

    def print(self):
        print("\n=== Data Quality Report ===")
        if not self.errors and not self.warnings:
            print("No issues found.")
            return
        if self.errors:
            print(f"\n{len(self.errors)} ERROR(S):")
            for e in self.errors:
                print(f"  [ERROR] {e}")
        if self.warnings:
            print(f"\n{len(self.warnings)} WARNING(S):")
            for w in self.warnings:
                print(f"  [WARN]  {w}")


def _raw_header_dupes(path):
    """
    pandas.read_csv silently renames duplicate header names on read (e.g. two
    "Status" columns become "Status" and "Status.1"), which would otherwise
    hide a rename collision from a plain df.columns.duplicated() check after
    loading. Check the literal header line first.
    """
    import csv
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        header = next(csv.reader(f), [])
    seen = {}
    dupes = []
    for c in header:
        seen[c] = seen.get(c, 0) + 1
        if seen[c] == 2:
            dupes.append(c)
    return dupes


def _load_csv(path):
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, dtype=str)
    except Exception as e:
        return e


def check_table(report: Report, name: str, filename: str, required_cols=None, key_cols=None) -> "pd.DataFrame | None":
    path = os.path.join(DATA_DIR, filename)
    df = _load_csv(path)

    if df is None:
        report.error(f"{name}: file missing ({path})")
        return None
    if isinstance(df, Exception):
        report.error(f"{name}: failed to read {path} ({df})")
        return None

    header_dupes = _raw_header_dupes(path)
    if header_dupes:
        report.error(
            f"{name}: duplicate column(s) {header_dupes} in {filename} — a rename "
            "likely collapsed two distinct source columns onto one name "
            "(pandas silently suffixes these .1, .2, ... on read, hiding the collision)"
        )
    elif df.columns.duplicated().any():
        dupes = sorted(set(df.columns[df.columns.duplicated()].tolist()))
        report.error(
            f"{name}: duplicate column(s) {dupes} in {filename} — a rename "
            "likely collapsed two distinct source columns onto one name"
        )

    if required_cols:
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            report.error(f"{name}: missing expected column(s) {missing} in {filename}")

    if df.empty:
        report.error(f"{name}: table is empty (0 data rows) in {filename}")
        return df

    if key_cols and all(c in df.columns for c in key_cols):
        n_dupes = int(df.duplicated(subset=key_cols).sum())
        if n_dupes:
            report.warn(f"{name}: {n_dupes} rows share a duplicate key {key_cols}")
        n_null_key = int(df[key_cols].isna().any(axis=1).sum())
        if n_null_key:
            report.warn(f"{name}: {n_null_key} rows have a null value in key {key_cols}")

    return df


def check_foreign_key(report: Report, fact_name, fact_df, fact_cols, dim_name, dim_df, dim_cols):
    if fact_df is None or dim_df is None or fact_df.empty or dim_df.empty:
        return
    if not all(c in fact_df.columns for c in fact_cols) or not all(c in dim_df.columns for c in dim_cols):
        return

    fact_keys = fact_df[fact_cols].dropna().drop_duplicates()
    dim_keys = set(map(tuple, dim_df[dim_cols].dropna().values.tolist()))
    if fact_keys.empty:
        return

    orphans = [tuple(r) for r in fact_keys.values.tolist() if tuple(r) not in dim_keys]
    if orphans:
        pct = 100.0 * len(orphans) / len(fact_keys)
        sample = orphans[:3]
        report.warn(
            f"{fact_name}{fact_cols} -> {dim_name}{dim_cols}: "
            f"{len(orphans)}/{len(fact_keys)} ({pct:.1f}%) values have no matching dimension row, e.g. {sample}"
        )


# Magic-byte / content signatures for file types that should never end up
# inside a .txt or .csv raw download from FDA or RxNorm.
_BAD_SIGNATURES = (
    (b"\xff\xd8\xff", "a JPEG image"),
    (b"\x89PNG\r\n\x1a\n", "a PNG image"),
    (b"GIF8", "a GIF image"),
    (b"%PDF", "a PDF"),
    (b"PK\x03\x04", "a ZIP archive"),
)


def scan_raw_files(report: Report, raw_dirs):
    for raw_dir in raw_dirs:
        if not os.path.isdir(raw_dir):
            continue
        for fn in sorted(os.listdir(raw_dir)):
            if not fn.lower().endswith((".txt", ".csv")):
                continue
            path = os.path.join(raw_dir, fn)
            try:
                with open(path, "rb") as f:
                    head = f.read(256)
            except OSError:
                continue

            for sig, label in _BAD_SIGNATURES:
                if head.startswith(sig):
                    report.error(f"{path}: content is {label}, not text/CSV data — the source download failed")
                    break
            else:
                stripped = head.lstrip().lower()
                if stripped.startswith(b"<!doctype html") or stripped.startswith(b"<html"):
                    report.error(f"{path}: content is an HTML page, not text/CSV data — the source download failed (likely an error or redirect page)")


def main():
    parser = argparse.ArgumentParser(description="Validate PharmaDB ETL output CSVs before/after loading to Postgres")
    parser.add_argument("--skip-raw-scan", action="store_true", help="Skip scanning raw/*_raw input files for wrong-file-type downloads")
    args = parser.parse_args()

    report = Report()

    if not args.skip_raw_scan:
        scan_raw_files(report, [
            os.path.join(DATA_DIR, "orange_book_raw"),
            os.path.join(DATA_DIR, "purple_book_raw"),
        ])

    # ---- Orange Book ----
    ob_app = check_table(report, "ob_dim_application", "ob_dim_application.csv",
                          required_cols=["ApplNo", "ApplType", "ApplicantName", "ApplStatus"],
                          key_cols=["ApplNo"])
    ob_prod = check_table(report, "ob_dim_product", "ob_dim_product.csv",
                           required_cols=["ApplNo", "ProductNo", "TradeName"],
                           key_cols=["ApplNo", "ProductNo"])
    check_table(report, "ob_dim_tecode", "ob_dim_tecode.csv", required_cols=["TECode"])
    ob_pte = check_table(report, "ob_dim_product_te", "ob_dim_product_te.csv",
                          required_cols=["ApplNo", "ProductNo", "TECode"])
    ob_excl = check_table(report, "ob_fact_exclusivity", "ob_fact_exclusivity.csv",
                           required_cols=["ApplNo", "ProductNo", "ExclusivityCode"])
    ob_pat = check_table(report, "ob_fact_patent", "ob_fact_patent.csv",
                          required_cols=["ApplNo", "ProductNo", "PatentNo"])

    if ob_prod is not None and not ob_prod.empty:
        check_foreign_key(report, "ob_dim_product", ob_prod, ["ApplNo"], "ob_dim_application", ob_app, ["ApplNo"])
        check_foreign_key(report, "ob_fact_patent", ob_pat, ["ApplNo", "ProductNo"], "ob_dim_product", ob_prod, ["ApplNo", "ProductNo"])
        check_foreign_key(report, "ob_fact_exclusivity", ob_excl, ["ApplNo", "ProductNo"], "ob_dim_product", ob_prod, ["ApplNo", "ProductNo"])
        check_foreign_key(report, "ob_dim_product_te", ob_pte, ["ApplNo", "ProductNo"], "ob_dim_product", ob_prod, ["ApplNo", "ProductNo"])

    # ---- Purple Book ----
    pb_prod = check_table(report, "pb_dim_product", "pb_dim_product.csv",
                           required_cols=["BLANumber", "ProperName", "ProprietaryName"])
    pb_bio = check_table(report, "pb_dim_biosimilar", "pb_dim_biosimilar.csv",
                          required_cols=["BLANumber", "ReferenceProductName"])
    pb_excl = check_table(report, "pb_fact_exclusivity", "pb_fact_exclusivity.csv",
                           required_cols=["BLANumber"])
    pb_app = check_table(report, "pb_dim_application", "pb_dim_application.csv",
                          required_cols=["BLANumber", "Applicant", "Status"],
                          key_cols=["BLANumber"])

    if pb_prod is not None and not pb_prod.empty:
        check_foreign_key(report, "pb_dim_biosimilar", pb_bio, ["BLANumber"], "pb_dim_product", pb_prod, ["BLANumber"])
        check_foreign_key(report, "pb_fact_exclusivity", pb_excl, ["BLANumber"], "pb_dim_product", pb_prod, ["BLANumber"])
        check_foreign_key(report, "pb_dim_application", pb_app, ["BLANumber"], "pb_dim_product", pb_prod, ["BLANumber"])

    # ---- RxNorm ----
    rx_ing = check_table(report, "rxnorm_dim_ingredient", "dim_drug_ingredient.csv",
                          required_cols=["IngredientRxCUI"], key_cols=["IngredientRxCUI"])
    rx_prod = check_table(report, "rxnorm_dim_product", "dim_drug_product.csv",
                           required_cols=["ProductRxCUI", "IngredientRxCUI"], key_cols=["ProductRxCUI"])
    rx_pack = check_table(report, "rxnorm_dim_productpack", "dim_productpack_ndc.csv",
                           required_cols=["ProductRxCUI", "NDC11"])

    if rx_prod is not None and not rx_prod.empty:
        check_foreign_key(report, "rxnorm_dim_product", rx_prod, ["IngredientRxCUI"], "rxnorm_dim_ingredient", rx_ing, ["IngredientRxCUI"])
        check_foreign_key(report, "rxnorm_dim_productpack", rx_pack, ["ProductRxCUI"], "rxnorm_dim_product", rx_prod, ["ProductRxCUI"])

    report.print()
    sys.exit(1 if report.errors else 0)


if __name__ == "__main__":
    main()
