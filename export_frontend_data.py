#!/usr/bin/env python
"""
Builds a slim, pre-joined data bundle for the static frontend in frontend/data/.

This is a read-only export step over the already-validated CSVs in data/ --
it doesn't touch ingest or Postgres. It exists so the frontend never has to
join ApplNo/ProductNo/BLANumber across files in the browser: each exported
file already carries the applicant/product context it needs, and only the
columns the UI actually renders (dropping always-empty columns like
RxCUI_Ingredient, which nothing currently populates).

Run after the ETL ingest scripts (or after editing data/*.csv directly):
    python export_frontend_data.py
"""
import os
import pandas as pd

import config

DATA_DIR = config.DATA_DIR
OUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend", "data")


def _read(name, **kwargs):
    path = os.path.join(DATA_DIR, name)
    return pd.read_csv(path, dtype=str, **kwargs)


def build_orange_book():
    prod = _read("ob_dim_product.csv")
    app = _read("ob_dim_application.csv")

    merged = prod.merge(
        app[["ApplNo", "ApplicantName", "ApplType", "ApplStatus", "FirstApprovalDate"]],
        on="ApplNo", how="left"
    )
    products = merged[[
        "ApplNo", "ProductNo", "TradeName", "ActiveIngredient", "Strength",
        "DosageForm", "Route", "MarketingStatus", "TECode",
        "ReferenceListedDrugFlag", "ReferenceStandardFlag",
        "ApplicantName", "ApplType", "ApplStatus", "FirstApprovalDate",
    ]].copy()

    patents = _read("ob_fact_patent.csv")[[
        "ApplNo", "ProductNo", "PatentNo", "PatentExpiration",
        "PatentUseCode", "DrugSubstanceFlag", "DrugProductFlag", "DelistRequested",
    ]].copy()
    patents = patents[patents["PatentExpiration"].notna()]

    excl = _read("ob_fact_exclusivity.csv")[[
        "ApplNo", "ProductNo", "ExclusivityCode", "ExclusivityEndDate",
    ]].copy()
    excl = excl[excl["ExclusivityEndDate"].notna()]

    products.to_csv(os.path.join(OUT_DIR, "ob_products.csv"), index=False)
    patents.to_csv(os.path.join(OUT_DIR, "ob_patents.csv"), index=False)
    excl.to_csv(os.path.join(OUT_DIR, "ob_exclusivity.csv"), index=False)
    return len(products), len(patents), len(excl)


def build_purple_book():
    prod = _read("pb_dim_product.csv")[[
        "BLANumber", "ProperName", "ProprietaryName", "Applicant",
        "DosageForm", "Route", "Strength", "IsReferenceProductFlag", "BLAApprovalDate",
    ]].copy()
    bio = _read("pb_dim_biosimilar.csv")[[
        "BLANumber", "BiosimilarProprietaryName", "BiosimilarProperName",
        "ReferenceProductName", "InterchangeabilityFlag", "LicensureDate",
    ]].copy()
    bio = bio[bio["ReferenceProductName"].notna()]

    prod.to_csv(os.path.join(OUT_DIR, "pb_products.csv"), index=False)
    bio.to_csv(os.path.join(OUT_DIR, "pb_biosimilars.csv"), index=False)
    return len(prod), len(bio)


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    n_prod, n_pat, n_excl = build_orange_book()
    n_pb_prod, n_pb_bio = build_purple_book()

    print("=== Frontend Data Export ===")
    print(f"Orange Book products:    {n_prod:,} -> frontend/data/ob_products.csv")
    print(f"Orange Book patents:     {n_pat:,} -> frontend/data/ob_patents.csv")
    print(f"Orange Book exclusivity: {n_excl:,} -> frontend/data/ob_exclusivity.csv")
    print(f"Purple Book products:    {n_pb_prod:,} -> frontend/data/pb_products.csv")
    print(f"Purple Book biosimilars: {n_pb_bio:,} -> frontend/data/pb_biosimilars.csv")


if __name__ == "__main__":
    main()
