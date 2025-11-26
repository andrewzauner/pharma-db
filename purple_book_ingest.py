#!/usr/bin/env python
# purple_book_ingest.py — FDA Purple Book ingest with auto-download
# - Builds candidate URLs for recent months (CSV+XLSX) and downloads the first that works
# - Also works with local files if you place them under data/purple_book_raw
# - Robust header normalization + flexible sheet/column mapping
# - Outputs clean CSVs in data/ for easy loading into SQL Server

import os
import io
import re
import csv
import sys
import time
import math
import zipfile
import logging
import unicodedata
import shutil
import datetime as dt
from typing import Dict, List, Optional

import requests
import pandas as pd


import argparse
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter



# --------------------
# CONFIG
# --------------------
DATA_DIR = r"C:\Users\andre\Documents\code\PharmaDB\data"
RAW_DIR  = os.path.join(DATA_DIR, "purple_book_raw")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)

USER_AGENT = "purple-book-ingest/1.1 (+industry-db)"
TIMEOUT, RETRIES = 40, 3

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

# Robust retry/backoff on the session
retry = Retry(
    total=5,                # total retries per URL
    backoff_factor=1.5,     # exponential backoff: 1.5, 3.0, 4.5, ...
    status_forcelist=(429, 500, 502, 503, 504),
    allowed_methods=frozenset(["GET", "HEAD"]),
    raise_on_status=False,
)
adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.mount("http://", adapter)


# Expected final outputs → copied or generated into DATA_DIR
PB_OUTPUTS = {
    "pb_dim_product.csv":       "pb_dim_product.csv",
    "pb_dim_biosimilar.csv":    "pb_dim_biosimilar.csv",
    "pb_fact_exclusivity.csv":  "pb_fact_exclusivity.csv",
    "pb_dim_application.csv":   "pb_dim_application.csv",
}

# --------------------
# Dynamic Purple Book URL candidates
# --------------------
def _month_slug(d: dt.date) -> str:
    # e.g., "july", "august"
    return d.strftime("%B").lower()

def build_pb_candidate_urls(months_back: int = 8) -> List[str]:
    """
    Build a list of plausible FDA Purple Book download URLs for the current
    and previous N months, trying CSV then XLSX for each month.
    """
    base = "https://purplebooksearch.fda.gov/downloads/files/{year}/purplebook-search-{mon}-data-download.{ext}"
    today = dt.date.today()
    urls: List[str] = []
    for i in range(months_back):
        # rolling back i months
        year = (today.replace(day=1) - pd.DateOffset(months=i)).date().year
        month_dt = (today.replace(day=1) - pd.DateOffset(months=i)).date()
        mon = _month_slug(month_dt)
        # CSV first, then XLSX
        urls.append(base.format(year=year, mon=mon, ext="csv"))
        urls.append(base.format(year=year, mon=mon, ext="xlsx"))
    # A couple of generic fallbacks (if FDA changes naming)
    urls += [
        "https://purplebooksearch.fda.gov/downloads/files/purplebook-search-data-download.csv",
        "https://purplebooksearch.fda.gov/downloads/files/purplebook-search-data-download.xlsx",
    ]
    return urls

# --------------------
# Helpers (shared)
# --------------------
def _download(url: str, out_path: str, timeout: int = 15) -> bool:
    # honor corporate proxies if set (HTTP(S)_PROXY env)
    proxies = {
        "http": os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy"),
        "https": os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"),
    }
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True, stream=True, proxies=proxies)
            if r.status_code == 200:
                clen = int(r.headers.get("content-length", "0") or "0")
                if clen and clen < 512:
                    logging.warning(f"{url} -> tiny content-length={clen}, skipping (attempt {attempt}/{RETRIES})")
                else:
                    with open(out_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1 << 17):
                            if chunk:
                                f.write(chunk)
                    logging.info(f"Downloaded → {out_path}")
                    return True
            else:
                logging.warning(f"{url} -> HTTP {r.status_code} (attempt {attempt}/{RETRIES})")
        except requests.RequestException as e:
            logging.warning(f"Download error: {e} (attempt {attempt}/{RETRIES})")
        time.sleep(1.25 * attempt)
    return False


def _file_magic(path: str) -> bytes:
    if not os.path.exists(path):
        return b""
    with open(path, "rb") as f:
        return f.read(8)

def _is_zip(path: str) -> bool:
    return _file_magic(path).startswith(b"PK")

def _norm_col(s: str) -> str:
    if s is None:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    return re.sub(r"[^a-z0-9]+", "", s)

def _parse_date_like(s: Optional[str]) -> Optional[str]:
    if s is None or s == "":
        return None
    try:
        return pd.to_datetime(s, errors="coerce").date().isoformat()
    except Exception:
        return None

def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c not in df.columns:
            df[c] = pd.NA
    return df

def _best_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    if df is None or df.empty:
        return None
    norm = {_norm_col(c): c for c in df.columns}
    keys = list(norm.keys())
    for cand in candidates:
        token = _norm_col(cand)
        if token in norm:
            return norm[token]
        for k in keys:
            if token and token in k:
                return norm[k]
    return None

def _read_any_table(path: str) -> Dict[str, pd.DataFrame]:
    """
    Read CSV/XLSX (or ZIP of CSV/XLSX) into dict of {name: DataFrame}.
    - For CSV: returns {"data": df}
    - For XLSX: returns {sheet_name: df}
    """
    out: Dict[str, pd.DataFrame] = {}
    if _is_zip(path):
        with zipfile.ZipFile(path, "r") as zf:
            for name in zf.namelist():
                base = os.path.basename(name).lower()
                if base.endswith(".csv"):
                    df = pd.read_csv(zf.open(name), dtype=str, keep_default_na=False, na_values=[""])
                    out[os.path.splitext(base)[0]] = df
                elif base.endswith(".xlsx") or base.endswith(".xls"):
                    bio = io.BytesIO(zf.read(name))
                    xls = pd.ExcelFile(bio)
                    for sheet in xls.sheet_names:
                        df = xls.parse(sheet_name=sheet, dtype=str)
                        out[sheet] = df
    else:
        base = path.lower()
        if base.endswith(".csv"):
            df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
            out["data"] = df
        elif base.endswith(".xlsx") or base.endswith(".xls"):
            xls = pd.ExcelFile(path)
            for sheet in xls.sheet_names:
                df = xls.parse(sheet_name=sheet, dtype=str)
                out[sheet] = df
        else:
            logging.error(f"Unsupported file type: {path}")
    # Trim whitespace
    for k, df in list(out.items()):
        df.columns = [str(c).strip() for c in df.columns]
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = df[c].astype(str).str.strip()
        out[k] = df
    return out

def _find_local_raw_files() -> List[str]:
    files = []
    for fn in os.listdir(RAW_DIR):
        if fn.lower().endswith((".csv", ".xlsx", ".xls", ".zip")):
            files.append(os.path.join(RAW_DIR, fn))
    return files

def _pass_through_if_already_curated(raw_files: list) -> bool:
    """
    If user already placed final pb_* CSVs in RAW_DIR, copy to DATA_DIR
    and return True (handled). Otherwise False so normal ingest runs.
    """
    handled = False
    for p in raw_files:
        base = os.path.basename(p).lower()
        if base in PB_OUTPUTS:
            dest = os.path.join(DATA_DIR, PB_OUTPUTS[base])
            shutil.copy2(p, dest)
            try:
                rows = max(0, len(pd.read_csv(dest)))
            except Exception:
                rows = "?"
            logging.info(f"Pass-through: {base} -> {dest} ({rows} rows)")
            handled = True
    return handled

# --------------------
# Purple Book mappers
# --------------------
def _map_product(df: pd.DataFrame) -> pd.DataFrame:
    """
    DimPB_Product:
      BLANumber, ProperName, ProprietaryName, Applicant, DosageForm, Route,
      Strength, ReferenceProductName, IsReferenceProductFlag, BLAApprovalDate
    """
    keep = [
        "BLANumber","ProperName","ProprietaryName","Applicant",
        "DosageForm","Route","Strength",
        "ReferenceProductName","IsReferenceProductFlag","BLAApprovalDate"
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=keep)

    alias = {
        "blanumber": "BLANumber", "blano": "BLANumber", "licenseapplno": "BLANumber",
        "biologiclicenseapplication": "BLANumber",
        "propername": "ProperName", "nonproprietaryname": "ProperName",
        "proprietaryname": "ProprietaryName", "tradename": "ProprietaryName",
        "productname": "ProprietaryName",
        "applicant": "Applicant", "sponsor": "Applicant", "licenseholder": "Applicant",
        "dosageform": "DosageForm", "dosageformroute": "DosageForm",
        "route": "Route", "routeofadministration": "Route",
        "strength": "Strength", "strengthconcentration": "Strength",
        "referenceproduct": "ReferenceProductName", "referenceproductname": "ReferenceProductName",
        "referenceproductproprietaryname": "ReferenceProductName",
        "isreferenceproduct": "IsReferenceProductFlag", "referenceproductflag": "IsReferenceProductFlag",
        "approvaldate": "BLAApprovalDate", "blaapprovaldate": "BLAApprovalDate",
        "dateoflicensure": "BLAApprovalDate", "originallicensuredate": "BLAApprovalDate",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    prod = df.rename(columns=ren)

    if "DosageForm" in prod.columns and "Route" not in prod.columns:
        parts = prod["DosageForm"].str.split(";", n=1, expand=True)
        if isinstance(parts, pd.DataFrame) and parts.shape[1] == 2:
            prod["DosageForm"] = parts[0].str.strip()
            prod["Route"] = parts[1].str.strip()

    prod["IsReferenceProductFlag"] = prod.get("IsReferenceProductFlag").map(
        lambda v: "Y" if str(v).strip().upper() in ("Y","YES","TRUE","1") else (
                  "N" if str(v).strip().upper() in ("N","NO","FALSE","0") else pd.NA)
    )
    if "BLAApprovalDate" in prod.columns:
        prod["BLAApprovalDate"] = prod["BLAApprovalDate"].map(_parse_date_like)

    prod = _ensure_cols(prod, keep)
    return prod[keep].drop_duplicates()

def _map_biosimilar(df: pd.DataFrame) -> pd.DataFrame:
    """
    DimPB_Biosimilar:
      BLANumber, BiosimilarProprietaryName, BiosimilarProperName,
      ReferenceProductName, ReferenceBLANumber, InterchangeabilityFlag, LicensureDate
    """
    keep = [
        "BLANumber","BiosimilarProprietaryName","BiosimilarProperName",
        "ReferenceProductName","ReferenceBLANumber","InterchangeabilityFlag","LicensureDate"
    ]
    if df is None or df.empty:
        return pd.DataFrame(columns=keep)

    alias = {
        "blanumber": "BLANumber", "blano": "BLANumber", "biosimilarblanumber": "BLANumber",
        "biosimilarname": "BiosimilarProprietaryName", "proprietaryname": "BiosimilarProprietaryName",
        "tradename": "BiosimilarProprietaryName",
        "propername": "BiosimilarProperName", "nonproprietaryname": "BiosimilarProperName",
        "referenceproduct": "ReferenceProductName", "referenceproductname": "ReferenceProductName",
        "referenceblanumber": "ReferenceBLANumber", "referenceblano": "ReferenceBLANumber",
        "interchangeability": "InterchangeabilityFlag", "interchangeabilitystatus": "InterchangeabilityFlag",
        "licensuredate": "LicensureDate", "approvaldate": "LicensureDate", "dateoflicensure": "LicensureDate",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    bs = df.rename(columns=ren)

    bs["InterchangeabilityFlag"] = bs.get("InterchangeabilityFlag").map(
        lambda v: "Y" if str(v).strip().upper() in ("Y","YES","TRUE","1","INTERCHANGEABLE") else (
                  "N" if str(v).strip().upper() in ("N","NO","FALSE","0","NOTINTERCHANGEABLE") else pd.NA)
    )
    if "LicensureDate" in bs.columns:
        bs["LicensureDate"] = bs["LicensureDate"].map(_parse_date_like)

    bs = _ensure_cols(bs, keep)
    return bs[keep].drop_duplicates()

def _map_exclusivity(df: pd.DataFrame) -> pd.DataFrame:
    """
    FactPB_Exclusivity:
      BLANumber, ReferenceProductName, ExclusivityType, ExclusivityEndDate, Notes
    """
    keep = ["BLANumber","ReferenceProductName","ExclusivityType","ExclusivityEndDate","Notes"]
    if df is None or df.empty:
        return pd.DataFrame(columns=keep)

    alias = {
        "blanumber": "BLANumber", "blano": "BLANumber",
        "referenceproductname": "ReferenceProductName", "referenceproduct": "ReferenceProductName",
        "exclusivitytype": "ExclusivityType", "exclusivitycategory": "ExclusivityType", "exclusivitycode": "ExclusivityType",
        "exclusivityenddate": "ExclusivityEndDate", "enddate": "ExclusivityEndDate", "expirationdate": "ExclusivityEndDate",
        "notes": "Notes", "note": "Notes", "comment": "Notes",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    ex = df.rename(columns=ren)

    if "ExclusivityEndDate" in ex.columns:
        ex["ExclusivityEndDate"] = ex["ExclusivityEndDate"].map(_parse_date_like)

    ex = _ensure_cols(ex, keep)
    return ex[keep].drop_duplicates()

def _map_application(df: pd.DataFrame) -> pd.DataFrame:
    """
    DimPB_Application (optional):
      BLANumber, Applicant, Status, OriginalLicensureDate, LastUpdateDate
    """
    keep = ["BLANumber","Applicant","Status","OriginalLicensureDate","LastUpdateDate"]
    if df is None or df.empty:
        return pd.DataFrame(columns=keep)

    alias = {
        "blanumber": "BLANumber", "blano": "BLANumber",
        "licenseholder": "Applicant", "applicant": "Applicant", "sponsor": "Applicant",
        "status": "Status", "applicationstatus": "Status",
        "originallicensuredate": "OriginalLicensureDate", "approvaldate": "OriginalLicensureDate",
        "lastupdatedate": "LastUpdateDate", "updatedate": "LastUpdateDate", "filedate": "LastUpdateDate",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    app = df.rename(columns=ren)

    if "OriginalLicensureDate" in app.columns:
        app["OriginalLicensureDate"] = app["OriginalLicensureDate"].map(_parse_date_like)
    if "LastUpdateDate" in app.columns:
        app["LastUpdateDate"] = app["LastUpdateDate"].map(_parse_date_like)

    app = _ensure_cols(app, keep)
    return app[keep].drop_duplicates()

# --------------------
# Orchestration
# --------------------
def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Purple Book ingest")
    parser.add_argument("--force-download", action="store_true",
                        help="Ignore curated pb_* files and try to download.")
    parser.add_argument("--months-back", type=int, default=8,
                        help="How many past months of URLs to try (default 8).")
    parser.add_argument("--url", type=str, default=None,
                        help="Direct CSV/XLSX URL to download (overrides candidates).")
    parser.add_argument("--local", type=str, default=None,
                        help="Path to a local CSV/XLSX to parse directly.")
    args = parser.parse_args()


    # 0) If final pb_* CSVs already exist in RAW_DIR, pass-through and exit early
    raw_files = _find_local_raw_files()
    if raw_files and not args.force_download and _pass_through_if_already_curated(raw_files):
        out_prod = os.path.join(DATA_DIR, "pb_dim_product.csv")
        out_bio  = os.path.join(DATA_DIR, "pb_dim_biosimilar.csv")
        out_excl = os.path.join(DATA_DIR, "pb_fact_exclusivity.csv")
        out_app  = os.path.join(DATA_DIR, "pb_dim_application.csv")

        def _safe_len(path):
            try:
                return max(0, len(pd.read_csv(path)))
            except Exception:
                return 0

        print("\n=== Purple Book Ingest Summary (pass-through) ===")
        print(f"DimPB_Product rows:       {_safe_len(out_prod):,} -> {out_prod}")
        print(f"DimPB_Biosimilar rows:    {_safe_len(out_bio):,} -> {out_bio}")
        print(f"FactPB_Exclusivity rows:  {_safe_len(out_excl):,} -> {out_excl}")
        print(f"DimPB_Application rows:   {_safe_len(out_app):,} -> {out_app}")
        return

    # 1) Auto-download the latest available PB CSV/XLSX into RAW_DIR
    downloaded_path = None

    if args.local:
        # Use a local file directly
        if os.path.exists(args.local):
            dest = os.path.join(RAW_DIR, os.path.basename(args.local))
            shutil.copy2(args.local, dest)
            logging.info(f"Using local file → {dest}")
            downloaded_path = dest
        else:
            logging.error(f"--local file not found: {args.local}")

    elif args.url:
        # Use a direct URL
        dest = os.path.join(RAW_DIR, os.path.basename(args.url) or "purplebook_download")
        if _download(args.url, dest):
            downloaded_path = dest
        else:
            logging.error(f"Failed to download from --url: {args.url}")

    else:
        # Build candidate URLs for recent months (CSV then XLSX)
        urls = build_pb_candidate_urls(months_back=args.months_back)
        for url in urls:
            ext = ".xlsx" if url.endswith(".xlsx") else ".csv"
            safe = os.path.basename(url).replace("/", "_")
            out_path = os.path.join(RAW_DIR, f"purplebook_{safe}{ext}")
            if _download(url, out_path):
                downloaded_path = out_path
                break

    # Refresh raw_files list to include any newly downloaded file
    raw_files = _find_local_raw_files()
    if not raw_files:
        print("\n=== Purple Book Ingest Summary ===")
        print(f"DimPB_Product rows:       0 -> {os.path.join(DATA_DIR,'pb_dim_product.csv')}")
        print(f"DimPB_Biosimilar rows:    0 -> {os.path.join(DATA_DIR,'pb_dim_biosimilar.csv')}")
        print(f"FactPB_Exclusivity rows:  0 -> {os.path.join(DATA_DIR,'pb_fact_exclusivity.csv')}")
        print(f"DimPB_Application rows:   0 -> {os.path.join(DATA_DIR,'pb_dim_application.csv')}")
        print("\nNo files to parse. You can:")
        print("  • Run with --url <direct-download-URL>")
        print("  • Run with --local <path-to-CSV-or-XLSX>")
        print("  • Or place a file in:", RAW_DIR)
        return

    # 2) Read all tables/sheets from detected files
    tables: Dict[str, pd.DataFrame] = {}
    for path in raw_files:
        try:
            dfs = _read_any_table(path)
            for k, df in dfs.items():
                key = f"{os.path.splitext(os.path.basename(path))[0]}::{k}"
                tables[key] = df
        except Exception as e:
            logging.warning(f"Failed to read {path}: {e}")

    # 3) Heuristics to pick which sheet/table is which
    def has_any(df, names: List[str]) -> bool:
        cols = [_norm_col(c) for c in df.columns]
        want = set(_norm_col(n) for n in names)
        return any(w in cols for w in want)

    product_df = None
    biosim_df  = None
    excl_df    = None
    app_df     = None

    for name, df in tables.items():
        ncols = [_norm_col(c) for c in df.columns]

        if ("blanumber" in ncols or "blano" in ncols) and any(k in ncols for k in ("exclusivitytype","exclusivitydate","exclusivityenddate","expirationdate")):
            excl_df = df if excl_df is None else pd.concat([excl_df, df], ignore_index=True)
            continue

        if any(k in ncols for k in ("biosimilarname","interchangeability","interchangeabilitystatus")) or \
           ("referenceblanumber" in ncols or "referenceblano" in ncols):
            biosim_df = df if biosim_df is None else pd.concat([biosim_df, df], ignore_index=True)
            continue

        if any(k in ncols for k in ("propername","nonproprietaryname","proprietaryname","tradename")) and \
           any(k in ncols for k in ("applicant","sponsor","licenseholder")):
            product_df = df if product_df is None else pd.concat([product_df, df], ignore_index=True)
            if has_any(df, ["status","application status","last update date","original licensure date"]):
                app_df = df if app_df is None else pd.concat([app_df, df], ignore_index=True)
            continue

        if has_any(df, ["BLA Number","Status","Applicant"]) or has_any(df, ["Original Licensure Date","Last Update Date"]):
            app_df = df if app_df is None else pd.concat([app_df, df], ignore_index=True)

    # 4) Map to canonical outputs
    dim_product = _map_product(product_df)
    dim_biosim  = _map_biosimilar(biosim_df)
    fact_excl   = _map_exclusivity(excl_df)
    dim_app     = _map_application(app_df)

    # 5) Write CSV outputs
    out_prod = os.path.join(DATA_DIR, "pb_dim_product.csv")
    out_bio  = os.path.join(DATA_DIR, "pb_dim_biosimilar.csv")
    out_excl = os.path.join(DATA_DIR, "pb_fact_exclusivity.csv")
    out_app  = os.path.join(DATA_DIR, "pb_dim_application.csv")

    dim_product.to_csv(out_prod, index=False)
    dim_biosim.to_csv(out_bio, index=False)
    fact_excl.to_csv(out_excl, index=False)
    dim_app.to_csv(out_app, index=False)

    # 6) Summary
    print("\n=== Purple Book Ingest Summary ===")
    print(f"DimPB_Product rows:       {len(dim_product):,} -> {out_prod}")
    print(f"DimPB_Biosimilar rows:    {len(dim_biosim):,} -> {out_bio}")
    print(f"FactPB_Exclusivity rows:  {len(fact_excl):,} -> {out_excl}")
    print(f"DimPB_Application rows:   {len(dim_app):,} -> {out_app}")

    if not len(dim_product):
        print("\nTip: If counts are zero, open one of the downloaded files in "
              f"{RAW_DIR} to verify the format, or drop a known-good PB CSV/XLSX there and rerun.")

if __name__ == "__main__":
    main()
