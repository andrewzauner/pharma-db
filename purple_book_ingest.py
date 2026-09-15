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
import zipfile
import logging
import unicodedata
import shutil
import datetime as dt
from typing import Dict, List, Optional

import requests
import pandas as pd
from dateutil.relativedelta import relativedelta

import argparse
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter



# --------------------
# CONFIG
# --------------------
# Get the directory of this script, then go up one level to find data/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
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
    
    Also includes alternative URL patterns that the FDA might use.
    """
    urls: List[str] = []
    today = dt.date.today()
    
    # Pattern 1: /downloads/files/{year}/purplebook-search-{month}-data-download.{ext}
    base1 = "https://purplebooksearch.fda.gov/downloads/files/{year}/purplebook-search-{mon}-data-download.{ext}"
    
    # Pattern 2: Alternative with different month format
    base2 = "https://purplebooksearch.fda.gov/downloads/files/{year}/{mon}/purplebook-search-data-download.{ext}"
    
    # Pattern 3: Simple path without year/month
    base3 = "https://purplebooksearch.fda.gov/downloads/files/purplebook-search-data-download.{ext}"
    
    for i in range(months_back):
        # rolling back i months
        month_dt = (dt.date(today.year, today.month, 1) - relativedelta(months=i))
        year = month_dt.year
        mon = _month_slug(month_dt)
        mon_num = month_dt.strftime("%m")  # e.g., "01", "02"
        
        # Pattern 1: year/month-name format
        urls.append(base1.format(year=year, mon=mon, ext="csv"))
        urls.append(base1.format(year=year, mon=mon, ext="xlsx"))
        
        # Pattern 2: year/month-number format
        urls.append(base2.format(year=year, mon=mon_num, ext="csv"))
        urls.append(base2.format(year=year, mon=mon_num, ext="xlsx"))
    
    # Generic fallbacks (no date)
    urls.append(base3.format(ext="csv"))
    urls.append(base3.format(ext="xlsx"))
    
    # Additional fallback patterns
    urls += [
        "https://purplebooksearch.fda.gov/downloads/purplebook-search-data-download.csv",
        "https://purplebooksearch.fda.gov/downloads/purplebook-search-data-download.xlsx",
        "https://www.fda.gov/files/drugs/published/Purple-Book-Data-Download.csv",
        "https://www.fda.gov/files/drugs/published/Purple-Book-Data-Download.xlsx",
    ]
    
    return urls

# --------------------
# Helpers (shared)
# --------------------
def _download(url: str, out_path: str, timeout: int = 60) -> bool:
    """
    Download a file from URL with retries and verification.
    Returns True if download succeeded and file is valid.
    """
    # honor corporate proxies if set (HTTP(S)_PROXY env)
    proxies = {
        "http": os.environ.get("HTTP_PROXY") or os.environ.get("http_proxy"),
        "https": os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"),
    }
    
    logging.info(f"Attempting to download: {url}")
    
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.get(url, timeout=timeout, allow_redirects=True, stream=True, proxies=proxies)
            
            if r.status_code == 200:
                clen = int(r.headers.get("content-length", "0") or "0")
                
                # Check content-length if provided
                if clen and clen < 512:
                    logging.warning(f"{url} -> tiny content-length={clen}, skipping (attempt {attempt}/{RETRIES})")
                    continue
                
                # Download the file
                bytes_written = 0
                with open(out_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=1 << 17):  # 128KB chunks
                        if chunk:
                            f.write(chunk)
                            bytes_written += len(chunk)
                
                # Verify file was written and has content
                if not os.path.exists(out_path):
                    logging.warning(f"Download failed: file not created at {out_path} (attempt {attempt}/{RETRIES})")
                    continue
                
                file_size = os.path.getsize(out_path)
                if file_size < 512:  # Less than 512 bytes is likely an error page
                    logging.warning(f"Downloaded file too small ({file_size} bytes), likely an error page (attempt {attempt}/{RETRIES})")
                    try:
                        os.remove(out_path)  # Clean up invalid file
                    except:
                        pass
                    continue
                
                # Verify it's not HTML (error page)
                with open(out_path, "rb") as f:
                    first_bytes = f.read(500)
                    if b"<html" in first_bytes.lower() or b"<!doctype" in first_bytes.lower() or b"<script" in first_bytes.lower():
                        logging.warning(f"Downloaded file appears to be HTML (error page), not data (attempt {attempt}/{RETRIES})")
                        try:
                            os.remove(out_path)
                        except:
                            pass
                        continue
                
                # For CSV files, verify it's not just HTML with .csv extension
                if out_path.endswith(".csv"):
                    with open(out_path, "rb") as f:
                        first_line = f.readline(200).decode('utf-8', errors='ignore').lower()
                        if first_line.strip().startswith('<!') or 'html' in first_line[:50]:
                            logging.warning(f"CSV file appears to be HTML, rejecting (attempt {attempt}/{RETRIES})")
                            try:
                                os.remove(out_path)
                            except:
                                pass
                            continue
                
                logging.info(f"✓ Successfully downloaded {file_size:,} bytes → {out_path}")
                if clen and abs(file_size - clen) > 1024:  # More than 1KB difference
                    logging.warning(f"File size mismatch: expected {clen:,} bytes, got {file_size:,} bytes")
                
                return True
            elif r.status_code == 404:
                return False  # Don't retry 404s
            else:
                logging.warning(f"{url} -> HTTP {r.status_code} (attempt {attempt}/{RETRIES})")
                
        except requests.Timeout:
            logging.warning(f"Timeout downloading {url} (attempt {attempt}/{RETRIES})")
        except requests.RequestException as e:
            logging.warning(f"Download error: {e} (attempt {attempt}/{RETRIES})")
        except Exception as e:
            logging.error(f"Unexpected error downloading {url}: {e} (attempt {attempt}/{RETRIES})", exc_info=True)
        
        # Clean up partial file on retry
        if os.path.exists(out_path) and attempt < RETRIES:
            try:
                os.remove(out_path)
            except:
                pass
        
        if attempt < RETRIES:
            time.sleep(1.25 * attempt)
    
    logging.error(f"Failed to download after {RETRIES} attempts: {url}")
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

def _read_purple_book_csv(path: str) -> Optional[pd.DataFrame]:
    """
    Specialized reader for Purple Book CSV files that have metadata rows.
    Finds the actual header row and data, skipping metadata.
    """
    try:
        # Parse with csv.reader directly on the file (not readlines()), so that
        # legitimately-quoted fields containing an embedded newline (Purple Book
        # occasionally wraps multi-line Strength values, e.g. "80MG/4ML\n(20MG/ML)")
        # are reassembled into one row by the csv module instead of being split
        # across two lines and turned into two misaligned/garbage rows.
        with open(path, 'r', encoding='utf-8-sig', errors='replace', newline='') as f:
            all_rows = list(csv.reader(f))

        # Look for the header row - it should contain "N/R/U" and "BLA Number"
        header_idx = None
        for i, row in enumerate(all_rows):
            row_lower = ",".join(row).lower()
            # Check if this looks like the header row
            if 'n/r/u' in row_lower and 'bla number' in row_lower and 'applicant' in row_lower:
                header_idx = i
                break

        if header_idx is None:
            logging.warning(f"Could not find header row in {path}, trying standard CSV read")
            # Fallback to standard read
            return pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""], skipinitialspace=True)

        header = [h.strip() for h in all_rows[header_idx]]

        # Read data rows (skip header and any empty rows)
        data_rows = []
        for row in all_rows[header_idx + 1:]:
            if not row or all(not c.strip() for c in row):  # Skip empty lines
                continue

            # Check if this is another header row (sometimes headers repeat)
            row_lower = ",".join(row).lower()
            if row[0].strip().lower() == 'n/r/u' and 'bla number' in row_lower:
                continue  # Skip duplicate headers

            row = [c.strip() for c in row]
            # Only add if it has the expected number of columns (or close)
            if len(row) >= len(header) * 0.5:  # At least half the columns
                # Pad or truncate to match header length
                if len(row) < len(header):
                    row.extend([''] * (len(header) - len(row)))
                elif len(row) > len(header):
                    row = row[:len(header)]
                data_rows.append(row)
        
        if not data_rows:
            logging.warning(f"No data rows found after header in {path}")
            return pd.DataFrame(columns=header)
        
        # Create DataFrame
        df = pd.DataFrame(data_rows, columns=header)
        
        # Clean up
        df = df.replace('', pd.NA)
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].astype(str).str.strip()
                df[col] = df[col].replace('nan', pd.NA).replace('', pd.NA)
        
        logging.info(f"Parsed Purple Book CSV: {len(df)} rows, {len(df.columns)} columns")
        return df
        
    except Exception as e:
        logging.error(f"Error reading Purple Book CSV {path}: {e}", exc_info=True)
        return None


def _read_any_table(path: str) -> Dict[str, pd.DataFrame]:
    """
    Read CSV/XLSX (or ZIP of CSV/XLSX) into dict of {name: DataFrame}.
    - For CSV: returns {"data": df}
    - For XLSX: returns {sheet_name: df}
    - For Purple Book CSV: uses specialized parser
    """
    out: Dict[str, pd.DataFrame] = {}
    if _is_zip(path):
        with zipfile.ZipFile(path, "r") as zf:
            for name in zf.namelist():
                base = os.path.basename(name).lower()
                if base.endswith(".csv"):
                    # For ZIP files, extract and use file path approach
                    # Extract to temp location first
                    import tempfile
                    with tempfile.NamedTemporaryFile(mode='wb', delete=False, suffix='.csv') as tmp:
                        tmp.write(zf.read(name))
                        tmp_path = tmp.name
                    try:
                        df = _read_purple_book_csv(tmp_path)
                        if df is not None:
                            out[os.path.splitext(base)[0]] = df
                        else:
                            # Fallback to standard read
                            df = pd.read_csv(tmp_path, dtype=str, keep_default_na=False, na_values=[""])
                            out[os.path.splitext(base)[0]] = df
                    finally:
                        try:
                            os.unlink(tmp_path)
                        except:
                            pass
                elif base.endswith(".xlsx") or base.endswith(".xls"):
                    bio = io.BytesIO(zf.read(name))
                    xls = pd.ExcelFile(bio)
                    for sheet in xls.sheet_names:
                        df = xls.parse(sheet_name=sheet, dtype=str)
                        out[sheet] = df
    else:
        base = path.lower()
        if base.endswith(".csv"):
            # Check if file name suggests it's a Purple Book file
            filename_lower = os.path.basename(path).lower()
            is_purple_book = 'purple' in filename_lower or 'purplebook' in filename_lower
            
            if is_purple_book:
                # Use specialized Purple Book parser
                df = _read_purple_book_csv(path)
                if df is not None:
                    out["data"] = df
                else:
                    # Fallback
                    df = pd.read_csv(path, dtype=str, keep_default_na=False, na_values=[""])
                    out["data"] = df
            else:
                # Standard CSV read
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
        if df is not None and not df.empty:
            df.columns = [str(c).strip() for c in df.columns]
            for c in df.columns:
                if df[c].dtype == "object":
                    df[c] = df[c].astype(str).str.strip()
            out[k] = df
    return out

def _find_local_raw_files() -> List[str]:
    """Find all CSV/XLSX/XLS/ZIP files in the raw directory."""
    files = []
    if not os.path.exists(RAW_DIR):
        logging.warning(f"Raw directory does not exist: {RAW_DIR}")
        return files
    
    try:
        for fn in os.listdir(RAW_DIR):
            if fn.lower().endswith((".csv", ".xlsx", ".xls", ".zip")):
                full_path = os.path.join(RAW_DIR, fn)
                if os.path.isfile(full_path):
                    files.append(full_path)
    except Exception as e:
        logging.error(f"Error listing files in {RAW_DIR}: {e}")
    
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
        "blanumber": "BLANumber", "bla number": "BLANumber",
        "propername": "ProperName", "proper name": "ProperName",
        "proprietaryname": "ProprietaryName", "proprietary name": "ProprietaryName",
        "applicant": "Applicant",
        "dosageform": "DosageForm", "dosage form": "DosageForm",
        "route": "Route", "routeofadministration": "Route", "route of administration": "Route",
        "strength": "Strength",
        "refproductproprietaryname": "ReferenceProductName", "ref. product proprietary name": "ReferenceProductName",
        "referenceproductproprietaryname": "ReferenceProductName",
        "approvaldate": "BLAApprovalDate", "approval date": "BLAApprovalDate",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    prod = df.rename(columns=ren)

    # Handle Route - may be in separate column or combined with DosageForm
    route_col = None
    for col in df.columns:
        if _norm_col(col) == "routeofadministration" or _norm_col(col) == "route of administration":
            route_col = col
            break
    if "Route" not in prod.columns and route_col:
        prod["Route"] = df[route_col]
    elif "DosageForm" in prod.columns and "Route" not in prod.columns:
        # Try to split if combined
        parts = prod["DosageForm"].str.split(";", n=1, expand=True)
        if isinstance(parts, pd.DataFrame) and parts.shape[1] == 2:
            prod["DosageForm"] = parts[0].str.strip()
            prod["Route"] = parts[1].str.strip()
    
    # Set IsReferenceProductFlag - Y if ReferenceProductName is empty/N/A, N if it has a value
    if "IsReferenceProductFlag" not in prod.columns:
        if "ReferenceProductName" in prod.columns:
            prod["IsReferenceProductFlag"] = prod["ReferenceProductName"].apply(
                lambda x: "N" if pd.notna(x) and str(x).strip() != "" and str(x).strip().upper() != "N/A" else "Y"
            )
        else:
            prod["IsReferenceProductFlag"] = "Y"  # Default to reference product if no ref product name
    
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

    # Map columns - Purple Book CSV uses different names
    alias = {
        "blanumber": "BLANumber", "bla number": "BLANumber",
        "proprietaryname": "BiosimilarProprietaryName", "proprietary name": "BiosimilarProprietaryName",
        "propername": "BiosimilarProperName", "proper name": "BiosimilarProperName",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    bs = df.rename(columns=ren)

    # Reference product name: the raw file carries the reference product's
    # proprietary (brand) name and proper (generic) name as two separate
    # columns. Both used to be aliased onto the same "ReferenceProductName"
    # target, which made df.rename() collapse them into two duplicate columns
    # in the output CSV. Coalesce them into a single column instead, preferring
    # the brand name and falling back to the generic name.
    def _clean_ref(col: Optional[str]) -> pd.Series:
        if col is None:
            return pd.Series([pd.NA] * len(df), index=df.index)
        return df[col].apply(lambda x: x if pd.notna(x) and str(x).strip().upper() not in ("", "N/A") else pd.NA)

    ref_proprietary_col = _best_col(df, "Ref. Product Proprietary Name", "Ref Product Proprietary Name")
    ref_proper_col = _best_col(df, "Ref. Product Proper Name", "Ref Product Proper Name")
    bs["ReferenceProductName"] = _clean_ref(ref_proprietary_col).combine_first(_clean_ref(ref_proper_col))

    # Licensure date: similarly, "Approval Date" and "Date of First Licensure"
    # were both aliased onto "LicensureDate" and could collide the same way.
    licensure_col = _best_col(df, "Date of First Licensure")
    approval_col = _best_col(df, "Approval Date")
    date_cols = [c for c in (licensure_col, approval_col) if c is not None]
    if date_cols:
        combined_date = df[date_cols[0]]
        for c in date_cols[1:]:
            combined_date = combined_date.combine_first(df[c])
        bs["LicensureDate"] = combined_date

    # Set InterchangeabilityFlag - check if "First Interchangeable Exclusivity" date exists
    if "InterchangeabilityFlag" not in bs.columns:
        bs["InterchangeabilityFlag"] = pd.NA
        # Check for interchangeability exclusivity date column
        for col in df.columns:
            cn = _norm_col(col)
            if "firstinterchangeable" in cn and "exclusivity" in cn and "date" in cn:
                # If this date column has a value, it's interchangeable
                bs["InterchangeabilityFlag"] = bs.apply(
                    lambda row: "Y" if pd.notna(row[col]) and str(row[col]).strip() != "" else "N",
                    axis=1
                )
                break
    
    if "LicensureDate" in bs.columns:
        bs["LicensureDate"] = bs["LicensureDate"].map(_parse_date_like)
    
    # ReferenceBLANumber - not directly available, leave as None
    if "ReferenceBLANumber" not in bs.columns:
        bs["ReferenceBLANumber"] = pd.NA

    bs = _ensure_cols(bs, keep)
    return bs[keep].drop_duplicates()

def _map_exclusivity(excl_df: pd.DataFrame, product_df: pd.DataFrame) -> pd.DataFrame:
    """
    FactPB_Exclusivity:
      BLANumber, ReferenceProductName, ExclusivityType, ExclusivityEndDate, Notes
    
    Extract exclusivity data from product table where exclusivity date columns have values.
    Creates one row per exclusivity type per product.
    """
    keep = ["BLANumber","ReferenceProductName","ExclusivityType","ExclusivityEndDate","Notes"]
    
    if excl_df is None or excl_df.empty:
        return pd.DataFrame(columns=keep)
    
    # Find exclusivity date columns
    excl_cols = {}
    for col in excl_df.columns:
        cn = _norm_col(col)
        if "exclusivityexpirationdate" in cn:
            excl_cols["ExclusivityExpirationDate"] = col
        elif "firstinterchangeableexclusivityexpdate" in cn:
            excl_cols["FirstInterchangeableExclusivityExpDate"] = col
        elif "refproductexclusivityexpdate" in cn:
            excl_cols["RefProductExclusivityExpDate"] = col
        elif "orphexclusivityexpdate" in cn:
            excl_cols["OrphanExclusivityExpDate"] = col
    
    # Build exclusivity rows - one per exclusivity type
    excl_rows = []
    
    # Find BLA Number and Reference Product columns
    bla_col = None
    ref_prod_col = None
    for col in excl_df.columns:
        cn = _norm_col(col)
        if "blanumber" in cn:
            bla_col = col
        if "refproductproprietaryname" in cn or "referenceproductproprietaryname" in cn:
            ref_prod_col = col
    
    for _, row in excl_df.iterrows():
        bla_num = str(row[bla_col]).strip() if bla_col and bla_col in row else ""
        ref_prod = str(row[ref_prod_col]).strip() if ref_prod_col and ref_prod_col in row else ""
        
        # Create a row for each exclusivity type that has a date
        for excl_type, col_name in excl_cols.items():
            if col_name in row and pd.notna(row[col_name]):
                date_val = str(row[col_name]).strip()
                if date_val and date_val.upper() != "N/A":
                    excl_rows.append({
                        "BLANumber": bla_num,
                        "ReferenceProductName": ref_prod if ref_prod and ref_prod.upper() != "N/A" else None,
                        "ExclusivityType": excl_type,
                        "ExclusivityEndDate": _parse_date_like(date_val),
                        "Notes": None
                    })
    
    if not excl_rows:
        return pd.DataFrame(columns=keep)
    
    ex = pd.DataFrame(excl_rows)
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

    # Map columns from Purple Book CSV format
    alias = {
        "blanumber": "BLANumber", "bla number": "BLANumber",
        "applicant": "Applicant",
    }
    ren = {c: alias[_norm_col(c)] for c in df.columns if _norm_col(c) in alias}
    app = df.rename(columns=ren)

    # Status: "Licensure" (Licensed/Revoked) and "Marketing Status" (Rx/OTC/Disc)
    # were both aliased onto the same "Status" target, which made df.rename()
    # collapse them into two duplicate "Status" columns in the output CSV.
    # Prefer the licensure status (an application-level regulatory status,
    # matching what this table otherwise represents); fall back to marketing
    # status when licensure isn't present in the source file.
    licensure_col = _best_col(df, "Licensure")
    marketing_col = _best_col(df, "Marketing Status")
    status_cols = [c for c in (licensure_col, marketing_col) if c is not None]
    if status_cols:
        status = df[status_cols[0]]
        for c in status_cols[1:]:
            status = status.combine_first(df[c])
        app["Status"] = status

    # Use Date of First Licensure for OriginalLicensureDate
    for col in df.columns:
        cn = _norm_col(col)
        if "dateoffirstlicensure" in cn:
            app["OriginalLicensureDate"] = df[col].map(_parse_date_like)
            break
    # Fallback to Approval Date if Date of First Licensure not available
    if "OriginalLicensureDate" not in app.columns or app["OriginalLicensureDate"].isna().all():
        for col in df.columns:
            cn = _norm_col(col)
            if "approvaldate" in cn:
                app["OriginalLicensureDate"] = df[col].map(_parse_date_like)
                break
    
    # Use Approval Date as LastUpdateDate
    for col in df.columns:
        cn = _norm_col(col)
        if "approvaldate" in cn:
            app["LastUpdateDate"] = df[col].map(_parse_date_like)
            break

    app = _ensure_cols(app, keep)
    # Get unique applications (by BLA Number)
    if "BLANumber" in app.columns:
        app = app.drop_duplicates(subset=["BLANumber"], keep="first")
    else:
        app = app.drop_duplicates()
    
    return app[keep] if all(col in app.columns for col in keep) else pd.DataFrame(columns=keep)

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


    # 0) Check for manually placed files first (before trying to download)
    raw_files = _find_local_raw_files()
    
    # If we have manually placed files, prioritize them over downloads
    manually_placed = [f for f in raw_files if os.path.getsize(f) > 1024]  # Files > 1KB are likely real data
    
    # 0a) If final pb_* CSVs already exist in RAW_DIR, pass-through and exit early
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
            if not os.path.exists(dest) or not os.path.samefile(args.local, dest):
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
        # Skip download if we already have manually placed files
        if manually_placed:
            logging.info(f"Found {len(manually_placed)} manually placed file(s), skipping download")
            logging.info(f"Using: {[os.path.basename(f) for f in manually_placed]}")
        else:
            # Try direct download URLs
            urls = build_pb_candidate_urls(months_back=args.months_back)
            logging.info(f"Trying {len(urls)} candidate URLs for Purple Book download...")
            
            for i, url in enumerate(urls, 1):
                ext = ".xlsx" if url.endswith(".xlsx") else ".csv"
                safe = os.path.basename(url).replace("/", "_").replace("?", "_").replace("&", "_")
                if not safe or safe == "_":
                    safe = f"purplebook_download_{i}"
                out_path = os.path.join(RAW_DIR, f"purplebook_{safe}{ext}")
                
                logging.info(f"[{i}/{len(urls)}] Trying: {url}")
                if _download(url, out_path):
                    if os.path.exists(out_path) and os.path.getsize(out_path) > 1024:
                        downloaded_path = out_path
                        logging.info(f"✓ Successfully downloaded Purple Book data from: {url}")
                        break
                    else:
                        logging.warning(f"Downloaded file verification failed, trying next URL...")
        
        if not downloaded_path:
            logging.error(f"\n{'='*70}")
            logging.error("AUTOMATIC DOWNLOAD FAILED - MANUAL DOWNLOAD REQUIRED")
            logging.error(f"{'='*70}")
            logging.error("The FDA Purple Book website (https://purplebooksearch.fda.gov/)")
            logging.error("is a JavaScript application and doesn't provide direct download URLs.")
            logging.error("")
            logging.error("SOLUTION: Download the file manually, then:")
            logging.error("")
            logging.error("  Option 1: Place file in raw directory")
            logging.error(f"    • Download CSV/XLSX from https://purplebooksearch.fda.gov/")
            logging.error(f"    • Save to: {RAW_DIR}")
            logging.error(f"    • Run this script again")
            logging.error("")
            logging.error("  Option 2: Use --local flag")
            logging.error("    • python purple_book_ingest.py --local <path-to-file>")
            logging.error("")
            logging.error("See PURPLE_BOOK_MANUAL_DOWNLOAD.md for detailed instructions.")
            logging.error(f"{'='*70}\n")

    # Refresh raw_files list to include any newly downloaded file
    raw_files = _find_local_raw_files()
    
    # Filter out HTML files that were mistakenly downloaded
    valid_raw_files = []
    for f in raw_files:
        try:
            size = os.path.getsize(f)
            if size < 1024:  # Less than 1KB is suspicious
                continue
            
            # Check if it's HTML
            with open(f, "rb") as check_file:
                first_bytes = check_file.read(200)
                if b"<html" in first_bytes.lower() or b"<!doctype" in first_bytes.lower():
                    logging.warning(f"Skipping HTML file: {os.path.basename(f)}")
                    continue
            
            valid_raw_files.append(f)
        except Exception:
            continue
    
    raw_files = valid_raw_files
    
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

    # 3) Find the main product table (should have BLA Number, Applicant, etc.)
    product_df = None
    for name, df in tables.items():
        ncols = [_norm_col(c) for c in df.columns]
        # Look for the main Purple Book table with BLA Number
        if ("blanumber" in ncols or "bla number" in ncols) and \
           any(k in ncols for k in ("applicant", "proprietaryname", "propername")):
            product_df = df if product_df is None else pd.concat([product_df, df], ignore_index=True)
    
    if product_df is None or product_df.empty:
        logging.error("No product data found in files!")
        print("\n=== Purple Book Ingest Summary ===")
        print(f"DimPB_Product rows:       0 -> {os.path.join(DATA_DIR,'pb_dim_product.csv')}")
        print(f"DimPB_Biosimilar rows:    0 -> {os.path.join(DATA_DIR,'pb_dim_biosimilar.csv')}")
        print(f"FactPB_Exclusivity rows:  0 -> {os.path.join(DATA_DIR,'pb_fact_exclusivity.csv')}")
        print(f"DimPB_Application rows:   0 -> {os.path.join(DATA_DIR,'pb_dim_application.csv')}")
        return

    # 4) Extract different data types from the product table
    # All data is in one table, we need to split it:
    
    # 4a) Biosimilar products: where "Ref. Product Proper Name" or "Ref. Product Proprietary Name" is filled
    ncols = [_norm_col(c) for c in product_df.columns]
    ref_prod_proper_col = None
    ref_prod_proprietary_col = None
    for col in product_df.columns:
        cn = _norm_col(col)
        if "refproductpropername" in cn or "referenceproductpropername" in cn:
            ref_prod_proper_col = col
        if "refproductproprietaryname" in cn or "referenceproductproprietaryname" in cn:
            ref_prod_proprietary_col = col
    
    biosim_df = None
    if ref_prod_proper_col or ref_prod_proprietary_col:
        # Filter rows where reference product columns have values (not empty, not "N/A")
        mask = pd.Series([False] * len(product_df))
        if ref_prod_proper_col:
            mask |= (product_df[ref_prod_proper_col].notna()) & \
                    (product_df[ref_prod_proper_col].astype(str).str.strip() != "") & \
                    (product_df[ref_prod_proper_col].astype(str).str.upper() != "N/A")
        if ref_prod_proprietary_col:
            mask |= (product_df[ref_prod_proprietary_col].notna()) & \
                    (product_df[ref_prod_proprietary_col].astype(str).str.strip() != "") & \
                    (product_df[ref_prod_proprietary_col].astype(str).str.upper() != "N/A")
        biosim_df = product_df[mask].copy() if mask.any() else None
        logging.info(f"Found {len(biosim_df) if biosim_df is not None else 0} biosimilar products")
    
    # 4b) Exclusivity data: where any exclusivity date column has values
    excl_date_cols = []
    for col in product_df.columns:
        cn = _norm_col(col)
        if "exclusivity" in cn and "date" in cn:
            excl_date_cols.append(col)
    
    excl_df = None
    if excl_date_cols:
        # Filter rows where any exclusivity date column has a value
        mask = pd.Series([False] * len(product_df))
        for col in excl_date_cols:
            mask |= (product_df[col].notna()) & \
                    (product_df[col].astype(str).str.strip() != "")
        excl_df = product_df[mask].copy() if mask.any() else None
        logging.info(f"Found {len(excl_df) if excl_df is not None else 0} products with exclusivity data")
    
    # 4c) Application data: extract unique applications (BLA Number + Applicant)
    app_df = product_df.copy()  # Use all product data for application extraction
    
    # 5) Map to canonical outputs
    dim_product = _map_product(product_df)
    dim_biosim  = _map_biosimilar(biosim_df)
    fact_excl   = _map_exclusivity(excl_df, product_df)
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
