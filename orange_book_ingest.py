#!/usr/bin/env python
# orange_book_ingest.py — resilient Orange Book ETL (Products locked to known headers)
import os, io, csv, re, sys, time, logging, zipfile
from typing import Dict, List, Optional
import pandas as pd
import requests
import unicodedata

# Get the directory of this script, then go up one level to find data/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(SCRIPT_DIR, "data")
RAW_DIR  = os.path.join(DATA_DIR, "orange_book_raw")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(RAW_DIR, exist_ok=True)


# FDA Orange Book ZIP (contains Products.txt, Patent.txt, Exclusivity.txt)
OB_ZIP = ("eobzip.zip", "https://www.fda.gov/media/76860/download")

DOWNLOAD = True
TIMEOUT, RETRIES = 45, 3
USER_AGENT = "orange-book-ingest/1.4 (+industry-db)"

session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

# ————— helpers —————
def _download(url: str, out_path: str) -> bool:
    for attempt in range(1, RETRIES + 1):
        try:
            r = session.get(url, timeout=TIMEOUT, allow_redirects=True)
            if r.status_code == 200:
                with open(out_path, "wb") as f: f.write(r.content)
                logging.info(f"Downloaded → {out_path}")
                return True
            logging.warning(f"{url} -> HTTP {r.status_code}")
        except requests.RequestException as e:
            logging.warning(f"Download error: {e} (attempt {attempt}/{RETRIES})")
        time.sleep(1.5 * attempt)
    return False

def _file_magic(path: str) -> bytes:
    if not os.path.exists(path): 
        return b""
    with open(path, "rb") as f:
        return f.read(8)
    
def _norm_col(s: str) -> str:
    """
    Normalize a column name:
      - lowercase
      - unicode NFKD
      - strip accents
      - remove all non [a-z0-9]
    Example: 'Patent Expiration Date' -> 'patentexpirationdate'
    """
    if s is None: return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower()
    return re.sub(r"[^a-z0-9]+", "", s)

def _download_zip_and_extract(zip_url: str, zip_name: str, out_dir: str) -> Dict[str, str]:
    """
    Downloads the Orange Book ZIP and extracts the 3 TXT files.
    Returns a dict { 'Products.txt': path, 'Patent.txt': path, 'Exclusivity.txt': path }.
    """
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, zip_name)
    if not os.path.exists(zip_path):
        if not _download(zip_url, zip_path):
            raise RuntimeError(f"Failed to download Orange Book ZIP: {zip_url}")

    extracted = {}
    with zipfile.ZipFile(zip_path, "r") as zf:
        # Expect exactly these names (case-insensitive)
        wanted = {"products.txt", "patent.txt", "exclusivity.txt"}
        for name in zf.namelist():
            base = os.path.basename(name).lower()
            if base in wanted:
                dest = os.path.join(out_dir, os.path.basename(name))
                with zf.open(name) as src, open(dest, "wb") as dst:
                    dst.write(src.read())
                logging.info(f"Extracted {name} -> {dest}")
                extracted[base] = dest
    # Sanity check
    for req in wanted:
        if req not in extracted:
            raise RuntimeError(f"ZIP missing {req}. Contents: {list(extracted.keys())}")
    return {
        "Products.txt": extracted["products.txt"],
        "Patent.txt": extracted["patent.txt"],
        "Exclusivity.txt": extracted["exclusivity.txt"],
    }

def _read_tilde_txt(path: str) -> Optional[pd.DataFrame]:
    """
    Read a tilde-delimited Orange Book TXT (from the official ZIP).
    Try common encodings and return once we get a multi-column frame.
    """
    if not os.path.exists(path):
        return None

    encodings = ("ascii", "utf-8-sig", "cp1252", "latin1", "utf-16", "utf-16-le")
    for enc in encodings:
        try:
            df = pd.read_csv(
                path,
                sep="~",
                dtype=str,
                engine="python",
                quoting=csv.QUOTE_NONE,
                encoding=enc,
                on_bad_lines="skip",
            )
            # Clean minimal
            df.columns = [c.strip() for c in df.columns]
            for c in df.columns:
                if df[c].dtype == "object":
                    df[c] = df[c].astype(str).str.strip()
            # must be multi-column to be valid
            if df.shape[1] >= 3 and df.shape[0] >= 1:
                logging.info(f"Parsed {os.path.basename(path)} with forced '~' (enc={enc})")
                return df.fillna(pd.NA)
        except Exception:
            continue

    logging.error(f"Failed to read tilde-delimited file: {path}")
    return None


def _best_col(df: pd.DataFrame, *candidates: str) -> Optional[str]:
    """
    If no exact alias match, pick the first column whose normalized name
    contains any of the candidate tokens (ordered by preference).
    """
    if df is None or df.empty: return None
    norm_map = {_norm_col(c): c for c in df.columns}
    keys = list(norm_map.keys())
    for token in candidates:
        t = _norm_col(token)
        # exact
        if t in norm_map: return norm_map[t]
        # contains
        for k in keys:
            if t in k:
                return norm_map[k]
    return None

def _log_cols(df: Optional[pd.DataFrame], label: str):
    if df is None:
        logging.info(f"{label}: <None>")
        return
    cols = list(df.columns)
    logging.info(f"{label}: {len(cols)} columns -> {cols[:30]}")
    logging.info(f"{label} (normalized): {[ _norm_col(c) for c in cols[:30] ]}")


def _rename_with_aliases(df: pd.DataFrame, alias_map: Dict[str, str]) -> pd.DataFrame:
    """
    alias_map: { normalized_source_name -> target_name }
    We compute normalized names for existing columns and rename if found in alias_map.
    """
    if df is None or df.empty:
        return df
    ren = {}
    for c in df.columns:
        key = _norm_col(c)
        if key in alias_map:
            ren[c] = alias_map[key]
    return df.rename(columns=ren)

def _normalize_ob_file(raw_path: str, expected_basename: str) -> str:
    """If the .txt is actually a zip, extract; if html/pdf, raise; else return path."""
    magic = _file_magic(raw_path)
    if magic.startswith(b"PK"):
        out_txt = os.path.join(os.path.dirname(raw_path), f"EXTRACTED_{expected_basename}")
        with zipfile.ZipFile(raw_path, "r") as zf:
            stem = os.path.splitext(expected_basename)[0].lower()
            cand = [n for n in zf.namelist() if n.lower().endswith(".txt") and stem in n.lower()]
            if not cand:
                cand = [n for n in zf.namelist() if n.lower().endswith(".txt")]
            if not cand:
                raise RuntimeError(f"No .txt inside ZIP: {raw_path}")
            with zf.open(cand[0]) as src, open(out_txt, "wb") as dst:
                dst.write(src.read())
        logging.info(f"Extracted {os.path.basename(cand[0]).lower()} → {out_txt}")
        return out_txt

    if magic.startswith(b"%PDF"):
        raise RuntimeError(f"{raw_path} appears to be PDF.")
    with open(raw_path, "rb") as f:
        sniff = f.read(400).lower()
        if b"<html" in sniff:
            raise RuntimeError(f"{raw_path} appears to be HTML.")

    return raw_path

def _ensure_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        if col not in df.columns:
            df[col] = pd.NA
    return df

def _parse_date(s: Optional[str]) -> Optional[str]:
    if s is None or (isinstance(s, float) and pd.isna(s)) or str(s).strip()=="":
        return None
    ss = str(s).strip()
    # Orange Book convention for pre-1982 approvals, e.g. "Approved Prior to Jan 1, 1982"
    m = re.match(r"approved\s+prior\s+to\s+(.+)", ss, re.IGNORECASE)
    if m:
        ss = m.group(1).strip()
    for fmt in ("%Y-%m-%d","%m/%d/%Y","%Y%m%d"):
        try:
            return pd.to_datetime(ss, format=fmt).date().isoformat()
        except Exception:
            pass
    try:
        return pd.to_datetime(ss).date().isoformat()
    except Exception:
        return None

def _yn(val: Optional[str]) -> Optional[str]:
    if val is None or (isinstance(val,float) and pd.isna(val)) or str(val).strip()=="":
        return None
    v = str(val).strip().upper()
    if v in ("Y","YES","TRUE","1"): return "Y"
    if v in ("N","NO","FALSE","0"): return "N"
    return None

def _split_te_codes(s: Optional[str]) -> List[str]:
    if s is None or (isinstance(s,float) and pd.isna(s)): return []
    return [p for p in re.split(r"[,\s;]+", str(s).strip()) if p]

def _debug_preview(paths: Dict[str, str]):
    outf = os.path.join(RAW_DIR, "orange_book_debug_preview.txt")
    with open(outf, "w", encoding="utf-8") as w:
        for label, p in paths.items():
            w.write(f"=== {label} ({p}) ===\n")
            if not os.path.exists(p):
                w.write("  <missing>\n\n"); continue
            try:
                with open(p, "rb") as f:
                    raw = f.read()
                text = None
                for enc in ("utf-16","utf-16-le","utf-8-sig","cp1252"):
                    try:
                        text = raw.decode(enc, errors="replace").replace("\x00","")
                        break
                    except Exception:
                        continue
                if not text:
                    w.write("  <undecodable>\n\n"); continue
                lines = [ln for ln in text.splitlines() if ln.strip()][:3]
                for ln in lines:
                    w.write(ln + "\n")
                w.write("\n")
            except Exception as e:
                w.write(f"  <error: {e}>\n\n")

# ————— readers —————
def _read_products_known_headers(path: str) -> Optional[pd.DataFrame]:
    """
    Use the exact headers Andre showed:
    Ingredient | DF;Route | Trade_Name | Applicant | Strength | Appl_Type | Appl_No | Product_No |
    TE_Code | Approval_Date | RLD | RS | Type | Applicant_Full_Name
    Encoding: often utf-16, but we saw utf-8-sig via fallback.
    Separator: '~'
    """
    tried = []
    for enc in ("utf-16","utf-16-le","utf-8-sig","cp1252"):
        try:
            df = pd.read_csv(path, sep="~", dtype=str, engine="python",
                             quoting=csv.QUOTE_NONE, encoding=enc, on_bad_lines="skip")
            df.columns = [c.strip() for c in df.columns]
            # fast check for expected columns
            expected = {"Ingredient","DF;Route","Trade_Name","Applicant","Strength","Appl_Type",
                        "Appl_No","Product_No","TE_Code","Approval_Date","RLD","RS","Type","Applicant_Full_Name"}
            if expected.issubset(set(df.columns)):
                for c in df.columns:
                    if df[c].dtype == "object":
                        df[c] = df[c].astype(str).str.strip()
                return df.fillna(pd.NA)
            tried.append(enc)
        except Exception:
            tried.append(enc)
            continue
    logging.error(f"Products: could not find expected headers with encodings {tried}.")
    return None

def _read_generic_txt(path: str) -> Optional[pd.DataFrame]:
    """
    Ultra-forgiving reader for Patent/Exclusivity/Application:
    - Try multiple encodings
    - Normalize NULLs & NBSP
    - Detect delimiter from any repeated non-alnum char in header
    - Try csv.Sniffer, common delimiters, then fixed-width on 1+ spaces
    """
    if not os.path.exists(path):
        return None

    # 1) decode bytes
    with open(path, "rb") as f:
        raw = f.read()

    text = None
    used_enc = None
    for enc in ("utf-16", "utf-16-le", "utf-8-sig", "cp1252"):
        try:
            t = raw.decode(enc, errors="replace")
            if t:
                text = t.replace("\x00", "").replace("\u00A0", " ")  # strip NULL + NBSP
                used_enc = enc
                break
        except Exception:
            continue
    if not text:
        logging.error(f"Undecodable: {path}")
        return None

    # 2) sample lines
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        logging.error(f"No lines: {path}")
        return None
    header_line = lines[0]

    # 3) dynamic delimiter detection from header — any repeated non-alnum char
    from collections import Counter
    puncts = [ch for ch in header_line if not ch.isalnum() and ch not in " \"'"]
    delim_guess = None
    if puncts:
        counts = Counter(puncts)
        # Remove spaces from consideration here; we’ll try whitespace later
        if " " in counts:
            del counts[" "]
        if counts:
            delim_guess, _ = max(counts.items(), key=lambda kv: kv[1])

    # 4) csv.Sniffer as another hint
    sniffer_guess = None
    try:
        sniffer_guess = csv.Sniffer().sniff("\n".join(lines[:8]), delimiters="~|,\t;:^")
        sniffer_guess = sniffer_guess.delimiter
    except Exception:
        pass

    # Decide initial sep to try
    sep_candidates = []
    if delim_guess:
        sep_candidates.append(delim_guess)
    if sniffer_guess and sniffer_guess not in sep_candidates:
        sep_candidates.append(sniffer_guess)
    # add normal suspects
    for cand in ["~","|",",","\t",";","^",":"]:
        if cand not in sep_candidates:
            sep_candidates.append(cand)

    def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
        df.columns = [c.strip() for c in df.columns]
        for c in df.columns:
            if df[c].dtype == "object":
                df[c] = df[c].astype(str).str.strip()
        return df.fillna(pd.NA)

    # 5) Try pandas with our candidate delimiters
    for sep in sep_candidates:
        try:
            df = pd.read_csv(io.StringIO(text),
                             sep=sep, dtype=str, engine="python",
                             quoting=csv.QUOTE_NONE, on_bad_lines="skip")
            if df.shape[1] > 1:
                logging.info(f"Parsed {os.path.basename(path)} with sep='{sep}' (enc={used_enc})")
                return _clean_df(df)
        except Exception:
            continue

    # 6) Fixed-width fallback (split on 1+ spaces)
    try:
        header = [h.strip() for h in re.split(r"\s{1,}", header_line.strip()) if h.strip() != ""]
        col_count = len(header)
        if col_count >= 2:
            rows = []
            for ln in lines[1:]:
                parts = [p.strip() for p in re.split(r"\s{1,}", ln.strip()) if p.strip() != ""]
                if len(parts) == col_count:
                    rows.append(parts)
            if rows:
                logging.info(f"Parsed {os.path.basename(path)} as fixed-width whitespace (enc={used_enc})")
                return _clean_df(pd.DataFrame(rows, columns=header))
    except Exception:
        pass

    logging.error(f"Failed to parse {path} (enc={used_enc}); leaving empty.")
    return None

# ————— main —————
def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    # 1) Always pull the official ZIP and extract the 3 tilde-delimited TXT files
    paths = _download_zip_and_extract(OB_ZIP[1], OB_ZIP[0], RAW_DIR)
    products_path     = paths["Products.txt"]
    patent_path       = paths["Patent.txt"]
    exclusivity_path  = paths["Exclusivity.txt"]

    # 2) Read them (all are ASCII-ish, but utf-16/utf-8-sig tolerant)
    product_df = _read_products_known_headers(products_path)
    if product_df is None:
        logging.error("Products.txt from ZIP failed to parse; cannot continue.")
        sys.exit(1)

    # OLD:
    # patent_df  = _read_generic_txt(patent_path)
    # excl_df    = _read_generic_txt(exclusivity_path)

    # NEW: force tilde
    patent_df = _read_tilde_txt(patent_path)
    excl_df   = _read_tilde_txt(exclusivity_path)

    _log_cols(patent_df,  "Patent.txt")
    _log_cols(excl_df,    "Exclusivity.txt")

    # (Optional) remove any Application handling – it’s not in the official ZIP
    app_df = None



    # ---- Application dim (robust) ----
    app_cols = ["ApplNo","ApplType","ApplicantName","ApplStatus","FirstApprovalDate","LastUpdateDate"]
    dim_app = pd.DataFrame(columns=app_cols)
    if app_df is not None and not app_df.empty:
        app_alias = {
            "applno": "ApplNo",
            "applicationnumber": "ApplNo",
            "newdrugapplicationnumber": "ApplNo",
            "appltype": "ApplType",
            "applicationtype": "ApplType",
            "applstatus": "ApplStatus",
            "status": "ApplStatus",
            "applicantfullname": "ApplicantName",
            "applicant": "ApplicantName",
            "firm": "ApplicantName",
            "sponsor": "ApplicantName",
            "firstapprovaldate": "FirstApprovalDate",
            "originalapprovaldate": "FirstApprovalDate",
            "approvaldate": "FirstApprovalDate",
            "lastupdatedate": "LastUpdateDate",
            "filedate": "LastUpdateDate",
            "updatedate": "LastUpdateDate",
        }
        an = _rename_with_aliases(app_df, app_alias)

        # Fallbacks
        if "ApplNo" not in an:
            c = _best_col(an, "Appl No", "Application Number", "NDA", "ANDA", "ApplNo")
            if c: an = an.rename(columns={c: "ApplNo"})
        if "ApplType" not in an:
            c = _best_col(an, "Appl Type", "Application Type", "Type")
            if c: an = an.rename(columns={c: "ApplType"})
        if "ApplicantName" not in an:
            c = _best_col(an, "Applicant", "Applicant Full Name", "Firm", "Sponsor")
            if c: an = an.rename(columns={c: "ApplicantName"})
        if "ApplStatus" not in an:
            c = _best_col(an, "Status", "Application Status")
            if c: an = an.rename(columns={c: "ApplStatus"})
        if "FirstApprovalDate" not in an:
            c = _best_col(an, "First Approval Date", "Original Approval Date", "Approval Date")
            if c: an = an.rename(columns={c: "FirstApprovalDate"})
        if "LastUpdateDate" not in an:
            c = _best_col(an, "Last Update Date", "Update Date", "File Date")
            if c: an = an.rename(columns={c: "LastUpdateDate"})

        # Ensure and clean
        an = _ensure_cols(an, app_cols)
        an["FirstApprovalDate"] = an["FirstApprovalDate"].map(_parse_date)
        an["LastUpdateDate"]    = an["LastUpdateDate"].map(_parse_date)
        dim_app = an[app_cols].drop_duplicates()


    # 5) Build DimOB_Product (map your exact headers)
    # Map to our analytics-friendly names
    pmap = {}
    for c in product_df.columns:
        cl = c.lower()
        if cl == "ingredient": pmap[c]="ActiveIngredient"
        elif cl in ("df;route","dosage form; route of administration","dosage form; route","dosageform;route"): pmap[c]="DosageFormRoute"
        elif cl in ("trade_name","trade name","proprietary name","tradename","productname"): pmap[c]="TradeName"
        elif cl in ("applicant", "applicant condensed", "firm", "sponsor"): pmap[c]="Applicant"
        elif cl == "strength": pmap[c]="Strength"
        elif cl in ("appl_type","new drug application type","appltype","applicationtype"): pmap[c]="ApplType"
        elif cl in ("appl_no","new drug application number","applno","applicationnumber"): pmap[c]="ApplNo"
        elif cl in ("product_no","product number","productno","prodno","productnumber"): pmap[c]="ProductNo"
        elif cl in ("te_code","therapeutic equivalence (te) code","tecode","te code","te"): pmap[c]="TECodeRaw"
        elif cl in ("approval_date","approval date","approvaldate"): pmap[c]="ApprovalDate"
        elif cl == "rld": pmap[c]="RLD"
        elif cl == "rs":  pmap[c]="RS"
        elif cl in ("type","marketingstatus","marketing status"): pmap[c]="MarketingStatus"
        elif cl in ("applicant_full_name","applicant full name","applicantname"): pmap[c]="ApplicantName"

    prod = product_df.rename(columns=pmap)

    # Split DF;Route
    if "DosageFormRoute" in prod.columns:
        split = prod["DosageFormRoute"].str.split(";", n=1, expand=True)
        if isinstance(split, pd.DataFrame) and split.shape[1] == 2:
            prod["DosageForm"] = split[0].str.strip()
            prod["Route"]      = split[1].str.strip()
        else:
            prod["DosageForm"] = prod["DosageFormRoute"]; prod["Route"] = pd.NA
    else:
        prod["DosageForm"] = pd.NA
        prod["Route"]      = pd.NA

    # Flags / TE
    if "RLD" in prod: prod["RLD"] = prod["RLD"].map(_yn)
    if "RS"  in prod: prod["RS"]  = prod["RS"].map(_yn)
    prod["ReferenceListedDrugFlag"] = prod.get("RLD")
    prod["ReferenceStandardFlag"]   = prod.get("RS")
    if "TECodeRaw" in prod:
        prod["TECode"] = prod["TECodeRaw"].apply(lambda s: _split_te_codes(s)[0] if _split_te_codes(s) else pd.NA)
    else:
        prod["TECode"] = pd.NA

    # The official Orange Book ZIP has no standalone Application file, so when
    # app_df isn't available, derive one application-level row per ApplNo by
    # rolling up the product-level rows (which carry ApplType/Applicant/dates).
    if dim_app.empty and "ApplNo" in prod.columns:
        app_src = _ensure_cols(prod.copy(), ["ApplNo", "ApplType", "ApplicantName", "MarketingStatus", "ApprovalDate"])
        app_src = app_src.dropna(subset=["ApplNo"])
        app_src["FirstApprovalDate"] = app_src["ApprovalDate"].map(_parse_date)
        app_src["_IsActive"] = app_src["MarketingStatus"].astype(str).str.upper().isin(["RX", "OTC"])

        if not app_src.empty:
            grouped = app_src.groupby("ApplNo")
            first_non_null = lambda s: s.dropna().iloc[0] if s.notna().any() else pd.NA
            dim_app = pd.DataFrame({"ApplNo": list(grouped.groups.keys())})
            dim_app["ApplType"] = grouped["ApplType"].agg(first_non_null).values
            dim_app["ApplicantName"] = grouped["ApplicantName"].agg(first_non_null).values
            dim_app["ApplStatus"] = grouped["_IsActive"].any().map({True: "Active", False: "Discontinued"}).values
            dim_app["FirstApprovalDate"] = grouped["FirstApprovalDate"].agg(
                lambda s: s.dropna().min() if s.notna().any() else pd.NA
            ).values
            dim_app["LastUpdateDate"] = pd.NA
            dim_app = dim_app[app_cols]

    needed = ["ApplNo","ProductNo","TradeName","ActiveIngredient","Strength","DosageForm","Route",
              "ReferenceListedDrugFlag","ReferenceStandardFlag","MarketingStatus","TECode"]
    prod = _ensure_cols(prod, needed)
    dim_product = prod[needed].copy()
    dim_product["RxCUI_Ingredient"] = pd.NA
    dim_product["RxCUI_Product"]    = pd.NA
    dim_product["LastUpdateDate"]   = pd.NA  # not present in this layout

    # 6) TE Code dimension & bridge
    te_codes=set()
    if "TECodeRaw" in prod:
        for s in prod["TECodeRaw"].dropna().unique().tolist():
            for code in _split_te_codes(s): te_codes.add(code)
    dim_te = (pd.DataFrame({"TECode": sorted(te_codes)}) if te_codes else
              pd.DataFrame({"TECode": []}))
    dim_te["TEDescription"] = pd.NA

    product_te_rows=[]
    if "TECodeRaw" in prod:
        tmp = prod[["ApplNo","ProductNo","TECodeRaw"]].dropna(subset=["ApplNo","ProductNo","TECodeRaw"])
        for _, r in tmp.iterrows():
            for code in _split_te_codes(r["TECodeRaw"]):
                product_te_rows.append({"ApplNo": r["ApplNo"], "ProductNo": r["ProductNo"], "TECode": code})
    dim_product_te = pd.DataFrame(product_te_rows).drop_duplicates() if product_te_rows else pd.DataFrame({"ApplNo": [], "ProductNo": [], "TECode": []})

    # ---- Exclusivity fact (robust) ----
    fact_excl = pd.DataFrame(columns=["ApplNo","ProductNo","ExclusivityCode","ExclusivityEndDate","Notes","FileDate"])
    if excl_df is not None and not excl_df.empty:
        excl_alias = {
            "applno": "ApplNo",
            "applicationnumber": "ApplNo",
            "newdrugapplicationnumber": "ApplNo",
            "productno": "ProductNo",
            "productnumber": "ProductNo",
            "prodno": "ProductNo",
            "exclusivitycode": "ExclusivityCode",
            "exclusivity": "ExclusivityCode",
            "exclcode": "ExclusivityCode",
            "exclusivitydate": "ExclusivityEndDate",
            "enddate": "ExclusivityEndDate",
            "expirationdate": "ExclusivityEndDate",
            "expdate": "ExclusivityEndDate",
            "notes": "Notes",
            "note": "Notes",
            "filedate": "FileDate",
            "lastupdatedate": "FileDate",
            "updatedate": "FileDate",
        }
        en = _rename_with_aliases(excl_df, excl_alias)

        # Fallbacks if still missing
        if "ApplNo" not in en:
            c = _best_col(en, "Appl No", "Application Number", "NDA", "ANDA", "ApplNo")
            if c: en = en.rename(columns={c: "ApplNo"})
        if "ProductNo" not in en:
            c = _best_col(en, "Product No", "Prod No", "Product Number")
            if c: en = en.rename(columns={c: "ProductNo"})
        if "ExclusivityCode" not in en:
            c = _best_col(en, "Exclusivity Code", "Exclusivity", "EXC Code")
            if c: en = en.rename(columns={c: "ExclusivityCode"})
        if "ExclusivityEndDate" not in en:
            c = _best_col(en, "Exclusivity Date", "End Date", "Expiration Date", "Exp Date")
            if c: en = en.rename(columns={c: "ExclusivityEndDate"})
        if "Notes" not in en:
            c = _best_col(en, "Notes", "Note", "Comment")
            if c: en = en.rename(columns={c: "Notes"})
        if "FileDate" not in en:
            c = _best_col(en, "File Date", "Update Date", "Last Update Date")
            if c: en = en.rename(columns={c: "FileDate"})

        # Ensure columns and types
        for col in ["ApplNo","ProductNo","ExclusivityCode","ExclusivityEndDate","Notes","FileDate"]:
            if col not in en: en[col] = pd.NA
        en["ExclusivityEndDate"] = en["ExclusivityEndDate"].map(_parse_date)
        en["FileDate"] = en["FileDate"].map(_parse_date)

        fact_excl = en[["ApplNo","ProductNo","ExclusivityCode","ExclusivityEndDate","Notes","FileDate"]].drop_duplicates()


    # ---- Patent fact (robust) ----
    fact_pat = pd.DataFrame(columns=[
        "ApplNo","ProductNo","PatentNo","PatentExpiration",
        "DrugSubstanceFlag","DrugProductFlag","PatentUseCode",
        "DelistRequested","PediatricExtension","FileDate"
    ])
    if patent_df is not None and not patent_df.empty:
        pat_alias = {
            "applno": "ApplNo",
            "applicationnumber": "ApplNo",
            "newdrugapplicationnumber": "ApplNo",
            "productno": "ProductNo",
            "productnumber": "ProductNo",
            "prodno": "ProductNo",
            "patentno": "PatentNo",
            "patentnumber": "PatentNo",
            "patentexpirationdate": "PatentExpiration",
            "expirationdate": "PatentExpiration",
            "expdate": "PatentExpiration",
            "drugsubstanceflag": "DrugSubstanceFlag",
            "substance": "DrugSubstanceFlag",
            "drugproductflag": "DrugProductFlag",
            "product": "DrugProductFlag",
            "patentusecode": "PatentUseCode",
            "usecode": "PatentUseCode",
            "delistrequested": "DelistRequested",
            "delistrequestflag": "DelistRequested",
            "pediatricextension": "PediatricExtension",
            "pediatric": "PediatricExtension",
            "filedate": "FileDate",
            "lastupdatedate": "FileDate",
            "updatedate": "FileDate",
        }
        pn = _rename_with_aliases(patent_df, pat_alias)

        # Fallbacks if still missing
        if "ApplNo" not in pn:
            c = _best_col(pn, "Appl No", "Application Number", "NDA", "ANDA", "ApplNo")
            if c: pn = pn.rename(columns={c: "ApplNo"})
        if "ProductNo" not in pn:
            c = _best_col(pn, "Product No", "Prod No", "Product Number")
            if c: pn = pn.rename(columns={c: "ProductNo"})
        if "PatentNo" not in pn:
            c = _best_col(pn, "Patent No", "Patent Number")
            if c: pn = pn.rename(columns={c: "PatentNo"})
        if "PatentExpiration" not in pn:
            c = _best_col(pn, "Patent Expiration Date", "Expiration Date", "Exp Date")
            if c: pn = pn.rename(columns={c: "PatentExpiration"})
        if "PatentUseCode" not in pn:
            c = _best_col(pn, "Patent Use Code", "Use Code")
            if c: pn = pn.rename(columns={c: "PatentUseCode"})
        if "DrugSubstanceFlag" not in pn:
            c = _best_col(pn, "Drug Substance Flag", "Substance")
            if c: pn = pn.rename(columns={c: "DrugSubstanceFlag"})
        if "DrugProductFlag" not in pn:
            c = _best_col(pn, "Drug Product Flag", "Product")
            if c: pn = pn.rename(columns={c: "DrugProductFlag"})
        if "DelistRequested" not in pn:
            c = _best_col(pn, "Delist Requested", "Delist Request Flag")
            if c: pn = pn.rename(columns={c: "DelistRequested"})
        if "PediatricExtension" not in pn:
            c = _best_col(pn, "Pediatric Extension", "Pediatric")
            if c: pn = pn.rename(columns={c: "PediatricExtension"})
        if "FileDate" not in pn:
            c = _best_col(pn, "File Date", "Update Date", "Last Update Date")
            if c: pn = pn.rename(columns={c: "FileDate"})

        # Ensure columns, coerce booleans/dates
        for col in fact_pat.columns:
            if col not in pn: pn[col] = pd.NA
        for col in ("DrugSubstanceFlag","DrugProductFlag","DelistRequested","PediatricExtension"):
            pn[col] = pn[col].map(_yn)
        pn["PatentExpiration"] = pn["PatentExpiration"].map(_parse_date)
        pn["FileDate"] = pn["FileDate"].map(_parse_date)

        fact_pat = pn[list(fact_pat.columns)].drop_duplicates()


    # 9) Write outputs
    out_app = os.path.join(DATA_DIR, "ob_dim_application.csv")
    out_prod= os.path.join(DATA_DIR, "ob_dim_product.csv")
    out_te  = os.path.join(DATA_DIR, "ob_dim_tecode.csv")
    out_pte = os.path.join(DATA_DIR, "ob_dim_product_te.csv")
    out_excl= os.path.join(DATA_DIR, "ob_fact_exclusivity.csv")
    out_pat = os.path.join(DATA_DIR, "ob_fact_patent.csv")

    dim_app.to_csv(out_app, index=False)
    dim_product.to_csv(out_prod, index=False)
    dim_te.to_csv(out_te, index=False)
    dim_product_te.to_csv(out_pte, index=False)
    fact_excl.to_csv(out_excl, index=False)
    fact_pat.to_csv(out_pat, index=False)

    print("\n=== Orange Book Ingest Summary ===")
    print(f"Applications: {len(dim_app):,}  -> {out_app}")
    print(f"Products:     {len(dim_product):,}  -> {out_prod}")
    print(f"TE codes:     {len(dim_te):,}  -> {out_te}")
    print(f"Prod-TE rows: {len(dim_product_te):,}  -> {out_pte}")
    print(f"Exclusivity:  {len(fact_excl):,}  -> {out_excl}")
    print(f"Patents:      {len(fact_pat):,}  -> {out_pat}")
    print(f"\nWrote debug preview at: {os.path.join(RAW_DIR, 'orange_book_debug_preview.txt')}")

if __name__ == "__main__":
    main()
