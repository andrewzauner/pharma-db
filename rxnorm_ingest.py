# rxnorm_ingest.py
# Enhanced: split IN vs SCD/SBD into separate dimensions + better parsing
import os, time, json, logging, re, argparse
from typing import Dict, List, Optional, Tuple

import requests
import pandas as pd

RXNAV_BASE = "https://rxnav.nlm.nih.gov/REST"

# -------------------------
# Config
# -------------------------
USER_AGENT = "rxnorm-ingest/1.1 (+your-company-analytics)"
RETRY_COUNT = 5
RETRY_BACKOFF_SEC = 1.5
TIMEOUT_SEC = 20

# Get the directory of this script, then go up one level to find data/
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(SCRIPT_DIR, "data")
ING_OUTPUT = os.path.join(OUTPUT_DIR, "dim_drug_ingredient.csv")  # NEW
PRD_OUTPUT = os.path.join(OUTPUT_DIR, "dim_drug_product.csv")     # NEW
PACK_OUTPUT = os.path.join(OUTPUT_DIR, "dim_productpack_ndc.csv")
SUMMARY_OUTPUT = os.path.join(OUTPUT_DIR, "rxnorm_summary.csv")

SEED_DRUG_NAMES = [
    "adalimumab",
    "lipitor",
    "acetaminophen",
    "semaglutide"
]

# -------------------------
# HTTP helpers
# -------------------------
session = requests.Session()
session.headers.update({"User-Agent": USER_AGENT})

def _get(url: str, params: Optional[Dict] = None) -> Optional[dict]:
    params = params or {}
    for attempt in range(1, RETRY_COUNT + 1):
        try:
            r = session.get(url, params=params, timeout=TIMEOUT_SEC)
            if r.status_code == 200:
                try:
                    return r.json()
                except json.JSONDecodeError:
                    logging.warning(f"Non-JSON response for {url}: {r.text[:200]}")
                    return None
            elif r.status_code in (429, 500, 502, 503, 504):
                wait = RETRY_BACKOFF_SEC ** attempt
                logging.warning(f"{r.status_code} on {url} (params={params}). retry {attempt}/{RETRY_COUNT} in {wait:.1f}s")
                time.sleep(wait)
            else:
                logging.error(f"HTTP {r.status_code} for {url}: {r.text[:200]}")
                return None
        except requests.RequestException as e:
            wait = RETRY_BACKOFF_SEC ** attempt
            logging.warning(f"Request error {e} on {url}. retry {attempt}/{RETRY_COUNT} in {wait:.1f}s")
            time.sleep(wait)
    return None

# -------------------------
# RxNorm client
# -------------------------
class RxNormClient:
    def __init__(self, base: str = RXNAV_BASE):
        self.base = base

    def resolve_rxcui(self, name: str) -> Optional[str]:
        """Fuzzy resolve a display name to an RxCUI (often IN/ingredient)."""
        url = f"{self.base}/rxcui.json"
        data = _get(url, {"name": name, "search": 2})
        if not data:
            return None
        ids = data.get("idGroup", {}).get("rxnormId")
        return ids[0] if ids else None

    def get_properties(self, rxcui: str) -> Dict:
        url = f"{self.base}/rxcui/{rxcui}/properties.json"
        data = _get(url)
        return data.get("properties", {}) if data else {}

    def get_all_related(self, rxcui: str) -> List[Dict]:
        """Use allrelated.json to avoid 400 errors."""
        url = f"{self.base}/rxcui/{rxcui}/allrelated.json"
        data = _get(url)
        out: List[Dict] = []
        for grp in (data or {}).get("allRelatedGroup", {}).get("conceptGroup", []) or []:
            for c in grp.get("conceptProperties") or []:
                out.append(c)
        return out

    def get_all_ndcs(self, rxcui: str) -> Tuple[List[str], List[str]]:
        """Return (current_ndcs, historical_ndcs) for an SCD/SBD RxCUI."""
        cur_d = _get(f"{self.base}/rxcui/{rxcui}/ndcs.json") or {}
        cur = cur_d.get("ndcGroup", {}).get("ndcList", {}).get("ndc") or []

        all_d = _get(f"{self.base}/rxcui/{rxcui}/allndcs.json") or {}
        all_ndc = all_d.get("ndcGroup", {}).get("ndcList", {}).get("ndc") or []

        hist = [n for n in all_ndc if n not in cur]
        return cur, hist

# -------------------------
# Search helpers (prescribable)
# -------------------------
def find_prescribable_rxcuis_by_name(name: str) -> List[str]:
    """Return SCD/SBD RxCUIs for a name using /drugs.json."""
    data = _get(f"{RXNAV_BASE}/drugs.json", {"name": name})
    if not data:
        return []
    out = []
    for group in (data.get("drugGroup", {}).get("conceptGroup") or []):
        tty = group.get("tty")
        if tty in ("SCD", "SBD"):
            for c in group.get("conceptProperties") or []:
                r = c.get("rxcui")
                if r:
                    out.append(r)
    # de-dupe preserving order
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]

def get_prescribable_rxcuis(name: str, client: RxNormClient, rxcui_in: Optional[str]) -> List[str]:
    """Robust lookup with fallbacks and brand-based second pass."""
    seen = set()
    out: List[str] = []

    def _uniq_extend(xs):
        for x in xs or []:
            if x and x not in seen:
                seen.add(x); out.append(x)

    _uniq_extend(find_prescribable_rxcuis_by_name(name))

    if rxcui_in:
        rel = client.get_all_related(rxcui_in)
        _uniq_extend([c["rxcui"] for c in rel if c.get("tty") in ("SCD", "SBD")])
        # try brand names as new queries
        for bn in [c.get("name") for c in rel if c.get("tty") == "BN"][:3]:
            _uniq_extend(find_prescribable_rxcuis_by_name(bn))

    if not out:
        _uniq_extend(find_prescribable_rxcuis_by_name(name.lower()))
        _uniq_extend(find_prescribable_rxcuis_by_name(name.upper()))

    logging.info(f"[PRESCRIBABLE] name='{name}' seed_in={rxcui_in} -> {len(out)} SCD/SBD")
    return out

# -------------------------
# Parsing helpers
# -------------------------
# Compound units (mg/mL) must be tried before their own prefix (mg) --
# regex alternation stops at the first match, so with "mg" listed first
# "10 mg/mL" would match only "10 mg" and silently drop the "/mL".
_STRENGTH_RE = re.compile(
    r"(?:(\d+(?:\.\d+)?)\s*(mg/mL|mcg/mL|g/mL|units/mL|mg|mcg|g|units|mL|%))",
    re.IGNORECASE
)

def parse_strength(text: str) -> Optional[str]:
    if not text:
        return None
    m = _STRENGTH_RE.search(text)
    return m.group(0) if m else None

def norm(s: Optional[str]) -> Optional[str]:
    return (s or "").strip() or None

# RxNorm Dose Form (DF) names are typically "<route/site adjective> <form>",
# e.g. "Oral Tablet", "Injectable Solution", "Topical Cream", "Extended
# Release Oral Tablet" -- but not always: "Chewable Tablet" or "Disintegrating
# Oral Tablet" have a modifier before (or instead of) any route. So route
# can't be reliably taken as "the first word" -- instead scan for the first
# token that matches a known route/site vocabulary.
_ROUTE_VOCAB = [
    "Oral", "Sublingual", "Buccal", "Injectable", "Intravenous", "Intramuscular",
    "Subcutaneous", "Topical", "Transdermal", "Nasal", "Ophthalmic", "Otic",
    "Rectal", "Vaginal", "Inhalant", "Irrigation", "Dental", "Urethral",
    "Enteral", "Intraperitoneal", "Intrathecal", "Intraarticular", "Mucosal",
]
_ROUTE_LOOKUP = {r.lower(): r for r in _ROUTE_VOCAB}

def route_from_dose_form(dose_form: Optional[str]) -> Optional[str]:
    if not dose_form:
        return None
    for token in dose_form.replace("/", " ").split():
        hit = _ROUTE_LOOKUP.get(token.lower())
        if hit:
            return hit
    return None

# -------------------------
# Row builders
# -------------------------
def build_ingredient_row(rxcui_in: str, client: RxNormClient) -> Dict:
    props = client.get_properties(rxcui_in)
    related = client.get_all_related(rxcui_in)

    # Ingredient name
    props_tty = norm(props.get("tty"))
    props_name = norm(props.get("name"))

    # Prefer IN concept; if seed isn’t IN, try to find IN/PIN in related
    if props_tty != "IN":
        ing = [c for c in related if c.get("tty") in ("IN", "PIN")]
        if ing:
            props_name = norm(ing[0].get("name"))

    # A generic-only row
    return {
        "IngredientRxCUI": rxcui_in,
        "GenericName": props_name,
        "AtcClass": None,     # (optional future enrichment)
        "OrphanFlag": 0,
        "SpecialtyFlag": 0
    }

def build_product_rows(ingredient_rxcui: str, product_rxcuis: List[str], client: RxNormClient) -> List[Dict]:
    rows: List[Dict] = []
    for prx in product_rxcuis:
        props = client.get_properties(prx)
        related = client.get_all_related(prx)

        tty = norm(props.get("tty"))   # SCD or SBD
        pname = norm(props.get("name"))
        rxstring = norm(props.get("rxstring"))

        # Brand/generic derivation:
        brand = None
        generic = None

        if tty == "SBD":
            # Branded product — try BN in related
            bn = [c for c in related if c.get("tty") == "BN"]
            brand = norm(bn[0].get("name")) if bn else pname
            # Generic comes from ingredients
            ing = [c for c in related if c.get("tty") in ("IN", "PIN")]
            if ing:
                generic = "; ".join(sorted({norm(c.get("name")) for c in ing if norm(c.get("name"))}))
        else:
            # SCD — clinical drug, generic is the product name (or ingredients)
            generic = pname
            # brand stays None unless you want to fetch branded siblings

        # Dose form
        df = [c for c in related if c.get("tty") == "DF"]
        dose_form = norm(df[0].get("name")) if df else None

        # Strength — prefer rxstring/name
        strength = parse_strength(rxstring or pname)

        # Route is usually encoded as the site/route adjective in the dose
        # form name (e.g. "Oral Tablet" -> Oral); not every dose form has one.
        route = route_from_dose_form(dose_form)

        rows.append({
            "ProductRxCUI": prx,
            "IngredientRxCUI": ingredient_rxcui,
            "RxTTY": tty,                    # SCD or SBD
            "PreferredName": pname,          # RxNorm name at this level
            "BrandName": brand,
            "GenericName": generic,
            "DosageForm": dose_form,
            "Route": route,
            "Strength": strength
        })
    return rows

def pack_rows_from_ndcs(product_rxcui: str, ndcs: List[str], ingredient_rxcui: str, is_current: Optional[int]) -> List[Dict]:
    rows = []
    for ndc in ndcs:
        ndc11 = ndc.replace("-", "")
        rows.append({
            "IngredientRxCUI": ingredient_rxcui,
            "ProductRxCUI": product_rxcui,
            "NDC11": ndc11,
            "PackSize": None,
            "UoM": None,
            "UnitsPerPack": None,
            "GTIN": None,
            "RefrigeratedFlag": 0,
            "IsCurrent": is_current
        })
    return rows

# -------------------------
# Main ETL
# -------------------------
ING_COLS = ["IngredientRxCUI", "GenericName", "AtcClass", "OrphanFlag", "SpecialtyFlag"]
PRD_COLS = ["ProductRxCUI", "IngredientRxCUI", "RxTTY", "PreferredName", "BrandName", "GenericName", "DosageForm", "Route", "Strength"]
PACK_COLS = ["IngredientRxCUI", "ProductRxCUI", "NDC11", "PackSize", "UoM", "UnitsPerPack", "GTIN", "RefrigeratedFlag", "IsCurrent"]

def build_from_names(names: List[str]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    client = RxNormClient()
    dim_ing_rows: List[Dict] = []
    dim_prd_rows: List[Dict] = []
    dim_pack_rows: List[Dict] = []

    for nm in names:
        nm_clean = nm.strip()
        if not nm_clean:
            continue

        # Ingredient/base concept
        rxcui_in = client.resolve_rxcui(nm_clean)
        if not rxcui_in:
            logging.warning(f"[SKIP] No RxCUI for '{nm_clean}'")
            continue

        # Ingredient row (single)
        dim_ing_rows.append(build_ingredient_row(rxcui_in, client))

        # Prescribable set
        prescribables = get_prescribable_rxcuis(nm_clean, client, rxcui_in)

        # Product rows (one per SCD/SBD)
        prd_rows = build_product_rows(rxcui_in, prescribables, client)
        dim_prd_rows.extend(prd_rows)

        # Packs per product
        total_before = len(dim_pack_rows)
        for prx in prescribables:
            cur_ndcs, hist_ndcs = client.get_all_ndcs(prx)
            dim_pack_rows.extend(pack_rows_from_ndcs(prx, cur_ndcs, rxcui_in, is_current=1))
            dim_pack_rows.extend(pack_rows_from_ndcs(prx, hist_ndcs, rxcui_in, is_current=0))
        added = len(dim_pack_rows) - total_before
        logging.info(f"[NDC] name='{nm_clean}' prescribables={len(prescribables)} -> packs+={added}")

    # De-dupe. Build with an explicit column list so an all-misses run (e.g.
    # every name failed to resolve, or the RxNav API is unreachable) still
    # produces a correctly-shaped empty frame instead of a columnless one --
    # pd.DataFrame([]) has no columns at all, which breaks any downstream
    # column access or groupby.
    dim_ing_df = pd.DataFrame(dim_ing_rows, columns=ING_COLS).drop_duplicates(subset=["IngredientRxCUI"])
    dim_prd_df = pd.DataFrame(dim_prd_rows, columns=PRD_COLS).drop_duplicates(subset=["ProductRxCUI"])
    dim_pack_df = pd.DataFrame(dim_pack_rows, columns=PACK_COLS).drop_duplicates(subset=["ProductRxCUI", "NDC11"])

    return dim_ing_df, dim_prd_df, dim_pack_df

def summarize(names: List[str], ing_df: pd.DataFrame, prd_df: pd.DataFrame, pack_df: pd.DataFrame):
    print("\n=== RxNorm Ingest Summary ===")
    print(f"Input names: {len(names)}")
    print(f"DimDrugIngredient rows: {len(ing_df):,}")
    print(f"DimDrugProduct rows: {len(prd_df):,}")
    print(f"DimProductPack (NDC) rows: {len(pack_df):,}")

    if ing_df.empty:
        print("\nNo ingredients resolved -- nothing to summarize (check network access to rxnav.nlm.nih.gov and the input names).")
        pd.DataFrame(columns=PRD_COLS + ["NDC_Count"]).to_csv(SUMMARY_OUTPUT, index=False)
        print(f"\nWrote empty {SUMMARY_OUTPUT}")
        return

    # Per ingredient — NDC counts via pack bridge
    per_ing = (pack_df.groupby("IngredientRxCUI")["NDC11"].nunique().reset_index(name="NDC_Count"))
    sample = (ing_df[["IngredientRxCUI","GenericName"]]
              .merge(per_ing, on="IngredientRxCUI", how="left")
              .fillna({"NDC_Count":0})
              .sort_values("NDC_Count", ascending=False))
    print("\nPer-Ingredient NDC counts:")
    print(sample.to_string(index=False)[:1000])

    # Optional: write a joined sample of products
    prd_sample = prd_df.head(25)
    rep = prd_sample.merge(per_ing, on="IngredientRxCUI", how="left")
    rep.to_csv(SUMMARY_OUTPUT, index=False)
    print(f"\nWrote {SUMMARY_OUTPUT}")

def write_outputs(ing_df: pd.DataFrame, prd_df: pd.DataFrame, pack_df: pd.DataFrame):
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    ing_df.reindex(columns=ING_COLS).to_csv(ING_OUTPUT, index=False)
    prd_df.reindex(columns=PRD_COLS).to_csv(PRD_OUTPUT, index=False)
    pack_df.reindex(columns=PACK_COLS).to_csv(PACK_OUTPUT, index=False)

    print(f"Wrote {ING_OUTPUT} ({len(ing_df):,} rows)")
    print(f"Wrote {PRD_OUTPUT} ({len(prd_df):,} rows)")
    print(f"Wrote {PACK_OUTPUT} ({len(pack_df):,} rows)")

def names_from_orange_book(limit: int) -> List[str]:
    """
    Derive a seed list from the active ingredients already ingested from the
    Orange Book, most-prescribed first, so RxNorm coverage tracks what's
    actually in the catalog instead of a fixed 4-drug sample.

    Combination products list every ingredient joined with "; " (e.g.
    "AMPHETAMINE ASPARTATE; AMPHETAMINE SULFATE; ...") -- those don't resolve
    as a single RxNorm concept via the name lookup, so they're excluded here.
    """
    ob_path = os.path.join(OUTPUT_DIR, "ob_dim_product.csv")
    if not os.path.exists(ob_path):
        logging.error(f"Orange Book product data not found at {ob_path}; run orange_book_ingest.py first.")
        return []
    ob = pd.read_csv(ob_path, usecols=["ActiveIngredient", "MarketingStatus"], dtype=str)
    ob = ob[ob["MarketingStatus"].isin(["RX", "OTC"])]
    ob = ob[~ob["ActiveIngredient"].str.contains(";", na=True)]
    counts = ob["ActiveIngredient"].value_counts()
    return [name.title() for name in counts.head(limit).index.tolist()]

def main():
    parser = argparse.ArgumentParser(description="Fetch RxNorm ingredient/product/NDC data for a set of drug names")
    parser.add_argument("--names", nargs="+", help="Drug names to look up (overrides the built-in seed list)")
    parser.add_argument("--from-orange-book", type=int, metavar="N", default=None,
                         help="Use the top N most-common active ingredients from data/ob_dim_product.csv instead of --names/the seed list")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    if args.from_orange_book:
        names = names_from_orange_book(args.from_orange_book)
        if not names:
            logging.error("No names derived from Orange Book data; falling back to the built-in seed list.")
            names = SEED_DRUG_NAMES
    elif args.names:
        names = args.names
    else:
        names = SEED_DRUG_NAMES

    ing_df, prd_df, pack_df = build_from_names(names)
    summarize(names, ing_df, prd_df, pack_df)
    write_outputs(ing_df, prd_df, pack_df)

    # Note: Database loading is handled by load_to_postgres.py

if __name__ == "__main__":
    main()
