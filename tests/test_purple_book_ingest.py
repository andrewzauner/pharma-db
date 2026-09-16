"""
Regression tests for purple_book_ingest.py. Each of these mirrors a real bug
found in the FDA Purple Book ETL:

- _map_biosimilar aliased two distinct source columns (the reference
  product's brand name and its generic name; "Approval Date" and "Date of
  First Licensure") onto the same output column name. pandas silently
  resolves a rename collision into duplicate columns instead of erroring,
  so this went unnoticed until the output CSV was inspected by hand.
- _map_application had the same collision for "Licensure" vs "Marketing
  Status" both aliased to "Status".
- _map_exclusivity's Orphan Drug Exclusivity matcher checked for the
  substring "orphexclusivityexpdate" -- missing the "an" from "orphan" --
  so it never matched the real column and every Orphan Exclusivity date
  was silently dropped.
- _read_purple_book_csv used readlines() + per-line csv.reader, so a
  legitimately quoted field spanning two physical lines (a multi-line
  Strength value) was split into two garbage, column-shifted rows.
"""
import os
import tempfile

import pandas as pd

import purple_book_ingest as pb


# ---------------------------------------------------------------------------
# _map_biosimilar -- regression for the ReferenceProductName/LicensureDate
# column-collision bug
# ---------------------------------------------------------------------------

def _raw_biosimilar_df():
    return pd.DataFrame([
        {
            "BLA Number": "761439", "Proprietary Name": "Enoby", "Proper Name": "denosumab-qbde",
            "Ref. Product Proper Name": "denosumab", "Ref. Product Proprietary Name": "Prolia",
            "Approval Date": "26-Sep-25", "Date of First Licensure": None,
        },
        {
            # No brand name for the reference product -- should fall back to
            # the generic (proper) name instead of leaving it blank.
            "BLA Number": "125384", "Proprietary Name": "Kedbumin", "Proper Name": "Albumin (Human)",
            "Ref. Product Proper Name": "N/A", "Ref. Product Proprietary Name": "N/A",
            "Approval Date": "3-Jun-11", "Date of First Licensure": None,
        },
    ])


def test_map_biosimilar_has_no_duplicate_columns():
    out = pb._map_biosimilar(_raw_biosimilar_df())
    assert not out.columns.duplicated().any()
    assert list(out.columns) == [
        "BLANumber", "BiosimilarProprietaryName", "BiosimilarProperName",
        "ReferenceProductName", "ReferenceBLANumber", "InterchangeabilityFlag", "LicensureDate",
    ]


def test_map_biosimilar_prefers_brand_name_falls_back_to_generic():
    out = pb._map_biosimilar(_raw_biosimilar_df())
    row = out[out["BLANumber"] == "761439"].iloc[0]
    assert row["ReferenceProductName"] == "Prolia"  # brand, not "denosumab"

    row2 = out[out["BLANumber"] == "125384"].iloc[0]
    assert pd.isna(row2["ReferenceProductName"])  # both sides were N/A


def test_map_biosimilar_licensure_date_is_parsed():
    out = pb._map_biosimilar(_raw_biosimilar_df())
    row = out[out["BLANumber"] == "761439"].iloc[0]
    assert row["LicensureDate"] == "2025-09-26"


# ---------------------------------------------------------------------------
# _map_application -- regression for the Status column-collision bug
# ---------------------------------------------------------------------------

def test_map_application_has_no_duplicate_columns_and_prefers_licensure_status():
    raw = pd.DataFrame([
        {"BLA Number": "125197", "Applicant": "Dendreon Pharmaceuticals LLC",
         "Licensure": "Licensed", "Marketing Status": "Rx", "Approval Date": "29-Apr-10"},
    ])
    out = pb._map_application(raw)
    assert not out.columns.duplicated().any()
    assert out.iloc[0]["Status"] == "Licensed"


def test_map_application_falls_back_to_marketing_status_when_no_licensure_col():
    raw = pd.DataFrame([
        {"BLA Number": "125197", "Applicant": "Dendreon Pharmaceuticals LLC",
         "Marketing Status": "Rx", "Approval Date": "29-Apr-10"},
    ])
    out = pb._map_application(raw)
    assert out.iloc[0]["Status"] == "Rx"


# ---------------------------------------------------------------------------
# _map_exclusivity -- regression for the Orphan Exclusivity typo
# ---------------------------------------------------------------------------

def test_map_exclusivity_captures_orphan_exclusivity_date():
    raw = pd.DataFrame([
        {"BLA Number": "19640", "Orphan Exclusivity Exp. Date": "1-Nov-13",
         "Exclusivity Expiration Date": None,
         "First Interchangeable Exclusivity Exp. Date": None,
         "Ref. Product Exclusivity Exp. Date": None},
    ])
    out = pb._map_exclusivity(raw, pd.DataFrame())
    assert len(out) == 1
    assert out.iloc[0]["ExclusivityType"] == "OrphanExclusivityExpDate"
    assert out.iloc[0]["ExclusivityEndDate"] == "2013-11-01"


def test_map_exclusivity_handles_multiple_types_for_one_product():
    raw = pd.DataFrame([
        {"BLA Number": "19640", "Orphan Exclusivity Exp. Date": "1-Nov-13",
         "Exclusivity Expiration Date": "1-Jan-15",
         "First Interchangeable Exclusivity Exp. Date": None,
         "Ref. Product Exclusivity Exp. Date": None},
    ])
    out = pb._map_exclusivity(raw, pd.DataFrame())
    assert len(out) == 2
    assert set(out["ExclusivityType"]) == {"OrphanExclusivityExpDate", "ExclusivityExpirationDate"}


# ---------------------------------------------------------------------------
# _read_purple_book_csv -- regression for the multi-line-quoted-field bug
# ---------------------------------------------------------------------------

def test_read_purple_book_csv_handles_embedded_newline_in_quoted_field():
    # A legitimately quoted CSV field with an embedded newline (Purple Book
    # sometimes wraps a Strength value like this). This must be parsed as
    # ONE row, not split into two column-shifted rows.
    content = (
        "Purple Book Monthly Historical Data Changes Report\n"
        "\n"
        "Newly Approved Products\n"
        "N/R/U,Applicant,BLA Number,Proprietary Name,Proper Name,BLA Type,Strength,"
        "Dosage Form,Route of Administration\n"
        'U,"Genentech, Inc.",125276,Actemra,tocilizumab,351(a),"80MG/4ML\n'
        '(20MG/ML)",Injection,Intravenous\n'
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        f.write(content)
        path = f.name
    try:
        df = pb._read_purple_book_csv(path)
    finally:
        os.unlink(path)

    assert len(df) == 1
    row = df.iloc[0]
    assert row["Strength"] == "80MG/4ML\n(20MG/ML)"
    assert row["BLA Number"] == "125276"
    assert row["Dosage Form"] == "Injection"


def test_read_purple_book_csv_skips_duplicate_header_rows():
    content = (
        "Purple Book Report\n"
        "\n"
        "N/R/U,Applicant,BLA Number,Proprietary Name,Proper Name\n"
        "U,Acme Inc,111111,Foo,foo-ingredient\n"
        "N/R/U,Applicant,BLA Number,Proprietary Name,Proper Name\n"
        "U,Acme Inc,222222,Bar,bar-ingredient\n"
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        f.write(content)
        path = f.name
    try:
        df = pb._read_purple_book_csv(path)
    finally:
        os.unlink(path)

    assert len(df) == 2
    assert list(df["BLA Number"]) == ["111111", "222222"]
