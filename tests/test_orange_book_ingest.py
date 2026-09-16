"""
Regression tests for orange_book_ingest.py's column-mapping and parsing
logic. These target real bugs found in the FDA Orange Book ETL:

- PatentExpiration/DelistRequested/FileDate came out 100% empty because the
  alias map didn't recognize the actual header names in the official ZIP's
  Patent.txt (Patent_Expire_Date_Text, Delist_Flag, Submission_Date).
- ob_dim_application.csv was always empty (no Application file ships in the
  official ZIP) until it was derived from product-level rows.
- "Approved Prior to Jan 1, 1982" (the Orange Book's convention for
  pre-1982 approvals) parsed to None instead of a real date.
"""
import pandas as pd
import pytest

import orange_book_ingest as ob


# ---------------------------------------------------------------------------
# _parse_date
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("raw,expected", [
    ("2023-04-12", "2023-04-12"),
    ("Apr 12, 2023", "2023-04-12"),
    ("04/12/2023", "2023-04-12"),
    ("Approved Prior to Jan 1, 1982", "1982-01-01"),
    ("approved prior to Jan 1, 1982", "1982-01-01"),  # case-insensitive
    ("", None),
    (None, None),
    ("not a date", None),
])
def test_parse_date(raw, expected):
    assert ob._parse_date(raw) == expected


# ---------------------------------------------------------------------------
# _norm_col / _best_col / _rename_with_aliases
# ---------------------------------------------------------------------------

def test_norm_col_strips_punctuation_and_case():
    assert ob._norm_col("Patent_Expire_Date_Text") == "patentexpiredatetext"
    assert ob._norm_col("Exclusivity Date") == "exclusivitydate"
    assert ob._norm_col("Ref. Product Proper Name") == "refproductpropername"


def test_best_col_prefers_exact_match_over_substring():
    df = pd.DataFrame([{"Licensure": "Licensed", "Date of First Licensure": "2020-01-01"}])
    # "Licensure" should match itself exactly rather than the longer column
    # that happens to contain "licensure" as a substring.
    assert ob._best_col(df, "Licensure") == "Licensure"


def test_rename_with_aliases_only_renames_known_columns():
    df = pd.DataFrame([{"Appl_No": "123456", "Some_Other_Col": "x"}])
    out = ob._rename_with_aliases(df, {"applno": "ApplNo"})
    assert list(out.columns) == ["ApplNo", "Some_Other_Col"]


# ---------------------------------------------------------------------------
# map_patent_columns -- regression for the PatentExpiration/DelistRequested/
# FileDate bug (real FDA headers, not invented ones)
# ---------------------------------------------------------------------------

def _raw_patent_df():
    return pd.DataFrame([
        {
            "Appl_Type": "N", "Appl_No": "020610", "Product_No": "001",
            "Patent_No": "7625884", "Patent_Expire_Date_Text": "Aug 24, 2026",
            "Drug_Substance_Flag": None, "Drug_Product_Flag": None,
            "Patent_Use_Code": "U-141", "Delist_Flag": None,
            "Submission_Date": None,
        },
        {
            "Appl_Type": "N", "Appl_No": "018613", "Product_No": "001",
            "Patent_No": "7560445", "Patent_Expire_Date_Text": "Feb 1, 2027",
            "Drug_Substance_Flag": "Y", "Drug_Product_Flag": "Y",
            "Patent_Use_Code": "U-986", "Delist_Flag": "Y",
            "Submission_Date": "Jun 27, 2013",
        },
    ])


def test_map_patent_columns_reads_real_fda_headers():
    out = ob.map_patent_columns(_raw_patent_df())
    assert list(out["PatentExpiration"]) == ["2026-08-24", "2027-02-01"]
    assert out["DelistRequested"].tolist()[0] is None or pd.isna(out["DelistRequested"].tolist()[0])
    assert out["DelistRequested"].tolist()[1] == "Y"
    assert list(out["FileDate"])[1] == "2013-06-27"
    assert list(out["ApplNo"]) == ["020610", "018613"]
    assert list(out["DrugSubstanceFlag"])[1] == "Y"


def test_map_patent_columns_empty_input():
    out = ob.map_patent_columns(None)
    assert out.empty
    assert list(out.columns) == [
        "ApplNo", "ProductNo", "PatentNo", "PatentExpiration",
        "DrugSubstanceFlag", "DrugProductFlag", "PatentUseCode",
        "DelistRequested", "PediatricExtension", "FileDate",
    ]
    assert ob.map_patent_columns(pd.DataFrame()).empty


# ---------------------------------------------------------------------------
# map_exclusivity_columns
# ---------------------------------------------------------------------------

def test_map_exclusivity_columns_reads_real_fda_headers():
    raw = pd.DataFrame([
        {"Appl_Type": "N", "Appl_No": "017031", "Product_No": "001",
         "Exclusivity_Code": "RTO", "Exclusivity_Date": "Jul 13, 2026"},
    ])
    out = ob.map_exclusivity_columns(raw)
    assert out.iloc[0]["ExclusivityCode"] == "RTO"
    assert out.iloc[0]["ExclusivityEndDate"] == "2026-07-13"
    assert out.iloc[0]["ApplNo"] == "017031"


def test_map_exclusivity_columns_empty_input():
    assert ob.map_exclusivity_columns(None).empty
    assert ob.map_exclusivity_columns(pd.DataFrame()).empty


# ---------------------------------------------------------------------------
# derive_application_from_products -- regression for ob_dim_application.csv
# always being empty
# ---------------------------------------------------------------------------

APP_COLS = ["ApplNo", "ApplType", "ApplicantName", "ApplStatus", "FirstApprovalDate", "LastUpdateDate"]


def test_derive_application_rolls_up_one_row_per_appl_no():
    prod = pd.DataFrame([
        {"ApplNo": "205613", "ApplType": "N", "ApplicantName": "SALIX", "MarketingStatus": "RX", "ApprovalDate": "Oct 7, 2014"},
        {"ApplNo": "205613", "ApplType": "N", "ApplicantName": "SALIX", "MarketingStatus": "DISCN", "ApprovalDate": "Oct 7, 2014"},
        {"ApplNo": "999999", "ApplType": "A", "ApplicantName": "GENERIC CO", "MarketingStatus": "DISCN", "ApprovalDate": "Jan 1, 2000"},
    ])
    out = ob.derive_application_from_products(prod, APP_COLS)
    assert set(out.columns) == set(APP_COLS)
    assert len(out) == 2

    salix = out[out["ApplNo"] == "205613"].iloc[0]
    assert salix["ApplicantName"] == "SALIX"
    assert salix["FirstApprovalDate"] == "2014-10-07"
    # One product is still RX, so the application counts as active even
    # though another product line under it was discontinued.
    assert salix["ApplStatus"] == "Active"

    generic = out[out["ApplNo"] == "999999"].iloc[0]
    assert generic["ApplStatus"] == "Discontinued"


def test_derive_application_handles_pre_1982_approval_convention():
    prod = pd.DataFrame([
        {"ApplNo": "000004", "ApplType": "N", "ApplicantName": "PHARMICS INC",
         "MarketingStatus": "DISCN", "ApprovalDate": "Approved Prior to Jan 1, 1982"},
    ])
    out = ob.derive_application_from_products(prod, APP_COLS)
    assert out.iloc[0]["FirstApprovalDate"] == "1982-01-01"


def test_derive_application_empty_when_no_appl_no_column():
    out = ob.derive_application_from_products(pd.DataFrame({"foo": [1, 2]}), APP_COLS)
    assert out.empty
    assert list(out.columns) == APP_COLS
