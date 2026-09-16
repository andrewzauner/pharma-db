"""
Regression tests for rxnorm_ingest.py. Each targets a real bug found while
auditing the RxNorm ETL:

- parse_strength's regex tried "mg" before "mg/mL" in its alternation, so
  concentration strengths like "10 mg/mL" matched only "10 mg", silently
  dropping the "/mL".
- Route was hardcoded to None; RxNorm's dose form name usually encodes it.
- build_from_names() built its output frames with pd.DataFrame(rows) and no
  explicit columns, so a run where every name failed to resolve (bad names,
  or the RxNav API unreachable) produced a columnless empty frame and
  crashed downstream instead of reporting "0 rows".

These tests don't hit the network: RxNormClient/_get is monkeypatched with a
small fixture mimicking real RxNav JSON response shapes.
"""
import pandas as pd
import pytest

import rxnorm_ingest as rx


# ---------------------------------------------------------------------------
# parse_strength -- regression for the mg vs mg/mL regex ordering bug
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("text,expected", [
    ("10 mg/mL Injectable Solution", "10 mg/mL"),
    ("500 mg Oral Tablet", "500 mg"),
    ("2.5 mcg/mL", "2.5 mcg/mL"),
    ("100 units/mL", "100 units/mL"),
    ("2%", "2%"),
    ("", None),
    (None, None),
])
def test_parse_strength(text, expected):
    assert rx.parse_strength(text) == expected


# ---------------------------------------------------------------------------
# route_from_dose_form -- regression for Route always being None
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dose_form,expected", [
    ("Oral Tablet", "Oral"),
    ("Injectable Solution", "Injectable"),
    ("Extended Release Oral Tablet", "Oral"),
    ("Disintegrating Oral Tablet", "Oral"),
    ("Topical Cream", "Topical"),
    ("Transdermal System", "Transdermal"),
    (None, None),
    ("Auto-Injector", None),      # no route encoded -- must not guess
    ("Chewable Tablet", None),    # modifier only, no route -- must not guess
])
def test_route_from_dose_form(dose_form, expected):
    assert rx.route_from_dose_form(dose_form) == expected


# ---------------------------------------------------------------------------
# build_from_names -- full pipeline against a mocked RxNav client
# ---------------------------------------------------------------------------

def _fake_rxnav(monkeypatch):
    """Mimics real RxNav JSON shapes for a single ingredient with two products."""
    responses = {
        ("/rxcui.json", "acetaminophen"): {"idGroup": {"rxnormId": ["161"]}},
        ("/rxcui/161/properties.json", None): {"properties": {"rxcui": "161", "name": "acetaminophen", "tty": "IN"}},
        ("/rxcui/161/allrelated.json", None): {"allRelatedGroup": {"conceptGroup": []}},
        ("/drugs.json", "acetaminophen"): {"drugGroup": {"conceptGroup": [
            {"tty": "SCD", "conceptProperties": [{"rxcui": "313782", "name": "acetaminophen 325 MG Oral Tablet"}]},
        ]}},
        ("/rxcui/313782/properties.json", None): {"properties": {
            "rxcui": "313782", "name": "acetaminophen 325 MG Oral Tablet", "tty": "SCD",
            "rxstring": "acetaminophen 325 MG Oral Tablet",
        }},
        ("/rxcui/313782/allrelated.json", None): {"allRelatedGroup": {"conceptGroup": [
            {"tty": "DF", "conceptProperties": [{"rxcui": "317541", "name": "Oral Tablet", "tty": "DF"}]},
        ]}},
        ("/rxcui/313782/ndcs.json", None): {"ndcGroup": {"ndcList": {"ndc": ["00093-0311-01"]}}},
        ("/rxcui/313782/allndcs.json", None): {"ndcGroup": {"ndcList": {"ndc": ["00093-0311-01", "00093-0311-99"]}}},
    }

    def fake_get(url, params=None):
        path = url.replace(rx.RXNAV_BASE, "")
        key_param = (params or {}).get("name")
        result = responses.get((path, key_param))
        if result is None:
            result = responses.get((path, None))
        return result

    monkeypatch.setattr(rx, "_get", fake_get)


def test_build_from_names_happy_path(monkeypatch):
    _fake_rxnav(monkeypatch)
    ing_df, prd_df, pack_df = rx.build_from_names(["acetaminophen"])

    assert len(ing_df) == 1
    assert ing_df.iloc[0]["GenericName"] == "acetaminophen"

    assert len(prd_df) == 1
    row = prd_df.iloc[0]
    assert row["Route"] == "Oral"
    assert row["Strength"] == "325 MG"

    assert len(pack_df) == 2
    current = pack_df[pack_df["IsCurrent"] == 1]
    historical = pack_df[pack_df["IsCurrent"] == 0]
    assert len(current) == 1 and current.iloc[0]["NDC11"] == "00093031101"
    assert len(historical) == 1 and historical.iloc[0]["NDC11"] == "00093031199"


def test_build_from_names_total_failure_returns_correctly_shaped_empty_frames(monkeypatch):
    """
    Regression test: when every name fails to resolve (bad names, or the
    RxNav API unreachable), build_from_names() must still return frames with
    the right columns -- not a columnless pd.DataFrame([]) that crashes any
    downstream .groupby() or column access.
    """
    monkeypatch.setattr(rx, "_get", lambda url, params=None: None)

    ing_df, prd_df, pack_df = rx.build_from_names(["not-a-real-drug"])

    assert ing_df.empty and list(ing_df.columns) == rx.ING_COLS
    assert prd_df.empty and list(prd_df.columns) == rx.PRD_COLS
    assert pack_df.empty and list(pack_df.columns) == rx.PACK_COLS

    # summarize() must degrade gracefully too, not crash on the groupby.
    rx.summarize(["not-a-real-drug"], ing_df, prd_df, pack_df)


def test_build_from_names_skips_blank_names(monkeypatch):
    _fake_rxnav(monkeypatch)
    ing_df, prd_df, pack_df = rx.build_from_names(["", "  ", "acetaminophen"])
    assert len(ing_df) == 1
