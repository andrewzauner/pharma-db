"""
Tests for validate_data.py's own checks -- these are the safety net for the
bug class found repeatedly in the ingest scripts (a source column silently
not mapping, leaving an output column 100% empty), so the checker itself
needs coverage too.
"""
import os
import tempfile

import pandas as pd

import validate_data as vd


def _write_csv(tmp_path, name, header_line, *data_lines):
    path = os.path.join(tmp_path, name)
    with open(path, "w", encoding="utf-8") as f:
        f.write(header_line + "\n")
        for line in data_lines:
            f.write(line + "\n")
    return path


def test_raw_header_dupes_detects_duplicate_header(tmp_path):
    path = _write_csv(tmp_path, "dup.csv", "A,B,B,C", "1,2,3,4")
    assert vd._raw_header_dupes(path) == ["B"]


def test_raw_header_dupes_clean_header(tmp_path):
    path = _write_csv(tmp_path, "clean.csv", "A,B,C", "1,2,3")
    assert vd._raw_header_dupes(path) == []


def test_check_table_flags_pandas_hidden_duplicate_columns(tmp_path):
    """
    pandas.read_csv silently renames a duplicate header ("B", "B") to
    ("B", "B.1") on read, which would hide the collision from a plain
    df.columns.duplicated() check done *after* loading. check_table must
    catch it from the raw header line instead.
    """
    vd.DATA_DIR = str(tmp_path)
    _write_csv(tmp_path, "dup.csv", "ApplNo,Status,Status", "1,Licensed,Rx")
    report = vd.Report()
    vd.check_table(report, "dup_table", "dup.csv")
    assert any("duplicate column" in e and "Status" in e for e in report.errors)


def test_check_table_flags_unexpected_all_null_column(tmp_path):
    vd.DATA_DIR = str(tmp_path)
    _write_csv(tmp_path, "t.csv", "ApplNo,PatentExpiration", "1,", "2,")
    report = vd.Report()
    vd.check_table(report, "t", "t.csv")
    assert any("PatentExpiration" in w and "100% null" in w for w in report.warnings)


def test_check_table_allows_documented_empty_column(tmp_path):
    vd.DATA_DIR = str(tmp_path)
    _write_csv(tmp_path, "t.csv", "ApplNo,Notes", "1,", "2,")
    report = vd.Report()
    vd.check_table(report, "t", "t.csv", expected_empty_cols=["Notes"])
    assert not any("Notes" in w for w in report.warnings)
    assert not report.errors


def test_check_table_flags_empty_table(tmp_path):
    vd.DATA_DIR = str(tmp_path)
    _write_csv(tmp_path, "empty.csv", "ApplNo,Status")
    report = vd.Report()
    vd.check_table(report, "empty_table", "empty.csv")
    assert any("empty" in e for e in report.errors)


def test_check_table_flags_missing_file(tmp_path):
    vd.DATA_DIR = str(tmp_path)
    report = vd.Report()
    vd.check_table(report, "missing", "does_not_exist.csv")
    assert any("missing" in e for e in report.errors)


def test_check_foreign_key_flags_orphans():
    report = vd.Report()
    fact_df = pd.DataFrame({"ApplNo": ["1", "2", "3"]})
    dim_df = pd.DataFrame({"ApplNo": ["1", "2"]})
    vd.check_foreign_key(report, "fact", fact_df, ["ApplNo"], "dim", dim_df, ["ApplNo"])
    assert len(report.warnings) == 1
    assert "1/3" in report.warnings[0] or "3" in report.warnings[0]


def test_check_foreign_key_clean_when_all_present():
    report = vd.Report()
    fact_df = pd.DataFrame({"ApplNo": ["1", "2"]})
    dim_df = pd.DataFrame({"ApplNo": ["1", "2"]})
    vd.check_foreign_key(report, "fact", fact_df, ["ApplNo"], "dim", dim_df, ["ApplNo"])
    assert not report.warnings
