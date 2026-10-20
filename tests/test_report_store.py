"""Tests for the SQLite report store and TranslatedStr."""

import json
import sqlite3

import pytest

from folio_migration_tools.i18n_cache import TranslatedStr, i18n_t
from folio_migration_tools.report_store import ReportStore, normalize_record_ref


def test_translated_str_keeps_key_and_params():
    s = i18n_t("Some key that is missing %{x}", x="1")
    assert isinstance(s, TranslatedStr)
    assert s.key == "Some key that is missing %{x}"
    assert s.params == {"x": "1"}


@pytest.mark.parametrize(
    "value,ids,kind",
    [
        (["b123"], ["b123"], "legacy_id"),
        (["a", "b"], ["a", "b"], "legacy_id"),
        ("['b123']", ["b123"], "legacy_id"),
        ("['a', 'b']", ["a", "b"], "legacy_id"),
        (12, ["12"], "row_number"),
        ("", [], "none"),
        (None, [], "none"),
        ([], [], "none"),
        ("[not a list", ["[not a list"], "legacy_id"),
        ("12345", ["12345"], "legacy_id"),
    ],
)
def test_normalize_record_ref(value, ids, kind):
    ref = normalize_record_ref(value)
    assert ref.ids == ids
    assert ref.kind == kind


def test_store_replaces_existing_db(tmp_path):
    path = tmp_path / "m.sqlite"
    store = ReportStore(path)
    store.start_run("t")
    store.add_issue("DATA ISSUE", "x", "m", "v")
    store.close()
    store = ReportStore(path)
    store.start_run("t")
    store.close()
    conn = sqlite3.connect(path)
    assert conn.execute("SELECT count(*) FROM data_issue").fetchone()[0] == 0


def test_issues_and_stats_round_trip(tmp_path):
    store = ReportStore(tmp_path / "m.sqlite")
    store.start_run("task")
    msg = i18n_t("Dup %{n}", n="1")
    store.add_issue("DATA ISSUE", ["b1"], msg, "val")
    store.add_issue("RECORD FAILED", 7, "plain", None)
    store.write_report_stats({"Sec": {"blurb_id": "Sec", msg: 3, "lit": 2}})
    store.finish_run("now")
    store.close()
    conn = sqlite3.connect(tmp_path / "m.sqlite")
    rows = conn.execute(
        "SELECT record_id, record_id_kind, message_key, translated FROM data_issue ORDER BY id"
    ).fetchall()
    assert rows == [("b1", "legacy_id", "Dup %{n}", 1), ("7", "row_number", "plain", 0)]
    stats = {
        r[0]: (r[1], r[2])
        for r in conn.execute("SELECT measure_key, translated, count FROM report_stat")
    }
    assert stats == {"Dup %{n}": (1, 3), "lit": (0, 2)}


def test_active_store_collects_reports_and_issues(tmp_path):
    from folio_migration_tools.helper import Helper
    from folio_migration_tools.migration_report import MigrationReport
    from folio_migration_tools.report_store import get_active_store, set_active_store

    store = ReportStore(tmp_path / "m.sqlite")
    store.start_run("task")
    set_active_store(store)
    try:
        report = MigrationReport()
        report.add_general_statistics(i18n_t("Number of records processed"))
        report.add_general_statistics(i18n_t("Number of records processed"))
        Helper.log_data_issue(["b1"], i18n_t("Some issue"), "val")
        Helper.log_data_issue_failed("", "plain failure", "x")
        store.finalize("done")
    finally:
        set_active_store(None)
    assert get_active_store() is None
    conn = sqlite3.connect(tmp_path / "m.sqlite")
    assert conn.execute("SELECT section_id, measure_key, count FROM report_stat").fetchall() == [
        ("GeneralStatistics", "Number of records processed", 2)
    ]
    assert conn.execute(
        "SELECT issue_type, record_id, record_id_kind FROM data_issue ORDER BY id"
    ).fetchall() == [("DATA ISSUE", "b1", "legacy_id"), ("RECORD FAILED", "", "none")]
    assert conn.execute("SELECT finished FROM run").fetchone()[0] == "done"


def test_helper_without_active_store_does_not_fail():
    from folio_migration_tools.helper import Helper

    Helper.log_data_issue("x", "m", "v")


def test_store_requires_existing_folder(tmp_path):
    with pytest.raises(FileNotFoundError):
        ReportStore(tmp_path / "missing" / "m.sqlite")


def test_composed_text_round_trip(tmp_path):
    store = ReportStore(tmp_path / "m.sqlite")
    store.start_run("t")
    composed = i18n_t("Unmapped code") + ": 'x'"
    store.write_report_stats({"S": {composed: 3, "plain": 1}})
    store.add_issue("WARNING", "id1", composed, "v")
    store.finish_run("now")
    stat = store.conn.execute(
        "SELECT translated, params_json, count FROM report_stat WHERE measure_key = ?",
        (str(composed),),
    ).fetchone()
    assert stat[0] == 2 and stat[2] == 3
    parts = json.loads(stat[1])["parts"]
    assert parts[0]["key"] == "Unmapped code" and parts[1] == {"text": ": 'x'"}
    issue = store.conn.execute("SELECT translated, params_json FROM data_issue").fetchone()
    assert issue[0] == 2 and json.loads(issue[1])["parts"] == parts
    store.close()
