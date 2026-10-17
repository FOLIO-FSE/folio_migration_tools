"""Tests for the SQLite report store and TranslatedStr."""

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
    path = tmp_path / "r" / "m.sqlite"
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
