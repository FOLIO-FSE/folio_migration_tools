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


def test_current_record_captured_lazily_once(tmp_path):
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    calls = []
    store.set_current_record(
        ["b2", "a1"], "f.mrc", 3, "marc", lambda: calls.append(1) or b"raw"
    )
    assert store.conn.execute("SELECT count(*) FROM legacy_record").fetchone()[0] == 0
    store.add_issue("warning", ["b2", "a1"], "one", "v")
    store.add_issue("warning", "a1", "two", "v")
    store.add_issue("warning", "other", "three", "v")
    store.flush()
    assert calls == [1]
    rec = store.conn.execute("SELECT record_id, payload FROM legacy_record").fetchone()
    assert rec == ("a1", b"raw")
    assert store.conn.execute("SELECT count(*) FROM record_alias").fetchone()[0] == 2
    pks = [r[0] for r in store.conn.execute("SELECT record_pk FROM data_issue ORDER BY id")]
    assert pks[0] == pks[1] and pks[2] is None
    store.close()


def test_marc_processor_sets_current_record(tmp_path):
    from unittest.mock import Mock

    from pymarc import Field, Record, Subfield

    from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
        MarcFileProcessor,
    )
    from folio_migration_tools.report_store import set_active_store

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        record = Record()
        record.add_field(Field(tag="001", data="x1"))
        record.add_field(Field(tag="245", indicators=[" ", " "], subfields=[Subfield("a", "t")]))
        proc = MarcFileProcessor.__new__(MarcFileProcessor)
        proc.records_count = 0
        proc.mapper = Mock()
        proc.mapper.get_legacy_ids.return_value = ["x1"]
        proc.mapper.parse_record.return_value = []
        proc.legacy_ids = set()
        proc.failed_records_count = 0
        proc.exit_on_too_many_exceptions = Mock()
        file_def = Mock(file_name="f.mrc")
        proc.process_record(4, record, file_def)
        store.add_issue("warning", ["x1"], "msg", "v")
        store.flush()
        row = store.conn.execute(
            "SELECT record_id, source_file, source_index, format FROM legacy_record"
        ).fetchone()
        assert row == ("x1", "f.mrc", 4, "marc")
    finally:
        set_active_store(None)
        store.close()


def test_issue_location_and_helpers(tmp_path):
    from folio_migration_tools.helper import Helper
    from folio_migration_tools.report_store import (
        column_location,
        marc_location,
        set_active_store,
    )

    assert marc_location("245") == ("marc", "245")
    assert marc_location("245", "a", 0) == ("marc", "245[0]$a")
    assert marc_location("LDR", span=5) == ("marc", "LDR/5")
    assert marc_location("008", span=(35, 37)) == ("marc", "008/35-37")
    assert marc_location("245", "a", 0, (3, 5)) == ("marc", "245[0]$a/3-5")
    assert column_location("barcode") == ("column", "barcode")
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        Helper.log_data_issue("id1", "bad", "v", marc_location("700", "e"))
        Helper.log_data_issue("id2", "no location", "v")
        store.flush()
        rows = store.conn.execute(
            "SELECT location_kind, location_path FROM data_issue ORDER BY id"
        ).fetchall()
        assert rows == [("marc", "700$e"), (None, None)]
    finally:
        set_active_store(None)
        store.close()


def test_marc_location_uses_occurrence_of_current_field(tmp_path):
    from pymarc import Field, Record, Subfield

    from folio_migration_tools.report_store import iter_fields, marc_location, set_active_store

    record = Record()
    for name in ("a", "b", "c"):
        record.add_field(Field("700", [" ", " "], [Subfield("a", name), Subfield("e", name)]))
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        paths = []
        for field in iter_fields(record):
            if field["a"] == "b":
                paths.append(marc_location(field.tag, "e")[1])
        assert paths == ["700[1]$e"]
        assert store.current_field is None
        assert marc_location("700", "e")[1] == "700$e"
        assert marc_location("700", "e", 2)[1] == "700[2]$e"
    finally:
        set_active_store(None)
        store.close()


def test_print_mapping_report_stores_mapping_stats(tmp_path):
    import io

    from folio_migration_tools.helper import Helper
    from folio_migration_tools.report_store import set_active_store

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        Helper.print_mapping_report(
            io.StringIO(), 10, {"barcode": [8], "title": [10]}, {"BARCODE": [9, 8]}
        )
        rows = store.conn.execute(
            "SELECT side, field_name, present, mapped FROM mapping_stat ORDER BY side, field_name"
        ).fetchall()
        assert rows == [
            ("folio", "barcode", None, 8),
            ("folio", "title", None, 10),
            ("legacy", "BARCODE", 9, 8),
        ]
        total = store.conn.execute("SELECT mapping_total_records FROM run").fetchone()[0]
        assert total == 10
    finally:
        set_active_store(None)
        store.close()


def test_reference_files_are_snapshotted_and_deduplicated(tmp_path):
    import csv
    import zlib

    from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
    from folio_migration_tools.report_store import set_active_store

    csv.register_dialect("tsv", delimiter="\t")
    tsv = tmp_path / "locations.tsv"
    tsv.write_text("legacy_code\tfolio_code\nmain\tMAIN\nbranch\tBR\n")
    mapping = tmp_path / "item_mapping.json"
    mapping.write_text('{"data": []}')
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        rows = MigrationTaskBase.load_ref_data_mapping_file("permanentLocationId", tsv, [])
        MigrationTaskBase.load_ref_data_mapping_file("temporaryLocationId", tsv, [], True)
        store.capture_reference_file("mapping", mapping)
        files = store.conn.execute(
            "SELECT role, format, header_json, size FROM reference_file ORDER BY role"
        ).fetchall()
        assert files == [
            ("mapping", "json", None, len(b'{"data": []}')),
            ("permanentLocationId", "tsv", '["legacy_code", "folio_code"]', tsv.stat().st_size),
            ("temporaryLocationId", "tsv", '["legacy_code", "folio_code"]', tsv.stat().st_size),
        ]
        assert store.conn.execute("SELECT COUNT(*) FROM reference_content").fetchone()[0] == 2
        content = store.conn.execute(
            "SELECT content FROM reference_content WHERE sha256 = (SELECT sha256 FROM"
            " reference_file WHERE role = 'permanentLocationId')"
        ).fetchone()[0]
        assert zlib.decompress(content) == tsv.read_bytes()
        stored = store.conn.execute(
            "SELECT row_json FROM reference_row r JOIN reference_file f USING (ref_pk)"
            " WHERE f.role = 'permanentLocationId' ORDER BY row_index"
        ).fetchall()
        assert [json.loads(r[0]) for r in stored] == rows
    finally:
        set_active_store(None)
        store.close()
