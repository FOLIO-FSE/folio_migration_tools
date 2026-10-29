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
    store.set_current_record(["b2", "a1"], "f.mrc", 3, "marc", lambda: calls.append(1) or b"raw")
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


def test_scrub_secrets_masks_nested_secret_keys():
    from folio_migration_tools.report_store import MASK_TOKEN, scrub_secrets

    cfg = {"name": "x", "folioPassword": "pw", "nested": [{"apiKey": "k", "ok": 1}]}
    assert scrub_secrets(cfg) == {
        "name": "x",
        "folioPassword": MASK_TOKEN,
        "nested": [{"apiKey": MASK_TOKEN, "ok": 1}],
    }


def test_task_configurations_declare_no_secret_fields():
    import importlib
    import pkgutil

    import folio_migration_tools.migration_tasks as pkg
    from folio_migration_tools.report_store import _SECRET_KEY
    from folio_migration_tools.task_configuration import AbstractTaskConfiguration

    for mod in pkgutil.iter_modules(pkg.__path__):
        importlib.import_module(f"{pkg.__name__}.{mod.name}")

    def subclasses(cls):
        for sub in cls.__subclasses__():
            yield sub
            yield from subclasses(sub)

    for model in subclasses(AbstractTaskConfiguration):
        for name in model.model_fields:
            assert not _SECRET_KEY.search(name), f"{model.__name__}.{name}"


def _marc_bytes():
    import pymarc

    rec = pymarc.Record()
    rec.add_field(pymarc.Field("001", data="b1"))
    rec.add_field(
        pymarc.Field(
            "100",
            indicators=[" ", " "],
            subfields=[pymarc.Subfield("a", "Secret, Person"), pymarc.Subfield("d", "1900")],
        )
    )
    return rec.as_marc()


def test_masked_marc_and_json_never_reach_db(tmp_path):
    from folio_migration_tools.report_store import MASK_TOKEN

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    store.configure_privacy(True, ["100$a", "ssn"])
    store.set_current_record("b1", "f.mrc", 0, "marc", _marc_bytes)
    store.add_issue("DATA ISSUE", "b1", "m", "Secret, Person", "marc", "100[0]$a")
    store.set_current_record(
        "r2", "p.tsv", 1, "json", lambda: json.dumps({"ssn": "123", "n": "x"})
    )
    store.add_issue("DATA ISSUE", "r2", "m", "123", "column", "ssn")
    store.flush()
    rows = store.conn.execute("SELECT payload, masked, mask_note FROM legacy_record").fetchall()
    assert all(r[1] == 1 and "f.mrc" in (r[2] or "") + "f.mrc" for r in rows)
    blob = b"".join(r[0] for r in rows)
    assert b"Secret" not in blob and b"123" not in blob and b"1900" in blob
    assert MASK_TOKEN.encode() in blob
    legacy = [r[0] for r in store.conn.execute("SELECT legacy_value FROM data_issue")]
    assert legacy == [MASK_TOKEN, MASK_TOKEN]
    store.close()


def test_capture_can_be_disabled_and_default_is_unmasked(tmp_path):
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    store.set_current_record("b1", "f", 0, "json", lambda: '{"a": 1}')
    store.add_issue("DATA ISSUE", "b1", "m", "v")
    store.configure_privacy(False)
    store.set_current_record("b2", "f", 1, "json", lambda: '{"a": 2}')
    store.add_issue("DATA ISSUE", "b2", "m", "v")
    rows = store.conn.execute(
        "SELECT record_id, payload, masked FROM legacy_record ORDER BY record_id"
    ).fetchall()
    assert rows == [("b1", b'{"a": 1}', 0), ("b2", None, 0)]
    store.close()


def test_file_index_record_collects_parse_issues(tmp_path):
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    store.set_current_record("f.mrc:3", "f.mrc", 3, "marc", lambda: b"rawbytes", kind="file_index")
    store.add_issue("DATA ISSUE", "some legacy label", "bad record", "ctx")
    store.flush()
    row = store.conn.execute(
        "SELECT l.record_id_kind, l.payload FROM data_issue d JOIN legacy_record l"
        " ON d.record_pk = l.record_pk"
    ).fetchone()
    assert row == ("file_index", b"rawbytes")
    store.close()


def test_clear_current_record_stops_adoption_by_blank_ids(tmp_path):
    from folio_migration_tools.report_store import clear_current_record, set_active_store

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        store.set_current_record("12", "f", 12, "json", lambda: "{}")
        clear_current_record()
        store.add_issue("DATA ISSUE", "", "m", "v")
        store.flush()
        assert store.conn.execute("SELECT record_pk FROM data_issue").fetchone() == (None,)
    finally:
        set_active_store(None)
        store.close()


def test_capture_row_file_index_links_barcode_issues(tmp_path):
    from folio_migration_tools.report_store import capture_row, set_active_store

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        capture_row(4, "loans file", {"item_barcode": "i1"}, kind="file_index")
        store.add_issue("DATA ISSUE", "i1-barcode", "m", "v")
        capture_row(4, "loans file", {"item_barcode": "i1"}, kind="file_index")
        store.add_issue("DATA ISSUE", "p1-barcode", "m", "v")
        store.flush()
        pks = {r[0] for r in store.conn.execute("SELECT record_pk FROM data_issue")}
        assert len(pks) == 1 and None not in pks
        assert store.conn.execute("SELECT count(*) FROM legacy_record").fetchone() == (1,)
    finally:
        set_active_store(None)
        store.close()


def test_same_row_index_in_different_files_is_not_merged(tmp_path):
    from folio_migration_tools.report_store import capture_row, set_active_store

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        for name in ("a.tsv", "b.tsv"):
            capture_row(3, name, {"item_barcode": name}, kind="file_index")
            store.add_issue("DATA ISSUE", "", "m", "v")
        store.flush()
        rows = store.conn.execute(
            "SELECT source_file, payload FROM legacy_record ORDER BY source_file"
        ).fetchall()
        assert rows == [
            ("a.tsv", b'{"item_barcode": "a.tsv"}'),
            ("b.tsv", b'{"item_barcode": "b.tsv"}'),
        ]
    finally:
        set_active_store(None)
        store.close()


def test_debug_logging_flags_untranslated_measures(tmp_path, caplog):
    import logging

    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    with caplog.at_level(logging.DEBUG, logger="folio_migration_tools.report_store"):
        store.write_report_stats(
            {"GeneralStatistics": {i18n_t("Records processed"): 1, "plain text": 2}}
        )
        store.add_issue("DATA ISSUE", "r1", "plain issue", "v")
        store.flush()
    assert "Untranslated report measure in GeneralStatistics: 'plain text'" in caplog.text
    assert "Records processed" not in caplog.text
    assert "Untranslated data issue message: 'plain issue'" in caplog.text
    store.close()


def test_holdings_csv_transformer_snapshots_its_reference_files(tmp_path):
    import csv
    from functools import partial
    from unittest.mock import Mock

    from folio_migration_tools.migration_tasks.holdings_csv_transformer import (
        HoldingsCsvTransformer,
    )
    from folio_migration_tools.report_store import set_active_store

    csv.register_dialect("tsv", delimiter="\t")
    (tmp_path / "locations.tsv").write_text("legacy_code\tfolio_code\nmain\tMAIN\n")
    (tmp_path / "callnumbers.tsv").write_text("legacy_code\tfolio_code\nlc\tLC\n")
    (tmp_path / "holdings_mapping.json").write_text('{"data": []}')
    transformer = Mock(spec=HoldingsCsvTransformer)
    transformer.folder_structure = Mock()
    transformer.task_configuration = Mock()
    transformer.folder_structure.mapping_files_folder = tmp_path
    transformer.task_configuration.location_map_file_name = "locations.tsv"
    transformer.task_configuration.call_number_type_map_file_name = "callnumbers.tsv"
    transformer.task_configuration.holdings_map_file_name = "holdings_mapping.json"
    transformer.load_ref_data_map_from_file = partial(
        HoldingsCsvTransformer.load_ref_data_map_from_file, transformer
    )
    store = ReportStore(tmp_path / "r.sqlite")
    store.start_run("t")
    set_active_store(store)
    try:
        locations = HoldingsCsvTransformer.load_location_map(transformer)
        HoldingsCsvTransformer.load_call_number_type_map(transformer)
        HoldingsCsvTransformer.load_mapped_fields(transformer)
        roles = store.conn.execute(
            "SELECT role, format FROM reference_file ORDER BY role"
        ).fetchall()
        assert roles == [("callNumberTypeId", "tsv"), ("locationMap", "tsv"), ("mapping", "json")]
        assert locations == [{"legacy_code": "main", "folio_code": "MAIN"}]
    finally:
        set_active_store(None)
        store.close()


def _link_run(tmp_path, maps):
    """Run mapped-value lookups against `maps` ({role: (rows, lookups)}) and finalize a run DB.

    An empty role gives a plain list (no reference role). Returns the db path, the rendered
    markdown, the neutral JSON and the markdown the live report wrote.
    """
    import io
    from datetime import datetime, timezone
    from types import SimpleNamespace

    from folio_migration_tools.mapper_base import MapperBase
    from folio_migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
    from folio_migration_tools.migration_report import MigrationReport
    from folio_migration_tools.report_renderer import render_markdown, render_report_json
    from folio_migration_tools.report_store import ReferenceRows, set_active_store

    from .test_infrastructure import mocked_classes

    db = tmp_path / "r.sqlite"
    store = ReportStore(db)
    store.start_run("t", started=datetime.now(timezone.utc).isoformat())
    set_active_store(store)
    report = MigrationReport()
    fake = SimpleNamespace(migration_report=report)
    out = io.StringIO()
    try:
        for role, (rows, lookups) in maps.items():
            mapping = RefDataMapping(
                mocked_classes.mocked_folio_client(),
                "/material-types",
                "mtypes",
                ReferenceRows(rows, role) if role else rows,
                "name",
                "MaterialTypeMapping",
            )
            for legacy in lookups:
                MapperBase.get_mapped_ref_data_value(fake, mapping, legacy, "id")
        report.write_migration_report("Test report", out, datetime.now(timezone.utc))
        store.finalize(datetime.now(timezone.utc).isoformat())
    finally:
        set_active_store(None)
        store.close()
    return db, render_markdown(db), render_report_json(db), out.getvalue()


def _stat_rows(db):
    conn = sqlite3.connect(db)
    try:
        return conn.execute(
            "SELECT measure_key, params_json, count FROM report_stat"
            " WHERE section_id = 'MaterialTypeMapping' ORDER BY params_json"
        ).fetchall()
    finally:
        conn.close()


def _label_lines(text):
    return [ln for ln in text.splitlines() if "-> Book" in ln]


def test_same_legacy_key_in_two_files_links_separately_but_renders_as_one(tmp_path):
    one = [{"mt": "book", "folio_name": "Book"}, {"mt": "*", "folio_name": "Book"}]
    two = [{"mt": "book", "folio_name": "Book"}, {"mt": "*", "folio_name": "Book"}]
    book = {"mt": "book"}
    db, md, js, live = _link_run(
        tmp_path,
        {"materialTypeId": (one, [book, book]), "otherMaterialTypeId": (two, [book])},
    )
    stats = [(k, json.loads(p), c) for k, p, c in _stat_rows(db)]
    assert stats == [
        ("book -> Book", {"mapping_ref": {"key": ["book"], "role": "materialTypeId", "row": 0}}, 2),
        (
            "book -> Book",
            {"mapping_ref": {"key": ["book"], "role": "otherMaterialTypeId", "row": 0}},
            1,
        ),
    ]
    assert _label_lines(md) and _label_lines(md) == _label_lines(live)
    assert "3" in _label_lines(md)[0]
    measures = json.loads(js)["sections"]["MaterialTypeMapping"]
    assert [(m["key"], m["params"], m["count"]) for m in measures] == [("book -> Book", {}, 3)]


def test_link_key_with_spaces_and_wildcard_row(tmp_path):
    rows = [
        {"mt": "Rare Book", "folio_name": "Book"},
        {"mt": "*", "folio_name": "Book"},
    ]
    db, md, js, live = _link_run(
        tmp_path, {"materialTypeId": (rows, [{"mt": "Rare Book"}, {"mt": "zzz"}])}
    )
    stats = {k: json.loads(p) for k, p, c in _stat_rows(db)}
    assert stats["Rare Book -> Book"] == {
        "mapping_ref": {"key": ["Rare Book"], "role": "materialTypeId", "row": 0}
    }
    # A value that falls back to the "*" row is reported as unmapped: no map row produced it
    unmapped = [p for k, p in stats.items() if "zzz" in k]
    assert unmapped and all("mapping_ref" not in p for p in unmapped)
    assert _label_lines(md) == _label_lines(live)


def test_multi_key_link_is_in_file_column_order():
    from folio_migration_tools.mapping_file_transformation.ref_data_mapping import RefDataMapping
    from folio_migration_tools.report_store import ReferenceRows

    from .test_infrastructure import mocked_classes

    rows = [
        {"branch": "Main Library", "type": "Book Club", "folio_name": "Book"},
        {"branch": "*", "type": "*", "folio_name": "Book"},
    ]
    mapping = RefDataMapping(
        mocked_classes.mocked_folio_client(),
        "/material-types",
        "mtypes",
        ReferenceRows(rows, "materialTypeId"),
        "name",
        "MaterialTypeMapping",
    )
    assert mapping.mapped_legacy_keys == ["branch", "type"]
    assert json.loads(mapping.link_json(rows[0])) == {
        "mapping_ref": {"key": ["Main Library", "Book Club"], "role": "materialTypeId", "row": 0}
    }
    assert json.loads(mapping.link_json(rows[1]))["mapping_ref"]["key"] == ["*", "*"]


def test_plain_list_map_has_no_link(tmp_path):
    rows = [{"mt": "book", "folio_name": "Book"}, {"mt": "*", "folio_name": "Book"}]
    db, md, js, live = _link_run(tmp_path, {"": (rows, [{"mt": "book"}])})
    assert _stat_rows(db) == [("book -> Book", "{}", 1)]
