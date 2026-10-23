"""Tests for rendering reports from the report database."""

import io
import json
from datetime import datetime, timezone

import pytest

from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_cli import main as report_main
from folio_migration_tools.report_renderer import (
    ReportFilters,
    list_sections,
    render_issues_tsv,
    render_markdown,
    render_report_json,
)
from folio_migration_tools.report_store import ReportStore, set_active_store

STARTED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)


@pytest.fixture
def run(tmp_path, report_language):
    """A finished run with a live report, mapping stats and the markdown it wrote."""
    db = tmp_path / "migration_test.sqlite"
    store = ReportStore(db)
    store.start_run("test", started=STARTED.isoformat())
    set_active_store(store)
    report = MigrationReport()
    report.add("GeneralStatistics", i18n_t("Records processed"), 5)
    report.add_general_statistics(i18n_t("HTTP %{code} Error creating request", code=500) + ": x")
    report.add("FieldMappingErrors", i18n_t("Could not parse catalogedDate"), 2)
    report.add("LocationMapping", "'a' (b) -> c")
    report.set("Trivia", "plain", 3)
    folio = {"title": [4], "id": [5]}
    legacy = {"col": [5, 4]}
    out = io.StringIO()
    report.write_migration_report(i18n_t("Item transformation report"), out, STARTED)
    Helper.print_mapping_report(out, 5, folio, legacy)
    store.finalize(datetime.now(timezone.utc).isoformat())
    set_active_store(None)
    return db, out.getvalue()


def _without_finish(text: str) -> list[str]:
    """Drop the lines that depend on when the report was written."""
    return [ln for ln in text.splitlines() if not ln.startswith(("Heure de fin", "Temps écoulé"))]


def test_rendered_report_matches_live_report(run):
    db, live = run
    rendered = render_markdown(db)
    assert _without_finish(rendered) == _without_finish(live)


def test_rendered_report_can_change_locale(run):
    db, _ = run
    english = render_markdown(db, locale="en")
    assert "# Item transformation report" in english
    assert "Records processed" in english
    assert "# Item transformation report" not in render_markdown(db, locale="fr")


def test_section_filters(run):
    db, _ = run
    only = render_markdown(
        db, locale="en", filters=ReportFilters(sections=["Trivia"], mapping=False)
    )
    assert "plain | 3" in only
    assert "Records processed" not in only
    assert "Mapped FOLIO fields" not in only
    without = render_markdown(db, locale="en", filters=ReportFilters(exclude_sections=["Trivia"]))
    assert "plain | 3" not in without
    assert "Records processed" in without


def test_measure_filters(run):
    db, _ = run
    big = render_markdown(db, locale="en", filters=ReportFilters(min_count=3, mapping=False))
    assert "plain | 3" in big
    assert "Records processed" in big  # count 5
    assert "Could not parse catalogedDate" not in big  # count 2
    only = render_markdown(
        db, locale="en", filters=ReportFilters(measure_contains="PLAIN", mapping=False)
    )
    assert "plain | 3" in only
    assert "Records processed" not in only


def test_unknown_section_is_an_error(run):
    db, _ = run
    with pytest.raises(ValueError, match="Nope"):
        render_markdown(db, filters=ReportFilters(sections=["Nope"]))


def test_list_sections(run):
    db, _ = run
    sections = {s[0]: s for s in list_sections(db, "en")}
    assert sections["Trivia"] == ("Trivia", 1, 3)


def test_issues_tsv_is_localized(tmp_path, report_language):
    store = ReportStore(tmp_path / "i.sqlite")
    store.start_run("t", started=STARTED.isoformat())
    set_active_store(store)
    Helper.log_data_issue(["a1"], i18n_t("Could not parse catalogedDate"), "bad")
    Helper.log_data_issue_failed("b2", "plain message", "val")
    store.finalize(datetime.now(timezone.utc).isoformat())
    set_active_store(None)
    fr = render_issues_tsv(tmp_path / "i.sqlite").splitlines()
    en = render_issues_tsv(tmp_path / "i.sqlite", locale="en").splitlines()
    assert fr[0].split("\t")[:2] == ["DATA ISSUE", "a1"]
    assert en[0].split("\t")[2:] == ["Could not parse catalogedDate", "bad"]
    assert fr[0].split("\t")[2] != en[0].split("\t")[2]
    assert fr[1].split("\t") == ["RECORD FAILED", "b2", "plain message", "val"]


def test_report_cli(run, tmp_path, capsys):
    db, _ = run
    out = tmp_path / "r.md"
    assert report_main(["--db", str(db), "--locale", "en,fr", "--output", str(out)]) == 0
    assert "# Item transformation report" in (tmp_path / "r_en.md").read_text()
    assert (tmp_path / "r_fr.md").exists()
    assert report_main(["--db", str(db), "--list-sections"]) == 0
    assert "Trivia" in capsys.readouterr().out
    assert report_main(["--db", str(db), "--sections", "Nope"]) == 1
    assert report_main(["--db", str(tmp_path / "missing.sqlite")]) == 1


def test_report_cli_config_file(run, tmp_path):
    db, _ = run
    config = tmp_path / "filters.json"
    config.write_text(
        json.dumps(
            {
                "locale": "en",
                "excludeSections": ["Trivia"],
                "mapping": False,
                "output": str(tmp_path / "from_config.md"),
            }
        )
    )
    assert report_main(["--db", str(db), "--config", str(config)]) == 0
    text = (tmp_path / "from_config.md").read_text()
    assert "Records processed" in text
    assert "plain | 3" not in text
    assert "Mapped FOLIO fields" not in text
    # command line wins over the config file
    other = tmp_path / "cli.md"
    assert report_main(["--db", str(db), "--config", str(config), "--output", str(other)]) == 0
    assert other.exists()


@pytest.mark.parametrize(
    "content", ['{"bogus": 1}', "[1]", "not json", '{"minCount": "x"}', '{"sections": [1]}']
)
def test_report_cli_bad_config(run, tmp_path, content):
    db, _ = run
    config = tmp_path / "bad.json"
    config.write_text(content)
    assert report_main(["--db", str(db), "--config", str(config)]) == 1


def test_report_json_is_locale_neutral(run):
    db, _ = run
    data = json.loads(render_report_json(db))
    assert data["run"]["task_name"]
    assert data["sections"]
    measures = [m for ms in data["sections"].values() for m in ms]
    assert all({"kind", "key", "params", "count"} <= set(m) for m in measures)
    assert any(m["kind"] == "key" for m in measures)
    # neutral output does not depend on the locale
    assert render_report_json(db, locale="en") == render_report_json(db, locale="fr")


def test_report_json_localized_and_filtered(run):
    db, _ = run
    english = json.loads(render_report_json(db, "en", localized=True))
    french = json.loads(render_report_json(db, "fr", localized=True))
    assert english.keys() == french.keys()
    assert english != french
    only = json.loads(
        render_report_json(db, "en", ReportFilters(sections=["Trivia"], mapping=False))
    )
    assert list(only["sections"]) == ["Trivia"]
    assert "mapping" not in only
    with pytest.raises(ValueError):
        render_report_json(db, filters=ReportFilters(sections=["Nope"]))


def test_report_cli_json_output(run, tmp_path):
    db, _ = run
    out = tmp_path / "stats.json"
    assert report_main(["--db", str(db), "--json-output", str(out)]) == 0
    assert "sections" in json.loads(out.read_text())
    localized = tmp_path / "labels.json"
    args = ["--db", str(db), "--locale", "en", "--json-output", str(localized), "--json-localized"]
    assert report_main(args) == 0
    assert "sections" not in json.loads(localized.read_text())


@pytest.fixture
def library(tmp_path, run):
    """A base folder with two timestamped runs of task 'items' and a config naming it."""
    db, _ = run
    base = tmp_path / "base"
    raw = base / "iterations" / "it1" / "reports" / ".raw"
    raw.mkdir(parents=True)
    for stamp in ("20260101-000000", "20260102-000000"):
        (raw / f"migration_{stamp}_items.sqlite").write_bytes(db.read_bytes())
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "libraryInformation": {"iterationIdentifier": "it1"},
                "migrationTasks": [{"name": "items", "migrationTaskType": "ItemsTransformer"}],
            }
        )
    )
    return base, config, raw.parent


def _by_task(library, *extra):
    base, config, _ = library
    return report_main([str(config), "items", "--base_folder_path", str(base), *extra])


def test_report_cli_by_task_name(library, capsys):
    reports = library[2]
    assert _by_task(library, "--run", "latest", "--locale", "en") == 0
    assert (reports / "report_20260102-000000_items_en.md").exists()
    assert not (reports / "report_20260101-000000_items_en.md").exists()
    assert _by_task(library, "--run", "20260101-000000", "--locale", "en") == 0
    assert (reports / "report_20260101-000000_items_en.md").exists()
    assert _by_task(library, "--run", "nope") == 1
    assert "Available" in capsys.readouterr().err


def test_report_cli_task_name_errors(library, capsys):
    base, config, _ = library
    assert report_main([str(config), "bogus", "--base_folder_path", str(base)]) == 1
    assert "Use one of: items" in capsys.readouterr().err
    assert _by_task(library) == 1  # several runs, stdin is not a terminal
    assert "--run" in capsys.readouterr().err
    assert report_main(["--db", str(library[0]), "--run", "latest"]) == 1


def test_report_cli_prompts_for_run(library, monkeypatch, capsys):
    monkeypatch.setattr("sys.stdin", type("T", (), {"isatty": lambda self: True})())
    monkeypatch.setattr("builtins.input", lambda prompt="": "1")
    assert _by_task(library, "--locale", "en") == 0
    assert (library[2] / "report_20260101-000000_items_en.md").exists()


def test_report_cli_output_to_stdout(run, capsys):
    db, _ = run
    assert report_main(["--db", str(db), "--locale", "en", "--output", "-"]) == 0
    assert "# Item transformation report" in capsys.readouterr().out
