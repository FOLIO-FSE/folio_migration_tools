"""Tests for rendering reports from the report database."""

import io
from datetime import datetime, timezone

import pytest

from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.report_renderer import render_markdown
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
    only = render_markdown(db, locale="en", sections=["Trivia"], include_mapping=False)
    assert "plain | 3" in only
    assert "Records processed" not in only
    assert "Mapped FOLIO fields" not in only
    without = render_markdown(db, locale="en", exclude_sections=["Trivia"])
    assert "plain | 3" not in without
    assert "Records processed" in without

