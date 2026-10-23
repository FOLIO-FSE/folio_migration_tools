"""Parity of the database-rendered report with the live report, using a real mapper run."""

import contextlib
import io
from datetime import datetime, timezone
from pathlib import Path

import pymarc
import pytest

from folio_migration_tools.helper import Helper
from folio_migration_tools.i18n_cache import i18n_t
from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    HridHandling,
    IlsFlavour,
    LibraryConfiguration,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper
from folio_migration_tools.migration_tasks.bibs_transformer import BibsTransformer
from folio_migration_tools.report_renderer import render_markdown
from folio_migration_tools.report_store import ReportStore, set_active_store

from .test_infrastructure import mocked_classes

STARTED = datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
DATA = Path(__file__).parent / "test_data" / "default"


def _build_mapper() -> BibsRulesMapper:
    folio = mocked_classes.mocked_folio_client()
    lib = LibraryConfiguration(
        okapi_url=folio.gateway_url,
        tenant_id=folio.tenant_id,
        okapi_username=folio.username,
        okapi_password=folio.password,
        folio_release=FolioRelease.ramsons,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="parity",
        base_folder="/",
        multi_field_delimiter="^-^",
    )
    conf = BibsTransformer.TaskConfiguration(
        name="test",
        migration_task_type="BibsTransformer",
        hrid_handling=HridHandling.default,
        files=[],
        ils_flavour=IlsFlavour.sierra,
        reset_hrid_settings=False,
        data_import_marc=False,
        create_source_records=True,
    )
    return BibsRulesMapper(folio, lib, conf)


def _without_finish(text: str) -> list[str]:
    volatile = ("Heure de fin", "Temps écoulé", "Time Finished", "Elapsed time")
    return [ln for ln in text.splitlines() if not ln.startswith(volatile)]


@pytest.mark.parametrize("report_language", ["fr", "en"], indirect=True)
def test_bib_mapper_run_renders_identically(tmp_path, report_language):
    db = tmp_path / "migration_bibs.sqlite"
    store = ReportStore(db)
    store.start_run("bibs", started=STARTED.isoformat())
    set_active_store(store)
    try:
        mapper = _build_mapper()
        file_def = FileDefinition(file_name="", discovery_suppressed=False, staff_suppressed=False)
        parsed = 0
        for path in sorted(DATA.glob("*.xml")):
            records = pymarc.parse_xml_to_array(str(path))
            for record in records:
                with contextlib.suppress(Exception):  # failures are counted in the report
                    mapper.parse_record(record, file_def, [path.stem])
                parsed += 1
        assert parsed > 10
        out = io.StringIO()
        mapper.migration_report.write_migration_report(
            i18n_t("Bibliographic records transformation report"), out, STARTED
        )
        Helper.print_mapping_report(
            out, parsed, mapper.mapped_folio_fields, mapper.mapped_legacy_fields
        )
        store.finalize(datetime.now(timezone.utc).isoformat())
    finally:
        set_active_store(None)
    assert len(out.getvalue().splitlines()) > 20
    assert _without_finish(render_markdown(db)) == _without_finish(out.getvalue())
