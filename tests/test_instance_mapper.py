from unittest.mock import Mock

from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.mapping_file_transformation.instance_mapper import (
    InstanceMapper,
)


def test_perform_additional_mappings_sets_source_and_suppression():
    mapper = Mock(spec=InstanceMapper)
    mapper.task_configuration = Mock(add_administrative_notes_with_legacy_ids=True)
    mapper.handle_suppression.side_effect = lambda rec, fd: rec.update(
        discoverySuppress=fd.discovery_suppressed, staffSuppress=fd.staff_suppressed
    )
    rec: dict = {}
    file_def = FileDefinition(file_name="a.tsv", discovery_suppressed=True)
    InstanceMapper.perform_additional_mappings(mapper, "legacy1", rec, file_def)
    assert rec["source"] == "FOLIO"
    assert rec["discoverySuppress"] is True
    mapper.add_legacy_id_to_admin_note.assert_called_once_with(rec, "legacy1")


def test_admin_note_skipped_when_disabled():
    mapper = Mock(spec=InstanceMapper)
    mapper.task_configuration = Mock(add_administrative_notes_with_legacy_ids=False)
    InstanceMapper.perform_additional_mappings(
        mapper, "l", {}, FileDefinition(file_name="a.tsv")
    )
    mapper.add_legacy_id_to_admin_note.assert_not_called()
