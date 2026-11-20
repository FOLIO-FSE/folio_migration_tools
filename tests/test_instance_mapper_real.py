"""Tests that build a real InstanceMapper against the mocked FOLIO client."""

import types
from unittest.mock import create_autospec

import pytest
from folioclient import FolioClient

from folio_migration_tools.library_configuration import (
    FileDefinition,
    FolioRelease,
    LibraryConfiguration,
)
from folio_migration_tools.mapping_file_transformation.instance_mapper import InstanceMapper
from folio_migration_tools.migration_tasks.instance_csv_transformer import (
    InstanceCsvTransformer,
)

from .test_infrastructure import mocked_classes


def _client():
    fc = mocked_classes.mocked_folio_client()
    fc.get_instance_json_schema = types.MethodType(FolioClient.get_instance_json_schema, fc)
    return fc


def _build(**kwargs) -> InstanceMapper:
    lib_config = LibraryConfiguration(
        gateway_url="https://folio-snapshot.dev.folio.org",
        tenant_id="diku",
        folio_username="diku_admin",
        folio_password="admin",  # noqa: S106
        folio_release=FolioRelease.ramsons,
        library_name="Instance Tester Library",
        log_level_debug=False,
        iteration_identifier="Test!",
        base_folder="/",
    )
    task_config = create_autospec(InstanceCsvTransformer.TaskConfiguration)
    task_config.name = "Test Task"
    task_config.migration_task_type = "InstanceCsvTransformer"
    task_config.add_administrative_notes_with_legacy_ids = False
    instance_map = {
        "data": [
            {"folio_field": "legacyIdentifier", "legacy_field": "id", "value": ""},
            {"folio_field": "title", "legacy_field": "title", "value": ""},
            {"folio_field": "instanceTypeId", "legacy_field": "type", "value": ""},
        ]
    }
    type_map = kwargs.pop("instance_type_map", [{"type": "*", "folio_name": "text"}])
    return InstanceMapper(
        _client(), instance_map, type_map, lib_config, task_config, **kwargs
    )


def test_init_with_name_and_code_maps():
    by_name = _build()
    assert by_name.instance_type_mapping is not None
    by_code = _build(instance_type_map=[{"type": "*", "folio_code": "txt"}])
    assert by_code.instance_type_mapping is not None
    assert by_name.nature_of_content_mapping is None


def test_init_with_optional_maps():
    mapper = _build(
        nature_of_content_map=[{"n": "*", "folio_name": "audiobook"}],
        instance_format_map=[{"f": "*", "folio_name": "audio -- audio disc"}],
        mode_of_issuance_map=[{"m": "*", "folio_name": "single unit"}],
        contributor_type_map=[{"c": "*", "folio_code": "abr"}],
    )
    assert mapper.contributor_type_mapping is not None
    assert mapper.mode_of_issuance_mapping is not None


def test_handle_suppression_sets_flags():
    mapper = _build()
    rec = {}
    mapper.handle_suppression(
        rec, FileDefinition(file_name="a.tsv", discovery_suppressed=True, staff_suppressed=True)
    )
    assert rec == {"discoverySuppress": True, "staffSuppress": True}


def test_perform_additional_mappings_sets_source():
    mapper = _build()
    rec = {}
    mapper.perform_additional_mappings("l1", rec, FileDefinition(file_name="a.tsv"))
    assert rec["source"] == "FOLIO"


def test_get_prop_instance_type_wildcard():
    mapper = _build()
    value = mapper.get_prop({"type": "anything", "id": "1"}, "instanceTypeId", "1", "")
    assert value


@pytest.mark.parametrize(
    "prop,attr",
    [
        ("natureOfContentTermIds[0]", "nature_of_content_mapping"),
        ("instanceFormatIds[0]", "instance_format_mapping"),
        ("modeOfIssuanceId", "mode_of_issuance_mapping"),
        ("contributors[0].contributorTypeId", "contributor_type_mapping"),
    ],
)
def test_get_prop_optional_maps_without_map_passes_through(prop, attr):
    mapper = _build()
    assert getattr(mapper, attr) is None
    mapper.mapped_from_values = {}
    mapper.get_prop({"id": "1", "type": "x"}, prop, "1", "")


def test_get_prop_unmapped_property_reports():
    mapper = _build()
    assert mapper.get_prop({"id": "1"}, "unknownProp", "1", "") == ""
