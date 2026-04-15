from io import BytesIO
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces
from pymarc import Field
from pymarc import Record
from pymarc import Subfield

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_report import MigrationReport
from .test_infrastructure import mocked_classes


def test_add_mapped_location_code_to_record_one_852_no_b():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_processor.mapper = mock_mapper
    record = Record()
    record.add_field(
        Field(
            tag="852",
            indicators=["0", "1"],
            subfields=[Subfield(code="a", value="code1"), Subfield(code="c", value="code2")],
        )
    )
    folio_rec = {"permanentLocationId": "new_loc"}
    MarcFileProcessor.add_mapped_location_code_to_record(mock_processor, record, folio_rec)
    if "852" not in record:
        raise Exception()
    first_852 = record.get_fields("852")[0]
    first_852.delete_subfield("b")
    first_852.add_subfield("b", "new_loc", 0)
    assert record["852"].get_subfields("b")[0] == "new_loc"


def test_add_mapped_location_code_to_record_one_852_two_b():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_processor.mapper = mock_mapper
    record = Record()
    record.add_field(
        Field(
            tag="852",
            indicators=["0", "1"],
            subfields=[Subfield(code="b", value="code1"), Subfield(code="b", value="code2")],
        )
    )
    folio_rec = {"permanentLocationId": "new_loc"}
    MarcFileProcessor.add_mapped_location_code_to_record(mock_processor, record, folio_rec)
    if "852" not in record:
        raise Exception()
    first_852 = record.get_fields("852")[0]
    first_852.delete_subfield("b")
    first_852.add_subfield("b", "new_loc", 0)
    assert record["852"].get_subfields("b")[0] == "new_loc"
    assert record["852"].get_subfields("x")[0] == "code2"


def test_add_mapped_location_code_to_record_one_852_one_b():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_processor.mapper = mock_mapper
    record = Record()
    record.add_field(
        Field(
            tag="852",
            indicators=["0", "1"],
            subfields=[Subfield(code="b", value="code1")],
        )
    )
    folio_rec = {"permanentLocationId": "new_loc"}
    MarcFileProcessor.add_mapped_location_code_to_record(mock_processor, record, folio_rec)
    if "852" not in record:
        raise Exception()
    first_852 = record.get_fields("852")[0]
    first_852.delete_subfield("b")
    first_852.add_subfield("b", "new_loc", 0)
    assert record["852"].get_subfields("b")[0] == "new_loc"


def test_add_mapped_location_code_to_record_two_852_one_b():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_processor.mapper = mock_mapper
    record = Record()
    record.add_field(
        Field(
            tag="852",
            indicators=["0", "1"],
            subfields=[Subfield(code="b", value="code1")],
        )
    )
    record.add_field(
        Field(
            tag="852",
            indicators=["0", "1"],
            subfields=[Subfield(code="b", value="code2")],
        )
    )
    folio_rec = {"permanentLocationId": "new_loc"}
    MarcFileProcessor.add_mapped_location_code_to_record(mock_processor, record, folio_rec)
    if "852" not in record:
        raise Exception()
    first_852 = record.get_fields("852")[0]
    first_852.delete_subfield("b")
    first_852.add_subfield("b", "new_loc", 0)
    assert record["852"].get_subfields("b")[0] == "new_loc"


def test_add_mapped_location_code_to_record_no_852():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.folio_client = mocked_classes.mocked_folio_client()
    mock_processor.mapper = mock_mapper
    record = Record()
    folio_rec = {"permanentLocationId": "new_loc"}
    with pytest.raises(TransformationRecordFailedError):
        MarcFileProcessor.add_mapped_location_code_to_record(mock_processor, record, folio_rec)
        if "852" not in record:
            raise Exception()
        first_852 = record.get_fields("852")[0]
        first_852.delete_subfield("b")
        first_852.add_subfield("b", "new_loc", 0)
        assert record["852"].get_subfields("b")[0] == "new_loc"


def test_save_marc_record_both_flags_true():
    """Test save_marc_record when both data_import_marc flags are True"""
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = Mock()
    mock_mapper.task_configuration.data_import_marc = True
    mock_mapper.save_data_import_marc_record = Mock()
    mock_processor.mapper = mock_mapper
    mock_processor.data_import_marc_file = BytesIO()
    
    record = Record()
    record.add_field(Field(tag="245", indicators=["0", "0"], subfields=[Subfield(code="a", value="Test Title")]))
    file_def = FileDefinition(file_name="test.mrc", data_import_marc=True)
    folio_rec = {"id": "test-id"}
    object_type = FOLIONamespaces.instances
    
    MarcFileProcessor.save_marc_record(mock_processor, record, file_def, folio_rec, object_type)
    
    mock_mapper.save_data_import_marc_record.assert_called_once_with(
        mock_processor.data_import_marc_file,
        object_type,
        record,
        folio_rec,
    )


def test_save_marc_record_task_config_false():
    """Test save_marc_record when task_configuration.data_import_marc is False"""
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = Mock()
    mock_mapper.task_configuration.data_import_marc = False
    mock_mapper.save_data_import_marc_record = Mock()
    mock_processor.mapper = mock_mapper
    mock_processor.data_import_marc_file = BytesIO()
    
    record = Record()
    record.add_field(Field(tag="245", indicators=["0", "0"], subfields=[Subfield(code="a", value="Test Title")]))
    file_def = FileDefinition(file_name="test.mrc", data_import_marc=True)
    folio_rec = {"id": "test-id"}
    object_type = FOLIONamespaces.instances
    
    MarcFileProcessor.save_marc_record(mock_processor, record, file_def, folio_rec, object_type)
    
    mock_mapper.save_data_import_marc_record.assert_not_called()


def test_save_marc_record_file_def_false():
    """Test save_marc_record when file_def.data_import_marc is False"""
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = Mock()
    mock_mapper.task_configuration.data_import_marc = True
    mock_mapper.save_data_import_marc_record = Mock()
    mock_processor.mapper = mock_mapper
    mock_processor.data_import_marc_file = BytesIO()
    
    record = Record()
    record.add_field(Field(tag="245", indicators=["0", "0"], subfields=[Subfield(code="a", value="Test Title")]))
    file_def = FileDefinition(file_name="test.mrc", data_import_marc=False)
    folio_rec = {"id": "test-id"}
    object_type = FOLIONamespaces.instances
    
    MarcFileProcessor.save_marc_record(mock_processor, record, file_def, folio_rec, object_type)
    
    mock_mapper.save_data_import_marc_record.assert_not_called()


def test_save_marc_record_no_data_import_marc_attribute():
    """Test save_marc_record when task_configuration has no data_import_marc attribute"""
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.task_configuration = Mock(spec=[])  # No attributes
    mock_mapper.save_data_import_marc_record = Mock()
    mock_processor.mapper = mock_mapper
    mock_processor.data_import_marc_file = BytesIO()
    
    record = Record()
    record.add_field(Field(tag="245", indicators=["0", "0"], subfields=[Subfield(code="a", value="Test Title")]))
    file_def = FileDefinition(file_name="test.mrc", data_import_marc=True)
    folio_rec = {"id": "test-id"}
    object_type = FOLIONamespaces.instances
    
    MarcFileProcessor.save_marc_record(mock_processor, record, file_def, folio_rec, object_type)
    
    mock_mapper.save_data_import_marc_record.assert_not_called()


def test_add_legacy_ids_to_map_returns_added_ids():
    """Test that add_legacy_ids_to_map returns the IDs that were actually added."""
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_processor.legacy_ids = set()
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.id_map = {}
    mock_mapper.get_id_map_tuple.side_effect = lambda lid, rec, ot: (lid, rec["id"])
    mock_processor.mapper = mock_mapper
    mock_processor.object_type = FOLIONamespaces.holdings

    folio_rec = {"id": "folio-uuid-1"}
    result = MarcFileProcessor.add_legacy_ids_to_map(
        mock_processor, folio_rec, ["legacy-1", "legacy-2"]
    )

    assert result == ["legacy-1", "legacy-2"]
    assert "legacy-1" in mock_mapper.id_map
    assert "legacy-2" in mock_mapper.id_map


def test_add_legacy_ids_to_map_raises_on_duplicate_in_id_map():
    """Test that add_legacy_ids_to_map raises when a legacy ID is already in the id_map."""
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_processor.legacy_ids = set()
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.id_map = {"legacy-1": ("legacy-1", "existing-folio-uuid")}
    mock_mapper.get_id_map_tuple.side_effect = lambda lid, rec, ot: (lid, rec["id"])
    mock_processor.mapper = mock_mapper
    mock_processor.object_type = FOLIONamespaces.holdings

    folio_rec = {"id": "folio-uuid-2"}
    with pytest.raises(TransformationRecordFailedError):
        MarcFileProcessor.add_legacy_ids_to_map(mock_processor, folio_rec, ["legacy-1"])

    # The original entry must still be in the map
    assert mock_mapper.id_map["legacy-1"] == ("legacy-1", "existing-folio-uuid")


def test_process_record_duplicate_does_not_remove_original_from_id_map():
    """Test that a duplicate record does not remove the original record's entry from the id_map.

    This is the core regression test for the bug where the finally block in process_record
    would call remove_from_id_map with formerIds from a duplicate record, erroneously
    removing the first (valid) record's entry.
    """
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_processor.legacy_ids = set()
    mock_processor.records_count = 0
    mock_processor.failed_records_count = 0

    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.id_map = {}
    mock_mapper.get_id_map_tuple.side_effect = lambda lid, rec, ot: (lid, rec["id"])
    mock_mapper.migration_report = MigrationReport()
    mock_mapper.library_configuration = Mock()
    mock_mapper.library_configuration.failed_percentage_threshold = 20
    mock_mapper.library_configuration.failed_records_threshold = 10
    mock_mapper.create_source_records = False
    mock_processor.mapper = mock_mapper
    mock_processor.object_type = FOLIONamespaces.holdings
    mock_processor.created_objects_file = Mock()

    # Wire real implementations for methods called by process_record
    mock_processor.process_record = lambda idx, rec, fd: MarcFileProcessor.process_record(
        mock_processor, idx, rec, fd
    )
    mock_processor.add_legacy_ids_to_map = lambda rec, ids: MarcFileProcessor.add_legacy_ids_to_map(
        mock_processor, rec, ids
    )
    mock_processor.get_valid_folio_record_ids = MarcFileProcessor.get_valid_folio_record_ids
    mock_processor.exit_on_too_many_exceptions = lambda: MarcFileProcessor.exit_on_too_many_exceptions(
        mock_processor
    )

    record_a = Record()
    record_a.add_field(
        Field(tag="001", data="legacy-1"),
        Field(
            tag="852",
            indicators=["0", " "],
            subfields=[Subfield(code="b", value="MAIN")],
        ),
    )
    folio_rec_a = {"id": "folio-uuid-a", "formerIds": ["legacy-1"]}
    mock_mapper.get_legacy_ids.return_value = ["legacy-1"]
    mock_mapper.parse_record.return_value = [folio_rec_a]

    file_def = FileDefinition(file_name="test.mrc")

    # Process record A — should succeed
    mock_processor.process_record(0, record_a, file_def)
    assert "legacy-1" in mock_mapper.id_map

    # Now process record B — a duplicate with the same legacy ID
    record_b = Record()
    record_b.add_field(
        Field(tag="001", data="legacy-1"),
        Field(
            tag="852",
            indicators=["0", " "],
            subfields=[Subfield(code="b", value="MAIN")],
        ),
    )
    folio_rec_b = {"id": "folio-uuid-b", "formerIds": ["legacy-1"]}
    mock_mapper.parse_record.return_value = [folio_rec_b]

    # process_record raises TransformationRecordFailedError for the duplicate
    with pytest.raises(TransformationRecordFailedError):
        mock_processor.process_record(1, record_b, file_def)

    # The original record's entry must still be in the id_map
    assert "legacy-1" in mock_mapper.id_map
    assert mock_mapper.id_map["legacy-1"] == ("legacy-1", "folio-uuid-a")

