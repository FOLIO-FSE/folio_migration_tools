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

