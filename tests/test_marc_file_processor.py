from unittest.mock import Mock

import pytest
from pymarc import Field
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationRecordFailedError
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.test_infrastructure import mocked_classes


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
            subfields=["a", "code1", "c", "code2"],
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
            subfields=["b", "code1", "b", "code2"],
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
            subfields=["b", "code1"],
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
            subfields=["b", "code1"],
        )
    )
    record.add_field(
        Field(
            tag="852",
            indicators=["0", "1"],
            subfields=["b", "code2"],
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
