from unittest.mock import Mock

from pymarc import Field
from pymarc import Record

from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)


def test_chop_008():
    record = Record()
    record.add_field(Field(tag="008", data="2112204p    8   0001baeng0              "))
    assert len(record["008"].data) == 40
    remain, rest = record["008"].data[:32], record["008"].data[32:]
    assert len(remain) == 32
    assert len(rest) == 8
    record["008"].data = remain
    assert len(record["008"].data) == 32


def test_generate_num_part_retain_leading_zeroes():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.holdings_hrid_counter = "1"
    mock_mapper.common_retain_leading_zeroes = True
    mock_processor.mapper = mock_mapper
    assert MarcFileProcessor.generate_num_part(mock_processor) == "00000000001"


def test_generate_num_part_without_leading_zeroes():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)
    mock_mapper.holdings_hrid_counter = "1"
    mock_mapper.common_retain_leading_zeroes = False
    mock_processor.mapper = mock_mapper
    assert MarcFileProcessor.generate_num_part(mock_processor) == "1"
