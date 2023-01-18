from unittest.mock import Mock

from pymarc import Field
from pymarc import Record

from folio_migration_tools.library_configuration import HridHandling
from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler
from folio_migration_tools.marc_rules_transformation.marc_file_processor import (
    MarcFileProcessor,
)
from folio_migration_tools.marc_rules_transformation.rules_mapper_holdings import (
    RulesMapperHoldings,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.test_infrastructure import mocked_classes


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

    mock_processor.mapper = mock_mapper
    hrid_handler = HRIDHandler(
        mocked_classes.mocked_folio_client(), HridHandling.preserve001, MigrationReport(), True
    )
    hrid_handler.common_retain_leading_zeroes = True
    hrid_handler.holdings_hrid_counter = "1"
    assert hrid_handler.generate_numeric_part(hrid_handler.holdings_hrid_counter) == "00000000001"


def test_generate_num_part_no_retain_leading_zeroes():
    mock_processor = Mock(spec=MarcFileProcessor)
    mock_mapper = Mock(spec=RulesMapperHoldings)

    mock_processor.mapper = mock_mapper
    hrid_handler = HRIDHandler(
        mocked_classes.mocked_folio_client(), HridHandling.preserve001, MigrationReport(), True
    )
    hrid_handler.common_retain_leading_zeroes = False
    hrid_handler.holdings_hrid_counter = "1"
    assert hrid_handler.generate_numeric_part(hrid_handler.holdings_hrid_counter) == "1"
