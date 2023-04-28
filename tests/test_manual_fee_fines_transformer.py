import io
from pathlib import Path
from unittest.mock import Mock

from folio_migration_tools.extradata_writer import ExtradataWriter
from folio_migration_tools.mapping_file_transformation.manual_fee_fines_mapper import (
    ManualFeeFinesMapper,
)
from folio_migration_tools.migration_report import MigrationReport
from folio_migration_tools.migration_tasks.manual_fee_fines_transformer import (
    ManualFeeFinesTransformer,
)


def test_get_object_type():
    res = ManualFeeFinesTransformer.get_object_type()
    assert res.name == "feefines"
