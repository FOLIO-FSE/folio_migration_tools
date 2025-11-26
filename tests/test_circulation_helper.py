import uuid

from folio_migration_tools.circulation_helper import CirculationHelper
from folio_migration_tools.migration_report import MigrationReport
from .test_infrastructure import mocked_classes


def test_init():
    mocked_folio = mocked_classes.mocked_folio_client()
    sp_id = str(uuid.uuid4())
    circ_helper = CirculationHelper(mocked_folio, sp_id, MigrationReport())
    assert circ_helper.folio_client
    assert circ_helper.service_point_id == sp_id
    assert not any(circ_helper.missing_patron_barcodes)
    assert not any(circ_helper.missing_item_barcodes)
    assert not any(circ_helper.migration_report.report)
