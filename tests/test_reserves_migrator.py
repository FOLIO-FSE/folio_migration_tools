import csv
from unittest.mock import MagicMock, Mock

import httpx
import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.migration_tasks.reserves_migrator import ReservesMigrator


def test_get_object_type():
    assert ReservesMigrator.get_object_type() == FOLIONamespaces.reserve


def _response(status_code: int, body: str = '{"errors": [{"message": "bad"}]}'):
    return httpx.Response(status_code, text=body, request=httpx.Request("POST", "http://folio/x"))


class TestFolioPutPost:
    @pytest.fixture
    def migrator(self):
        m = Mock(spec=ReservesMigrator)
        m.folio_client = Mock()
        m.migration_report = Mock()
        m.http_client = Mock()
        return m

    @pytest.mark.parametrize("status", [200, 201, 204])
    def test_success(self, migrator, status):
        migrator.http_client.post.return_value = _response(status, "")
        assert ReservesMigrator.folio_put_post(migrator, "/x", {}, "POST", "act")
        details = [c.args[1] for c in migrator.migration_report.add.call_args_list]
        assert any("Successfully" in d for d in details)
        assert not any("error" in d for d in details)

    @pytest.mark.parametrize("status", [422, 500])
    def test_http_error_returns_false(self, migrator, status):
        migrator.http_client.post.return_value = _response(status)
        assert not ReservesMigrator.folio_put_post(migrator, "/x", {}, "POST", "act")

    def test_422_with_non_json_body_returns_false(self, migrator):
        migrator.http_client.post.return_value = _response(422, "<html>Bad gateway</html>")
        assert not ReservesMigrator.folio_put_post(migrator, "/x", {}, "POST", "act")

    def test_connection_error_returns_false(self, migrator):
        migrator.http_client.put.side_effect = httpx.ConnectError("boom")
        assert not ReservesMigrator.folio_put_post(migrator, "/x", {}, "PUT", "act")

    def test_posts_relative_path_with_client_headers(self, migrator):
        migrator.http_client.post.return_value = _response(201, "")
        ReservesMigrator.folio_put_post(migrator, "/coursereserves/x", {}, "POST", "act")
        call = migrator.http_client.post.call_args
        assert call.args[0] == "/coursereserves/x"
        assert "headers" not in call.kwargs


class _Reserve:
    def __init__(self, item_barcode, legacy_identifier="C1"):
        self.item_barcode = item_barcode
        self.legacy_identifier = legacy_identifier
        self.course_listing_id = "cl-1"

    def to_dict(self):
        return {"copiedItem": {"barcode": self.item_barcode}}

    def to_source_dict(self):
        return {"legacy_identifier": self.legacy_identifier, "item_barcode": self.item_barcode}


def _make_migrator(reserves, skip=False):
    from unittest.mock import Mock

    m = Mock(spec=ReservesMigrator)
    m.semi_valid_reserves = reserves
    m.skip_barcode_prevalidation = skip
    m.migration_report = Mock()
    m.folio_client = MagicMock()
    m.check_barcodes = lambda: ReservesMigrator.check_barcodes(m)
    m.failed_reserves = []
    return m


def test_check_barcodes_keeps_only_reserves_with_folio_items():
    good, bad = _Reserve("I1"), _Reserve("I2")
    m = _make_migrator([good, bad])
    m.folio_client.folio_post.return_value = {"items": [{"barcode": "I1"}]}

    assert list(ReservesMigrator.check_barcodes(m)) == [good]


def test_prevalidation_can_be_skipped():
    reserves = [_Reserve("I1")]
    m = _make_migrator(reserves, skip=True)

    ReservesMigrator._pre_validate_barcodes(m)

    assert m.valid_reserves == reserves
    m.folio_client.folio_post.assert_not_called()


@pytest.mark.asyncio
async def test_do_work_posts_only_prevalidated_reserves():
    good, bad = _Reserve("I1"), _Reserve("I2")
    m = _make_migrator([good, bad])
    m.folio_client.folio_post.return_value = {"items": [{"barcode": "I1"}]}
    m.t0 = 0
    m._pre_validate_barcodes = lambda: ReservesMigrator._pre_validate_barcodes(m)

    await ReservesMigrator.do_work(m)

    m.post_single_reserve.assert_called_once_with(good)


def _general_stats(m):
    return [c.args[0] for c in m.migration_report.add_general_statistics.call_args_list]


def test_check_barcodes_records_discarded_reserves_as_failed():
    good, bad = _Reserve("I1"), _Reserve("I2")
    m = _make_migrator([good, bad])
    m.folio_client.folio_post.return_value = {"items": [{"barcode": "I1"}]}

    list(ReservesMigrator.check_barcodes(m))

    assert m.failed_reserves == [bad]
    assert "Reserves discarded by barcode pre-validation" in _general_stats(m)


def test_failed_post_is_recorded():
    reserve = _Reserve("I1")
    m = _make_migrator([reserve])
    m.folio_put_post.return_value = False

    ReservesMigrator.post_single_reserve(m, reserve)

    assert m.failed_reserves == [reserve]
    assert "Failure to post reserve" in _general_stats(m)


@pytest.mark.asyncio
async def test_do_work_records_reserve_when_posting_raises():
    reserve = _Reserve("I1")
    m = _make_migrator([reserve], skip=True)
    m.t0 = 0
    m._pre_validate_barcodes = lambda: ReservesMigrator._pre_validate_barcodes(m)
    m.post_single_reserve.side_effect = RuntimeError("boom")

    await ReservesMigrator.do_work(m)

    assert m.failed_reserves == [reserve]


@pytest.mark.asyncio
async def test_wrap_up_writes_failed_file_with_input_headers(tmp_path):
    csv.register_dialect("tsv", delimiter="\t")
    discarded, not_posted = _Reserve("I1", "C1"), _Reserve("I2", "C2")
    m = _make_migrator([discarded, not_posted])
    m.failed_reserves = [discarded, not_posted]
    m.extradata_writer = Mock()
    m.start_datetime = None
    m.folder_structure = Mock(
        failed_recs_path=tmp_path / "failed.tsv",
        migration_reports_file=tmp_path / "report.md",
        migration_reports_raw_file=tmp_path / "report.json",
    )
    m.write_failed_reserves_to_file = lambda: ReservesMigrator.write_failed_reserves_to_file(m)

    await ReservesMigrator.wrap_up(m)

    m.migration_report.set.assert_called_once_with("GeneralStatistics", "Failed reserves", 2)
    with open(tmp_path / "failed.tsv") as failed_file:
        rows = list(csv.DictReader(failed_file, dialect="tsv"))
    assert rows == [
        {"legacy_identifier": "C1", "item_barcode": "I1"},
        {"legacy_identifier": "C2", "item_barcode": "I2"},
    ]


@pytest.mark.parametrize("rows", [[], [{"legacy_identifier": "C1", "item_barcode": "I1"}]])
def test_load_and_validate_small_files(rows):
    m = _make_migrator([])
    m.uuid_base_string = "test_tenant"

    reserves = list(ReservesMigrator.load_and_validate_legacy_reserves(m, iter(rows)))

    assert [r.item_barcode for r in reserves] == [row["item_barcode"] for row in rows]


@pytest.mark.parametrize(
    "update",
    [
        {},
        {"use_gateway_url_for_uuids": True},
        {"use_gateway_url_for_uuids": True, "is_ecs": True, "ecs_tenant_id": "member_tenant"},
    ],
)
def test_course_listing_ids_match_courses_migrator(tmp_path, monkeypatch, update):
    """Reserves must point at the course listing IDs that CoursesMigrator created."""
    from folio_migration_tools.mapper_base import MapperBase
    from folio_migration_tools.mapping_file_transformation.courses_mapper import CoursesMapper
    from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase

    from .test_infrastructure import mocked_classes

    library_config = mocked_classes.get_mocked_library_config().model_copy(update=update)
    folio_client = Mock()
    folio_client.gateway_url = "https://folio-gateway.example.com"
    (tmp_path / "reserves.tsv").write_text("legacy_identifier\titem_barcode\nC1\tI1\n")

    def fake_base_init(self, library_configuration, task_configuration, folio_client):
        self.library_configuration = library_configuration
        self.folio_client = folio_client
        self.folder_structure = Mock(legacy_records_folder=tmp_path)

    monkeypatch.setattr(MigrationTaskBase, "__init__", fake_base_init)
    task_config = Mock(skip_barcode_prevalidation=False)
    task_config.course_reserve_file_path.file_name = "reserves.tsv"
    migrator = ReservesMigrator(task_config, library_config, folio_client)

    mapper = Mock(spec=MapperBase)
    mapper.base_string_for_folio_uuid = MapperBase.base_string_for_folio_uuid.fget(
        Mock(library_configuration=library_config, folio_client=folio_client)
    )
    course_listing_id = CoursesMapper.get_uuid(
        mapper, ({}, "C1"), FOLIONamespaces.course_listing
    )

    assert [r.course_listing_id for r in migrator.semi_valid_reserves] == [course_listing_id]
