import pytest

from .test_infrastructure import mocked_classes

GATEWAY_URL = "https://folio-gateway.example.com"


@pytest.mark.parametrize(
    "update, expected",
    [
        ({}, "test_tenant"),
        ({"use_gateway_url_for_uuids": True}, GATEWAY_URL),
        ({"ecs_tenant_id": "member_tenant"}, "member_tenant"),
        ({"use_gateway_url_for_uuids": True, "is_ecs": True}, "test_tenant"),
        (
            {"use_gateway_url_for_uuids": True, "is_ecs": True, "ecs_tenant_id": "member_tenant"},
            "member_tenant",
        ),
    ],
)
def test_folio_uuid_base_string(update, expected):
    library_config = mocked_classes.get_mocked_library_config().model_copy(update=update)

    assert library_config.folio_uuid_base_string(GATEWAY_URL) == expected
