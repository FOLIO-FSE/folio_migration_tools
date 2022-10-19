import json
import logging
import uuid
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.user_mapper import UserMapper
from folio_migration_tools.migration_tasks.user_transformer import UserTransformer
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def test_get_object_type():
    assert UserTransformer.get_object_type() == FOLIONamespaces.users


def test_basic():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {"ext_id": "externalid_1", "user_name": "user_name_1", "id": "1"}
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    folio_user = user_mapper.perform_additional_mapping(
        legacy_user_record, folio_user, index_or_id
    )
    assert folio_user["externalSystemId"] == "externalid_1"
    assert folio_user["username"] == "user_name_1"
    assert folio_user["id"] == "c2a8733b-4fbc-5ef1-ace9-f02e7b3a6f35"
    assert folio_user["personal"]["preferredContactTypeId"] == "Email"
    assert folio_user["active"] is True
    assert folio_user["requestPreference"]["userId"] == folio_user["id"]


def test_one_to_one_group_mapping():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "patronGroup",
                "legacy_field": "group",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "group": "Group name",
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    folio_user = user_mapper.perform_additional_mapping(
        legacy_user_record, folio_user, index_or_id
    )
    assert folio_user["externalSystemId"] == "externalid_1"
    assert folio_user["username"] == "user_name_1"
    assert folio_user["id"] == "c2a8733b-4fbc-5ef1-ace9-f02e7b3a6f35"
    assert folio_user["personal"]["preferredContactTypeId"] == "Email"
    assert folio_user["active"] is True
    assert folio_user["patronGroup"] == "Group name"
    assert folio_user["requestPreference"]["userId"] == folio_user["id"]


def test_ref_data_group_mapping():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "patronGroup",
                "legacy_field": "group",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "group": "Group name",
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "id": "1",
    }
    groups_map = [
        {"group": "Group name", "folio_group": "FOLIO group name"},
        {"group": "*", "folio_group": "FOLIO fallback group name"},
    ]
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(
        mock_folio, mock_task_config, mock_library_conf, user_map, None, groups_map
    )
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    folio_user = user_mapper.perform_additional_mapping(
        legacy_user_record, folio_user, index_or_id
    )
    assert folio_user["externalSystemId"] == "externalid_1"
    assert folio_user["username"] == "user_name_1"
    assert folio_user["id"] == "c2a8733b-4fbc-5ef1-ace9-f02e7b3a6f35"
    assert folio_user["personal"]["preferredContactTypeId"] == "Email"
    assert folio_user["active"] is True
    assert folio_user["patronGroup"] == "FOLIO group name"
    assert folio_user["requestPreference"]["userId"] == folio_user["id"]


def test_ref_data_departments_mapping():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "departments[0]",
                "legacy_field": "dept",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "dept": "Department name",
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "id": "1",
    }
    departments_map = [
        {"dept": "Department name", "folio_name": "FOLIO user department name"},
        {"dept": "*", "folio_name": "FOLIO fallback user department name"},
    ]
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(
        mock_folio, mock_task_config, mock_library_conf, user_map, departments_map, None
    )
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    folio_user = user_mapper.perform_additional_mapping(
        legacy_user_record, folio_user, index_or_id
    )
    assert folio_user["externalSystemId"] == "externalid_1"
    assert folio_user["username"] == "user_name_1"
    assert folio_user["id"] == "c2a8733b-4fbc-5ef1-ace9-f02e7b3a6f35"
    assert folio_user["personal"]["preferredContactTypeId"] == "Email"
    assert folio_user["active"] is True
    assert folio_user["departments"][0] == "FOLIO user department name"
    assert folio_user["requestPreference"]["userId"] == folio_user["id"]


def test_remove_preferred_first_name_if_empty():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {"ext_id": "externalid_1", "user_name": "user_name_1", "id": "1"}
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)

    assert folio_user["externalSystemId"] == "externalid_1"
    assert folio_user["username"] == "user_name_1"
    assert "personal" not in folio_user


def test_boolean_values_explicitly_true():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine1",
                "legacy_field": "HOMEADDRESS1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine2",
                "legacy_field": "HOMEADDRESS2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressTypeId",
                "legacy_field": "Not mapped",
                "value": "Home",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].city",
                "legacy_field": "HOMECITY",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].postalCode",
                "legacy_field": "HOMEZIP",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].region",
                "legacy_field": "HOMESTATE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].primaryAddress",
                "legacy_field": "Not mapped",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    assert folio_user["personal"]["addresses"][0]["primaryAddress"] is True


def test_boolean_values_explicitly_true_json_string():
    user_map_str = '{ "data": [{ "folio_field": "legacyIdentifier", "legacy_field": "id", "value": "", "description": ""}, { "folio_field": "username", "legacy_field": "user_name", "value": "", "description": "" }, { "folio_field": "externalSystemId", "legacy_field": "ext_id", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine1", "legacy_field": "HOMEADDRESS1", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine2", "legacy_field": "HOMEADDRESS2", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressTypeId", "legacy_field": "Not mapped", "value": "Home", "description": "" }, { "folio_field": "personal.addresses[0].city", "legacy_field": "HOMECITY", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].postalCode", "legacy_field": "HOMEZIP", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].region", "legacy_field": "HOMESTATE", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].primaryAddress", "legacy_field": "Not mapped", "value": true, "description": "" } ] }'  # noqa
    user_map = json.loads(user_map_str)
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    assert folio_user["personal"]["addresses"][0]["primaryAddress"] is True


def test_boolean_values_explicitly_false_json_string():
    user_map_str = '{ "data": [{ "folio_field": "legacyIdentifier", "legacy_field": "id", "value": "", "description": ""},  { "folio_field": "username", "legacy_field": "user_name", "value": "", "description": "" }, { "folio_field": "externalSystemId", "legacy_field": "ext_id", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine1", "legacy_field": "HOMEADDRESS1", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine2", "legacy_field": "HOMEADDRESS2", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressTypeId", "legacy_field": "Not mapped", "value": "Home", "description": "" }, { "folio_field": "personal.addresses[0].city", "legacy_field": "HOMECITY", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].postalCode", "legacy_field": "HOMEZIP", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].region", "legacy_field": "HOMESTATE", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].primaryAddress", "legacy_field": "Not mapped", "value": false, "description": "" } ] }'  # noqa: E501, B950
    user_map = json.loads(user_map_str)
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    assert folio_user["personal"]["addresses"][0]["primaryAddress"] is False


def test_boolean_values_explicitly_true_string():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine1",
                "legacy_field": "HOMEADDRESS1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine2",
                "legacy_field": "HOMEADDRESS2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressTypeId",
                "legacy_field": "Not mapped",
                "value": "Home",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].city",
                "legacy_field": "HOMECITY",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].postalCode",
                "legacy_field": "HOMEZIP",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].region",
                "legacy_field": "HOMESTATE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].primaryAddress",
                "legacy_field": "Not mapped",
                "value": "true",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)

    assert folio_user["personal"]["addresses"][0]["primaryAddress"] == "true"


def test_boolean_values_explicitly_false_string():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine1",
                "legacy_field": "HOMEADDRESS1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine2",
                "legacy_field": "HOMEADDRESS2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressTypeId",
                "legacy_field": "Not mapped",
                "value": "Home",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].city",
                "legacy_field": "HOMECITY",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].postalCode",
                "legacy_field": "HOMEZIP",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].region",
                "legacy_field": "HOMESTATE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].primaryAddress",
                "legacy_field": "Not mapped",
                "value": "false",
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)

    assert folio_user["personal"]["addresses"][0]["primaryAddress"] == "false"


def test_boolean_values_explicitly_false():
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine1",
                "legacy_field": "HOMEADDRESS1",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressLine2",
                "legacy_field": "HOMEADDRESS2",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].addressTypeId",
                "legacy_field": "Not mapped",
                "value": "Home",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].city",
                "legacy_field": "HOMECITY",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].postalCode",
                "legacy_field": "HOMEZIP",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].region",
                "legacy_field": "HOMESTATE",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "personal.addresses[0].primaryAddress",
                "legacy_field": "Not mapped",
                "value": False,
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)

    assert folio_user["personal"]["addresses"][0]["primaryAddress"] is False


def test_json_load_s_booleans():
    json_str = '{"a": true, "b": "true", "c": false, "d": "false", "e": "True", "f": "False"}'
    my_json = json.loads(json_str)
    assert my_json["a"] is True
    assert my_json["b"] is not True
    assert my_json["c"] is False
    assert my_json["d"] is not True and my_json["d"] is not False
    assert my_json["e"] is not True and my_json["e"] is not False
    assert my_json["f"] is not True and my_json["f"] is not False
    serialized_json = json.dumps(my_json)
    assert json_str == serialized_json


def test_json_load_s_booleans_bad_json():
    json_str = '{"a": True}'
    with pytest.raises(Exception):
        json.loads(json_str)


def test_notes(caplog):
    caplog.set_level(25)
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].typeId",
                "legacy_field": "",
                "value": str(uuid.uuid4()),
                "description": "",
            },
            {
                "folio_field": "notes[0].domain",
                "legacy_field": "user_note_title",
                "value": "users",
                "description": "",
            },
            {
                "folio_field": "notes[0].title",
                "legacy_field": "user_note_title",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].content",
                "legacy_field": "user_note",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].popUpOnCheckOut",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "notes[0].popUpOnUser",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "user_note": "note",
        "user_note_title": "Some title",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    folio_user = user_mapper.perform_additional_mapping(
        legacy_user_record, folio_user, index_or_id
    )
    assert "Level 25" in caplog.text
    assert "Some title" in caplog.text

    assert folio_user["externalSystemId"] == "externalid_1"
    assert user_mapper.notes_mapper.noteprops is not None


def test_notes_empty_field(caplog):
    caplog.set_level(25)
    user_map = {
        "data": [
            {
                "folio_field": "username",
                "legacy_field": "user_name",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "externalSystemId",
                "legacy_field": "ext_id",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].typeId",
                "legacy_field": "",
                "value": str(uuid.uuid4()),
                "description": "",
            },
            {
                "folio_field": "domain",
                "legacy_field": "user_note_title",
                "value": "users",
                "description": "",
            },
            {
                "folio_field": "notes[0].title",
                "legacy_field": "user_note_title",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].content",
                "legacy_field": "user_note",
                "value": "",
                "description": "",
            },
            {
                "folio_field": "notes[0].popUpOnCheckOut",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "notes[0].popUpOnUser",
                "legacy_field": "",
                "value": True,
                "description": "",
            },
            {
                "folio_field": "legacyIdentifier",
                "legacy_field": "id",
                "value": "",
                "description": "",
            },
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "user_note": "",
        "user_note_title": "",
        "id": "1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"

    mock_folio = mocked_classes.mocked_folio_client()
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user, index_or_id = user_mapper.do_map(legacy_user_record, "001", FOLIONamespaces.users)
    folio_user = user_mapper.perform_additional_mapping(
        legacy_user_record, folio_user, index_or_id
    )
    assert "Level 25" not in caplog.text

    assert folio_user["externalSystemId"] == "externalid_1"
    assert user_mapper.notes_mapper.noteprops is not None
