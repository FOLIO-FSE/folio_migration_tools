import json
import logging
import uuid
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest
from folioclient import FolioClient

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.user_mapper import UserMapper
from folio_migration_tools.migration_tasks.user_transformer import UserTransformer

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


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
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, "001")

    assert folio_user["externalSystemId"] == "externalid_1"
    assert folio_user["username"] == "user_name_1"


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
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, user_map, "001")
    assert folio_user["personal"]["addresses"][0]["primaryAddress"] is True


def test_boolean_values_explicitly_true_json_string():
    user_map_str = '{ "data": [ { "folio_field": "username", "legacy_field": "user_name", "value": "", "description": "" }, { "folio_field": "externalSystemId", "legacy_field": "ext_id", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine1", "legacy_field": "HOMEADDRESS1", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine2", "legacy_field": "HOMEADDRESS2", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressTypeId", "legacy_field": "Not mapped", "value": "Home", "description": "" }, { "folio_field": "personal.addresses[0].city", "legacy_field": "HOMECITY", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].postalCode", "legacy_field": "HOMEZIP", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].region", "legacy_field": "HOMESTATE", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].primaryAddress", "legacy_field": "Not mapped", "value": true, "description": "" } ] }'  # noqa
    user_map = json.loads(user_map_str)
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, user_map, "001")
    assert folio_user["personal"]["addresses"][0]["primaryAddress"] is True


def test_boolean_values_explicitly_false_json_string():
    user_map_str = '{ "data": [ { "folio_field": "username", "legacy_field": "user_name", "value": "", "description": "" }, { "folio_field": "externalSystemId", "legacy_field": "ext_id", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine1", "legacy_field": "HOMEADDRESS1", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressLine2", "legacy_field": "HOMEADDRESS2", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].addressTypeId", "legacy_field": "Not mapped", "value": "Home", "description": "" }, { "folio_field": "personal.addresses[0].city", "legacy_field": "HOMECITY", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].postalCode", "legacy_field": "HOMEZIP", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].region", "legacy_field": "HOMESTATE", "value": "", "description": "" }, { "folio_field": "personal.addresses[0].primaryAddress", "legacy_field": "Not mapped", "value": false, "description": "" } ] }'  # noqa: E501, B950
    user_map = json.loads(user_map_str)
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "HOMEADDRESS1": "Line 1",
        "HOMEADDRESS2": "Line 2",
        "HOMEZIP": "12345",
        "HOMESTATE": "Sjuhärad",
        "HOMECITY": "Fritsla",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, user_map, "001")
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
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, user_map, "001")

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
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, user_map, "001")

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
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_task_config.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, user_map, "001")

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
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "user_note": "note",
        "user_note_title": "Some title",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, "001")

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
        ]
    }
    legacy_user_record = {
        "ext_id": "externalid_1",
        "user_name": "user_name_1",
        "user_note": "",
        "user_note_title": "",
    }
    mock_library_conf = Mock(spec=LibraryConfiguration)
    mock_task_config = Mock(spec=UserTransformer.TaskConfiguration)
    mock_library_conf.multi_field_delimiter = "<delimiter>"

    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    user_mapper = UserMapper(mock_folio, mock_task_config, mock_library_conf, user_map, None, None)
    folio_user = user_mapper.do_map(legacy_user_record, "001")

    assert "Level 25" not in caplog.text

    assert folio_user["externalSystemId"] == "externalid_1"
    assert user_mapper.notes_mapper.noteprops is not None
