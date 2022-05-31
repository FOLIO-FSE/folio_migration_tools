import logging
import uuid
from unittest.mock import MagicMock
from unittest.mock import Mock

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
    assert user_mapper.noteprops is not None


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
    assert user_mapper.noteprops is not None
