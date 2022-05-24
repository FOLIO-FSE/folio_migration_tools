import json
import logging
from unittest.mock import MagicMock
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces
from folioclient import FolioClient

from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.courses_mapper import (
    CoursesMapper,
)

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


def get_latest_from_github(owner, repo, file_path):
    return FolioClient.get_latest_from_github(owner, repo, file_path, "")


@pytest.fixture(scope="module")
def mapper(pytestconfig) -> CoursesMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio = Mock(spec=FolioClient)
    mock_folio.okapi_url = "okapi_url"
    mock_folio.tenant_id = "tenant_id"
    mock_folio.username = "username"
    mock_folio.password = "password"  # noqa: S105
    mock_folio.get_latest_from_github = get_latest_from_github
    mock_folio.folio_get_single_object = MagicMock(
        return_value={
            "instances": {"prefix": "pref", "startNumber": "1"},
            "holdings": {"prefix": "pref", "startNumber": "1"},
        }
    )
    """ conf = CoursesMigrator.TaskConfiguration(
        name="test",
        migration_task_type="BibsTransformer",
        courses_file=FileDefinition(file_name="some path"),
    ) """
    lib = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.kiwi,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    return CoursesMapper(mock_folio, basic_course_map, None, lib)


def test_schema():
    schema = CoursesMapper.get_composite_course_schema()
    assert schema


def test_basic_mapping2(mapper, caplog):
    caplog.set_level(25)
    data = {
        "RECORD #(COURSE)": ".r1",
        "COURSE NOTE": "Some note",
        "INSTRUCTOR": "Some instructor",
        "STAFF NOTE": "Some staff note",
        "COURSE": "Course 101",
    }
    res = mapper.do_map(data, 1, FOLIONamespaces.course)
    mapper.perform_additional_mappings(res)
    mapper.notes_mapper.map_notes(data, 1, res[0]["course"]["id"], FOLIONamespaces.course)
    mapper.store_objects(res)
    assert "Level 25" in caplog.text
    assert "notes\t" in caplog.text
    assert "courselisting\t" in caplog.text
    assert "instructor\t" in caplog.text
    assert "course\t" in caplog.text
    generated_objects = {}
    for m in caplog.messages:
        s = m.split("\t")
        generated_objects[s[0]] = json.loads(s[1])
    note = generated_objects["notes"]
    assert note["domain"] == "courses"
    assert note["content"] == "Some staff note"
    assert note["typeId"] == "b2809c3b-ef05-420b-a684-37272bfa70bf"
    assert note["title"] == "Staff note"
    assert note["links"] == [{"id": "3b65328f-9cc4-5987-bafb-6b82bd40864f", "type": "course"}]

    assert res[1] == ".r1"
    courselisting = generated_objects["courselisting"]
    assert courselisting["registrarId"] == "1"
    assert courselisting["locationId"] == "65ccd308-cd96-4590-9531-e29f7e896f80"
    assert courselisting["externalId"] == "11"
    assert courselisting["courseTypeId"] == "428ea311-c2c2-4805-a21e-1d198c7bcd58"

    course = generated_objects["course"]
    assert course["name"] == "Course 101"
    assert course["description"] == "Some note"

    instructor = generated_objects["instructor"]
    assert instructor["name"] == "Some instructor"
    # assert res[0]["courselisting"]["termId"] == ""
    assert res


basic_course_map = {
    "data": [
        {
            "folio_field": "legacyIdentifier",
            "legacy_field": "RECORD #(COURSE)",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "courselisting.termId",
            "legacy_field": "",
            "value": "",
            "description": "Add mapping file. BEGIN DATE and END DATE",
        },
        {
            "folio_field": "courselisting.courseTypeId",
            "legacy_field": "",
            "value": "428ea311-c2c2-4805-a21e-1d198c7bcd58",
            "description": "",
        },
        {
            "folio_field": "courselisting.externalId",
            "legacy_field": "",
            "value": "11",
            "description": "",
        },
        {
            "folio_field": "courselisting.registrarId",
            "legacy_field": "",
            "value": "1",
            "description": "",
        },
        {
            "folio_field": "courselisting.locationId",
            "legacy_field": "",
            "value": "65ccd308-cd96-4590-9531-e29f7e896f80",
            "description": "",
        },
        {"folio_field": "course.name", "legacy_field": "COURSE", "value": "", "description": ""},
        {
            "folio_field": "course.description",
            "legacy_field": "COURSE NOTE",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "instructor.name",
            "legacy_field": "INSTRUCTOR",
            "value": "",
            "description": "",
        },
        {
            "folio_field": "notes[0].domain",
            "legacy_field": "Not mapped",
            "value": "courses",
            "description": "",
        },
        {
            "folio_field": "notes[0].typeId",
            "legacy_field": "",
            "value": "b2809c3b-ef05-420b-a684-37272bfa70bf",
            "description": "",
        },
        {
            "folio_field": "notes[0].title",
            "legacy_field": "",
            "value": "Staff note",
            "description": "",
        },
        {
            "folio_field": "notes[0].content",
            "legacy_field": "STAFF NOTE",
            "value": "",
            "description": "",
        },
    ]
}
