import json
import logging
from unittest.mock import Mock

import pytest
from folio_uuid.folio_namespaces import FOLIONamespaces

from folio_migration_tools.library_configuration import FolioRelease
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.courses_mapper import (
    CoursesMapper,
)
from folio_migration_tools.migration_tasks.courses_migrator import CoursesMigrator
from folio_migration_tools.test_infrastructure import mocked_classes

LOGGER = logging.getLogger(__name__)
LOGGER.propagate = True


@pytest.fixture(scope="session", autouse=True)
def mapper(pytestconfig) -> CoursesMapper:
    okapi_url = "okapi_url"
    tenant_id = "tenant_id"
    username = "username"
    password = "password"  # noqa: S105

    print("init")
    mock_folio = mocked_classes.mocked_folio_client()

    lib = LibraryConfiguration(
        okapi_url=okapi_url,
        tenant_id=tenant_id,
        okapi_username=username,
        okapi_password=password,
        folio_release=FolioRelease.morning_glory,
        library_name="Test Run Library",
        log_level_debug=False,
        iteration_identifier="I have no clue",
        base_folder="/",
    )
    terms_map = [
        {"BEGIN DATE": "01-10-2022", "END DATE": "05-06-2022", "folio_name": "Fall 2022"},
        {"BEGIN DATE": "*", "END DATE": "*", "folio_name": "Summer 2022"},
    ]
    departments_map = [
        {"DEPT": "dep1", "folio_name": "Department_t"},
        {"DEPT": "*", "folio_name": "Department_FALLBACK"},
    ]
    mocked_config = Mock(spec=CoursesMigrator.TaskConfiguration)
    mocked_config.look_up_instructor = False
    return CoursesMapper(
        mock_folio, basic_course_map, terms_map, departments_map, lib, mocked_config
    )


def test_schema():
    schema = CoursesMapper.get_composite_course_schema()
    assert schema


def test_instructor_fetch(mapper: CoursesMapper, caplog):
    mapper.task_configuration.look_up_instructor = True
    instructor = {"userId": "Some external id"}
    mapper.populate_instructor_from_users(instructor)
    assert instructor["barcode"] == "some barcode"
    assert instructor["userId"] == "some id"
    assert instructor["patronGroup"] == "some group"


def test_instructor_fetch_no_user(mapper: CoursesMapper, caplog):
    mapper.task_configuration.look_up_instructor = True
    instructor = {"userId": "Some external id that does not exist"}
    mapper.populate_instructor_from_users(instructor)
    assert "userId" not in instructor


def test_instructor_cache(mapper: CoursesMapper, caplog):
    mapper.task_configuration.look_up_instructor = True
    instructor = {"userId": "Some external id"}
    mapper.user_cache["Some external id"] = {
        "id": "some id",
        "barcode": "some barcode",
        "patronGroup": "some group",
    }
    mapper.populate_instructor_from_users(instructor)
    assert instructor["barcode"] == "some barcode"
    assert instructor["userId"] == "some id"
    assert instructor["patronGroup"] == "some group"


def test_basic_mapping2(mapper: CoursesMapper, caplog):
    data = {
        "RECORD #(COURSE)": ".r1",
        "COURSE NOTE": "Some note",
        "INSTRUCTOR": "Some instructor",
        "STAFF NOTE": "Some staff note",
        "COURSE": "Course 101",
        "END DATE": "05-06-2022",
        "BEGIN DATE": "01-10-2022",
        "DEPT": "dep1",
    }
    mapper.task_configuration.look_up_instructor = False
    res = mapper.do_map(data, 1, FOLIONamespaces.course)
    mapper.perform_additional_mappings(res)
    mapper.notes_mapper.map_notes(data, 1, res[0]["course"]["id"], FOLIONamespaces.course)
    mapper.store_objects(res)
    assert any("notes\t" in ed for ed in mapper.extradata_writer.cache)

    assert any("courselisting\t" in ed for ed in mapper.extradata_writer.cache)
    assert any("instructor\t" in ed for ed in mapper.extradata_writer.cache)
    assert any("course\t" in ed for ed in mapper.extradata_writer.cache)
    generated_objects = {}
    for m in mapper.extradata_writer.cache:
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
    assert courselisting["termId"] == "42093be3-d1e7-4bb6-b2b9-18e153d109b2"

    course = generated_objects["course"]
    assert course["name"] == "Course 101"
    assert course["description"] == "Some note"
    assert course["departmentId"] == "7532e5ab-9812-496c-ab77-4fbb6a7e5dbf"

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
            "folio_field": "instructors[0].name",
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
