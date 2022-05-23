from folio_migration_tools.mapping_file_transformation.courses_mapper import (
    CoursesMapper,
)


def test_schema():
    schema = CoursesMapper.get_composite_course_schema()
    assert schema
