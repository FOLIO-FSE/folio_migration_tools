from folio_migration_tools import __main__
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from unittest import mock
import pytest


def test_inheritance():
    inheritors = [f.__name__ for f in __main__.inheritors(MigrationTaskBase)]
    assert "HoldingsMarcTransformer" in inheritors
    assert "CoursesMigrator" in inheritors
    assert "UserTransformer" in inheritors
    assert "LoansMigrator" in inheritors
    assert "ItemsTransformer" in inheritors
    assert "HoldingsCsvTransformer" in inheritors
    assert "RequestsMigrator" in inheritors
    assert "OrganizationTransformer" in inheritors
    assert "ReservesMigrator" in inheritors
    assert "BibsTransformer" in inheritors
    assert "BatchPoster" in inheritors
    assert "AuthorityTransformer" in inheritors


@mock.patch("getpass.getpass", create=True)
@mock.patch("builtins.input", create=True)
@mock.patch.dict(
    "os.environ",
    {},
    clear=True,
)
def test_arg_prompts(insecure_inputs, secure_inputs):
    secure_inputs.side_effect = ["okapi_password"]
    insecure_inputs.side_effect = ["folder_path"]
    args = __main__.parse_args(["config_path", "task_name"])
    assert args.__dict__ == {
        "configuration_path": "config_path",
        "task_name": "task_name",
        "base_folder_path": "folder_path",
        "okapi_password": "okapi_password",
        "report_language": "en",
    }


@mock.patch("getpass.getpass", create=True)
@mock.patch("builtins.input", create=True)
@mock.patch.dict(
    "os.environ",
    {},
    clear=True,
)
def test_args_positionally(insecure_inputs, secure_inputs):
    args = __main__.parse_args(
        [
            "config_path",
            "task_name",
            "--base_folder_path",
            "folder_path",
            "--okapi_password",
            "okapi_password",
        ]
    )
    assert args.__dict__ == {
        "configuration_path": "config_path",
        "task_name": "task_name",
        "base_folder_path": "folder_path",
        "okapi_password": "okapi_password",
        "report_language": "en",
    }


@mock.patch("getpass.getpass", create=True)
@mock.patch("builtins.input", create=True)
@mock.patch.dict(
    "os.environ",
    {
        "FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH": "config_path",
        "FOLIO_MIGRATION_TOOLS_TASK_NAME": "task_name",
        "FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH": "folder_path",
        "FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD": "okapi_password",
        "FOLIO_MIGRATION_TOOLS_REPORT_LANGUAGE": "fr",
    },
    clear=True,
)
def test_args_from_env(insecure_inputs, secure_inputs):
    args = __main__.parse_args([])
    assert args.__dict__ == {
        "configuration_path": "config_path",
        "task_name": "task_name",
        "base_folder_path": "folder_path",
        "okapi_password": "okapi_password",
        "report_language": "fr",
    }


@mock.patch("getpass.getpass", create=True)
@mock.patch("builtins.input", create=True)
@mock.patch.dict(
    "os.environ",
    {
        "FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH": "not_config_path",
        "FOLIO_MIGRATION_TOOLS_TASK_NAME": "not_task_name",
        "FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH": "not_folder_path",
        "FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD": "not_okapi_password",
        "FOLIO_MIGRATION_TOOLS_REPORT_LANGUAGE": "not_fr",
    },
    clear=True,
)
def test_args_overriding_env(insecure_inputs, secure_inputs):
    args = __main__.parse_args(
        [
            "config_path",
            "task_name",
            "--base_folder_path",
            "folder_path",
            "--okapi_password",
            "okapi_password",
            "--report_language",
            "fr",
        ]
    )
    assert args.__dict__ == {
        "configuration_path": "config_path",
        "task_name": "task_name",
        "base_folder_path": "folder_path",
        "okapi_password": "okapi_password",
        "report_language": "fr",
    }


@mock.patch("getpass.getpass", create=True)
@mock.patch("builtins.input", create=True)
@mock.patch.dict(
    "os.environ",
    {
        "FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH": "config_path",
        "FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH": "folder_path",
        "FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD": "okapi_password",
        "FOLIO_MIGRATION_TOOLS_REPORT_LANGUAGE": "fr",
    },
    clear=True,
)
def test_task_name_arg_exception(insecure_inputs, secure_inputs):
    args = __main__.parse_args(["task_name"])
    assert args.__dict__ == {
        "configuration_path": "config_path",
        "task_name": "task_name",
        "base_folder_path": "folder_path",
        "okapi_password": "okapi_password",
        "report_language": "fr",
    }


@mock.patch(
    "sys.argv",
    ["__main__.py", "tests/test_data/main/invalid_json.json", "task_name"],
)
@mock.patch.dict(
    "os.environ",
    {
        "FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD": "okapi_password",
        "FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH": ".",
    },
    clear=True,
)
def test_json_fail():
    with pytest.raises(SystemExit) as exit_info:
        __main__.main()

    assert exit_info.value.args[0] == "Invalid JSON"


@mock.patch(
    "sys.argv",
    ["__main__.py", "tests/test_data/main/json_not_matching_schema.json", "task_name"],
)
@mock.patch.dict(
    "os.environ",
    {
        "FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD": "okapi_password",
        "FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH": ".",
    },
    clear=True,
)
def test_validation_fail():
    with pytest.raises(SystemExit) as exit_info:
        __main__.main()
    assert exit_info.value.args[0] == "JSON Not Matching Spec"


@mock.patch(
    "sys.argv",
    ["__main__.py", "tests/test_data/main/basic_config.json", "task_name"],
)
@mock.patch.dict(
    "os.environ",
    {
        "FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD": "okapi_password",
        "FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH": ".",
    },
    clear=True,
)
def test_migration_task_exhaustion():
    with pytest.raises(SystemExit) as exit_info:
        __main__.main()
    assert exit_info.value.args[0] == "Task Name Not Found"
