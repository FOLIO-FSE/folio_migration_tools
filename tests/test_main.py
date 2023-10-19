from folio_migration_tools import __main__
from folio_migration_tools.migration_tasks.migration_task_base import MigrationTaskBase
from unittest import mock
import pytest
from pydantic import ValidationError
import json
from textwrap import dedent


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
def test_json_fail(capsys):
    with pytest.raises(SystemExit):
        __main__.main()
    assert capsys.readouterr().out == dedent(
        """\
        {
        
        Expecting property name enclosed in double quotes: line 1 column 2 (char 1)
        Error parsing the above JSON mapping or configruation file. Halting.
        """
    )


@mock.patch(
    "sys.argv",
    ["__main__.py", "tests/test_data/main/json_not_matching_schema.json", "task_name"],
)
def test_validation_fail(capsys):
    with pytest.raises(SystemExit):
        __main__.main()
    assert capsys.readouterr().out == dedent(
        """\
        [
          {
            "loc": [
              "tenant_id"
            ],
            "msg": "field required",
            "type": "value_error.missing"
          }
        ]
        Validation errors in configuration file:
        ==========================================
        field required\ttenantId
        Halting
        """
    )


@mock.patch(
    "sys.argv",
    ["__main__.py", "tests/test_data/main/basic_config.json", "task_name"],
)
def test_migration_task_exhaustion(capsys):
    with pytest.raises(SystemExit):
        __main__.main()
    assert capsys.readouterr().out == dedent(
        """\
        Referenced task name task_name not found in the configuration file. Use one of task
        Halting...
        """
    )
