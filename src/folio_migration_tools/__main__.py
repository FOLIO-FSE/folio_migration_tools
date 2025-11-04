from importlib import metadata
import json
import logging
import sys
from os import environ
from pathlib import Path

import httpx
import humps
import i18n
from argparse_prompt import PromptParser
from folioclient import FolioClient
from pydantic import ValidationError

from folio_migration_tools.config_file_load import merge_load
from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.migration_tasks import *  # noqa: F403, F401
from folio_migration_tools.migration_tasks import migration_task_base


def parse_args(args):
    task_classes = iter(inheritors(migration_task_base.MigrationTaskBase))
    parser = PromptParser()
    parser.add_argument(
        "configuration_path",
        help="Path to configuration file",
        nargs="?" if "FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH" in environ else None,
        prompt="FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH" not in environ,
        default=environ.get("FOLIO_MIGRATION_TOOLS_CONFIGURATION_PATH"),
    )
    tasks_string = ", ".join(sorted(tc.__name__ for tc in task_classes))

    parser.add_argument(
        "task_name",
        help=(f"Task name. Use one of: {tasks_string}"),
        nargs="?" if "FOLIO_MIGRATION_TOOLS_TASK_NAME" in environ else None,
        prompt="FOLIO_MIGRATION_TOOLS_TASK_NAME" not in environ,
        default=environ.get("FOLIO_MIGRATION_TOOLS_TASK_NAME"),
    )
    parser.add_argument(
        "--folio_password",
        "--okapi_password",
        help="password for the tenant in the configuration file",
        prompt="FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD" not in environ,
        default=environ.get(
            "FOLIO_MIGRATION_TOOLS_FOLIO_PASSWORD",
            environ.get("FOLIO_MIGRATION_TOOLS_OKAPI_PASSWORD"),
        ),
        secure=True,
    )
    parser.add_argument(
        "--base_folder_path",
        help=("path to the base folder for this library. Built on migration_repo_template"),
        prompt="FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH" not in environ,
        default=environ.get("FOLIO_MIGRATION_TOOLS_BASE_FOLDER_PATH"),
    )
    parser.add_argument(
        "--report_language",
        help=(
            "Language to write the reports. Defaults english for untranslated languages/strings."
        ),
        default=environ.get("FOLIO_MIGRATION_TOOLS_REPORT_LANGUAGE", "en"),
        prompt=False,
    )
    parser.add_argument(
        "--version",
        "-V",
        help="Show the version of the FOLIO Migration Tools",
        action="store_true",
        prompt=False,
    )
    return parser.parse_args(args)


def prep_library_config(args):
    config_file_humped = merge_load(args.configuration_path)

    # Only set folioPassword if neither folioPassword nor okapiPassword exist in config
    # The Pydantic validator will handle backward compatibility for existing okapiPassword
    if (
        "folioPassword" not in config_file_humped["libraryInformation"]
        and "okapiPassword" not in config_file_humped["libraryInformation"]
    ):
        config_file_humped["libraryInformation"]["folioPassword"] = args.folio_password

    config_file_humped["libraryInformation"]["baseFolder"] = args.base_folder_path
    config_file = humps.decamelize(config_file_humped)
    library_config = LibraryConfiguration(**config_file["library_information"])
    if library_config.ecs_tenant_id:
        library_config.is_ecs = True
    if library_config.ecs_tenant_id and not library_config.ecs_central_iteration_identifier:
        print(
            "ECS tenant ID is set, but no central iteration identifier is provided. "
            "Please provide the central iteration identifier in the configuration file."
        )
        sys.exit("ECS Central Iteration Identifier Not Found")
    return config_file, library_config


def print_version(args):
    if "-V" in args or "--version" in args:
        print(f"FOLIO Migration Tools: {metadata.version('folio_migration_tools')}")
        sys.exit(0)
    return None


def main():
    try:
        task_classes = list(inheritors(migration_task_base.MigrationTaskBase))
        # Check if the script is run with the --version or -V flag
        print_version(sys.argv)

        # Parse command line arguments
        args = parse_args(sys.argv[1:])
        try:
            i18n.load_config(
                Path(
                    environ.get("FOLIO_MIGRATION_TOOLS_I18N_CONFIG_BASE_PATH")
                    or args.base_folder_path
                )
                / "i18n_config.py"
            )
        except i18n.I18nFileLoadError:
            i18n.load_config(Path(__file__).parent / "i18n_config.py")
        i18n.set("locale", args.report_language)
        config_file, library_config = prep_library_config(args)
        try:
            migration_task_config = next(
                t for t in config_file["migration_tasks"] if t["name"] == args.task_name
            )
        except StopIteration:
            task_names = [t.get("name", "") for t in config_file["migration_tasks"]]
            print(
                f"Referenced task name {args.task_name} not found in the "
                f"configuration file. Use one of {', '.join(task_names)}"
                "\nHalting..."
            )
            sys.exit("Task Name Not Found")
        try:
            task_class = next(
                tc
                for tc in task_classes
                if tc.__name__ == migration_task_config["migration_task_type"]
            )
        except StopIteration:
            print(
                f"Referenced task {migration_task_config['migration_task_type']} "
                "is not a valid option. Update your task to incorporate "
                f"one of {json.dumps([tc.__name__ for tc in task_classes], indent=4)}"
            )
            sys.exit("Task Type Not Found")
        try:
            logging.getLogger("httpx").setLevel(
                logging.WARNING
            )  # Exclude info messages from httpx
            with FolioClient(
                library_config.gateway_url,
                library_config.tenant_id,
                library_config.folio_username,
                library_config.folio_password,
            ) as folio_client:
                task_config = task_class.TaskConfiguration(**migration_task_config)
                task_obj = task_class(task_config, library_config, folio_client)
                task_obj.do_work()
                task_obj.wrap_up()
        except TransformationProcessError as tpe:
            logging.critical(tpe.message)
            print(f"\n{tpe.message}: {tpe.data_value}")
            print("Task failure. Halting.")
            sys.exit(1)
        logging.info("Work done. Shutting down")
    except json.decoder.JSONDecodeError as json_error:
        logging.critical(json_error)
        print(json_error.doc)
        print(
            f"\n{json_error}\nError parsing the above JSON mapping or configruation file. Halting."
        )
        sys.exit("Invalid JSON")
    except ValidationError as e:
        print(e.json())
        print("Validation errors in configuration file:")
        print("==========================================")

        for validation_message in json.loads(e.json()):
            print(
                f"{validation_message['msg']}\t"
                f"{', '.join(humps.camelize(str(x)) for x in validation_message['loc'])}"
            )
        print("Halting")
        sys.exit("JSON Not Matching Spec")
    except httpx.HTTPError as connection_error:
        if hasattr(connection_error, "response"):
            print(
                f"\nHTTP Error when connecting to {connection_error.request.url}. "
                f"Status code: {connection_error.response.status_code}. "
                f"\nResponse: {connection_error.response.text}"
            )
        else:
            print(
                f"\nConnection Error when connecting to {connection_error.request.url}. "
                "Are you connected to the Internet/VPN? Do you need to update DNS settings?"
            )
        sys.exit("HTTP Not Connecting")
    except FileNotFoundError as fnf_error:
        print(f"\n{fnf_error.strerror}: {fnf_error.filename}")
        sys.exit("File not found")
    except Exception as ee:
        logging.exception("Unhandled exception")
        print(f"\n{ee}")
        sys.exit(ee.__class__.__name__)
    sys.exit(0)


def inheritors(base_class):
    subclasses = set()
    work = [base_class]
    while work:
        parent = work.pop()
        for child in parent.__subclasses__():
            if child not in subclasses:
                subclasses.add(child)
                work.append(child)
    return subclasses


if __name__ == "__main__":
    main()
