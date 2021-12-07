import json
import logging

import humps
import requests.exceptions
from argparse_prompt import PromptParser
from pydantic import ValidationError

from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.migration_tasks import *  # pylint: disable=wildcard-import, unused-wildcard-import
from migration_tools.migration_tasks import migration_task_base


def parse_args():
    """Parse CLI Arguments"""
    task_classes = [t for t in inheritors(migration_task_base.MigrationTaskBase)]
    parser = PromptParser()
    parser.add_argument("configuration_path", help="Path to configuration file")
    parser.add_argument(
        "task_name",
        help=(
            "Task name. One of one of "
            f'{", ".join([tc.__name__ for tc in task_classes])}'
        ),
    )
    parser.add_argument(
        "--okapi_password", help="pasword for the tenant in the configuration file"
    )
    parser.add_argument(
        "--base_folder_path",
        help=(
            "path to the base folder for this library. "
            " Built on migration_repo_template"
        ),
    )
    return parser.parse_args()


def main():
    try:
        task_classes = [t for t in inheritors(migration_task_base.MigrationTaskBase)]
        args = parse_args()
        with open(args.configuration_path) as config_file_path:
            try:
                config_file_humped = json.load(config_file_path)
                config_file_humped["libraryInformation"][
                    "okapiPassword"
                ] = args.okapi_password
                config_file_humped["libraryInformation"][
                    "baseFolder"
                ] = args.base_folder_path
                config_file = humps.decamelize(config_file_humped)
                library_config = LibraryConfiguration(
                    **config_file["library_information"]
                )

                migration_task_config = next(
                    t
                    for t in config_file["migration_tasks"]
                    if t["name"] == args.task_name
                )
                # This is how to get the schema print(library_config.schema_json(indent=2))
                try:
                    task_class = next(
                        tc
                        for tc in task_classes
                        if tc.__name__ == migration_task_config["migration_task_type"]
                    )
                    task_config = task_class.TaskConfiguration(**migration_task_config)
                    task_obj = task_class(task_config, library_config)
                    task_obj.do_work()
                    task_obj.wrap_up()
                except StopIteration:
                    print(
                        f'Referenced task {migration_task_config["migration_task_type"]} '
                        "is not a valid option. Update your task to incorporate "
                        f"one of {json.dumps([tc.__name__ for tc in task_classes], indent=4)}"
                    )

            except ValidationError as e:
                print(e.json())
                print("Validation errors in configuration file:")
                print("==========================================")

                for validation_message in json.loads(e.json()):
                    print(
                        f"{validation_message['msg']}\t"
                        f"{'-'.join(str(x) for x in validation_message['loc'])}"
                    )
                print("Halting")

            # task_obj.do_work()
            logging.info("Work done, wrapping up")
        # task_obj.wrap_up()
    except requests.exceptions.SSLError:
        print("\nSSL error. Are you connected to the Internet and the VPN?")


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
