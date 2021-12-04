from argparse import ArgumentParser
import logging
import json
import requests.exceptions
from argparse_prompt import PromptParser
from migration_tools.library_configuration import LibraryConfiguration
from migration_tasks import *  # pylint: disable=wildcard-import, unused-wildcard-import
from migration_tasks import migration_task_base
from migration_tools.migration_configuration import MigrationConfiguration
import humps
from pydantic import ValidationError


def parse_args():
    """Parse CLI Arguments"""
    parser = ArgumentParser()
    parser.add_argument("configuration_path", help="Path to configuration file")
    parser.add_argument(
        "task_name", help="Name of the Migration Task you want to invoke"
    )
    return parser.parse_args()


def main():
    try:
        task_classes = migration_tasks = [
            t for t in inheritors(migration_task_base.MigrationTaskBase)
        ]
        args = parse_args()
        with open(args.configuration_path) as config_file_path:
            try:
                config_file = humps.decamelize(json.load(config_file_path))
                library_config = LibraryConfiguration(
                    **config_file["library_information"]
                )

                migration_task_config = next(
                    t
                    for t in config_file["migration_tasks"]
                    if t["name"] == args.task_name
                )
                print(library_config.schema_json(indent=2))
                task_class = next(
                    tc
                    for tc in task_classes
                    if tc.__name__ == migration_task_config["migration_task_type"]
                )
                print(json.dumps(migration_task_config))
                task_config = task_class.TaskConfiguration(**migration_task_config)
                # configuration = MigrationConfiguration(args, task_class.get_object_type())
                task_obj = task_class(task_config, library_config)
                task_obj.do_work()
            except ValidationError as e:
                print(e.json())
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
