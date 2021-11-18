from folioclient import FolioClient
import requests.exceptions

from argparse_prompt import PromptParser
from migration_tasks import migration_task_base
from migration_tasks import *
from migration_tools.migration_configuration import MigrationConfiguration


def parse_args(task_classes):
    """Parse CLI Arguments"""
    parser = PromptParser()
    subs = parser.add_subparsers(help="commands", dest="command")
    for task_class in task_classes:
        sub_parser = subs.add_parser(task_class.__name__)
        try:
            task_class.add_arguments(sub_parser)
        except Exception as ee:
            print(task_class.__name__)
            raise ee
    return parser.parse_args()


def main():
    try:
        task_classes = inheritors(migration_task_base.MigrationTaskBase)
        args = parse_args(task_classes)
        task_class = next(tc for tc in task_classes if tc.__name__ == args.command)
        configuration = MigrationConfiguration(args, task_class.get_object_type())
        task_obj = task_class(configuration)
        task_obj.do_work()
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
