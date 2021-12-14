import os

from argparse_prompt import PromptParser

from migration_tools.library_configuration import LibraryConfiguration
from migration_tools.migration_tasks import *  # pylint: disable=wildcard-import, unused-wildcard-import
from migration_tools.migration_tasks import migration_task_base


def parse_args():
    """Parse CLI Arguments"""
    parser = PromptParser()
    parser.add_argument("results_path", help="Path to store schemas")
    return parser.parse_args()


def main():
    args = parse_args()
    with open(
        os.path.join(args.results_path, "LibraryConfigurationSchema.json"), "w"
    ) as outfile:
        outfile.write(LibraryConfiguration.schema_json(indent=4))

    for t in inheritors(migration_task_base.MigrationTaskBase):
        with open(
            os.path.join(args.results_path, f"{t.__name__}Schema.json"), "w"
        ) as outfile:
            outfile.write(t.TaskConfiguration.schema_json(indent=4))
    print("done generating schemas.")


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
