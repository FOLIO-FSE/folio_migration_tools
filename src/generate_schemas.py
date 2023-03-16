import json
import os
import sys

from argparse_prompt import PromptParser

from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.mapping_file_transformation.courses_mapper import (
    CoursesMapper,
)
from folio_migration_tools.mapping_file_transformation.order_mapper import (
    CompositeOrderMapper,
)
from folio_migration_tools.mapping_file_transformation.organization_mapper import (
    OrganizationMapper,
)
from folio_migration_tools.migration_tasks import *  # noqa: 403
from folio_migration_tools.migration_tasks import migration_task_base


def parse_args():
    """Parses arguments from the tool

    Returns:
        _type_: The parsed areguments
    """

    parser = PromptParser()
    parser.add_argument("results_path", help="Path to store schemas")
    return parser.parse_args()


def main():
    args = parse_args()
    with open(os.path.join(args.results_path, "LibraryConfigurationSchema.json"), "w") as outfile:
        outfile.write(LibraryConfiguration.schema_json(indent=4))
        outfile.write("\n")

    for t in inheritors(migration_task_base.MigrationTaskBase):
        with open(os.path.join(args.results_path, f"{t.__name__}Schema.json"), "w") as outfile:
            outfile.write(t.TaskConfiguration.schema_json(indent=4))
            outfile.write("\n")

    generate_extended_folio_object_schema(args)

    print("done generating schemas.")
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


def generate_extended_folio_object_schema(args):
    # Generate an organization schema with other objects baked in
    organization_schema = OrganizationMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-organizations-storage", "mod-orgs", "organization"
    )
    with open(os.path.join(args.results_path, "compositeOrganizationSchema.json"), "w") as outfile:
        outfile.write(json.dumps(organization_schema, indent=4))
        outfile.write("\n")

    courses_schema = CoursesMapper.get_composite_course_schema(None)
    with open(os.path.join(args.results_path, "compositeCoursesSchema.json"), "w") as outfile:
        outfile.write(json.dumps(courses_schema, indent=4))
        outfile.write("\n")

    composite_order_schema = CompositeOrderMapper.get_latest_acq_schemas_from_github(
        "folio-org", "mod-orders", "mod-orders", "composite_purchase_order"
    )
    with open(
        os.path.join(args.results_path, "compositePurchaseOrderLineSchema.json"), "w"
    ) as outfile:
        outfile.write(json.dumps(composite_order_schema, indent=4))
        outfile.write("\n")


if __name__ == "__main__":
    main()
