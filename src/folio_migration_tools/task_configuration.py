"""Base task configuration model.

Defines the abstract AbstractTaskConfiguration class that all migration task
configurations inherit from. Provides common configuration fields and utilities
for camelCase conversion for FOLIO API compatibility.
"""

from typing import Annotated

from humps import camelize
from pydantic import BaseModel, ConfigDict, Field


def to_camel(string):
    return camelize(string)


class ReportPrivacy(BaseModel):
    """Controls what legacy data is stored in the report database."""

    capture_legacy_records: Annotated[
        bool,
        Field(
            title="Capture legacy records",
            description=(
                "Store the legacy record (MARC record, delimited row or JSON object) for "
                "each record that has a data issue, so that issues can be reviewed in context. "
                "Set to false to store no legacy record data."
            ),
        ),
    ] = True
    mask_fields: Annotated[
        list[str],
        Field(
            title="Mask fields",
            description=(
                "Fields to mask in stored data. For delimited and JSON data, list the column "
                "or key names. For MARC data, list tags or tag and subfield codes, such as "
                "'100', '245$a' or '008'. Masked values are replaced with a fixed token "
                "before anything is written to the database, including the legacy value of "
                "data issues raised for those fields."
            ),
        ),
    ] = []

    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class AbstractTaskConfiguration(BaseModel):
    """Abstract class for task configuration."""

    name: Annotated[
        str,
        Field(
            description=(
                "Name of this migration task. The name is being used to call the specific "
                "task, and to distinguish tasks of similar types"
            )
        ),
    ]
    migration_task_type: Annotated[
        str,
        Field(
            title="Migration task type",
            description=("The type of migration task you want to perform."),
        ),
    ]
    ecs_tenant_id: Annotated[
        str,
        Field(
            title="ECS tenant ID",
            description=(
                "The tenant ID to use when making requests to FOLIO APIs "
                "for this task, if different from library configuration",
            ),
        ),
    ] = ""
    report_privacy: Annotated[
        ReportPrivacy,
        Field(
            title="Report privacy",
            description="Controls capture and masking of legacy data in the report database.",
        ),
    ] = ReportPrivacy()

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )
