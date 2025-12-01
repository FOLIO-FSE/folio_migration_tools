from typing import Annotated

from humps import camelize
from pydantic import BaseModel, ConfigDict
from pydantic import Field


def to_camel(string):
    return camelize(string)


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

    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
    )
