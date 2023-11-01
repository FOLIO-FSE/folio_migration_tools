from typing import Annotated

from humps import camelize
from pydantic import BaseModel
from pydantic import Field


def to_camel(string):
    return camelize(string)


class AbstractTaskConfiguration(BaseModel):
    name: str
    ecs_tenant_id: Annotated[
        str,
        Field(
            title="ECS tenant ID",
            description=(
                "The tenant ID to use when making requests to FOLIO APIs for this task, if ",
                "different from library configuration"
            )
        )
    ] = ""

    class Config:
        alias_generator = to_camel
        allow_population_by_field_name = True
