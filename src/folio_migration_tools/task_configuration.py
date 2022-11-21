from humps import camelize
from pydantic import BaseModel


def to_camel(string):
    return camelize(string)


class AbstractTaskConfiguration(BaseModel):
    class Config:
        alias_generator = to_camel
        allow_population_by_field_name = True
