from enum import Enum
from typing import Annotated
from typing import Optional

from pydantic import BaseModel
from pydantic import Field
from pydantic.types import DirectoryPath


class HridHandling(str, Enum):
    """Enum determining how the HRID generation should be handled.
        - default: Enumerates the HRID, building on the current value in the HRID settings
        - preserve001: Takes the 001 and uses this as the HRID.

    Args:
        str (_type_): _description_
        Enum (_type_): _description_
    """

    default = "default"
    preserve001 = "preserve001"


class FileDefinition(BaseModel):
    file_name: Annotated[
        str,
        Field(
            title="File name",
            description=(
                "Name of the file to be processed. "
                "The location of the file depends on the context"
            ),
        ),
    ] = ""
    discovery_suppressed: Annotated[bool, Field(title="Discovery suppressed")] = False
    staff_suppressed: Annotated[bool, Field(title="Staff suppressed")] = False
    service_point_id: Annotated[str, Field(title="Service point ID")] = ""


class IlsFlavour(str, Enum):
    """ """

    aleph = "aleph"
    voyager = "voyager"
    sierra = "sierra"
    millennium = "millennium"
    koha = "koha"
    tag907y = "tag907y"
    tag001 = "tag001"
    tagf990a = "tagf990a"
    custom = "custom"
    none = "none"


class FolioRelease(str, Enum):
    lotus = "lotus"
    morning_glory = "morning-glory"
    nolana = "nolana"
    orchid = "orchid"
    poppy = "poppy"
    quesnelia = "quesnelia"
    ramsons = "ramsons"
    sunflower = "sunflower"


class LibraryConfiguration(BaseModel):
    okapi_url: str
    tenant_id: str
    ecs_tenant_id: Annotated[
        str,
        Field(
            title="ECS tenant ID",
            description=(
                "For use in ECS environments when the configuration file is meant to target a ",
                "data tenant. Set to the tenant ID of the data tenant."
            )
        )
    ] = ""
    okapi_username: str
    okapi_password: str
    base_folder: DirectoryPath = Field(
        description=(
            "The base folder for migration. "
            "Should ideally be a github clone of the migration_repo_template"
        )
    )
    multi_field_delimiter: Optional[str] = "<delimiter>"
    failed_records_threshold: Annotated[
        int, Field(description=("Number of failed records until the process shuts down"))
    ] = 5000
    failed_percentage_threshold: Annotated[
        int, Field(description=("Percentage of failed records until the process shuts down"))
    ] = 20
    library_name: str
    log_level_debug: bool
    folio_release: FolioRelease = Field(
        description=(
            "The Flavour of the ILS you are migrating from. This choice is "
            "maninly tied to the handling of legacy identifiers and thereby the "
            "deterministic UUIDs generated from them."
        )
    )
    iteration_identifier: str
    add_time_stamp_to_file_names: Annotated[
        bool, 
        Field(title="Add time stamp to file names")
    ] = False
