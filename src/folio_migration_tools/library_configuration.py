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
    file_name: str
    suppressed: Optional[bool] = False
    staff_suppressed: Optional[bool] = False
    service_point_id: Optional[str] = ""


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
    none = "none"


class FolioRelease(str, Enum):
    lotus = "lotus"
    morning_glory = "morning-glory"


class LibraryConfiguration(BaseModel):
    okapi_url: str
    tenant_id: str
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
    add_time_stamp_to_file_names: Optional[bool] = False
