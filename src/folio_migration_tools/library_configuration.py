from enum import Enum
from typing import Annotated

from pydantic import BaseModel, Field
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
    create_source_records: Annotated[
        bool,
        Field(
            title="Create source records",
            description=(
                "If set to true, the source records will be created in FOLIO. "
                "If set to false, the source records will not be created in FOLIO. "
                "Only applied for MARC-based transformations."
            ),
        ),
    ] = True


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
    ramsons = "ramsons"
    sunflower = "sunflower"
    trillium = "trillium"


class LibraryConfiguration(BaseModel):
    gateway_url: Annotated[
        str,
        Field(
            title="FOLIO API Gateway URL",
            description=(
                "The URL of the FOLIO API gateway instance. "
                "You can find this in Settings > Software versions > API gateway services."
            ),
            alias="okapi_url"
        ),
    ]
    tenant_id: Annotated[
        str,
        Field(
            title="FOLIO tenant ID",
            description=(
                "The ID of the FOLIO tenant instance. "
                "You can find this in Settings > Software versions > API gateway services. "
                "In an ECS environment, this is the ID of the central tenant, for all configurations."
            ),
        ),
    ]
    ecs_tenant_id: Annotated[
        str,
        Field(
            title="ECS tenant ID",
            description=(
                "For use in ECS environments when the configuration file is meant to target a ",
                "data tenant. Set to the tenant ID of the data tenant.",
            ),
        ),
    ] = ""
    folio_username: Annotated[
        str,
        Field(
            title="FOLIO API Gateway username",
            description=(
                "The username for the FOLIO user account performing the migration. "
                "User should have a full admin permissions/roles in FOLIO. "
            ),
            alias="okapi_username"
        ),
    ]
    folio_password: Annotated[
        str,
        Field(
            title="FOLIO API Gateway password",
            description=(
                "The password for the FOLIO user account performing the migration. "
            ),
            alias="okapi_password"
        )
    ]
    base_folder: DirectoryPath = Field(
        description=(
            "The base folder for migration. "
            "Should ideally be a github clone of the migration_repo_template"
        )
    )
    multi_field_delimiter: Annotated[
        str,
        Field(
            title="Multi field delimiter",
            description=(
                "The delimiter used to separate multiple values in a single field. "
                "This is used for delimited text (CSV/TSV) fields with multiple sub-delimited values."
            ),
        ),
    ] = "<delimiter>"
    failed_records_threshold: Annotated[
        int,
        Field(description=("Number of failed records until the process shuts down")),
    ] = 5000
    failed_percentage_threshold: Annotated[
        int,
        Field(
            description=("Percentage of failed records until the process shuts down")
        ),
    ] = 20
    generic_exception_threshold: Annotated[
        int,
        Field(
            description=("Number of generic exceptions until the process shuts down")
        ),
    ] = 50
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
        bool, Field(title="Add time stamp to file names")
    ] = False
    use_gateway_url_for_uuids: Annotated[
        bool,
        Field(
            title="Use gateway URL for UUIDs",
            description=(
                "If set to true, folio_uuid will use the gateway URL when generating deterministic UUIDs for FOLIO records. "
                "If set to false (default), the UUIDs will be generated using the tenant_id (or ecs_tenant_id)."
            ),
        ),
    ] = False
    is_ecs: Annotated[
        bool,
        Field(
            title="Library is running ECS FOLIO",
            description=(
                "If set to true, the migration is running in an ECS environment. "
                "If set to false (default), the migration is running in a non-ECS environment. "
                "If ecs_tenant_id is set, this will be set to true, regardless of the value here."
            ),
        ),
    ] = False
    ecs_central_iteration_identifier: Annotated[
        str,
        Field(
            title="ECS central iteration identifier",
            description=(
                "The iteration_identifier value from the central tenant configuration that corresponds "
                "to this configuration's iteration_identifier. Used to access the central instances_id_map."
            ),
        ),
    ] = ""
