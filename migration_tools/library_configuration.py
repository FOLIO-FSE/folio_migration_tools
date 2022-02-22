from typing import List, Optional
from pydantic import BaseModel, Field
from pydantic.types import DirectoryPath
from enum import Enum


class HridHandling(str, Enum):
    default = "default"
    preserve001 = "preserve001"


class FileDefinition(BaseModel):
    file_name: str
    suppressed: Optional[bool] = False


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
    kiwi = "kiwi"
    juniper = "juniper"


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
    failed_records_threshold: Optional[int] = 5000
    failed_percentage_threshold: Optional[int] = 20
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
