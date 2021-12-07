from typing import List, Optional
from pydantic import BaseModel
from pydantic.types import DirectoryPath
from enum import Enum


class HridHandling(str, Enum):
    default = "default"
    preserve001 = "preserve001"


class FileDefinition(BaseModel):
    file_name: str
    suppressed: Optional[bool] = False


class IlsFlavour(str, Enum):
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
    juniper = "juniper"
    iris = "iris"


class LibraryConfiguration(BaseModel):
    okapi_url: str
    tenant_id: str
    okapi_username: str
    okapi_password: str
    ftp_password: Optional[str] = ""
    ftp_user_name: Optional[str] = ""
    base_folder: DirectoryPath
    library_name: str
    log_level_debug: bool
    folio_release: FolioRelease
    iteration_identifier: str
    add_time_stamp_to_file_names: Optional[bool] = True
