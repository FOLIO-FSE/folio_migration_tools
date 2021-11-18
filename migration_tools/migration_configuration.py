import logging

from folio_uuid.folio_namespaces import FOLIONamespaces
from migration_tools.folder_structure import FolderStructure
from folioclient import FolioClient
from requests.exceptions import SSLError
import sys


class MigrationConfiguration:
    def __init__(self, args, object_type: FOLIONamespaces) -> None:
        try:
            self.folio_client = FolioClient(
                args.okapi_url,
                args.tenant_id,
                args.username,
                args.password,
            )
        except SSLError:
            logging.critical(
                "SSL error. Check your VPN or Internet connection. Exiting"
            )
            sys.exit()
        self.log_level_debug: bool = args.log_level_debug
        self.folder_structure = FolderStructure(
            args.base_folder, object_type, args.timestamp
        )
        self.args = args
        self.object_type: FOLIONamespaces = object_type

    def log_configuration(self):
        logging.info("Okapi URL:\t%s", self.folio_client.okapi_url)
        logging.info("Tenant Id:\t%s", self.folio_client.tenant_id)
        logging.info("Username:   \t%s", self.folio_client.username)
        logging.info("Password:   \tSecret")
        self.folder_structure.log_folder_structure()

    def report_configuration(self):
        """This should be filled in eventually"""
        logging.info("Add configuration to migration report")
