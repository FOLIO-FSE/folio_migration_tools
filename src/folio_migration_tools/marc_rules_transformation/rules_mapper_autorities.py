"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import logging
import time
from typing import List

import pymarc
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from pymarc.record import Record

from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)


class AuthorityMapper(RulesMapperBase):
    """_summary_

    Args:
        RulesMapperBase (_type_): _description_
    """

    def __init__(
        self,
        folio_client,
        library_configuration: LibraryConfiguration,
        task_configuration,
    ):
        super().__init__(
            folio_client,
            library_configuration,
            task_configuration,
            Conditions(folio_client, self, "auth"),
        )
        self.record_status: dict = {}
        self.id_map: dict = {}
        self.srs_recs: list = []
        self.schema = self.get_autority_json_schema()
        self.mapped_folio_fields: dict = {}
        logging.info("Fetching mapping rules from the tenant")
        rules_endpoint = "/mapping-rules/marc-auth"
        self.mappings = self.folio_client.folio_get_single_object(rules_endpoint)
        self.start = time.time()

    def parse_auth(self, legacy_ids, marc_record: pymarc.Record, file_def: FileDefinition):
        """Parses an auth recod into a FOLIO Authority object
         This is the main function

        Args:
            legacy_ids (_type_): _description_
            marc_record (pymarc.Record): _description_
            file_def (FileDefinition): _description_

        Returns:
            _type_: _description_
        """
        self.print_progress()
        ignored_subsequent_fields: set = set()
        bad_tags = set(self.task_configuration.tags_to_delete)  # "907"
        folio_authority = self.perform_initial_preparation(marc_record, legacy_ids)
        for marc_field in marc_record:
            self.report_marc_stats(marc_field, bad_tags, legacy_ids, ignored_subsequent_fields)
            if marc_field.tag not in ignored_subsequent_fields:
                self.process_marc_field(
                    folio_authority,
                    marc_field,
                    ignored_subsequent_fields,
                    legacy_ids,
                )

        self.perform_additional_parsing(folio_authority, marc_record, legacy_ids, file_def)
        clean_folio_authority = self.validate_required_properties(
            "-".join(legacy_ids), folio_authority, self.schema, FOLIONamespaces.instances
        )
        self.dedupe_rec(clean_folio_authority)
        marc_record.remove_fields(*list(bad_tags))
        self.report_folio_mapping(clean_folio_authority, self.instance_json_schema)
        # TODO: trim away multiple whitespace and newlines..
        # TODO: createDate and update date and catalogeddate
        return clean_folio_authority

    def perform_initial_preparation(self, marc_record: pymarc.Record, legacy_ids):
        folio_authority = {
            "metadata": self.folio_client.get_metadata_construct(),
        }
        folio_authority["id"] = str(
            FolioUUID(
                str(self.folio_client.okapi_url),
                FOLIONamespaces.athorities,
                str(legacy_ids[-1]),
            )
        )
        self.hrid_handler.handle_hrid(
            FOLIONamespaces.athorities,
            folio_authority,
            marc_record,
            legacy_ids,
        )
        if self.task_configuration.add_administrative_notes_with_legacy_ids:
            for legacy_id in legacy_ids:
                self.add_legacy_id_to_admin_note(folio_authority, legacy_id)

        return folio_authority

    def perform_additional_parsing(
        self,
        folio_authority: dict,
        marc_record: Record,
        legacy_ids: List[str],
        file_def: FileDefinition,
    ) -> None:
        """Do stuff not easily captured by the mapping rules

        Args:
            folio_authority (dict): _description_
            marc_record (Record): _description_
            legacy_ids (List[str]): _description_
            file_def (FileDefinition): _description_
        """
        folio_authority["source"] = "MARC"

    def get_autority_json_schema(self, latest_release=True):
        """Fetches the JSON Schema for autorities"""
        return self.folio_client.get_latest_from_github(
            "folio-org", "mod-inventory-storage", "/ramls/authorities/authority.json"
        )
