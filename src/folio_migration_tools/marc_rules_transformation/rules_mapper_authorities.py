"""The default mapper, responsible for parsing MARC21 records acording to the
FOLIO community specifications"""
import logging
import re
import time
import uuid
from typing import List

import pymarc
from folio_uuid.folio_namespaces import FOLIONamespaces
from folio_uuid.folio_uuid import FolioUUID
from folioclient import FolioClient
from pymarc import Record

from folio_migration_tools.custom_exceptions import TransformationProcessError
from folio_migration_tools.library_configuration import FileDefinition
from folio_migration_tools.library_configuration import IlsFlavour
from folio_migration_tools.library_configuration import LibraryConfiguration
from folio_migration_tools.marc_rules_transformation.conditions import Conditions
from folio_migration_tools.marc_rules_transformation.hrid_handler import HRIDHandler
from folio_migration_tools.marc_rules_transformation.rules_mapper_base import (
    RulesMapperBase,
)
from folio_migration_tools.report_blurbs import Blurbs


class AuthorityMapper(RulesMapperBase):
    non_repatable_fields = [
        "100",
        "110",
        "111",
        "130",
        "147",
        "148",
        "150",
        "151",
        "155",
        "162",
        "180",
        "181",
        "182",
        "185",
        "378",
        "384",
    ]
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
            self.get_authority_json_schema(folio_client),
            Conditions(folio_client, self, "auth", library_configuration.folio_release),
        )
        self.srs_recs: list = []
        logging.info("Fetching mapping rules from the tenant")
        rules_endpoint = "/mapping-rules/marc-authority"
        self.mappings = self.folio_client.folio_get_single_object(rules_endpoint)
        self.source_file_mapping = {}
        self.setup_source_file_mapping()
        self.start = time.time()

    def get_legacy_ids(self, marc_record: Record, idx: int) -> List[str]:
        ils_flavour: IlsFlavour = self.task_configuration.ils_flavour
        if ils_flavour in {IlsFlavour.sierra, IlsFlavour.millennium}:
            raise TransformationProcessError("", f"ILS {ils_flavour} not configured")
        elif ils_flavour == IlsFlavour.tag907y:
            return RulesMapperBase.get_bib_id_from_907y(marc_record, idx)
        elif ils_flavour == IlsFlavour.tagf990a:
            return RulesMapperBase.get_bib_id_from_990a(marc_record, idx)
        elif ils_flavour == IlsFlavour.aleph:
            raise TransformationProcessError("", f"ILS {ils_flavour} not configured")
        elif ils_flavour in {IlsFlavour.voyager, "voyager", IlsFlavour.tag001}:
            return RulesMapperBase.get_bib_id_from_001(marc_record, idx)
        elif ils_flavour == IlsFlavour.koha:
            raise TransformationProcessError("", f"ILS {ils_flavour} not configured")
        elif ils_flavour == IlsFlavour.none:
            return [str(uuid.uuid4())]
        else:
            raise TransformationProcessError("", f"ILS {ils_flavour} not configured")

    def parse_record(
        self, marc_record: pymarc.Record, file_def: FileDefinition, legacy_ids: List[str]
    ) -> list[dict]:
        """Parses an auth recod into a FOLIO Authority object
         This is the main function

        Args:
            legacy_ids (_type_): _description_
            marc_record (Record): _description_
            file_def (FileDefinition): _description_

        Returns:
            dict: _description_
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

        self.perform_additional_parsing(folio_authority)
        clean_folio_authority = self.validate_required_properties(
            "-".join(legacy_ids), folio_authority, self.schema, FOLIONamespaces.instances
        )
        self.dedupe_rec(clean_folio_authority)
        marc_record.remove_fields(*list(bad_tags))
        self.report_folio_mapping(clean_folio_authority, self.schema)
        return [clean_folio_authority]

    def perform_initial_preparation(self, marc_record: pymarc.Record, legacy_ids):
        folio_authority = {
            "metadata": self.folio_client.get_metadata_construct(),
        }
        folio_authority["id"] = str(
            FolioUUID(
                str(self.folio_client.okapi_url),
                FOLIONamespaces.authorities,
                str(legacy_ids[-1]),
            )
        )
        HRIDHandler.handle_035_generation(
            marc_record, legacy_ids, self.migration_report, False, False
        )
        self.map_source_file_and_natural_id(marc_record, legacy_ids, folio_authority)
        return folio_authority

    def map_source_file_and_natural_id(self, marc_record, legacy_ids, folio_authority):
        """Implement source file and natural ID mappings according to MODDICORE-283"""
        match_prefix_patt = re.compile("^[A-Za-z]+")
        natural_id = None
        source_file_id = None
        if has_010 := marc_record.get("010"):
            if has_010a := has_010.get_subfields("a"):
                for a_subfield in has_010a:
                    natural_id_prefix = match_prefix_patt.match(a_subfield)
                    if natural_id_prefix and (
                        source_file := self.source_file_mapping.get(
                            natural_id_prefix.group(0), None
                        )
                    ):
                        natural_id = "".join(a_subfield.split())
                        source_file_id = source_file["id"]
                        self.migration_report.add_general_statistics("naturalId mapped from 010$a")
                        self.migration_report.add(
                            Blurbs.AuthoritySourceFileMapping,
                            f"{source_file['name']} -- {natural_id_prefix.group(0)} -- 010$a",
                            number=1,
                        )
                        break
        if not source_file_id:
            natural_id = "".join(marc_record["001"].data.split())
            self.migration_report.add_general_statistics("naturalId mapped from 001")
            natural_id_prefix = match_prefix_patt.match(natural_id)
            if natural_id_prefix:
                if source_file := self.source_file_mapping.get(natural_id_prefix.group(0), None):
                    source_file_id = source_file["id"]
                    self.migration_report.add(
                        Blurbs.AuthoritySourceFileMapping,
                        f"{source_file['name']} -- {natural_id_prefix.group(0)} -- 001",
                        number=1,
                    )
        folio_authority["naturalId"] = natural_id
        if source_file_id:
            folio_authority["sourceFileId"] = source_file_id

    def setup_source_file_mapping(self):
        if self.folio_client.authority_source_files:
            for source_file in self.folio_client.authority_source_files:
                for sf_code in source_file.get("codes", []):
                    self.source_file_mapping[sf_code] = source_file

    def perform_additional_parsing(
        self,
        folio_authority: dict,
    ) -> None:
        """Do stuff not easily captured by the mapping rules

        Args:
            folio_authority (dict): _description_
        """
        folio_authority["source"] = "MARC"

    def get_authority_json_schema(self, folio_client: FolioClient):
        """Fetches the JSON Schema for autorities"""
        return folio_client.get_from_github(
            "folio-org", "mod-inventory-storage", "/ramls/authorities/authority.json"
        )

    def wrap_up(self):
        logging.info("Mapper wrapping up")
