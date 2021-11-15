""" Class that processes each MARC record """
import json
import logging
from typing import List
import uuid
from pymarc.field import Field
from pymarc.leader import Leader

from pymarc.record import Record
from folio_uuid.folio_uuid import FOLIONamespaces, FolioUUID
from folioclient import FolioClient
from migration_tools.custom_exceptions import (
    TransformationRecordFailedError,
)
from migration_tools.folder_structure import FolderStructure
from migration_tools.folio_releases import FOLIOReleases
from migration_tools.helper import Helper
from migration_tools.marc_rules_transformation.rules_mapper_bibs import BibsRulesMapper

from migration_tools.report_blurbs import Blurbs


class BibsProcessor:
    """the processor"""

    def __init__(
        self,
        mapper,
        folio_client: FolioClient,
        results_file,
        folder_structure: FolderStructure,
        args,
    ):
        self.ils_flavour = args.ils_flavour
        self.suppress = args.suppress
        self.results_file = results_file
        self.folio_client = folio_client
        self.instance_schema = folio_client.get_instance_json_schema()
        self.mapper: BibsRulesMapper = mapper
        self.args = args
        self.folders = folder_structure
        self.srs_records_file = open(self.folders.srs_records_path, "w+")
        self.instance_id_map_file = open(self.folders.instance_id_map_path, "w+")

    def process_record(self, idx, marc_record: Record, inventory_only):

        """processes a marc record and saves it"""
        try:
            legacy_ids = self.mapper.get_legacy_ids(marc_record, self.ils_flavour, idx)
        except TransformationRecordFailedError as trf:
            trf.log_it()
        except Exception as ee:
            index_or_legacy_id = [
                f"Index in file: {idx}"
            ]  # Only used for reporting purposes
            Helper.log_data_issue(index_or_legacy_id, "001 nor legacy id found", ee)
        folio_rec = None
        try:
            # Transform the MARC21 to a FOLIO record
            (folio_rec, id_map_strings) = self.mapper.parse_bib(
                legacy_ids, marc_record, inventory_only
            )
            prec_titles = folio_rec.get("precedingTitles", [])
            if prec_titles:
                self.mapper.migration_report.add(
                    Blurbs.PrecedingSuccedingTitles, f"{len(prec_titles)}"
                )
                del folio_rec["precedingTitles"]
            succ_titles = folio_rec.get("succeedingTitles", [])
            if succ_titles:
                del folio_rec["succeedingTitles"]
                self.mapper.migration_report.add(
                    Blurbs.PrecedingSuccedingTitles, f"{len(succ_titles)}"
                )
            if self.validate_instance(folio_rec, marc_record, legacy_ids):
                Helper.write_to_file(self.results_file, folio_rec)
                self.save_source_record(marc_record, folio_rec, legacy_ids)
                self.mapper.migration_report.add_general_statistics(
                    "Records successfully transformed into FOLIO objects"
                )
                for id_map_string in id_map_strings:
                    self.instance_id_map_file.write(f"{id_map_string}\n")
                    self.mapper.migration_report.add_general_statistics(
                        "Lines written to identifier map"
                    )

        except ValueError as value_error:
            self.mapper.migration_report.add(
                Blurbs.FieldMappingErrors,
                f"{value_error} for {legacy_ids} ",
            )
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
        except TransformationRecordFailedError as error:
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
            error.id = legacy_ids
            error.log_it()

        except Exception as inst:
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
            self.mapper.migration_report.add_general_statistics(
                f"Transformation exceptions: {inst.__class__.__name__}",
            )
            logging.error(type(inst))
            logging.error(inst.args)
            logging.error(inst)
            logging.error(marc_record)
            if folio_rec:
                logging.error(folio_rec)
            raise inst

    def validate_instance(self, folio_rec, marc_record, index_or_legacy_id: str):
        if not folio_rec.get("title", ""):
            s = f"No title in {index_or_legacy_id}"
            self.mapper.migration_report.add(Blurbs.MissingTitles, s)
            logging.error(s)
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
            return False
        if not folio_rec.get("instanceTypeId", ""):
            s = f"No Instance Type Id in {index_or_legacy_id}"
            self.mapper.migration_report.add(Blurbs.MissingInstanceTypeIds, s)
            self.mapper.migration_report.add_general_statistics(
                "Records that failed transformation. Check log for details",
            )
            return False
        return True

    def wrap_up(self):  # sourcery skip: remove-redundant-fstring
        """Finalizes the mapping by writing things out."""
        try:
            self.mapper.wrap_up()
        except Exception:
            logging.exception("error during wrap up")
        if any(self.mapper.holdings_map):
            logging.info("Saving holdings created from bibs")
            holdings_path = self.folders.data_folder / "folio_holdings_from_bibs.json"
            with open(holdings_path, "w+") as holdings_file:
                for key, holding in self.mapper.holdings_map.items():
                    Helper.write_to_file(holdings_file, holding)
        self.srs_records_file.close()
        self.instance_id_map_file.close()

    def save_source_record(self, marc_record, instance, legacy_ids: List[str]):
        """Saves the source Marc_record to the Source record Storage module"""
        srs_id = str(
            FolioUUID(
                self.folio_client.okapi_url,
                FOLIONamespaces.srs_records,
                str(legacy_ids[0]),
            )
        )

        marc_record.add_ordered_field(
            Field(
                tag="999",
                indicators=["f", "f"],
                subfields=["i", instance["id"], "s", srs_id],
            )
        )
        # Since they all should be UTF encoded, make the leader align.
        try:
            temp_leader = Leader(marc_record.leader)
            temp_leader[9] = "a"
            marc_record.leader = temp_leader
        except Exception:
            logging.exception(
                "Something is wrong with the marc records leader: %s",
                marc_record.leader,
            )
        srs_record_string = get_srs_string(
            (
                marc_record,
                instance,
                srs_id,
                self.folio_client.get_metadata_construct(),
                self.suppress,
            ),
            self.mapper.folio_version,
        )
        self.srs_records_file.write(f"{srs_record_string}\n")


def get_srs_string(my_tuple, folio_version):
    my_tuple_json = my_tuple[0].as_json()
    raw_record = {"id": my_tuple[2], "content": my_tuple_json}
    parsed_record = {"id": my_tuple[2], "content": json.loads(my_tuple_json)}
    record = {
        "id": my_tuple[2],
        "deleted": False,
        "snapshotId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
        "matchedId": my_tuple[2],
        "generation": 0,
        "recordType": "MARC" if folio_version == FOLIOReleases.IRIS else "MARC_BIB",
        "rawRecord": raw_record,
        "parsedRecord": parsed_record,
        "additionalInfo": {"suppressDiscovery": my_tuple[4]},
        "externalIdsHolder": {
            "instanceId": my_tuple[1]["id"],
            "instanceHrid": my_tuple[1]["hrid"],
        },
        "metadata": my_tuple[3],
        "state": "ACTUAL",
        "leaderRecordStatus": parsed_record["content"]["leader"][5]
        if parsed_record["content"]["leader"][5] in [*"acdnposx"]
        else "d",
    }
    return f"{record['id']}\t{json.dumps(record)}"
