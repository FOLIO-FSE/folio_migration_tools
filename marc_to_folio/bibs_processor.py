""" Class that processes each MARC record """
from io import StringIO
from marc_to_folio.rules_mapper_bibs import BibsRulesMapper
import uuid
from pymarc.field import Field

from pymarc.writer import JSONWriter, XMLWriter
import time
import json
from datetime import datetime as dt
import os.path
from jsonschema import ValidationError, validate


class BibsProcessor:
    """the processor"""

    def __init__(self, mapper, folio_client, results_file, args):
        self.ils_flavour = args.ils_flavour
        self.create_marc_xml_dump = args.dump
        self.suppress = args.suppress
        self.results_folder = args.results_folder
        self.results_file = results_file
        self.folio_client = folio_client
        self.instance_schema = folio_client.get_instance_json_schema()
        self.mapper: BibsRulesMapper = mapper
        self.args = args
        if self.create_marc_xml_dump:
            self.marc_xml_writer = XMLWriter(
                open(os.path.join(self.results_folder, "marc_xml_dump.xml"), "wb+")
            )
        self.srs_records_file = open(
            os.path.join(self.results_folder, "srs.json"), "w+"
        )
        self.start = time.time()

    def process_record(self, marc_record, inventory_only):
        """processes a marc record and saves it"""
        try:
            legacy_id = self.mapper.get_legacy_id(marc_record, self.ils_flavour)
        except Exception as ee:
            legacy_id = ["unknown"]
        folio_rec = None
        try:
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.mapper.parse_bib(marc_record, inventory_only)
            if self.validate_instance(folio_rec, marc_record):
                write_to_file(self.results_file, self.args.postgres_dump, folio_rec)
                self.save_source_record(marc_record, folio_rec)
                self.mapper.add_stats(
                    self.mapper.stats, "Successfully transformed bibs"
                )

        except ValueError as value_error:
            self.mapper.add_to_migration_report(
                "Records failed to migrate due to Value errors found in Transformation",
                f"{value_error} for {legacy_id} ",
            )
            self.mapper.add_stats(
                self.mapper.stats, "Value Errors (records that failed transformation"
            )
            self.mapper.add_stats(
                self.mapper.stats, "Bib records that faile transformation"
            )
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
        except ValidationError as validation_error:
            self.mapper.add_stats(self.mapper.stats, "Validation Errors")
            self.mapper.add_stats(
                self.mapper.stats, "Bib records that failed transformation"
            )
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
        except Exception as inst:
            remove_from_id_map = getattr(self.mapper, "remove_from_id_map", None)
            if callable(remove_from_id_map):
                self.mapper.remove_from_id_map(marc_record)
            self.mapper.add_stats(
                self.mapper.stats, "Bib records that failed transformation"
            )
            self.mapper.add_stats(self.mapper.stats, "Transformation exceptions")
            print(type(inst))
            print(inst.args)
            print(inst)
            print(marc_record)
            if folio_rec:
                print(folio_rec)
            raise inst

    def validate_instance(self, folio_rec, marc_record):
        if self.args.validate:
            validate(folio_rec, self.instance_schema)
        if not folio_rec.get("title", ""):
            s = f"No title in {marc_record['001'].format_field()}"
            self.mapper.add_to_migration_report("Records without titles", s)
            print(s)
            self.mapper.add_stats(
                self.mapper.stats, "Bib records that failed transformation"
            )
            return False
        if not folio_rec.get("instanceTypeId", ""):
            s = f"No Instance Type Id in {marc_record['001'].format_field()}"
            self.mapper.add_to_migration_report("Records without Instance Type Ids", s)
            self.mapper.add_stats(
                self.mapper.stats, "Bib records that faile transformation"
            )
            return False
        return True

    def wrap_up(self):
        """Finalizes the mapping by writing things out."""
        try:
            self.mapper.wrap_up()
        except Exception as exception:
            print(f"error during wrap up {exception}")
        print("Saving map of old and new IDs")
        if self.mapper.id_map:
            map_path = os.path.join(self.results_folder, "instance_id_map.json")
            with open(map_path, "w+") as id_map_file:
                json.dump(self.mapper.id_map, id_map_file, sort_keys=True, indent=4)
            self.mapper.stats["Number of Instances in map"] = len(self.mapper.id_map)
        print("Saving holdings created from bibs")
        if any(self.mapper.holdings_map):
            holdings_path = os.path.join(self.results_folder, "folio_holdings.json")
            with open(holdings_path, "w+") as holdings_file:
                for key, holding in self.mapper.holdings_map.items():
                    write_to_file(holdings_file, False, holding)
        if self.create_marc_xml_dump:
            self.marc_xml_writer.close()
        self.srs_records_file.close()

    def save_source_record(self, marc_record, instance):
        """Saves the source Marc_record to the Source record Storage module"""
        srs_id = str(uuid.uuid4())

        marc_record.add_ordered_field(
            Field(
                tag="999",
                indicators=["f", "f"],
                subfields=["i", instance["id"], "s", srs_id],
            )
        )
        srs_record_string = get_srs_string(
            (
                marc_record,
                instance["id"],
                srs_id,
                self.folio_client.get_metadata_construct(),
                self.suppress,
            )
        )
        self.srs_records_file.write(f"{srs_record_string}\n")
        if not self.suppress and self.create_marc_xml_dump:
            self.marc_xml_writer.write(marc_record)


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))


def get_srs_string(my_tuple):
    json_string = StringIO()
    writer = JSONWriter(json_string)
    writer.write(my_tuple[0])
    writer.close(close_fh=False)
    raw_record = {"id": my_tuple[2], "content": my_tuple[0].as_json()}
    parsed_record = {"id": my_tuple[2], "content": json.loads(my_tuple[0].as_json())}
    record = {
        "id": my_tuple[2],
        "deleted": False,
        "snapshotId": "67dfac11-1caf-4470-9ad1-d533f6360bdd",
        "matchedId": my_tuple[2],
        "generation": 0,
        "recordType": "MARC",
        "rawRecord": raw_record,
        "parsedRecord": parsed_record,
        "additionalInfo": {"suppressDiscovery": my_tuple[4]},
        "externalIdsHolder": {"instanceId": my_tuple[1]},
        "metadata": my_tuple[3],
        "state": "ACTUAL",
        "leaderRecordStatus": parsed_record["content"]["leader"][5],
    }
    if parsed_record["content"]["leader"][5] in [*"acdnposx"]:
        record["leaderRecordStatus"] = parsed_record["content"]["leader"][5]
    else:
        record["leaderRecordStatus"] = "d"
    return f"{record['id']}\t{json.dumps(record)}\n"
