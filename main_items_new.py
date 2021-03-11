'''Main "script."'''
import argparse
import csv
import ctypes
from marc_to_folio.mapping_file_transformation.item_mapper import ItemMapper

import traceback
import json
import logging
import os
import pymarc
from os import listdir
from os.path import isfile, join
from folioclient.FolioClient import FolioClient
from marc_to_folio.items_default_mapper import ItemsDefaultMapper
from marc_to_folio.items_processor import ItemsProcessor
from typing import Dict, List

csv.field_size_limit(int(ctypes.c_ulong(-1).value // 2))


class Worker:
    """Class that is responsible for the acutal work"""

    def __init__(
        self, folio_client: FolioClient, mapper: ItemMapper, files, results_path
    ):
        self.folio_client = folio_client
        self.files = files
        self.results_path = results_path
        self.mapper = mapper
        self.failed_files: List[str] = list()
        print("Init done")

    def work(self):
        total_records = 0
        print("Starting....")
        for file_name in self.files:
            print(f"Processing {file_name}")
            try:
                with open(file_name, encoding="utf-8-sig") as records_file, open(
                    os.path.join(self.results_path, "folio_items.json"), "w+"
                ) as results_file:
                    self.mapper.add_stats("Number of files processed")
                    for idx, record in enumerate(self.mapper.get_objects(records_file)):
                        # if idx > 5:
                        #    continue
                        try:
                            folio_rec = self.mapper.do_map(record)
                            write_to_file(results_file, False, folio_rec)
                            self.mapper.add_stats("Number of records written to disk")
                        except Exception as excepion:
                            traceback.print_exc()
                            print(f"row {idx:,} failed with the following Exception: {excepion}")
                            # raise excepion
                        self.mapper.add_stats("Number of Legacy items in file")
                        if idx % 50000 == 0:
                            print(f"{idx:,} records processed")        
                    total_records += idx    
                    print(f"Done processing {file_name} containing {idx:,} records. Total records processed: {total_records:,}")
                    
            except Exception as ee:
                print(f"processing of {file_name} failed: {ee}.")
                print("Check source files for empty lines. ")
                self.mapper.add_to_migration_report("Failed files", f"{file_name} - {ee}")
        print(f"processed {total_records:,} records in {len(self.files)} files", flush=True)
        self.total_records = total_records

    def wrap_up(self):
        print("Done. Wrapping up...")
        self.mapper.print_dict_to_md_table(self.mapper.stats)
        p = os.path.join(self.results_path, "item_transformation_report.md",)
        with open(p, "w") as migration_report_file:
            self.mapper.write_migration_report(migration_report_file)
            self.mapper.print_mapping_report(migration_report_file, self.total_records)
        print("done")


def write_to_file(file, pg_dump, folio_record):
    """Writes record to file. pg_dump=true for importing directly via the
    psql copy command"""
    if pg_dump:
        file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
    else:
        file.write("{}\n".format(json.dumps(folio_record)))


def parse_args():
    """Parse CLI Arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("records_path", help="path to items file")
    parser.add_argument("result_path", help="path to Instance results file")
    parser.add_argument("map_path", help=("Path of the mapping rules folder"))
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    args = parser.parse_args()
    return args


def main():
    """Main Method. Used for bootstrapping. """
    csv.register_dialect("tsv", delimiter="\t")
    args = parse_args()
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    files = [
        join(args.records_path, f)
        for f in listdir(args.records_path)
        if isfile(join(args.records_path, f))
    ]
    print(f"Files to process: {files}")
    holdings_id_dict_path = os.path.join(args.result_path, "holdings_id_map.json")
    items_map_path = os.path.join(args.map_path, "item_mapping.json")
    location_map_path = os.path.join(args.map_path, "locations.tsv")
    # items_type_map_path = os.path.join(args.map_path, "item_types.tsv")
    loans_type_map_path = os.path.join(args.map_path, "loan_types.tsv")
    material_type_map_path = os.path.join(args.map_path, "material_types.tsv")
    # Item Type map trumps the others. That can have mappings to both LT and MT
    # if isfile(items_type_map_path):
    #    print("Item type map found. leaning on this file for mapping")
    #    with open(items_type_map_path) as item_types_file:
    #        item_type_map = list(csv.DictReader(item_types_file, dialect="tsv"))
    if not isfile(loans_type_map_path):
        raise Exception(f"No file called loan_types.tsv present in {args.map_path}")
    if not isfile(material_type_map_path):
        raise Exception(f"No file called material_types.tsv present in {args.map_path}")
    if not isfile(items_map_path):
        raise Exception(f"No file called item_to_item.tsv present in {args.map_path}")

    # Files found, let's go
    print("MaterialType & LoanType mapping files found. Relying on these for mapping")
    with open(material_type_map_path) as material_type_file:
        material_type_map = list(csv.DictReader(material_type_file, dialect="tsv"))
        print(f"Found {len(material_type_map)} rows in material type map")
    with open(loans_type_map_path) as loans_type_file:
        loan_type_map = list(csv.DictReader(loans_type_file, dialect="tsv"))
        print(f"Found {len(loan_type_map)} rows in loan type map")
        print(
            f'{",".join(loan_type_map[0].keys())} will be used for determinig loan type'
        )

    with open(holdings_id_dict_path, "r") as holdings_id_map_file, open(
        items_map_path
    ) as items_mapper_f, open(location_map_path) as location_map_f:
        holdings_id_map = json.load(holdings_id_map_file)
        items_map = json.load(items_mapper_f)
        print(f'{len(items_map["data"])} fields in item mapping file map')
        mapped_fields = (
            f
            for f in items_map["data"]
            if f["legacy_field"] and f["legacy_field"] != "Not mapped"
        )
        print(f'{len(list(mapped_fields))} Mapped fields in item mapping file map')
        location_map = list(csv.DictReader(location_map_f, dialect="tsv"))
        print(f"Found {len(location_map)} rows in location map")

    mapper = ItemMapper(
        folio_client,
        items_map,
        material_type_map,
        loan_type_map,
        location_map,
        holdings_id_map,
    )
    worker = Worker(folio_client, mapper, files, args.result_path)
    worker.work()
    worker.wrap_up()


if __name__ == "__main__":
    main()
