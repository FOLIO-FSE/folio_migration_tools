''' Class that processes each MARC record '''
import time
import json
from jsonschema import ValidationError, validate


class MarcProcessor():
    '''the processor'''
    def __init__(self, default_mapper, extra_mapper, folio_client,
                 results_file, args):
        self.holdings_count = 0
        self.results_file = results_file
        self.instance_schema = folio_client.get_instance_json_schema()
        self.records_count = 0
        self.default_mapper = default_mapper
        self.extra_mapper = extra_mapper
        self.args = args
        self.start = time.time()

    def process_record(self, marc_record):
        '''processes a marc record and saves it'''
        try:
            self.records_count += 1
            if marc_record['004']:
                self.holdings_count += 1
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.default_mapper.parse_bib(marc_record,
                                                      self.args.data_source)
            if self.extra_mapper:
                self.extra_mapper.parse_bib(marc_record, folio_rec)

            # validate record against json schema
            validate(folio_rec, self.instance_schema)
            # write record to file
            write_to_file(self.results_file,
                          self.args.postgres_dump,
                          folio_rec)
            # Print progress
            if self.records_count % 10000 == 0:
                elapsed = self.records_count/(time.time() - self.start)
                elapsed_formatted = '{0:.3g}'.format(elapsed)
                print("{}\t\t{}".format(elapsed_formatted, self.records_count),
                      flush=True)
        except ValueError as value_error:
            # print(marc_record)
            print(value_error)
            # print(marc_record)
            print("Removing record from idMap")
            if self.extra_mapper:
                self.extra_mapper.remove_from_id_map(marc_record)
            # raise value_error
        except ValidationError as validation_error:
            print("Error validating record. Halting...")
            raise validation_error
        except Exception as inst:
            print(type(inst))
            print(inst.args)
            print(inst)
            print(marc_record)
            raise inst

    def wrap_up(self):
        '''Finalizes the mapping by writing things out.'''
        print("Saving map of old and new IDs")
        if self.extra_mapper.id_map:
            with open(self.args.id_dict_path, 'w+') as id_map_file:
                json.dump(self.extra_mapper.id_map, id_map_file,
                          sort_keys=True, indent=4)
        print("Saving holdings created from bibs")
        if self.extra_mapper:
            if any(self.extra_mapper.holdings_map):
                with open(self.args.holdings_map_path, 'w+') as holdings_file:
                    for key, holding in self.extra_mapper.holdings_map.items():
                        write_to_file(holdings_file, False, holding)


def write_to_file(file, pg_dump, folio_record):
    '''Writes record to file. pg_dump=true for importing directly via the
    psql copy command'''
    if pg_dump:
        file.write('{}\t{}\n'.format(folio_record['id'],
                                     json.dumps(folio_record)))
    else:
        file.write('{}\n'.format(json.dumps(folio_record)))
