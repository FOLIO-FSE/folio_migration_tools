''' Class that processes each MARC record '''
import time
import json
from jsonschema import ValidationError, validate


class HoldingsMarcProcessor():
    '''the processor'''
    def __init__(self, default_mapper, folio_client,
                 results_file, args):
        self.results_file = results_file
        self.holdings_schema = folio_client.get_holdings_schema()
        self.records_count = 0
        self.default_mapper = default_mapper
        self.instance_id_map = {}
        self.holdings_id_map = {}
        self.args = args
        self.start = time.time()

    def process_record(self, marc_record):
        '''processes a marc holdings record and saves it'''
        try:
            self.records_count += 1
            # Transform the MARC21 to a FOLIO record
            folio_rec = self.default_mapper.parse_hold(marc_record)
            # validate record against json schema
            validate(folio_rec, self.holdings_schema)
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
        id_map = self.default_mapper.holdings_id_map
        path = self.args.holdings_id_dict_path
        print("Saving map of {} old and new IDs to {}"
              .format(len(id_map), path))
        with open(self.args.holdings_id_dict_path, 'w+') as id_map_file:
            json.dump(id_map, id_map_file,
                      indent=4)
        print(self.default_mapper.folio.missing_location_codes)


def write_to_file(file, pg_dump, folio_record):
    '''Writes record to file. pg_dump=true for importing directly via the
    psql copy command'''
    if pg_dump:
        file.write('{}\t{}\n'.format(folio_record['id'],
                                     json.dumps(folio_record)))
    else:
        file.write('{}\n'.format(json.dumps(folio_record)))
