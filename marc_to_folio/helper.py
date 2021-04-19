import json

class Helper():

    @staticmethod
    def write_to_file(file, folio_record, pg_dump = False):
        """Writes record to file. pg_dump=true for importing directly via the
        psql copy command"""
        if pg_dump:
            file.write("{}\t{}\n".format(folio_record["id"], json.dumps(folio_record)))
        else:
            file.write("{}\n".format(json.dumps(folio_record)))
