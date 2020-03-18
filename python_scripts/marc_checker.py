"""takes a marc21 dump and adds 999$i fields to it"""
import sys
import time
import traceback
from os import listdir
from os.path import isfile, join

from pymarc import MARCReader


def main():
    start = time.time()
    failed_files = []
    files = [
        join(sys.argv[1], f)
        for f in listdir(sys.argv[1])
        if isfile(join(sys.argv[1], f))
    ]
    print("Files to process: {}".format(len(files)))
    i = 0
    failed = 0
    for file_name in files:
        print(f"processing {file_name}", flush=True)
        try:
            with open(file_name, "rb") as marc_file:
                reader = MARCReader(marc_file, "rb")
                reader.hide_utf8_warnings = True
                for idx, marc_record in enumerate(reader):
                    i += 1
                    if i % 1000 == 0:
                        elapsed = i / (time.time() - start)
                        elapsed_formatted = "{0:.3g}".format(elapsed)
                        print(
                            f"{elapsed_formatted} Number of records: {i}, Failed: {failed}",
                            flush=True,
                        )
        except UnicodeDecodeError as decode_error:
            failed += 1
            print(
                f"UnicodeDecodeError in {file_name} for index {idx} (after record id {marc_record['001'].data}) {decode_error}",
                flush=True,
            )
            failed_files.append(file_name)
        except Exception as exception:
            failed += 1
            print(exception)
            traceback.print_exc()
            print(file_name)

    print(failed_files)


if __name__ == "__main__":
    main()
