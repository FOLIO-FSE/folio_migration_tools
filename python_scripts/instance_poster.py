'''Posts a file of Instances to OKAPI'''
import argparse
import json
import time
import traceback

import requests
from folioclient.FolioClient import FolioClient


def main():
    start = time.time()
    failed_files = []
    parser = argparse.ArgumentParser()
    parser.add_argument("data_source", help="path to marc records folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    args = parser.parse_args()
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    print("\tRecord source:\t", args.data_source, flush=True)
    folio_client = FolioClient(args.okapi_url,
                               args.tenant_id,
                               args.username,
                               args.password)
    i = 0
    batch = []
    with open(args.data_source, 'r') as file:
        for row in file:
            json_rec = json.loads(row.split('\t')[1])
            i += 1
            try:
                batch.append(json_rec)
                if len(batch) == 1000:
                    data = {"instances": batch}
                    path = "/instance-storage/batch/synchronous"
                    url = folio_client.okapi_url + path
                    response = requests.post(url,
                                             data=json.dumps(data),
                                             headers=folio_client.okapi_headers)
                    if response.status_code != 201:
                        print("Error Posting Instance")
                        print(response.status_code)
                        print(response.text)
                    else:
                        print(
                            f'Posting successfull! {i} {response.elapsed.total_seconds()}s', flush=True)
                    batch = []
            except Exception as exception:
                print(exception, flush=True)
                traceback.print_exc()


if __name__ == '__main__':
    main()
