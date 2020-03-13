"""Posts a file of Instances to OKAPI"""
import argparse
import json
import time
import traceback

import requests
from folioclient.FolioClient import FolioClient


def main():
    failed_ids = []
    parser = argparse.ArgumentParser()
    parser.add_argument("data_source", help="path to marc records folder")
    parser.add_argument("okapi_url", help=("OKAPI base url"))
    parser.add_argument("tenant_id", help=("id of the FOLIO tenant."))
    parser.add_argument("username", help=("the api user"))
    parser.add_argument("password", help=("the api users password"))
    parser.add_argument("batch_size", help=("batch size"))
    args = parser.parse_args()
    print("\tOkapi URL:\t", args.okapi_url)
    print("\tTenanti Id:\t", args.tenant_id)
    print("\tUsername:   \t", args.username)
    print("\tPassword:   \tSecret")
    print("\tRecord source:\t", args.data_source, flush=True)
    folio_client = FolioClient(
        args.okapi_url, args.tenant_id, args.username, args.password
    )
    i = 0
    batch = []
    with open(args.data_source, "r") as file:
        for row in file:
            i += 1
            try:
                json_rec = json.loads(row.split("\t")[-1])
                batch.append(json_rec)
                if len(batch) == int(args.batch_size):
                    post_batch(folio_client, batch, i, failed_ids)
                    batch = []
            except Exception as exception:
                print(exception, flush=True)
                traceback.print_exc()
    print("Posting last batch")
    post_batch(folio_client, batch, i, failed_ids)


def handle_failed_batch(folio_client, batch, i, failed_ids):
    new_list = list([it for it in batch if it["instanceId"] not in failed_ids])
    print(
        f"reposting new batch {len(failed_ids)} {len(batch)} {len(new_list)}",
        flush=True,
    )
    post_batch(folio_client, new_list, i, failed_ids, True)


def post_batch(folio_client, batch, i, failed_ids, repost=False):
    batch_size = len(batch)
    data = {"holdingsRecords": batch}
    path = "/holdings-storage/batch/synchronous"
    url = folio_client.okapi_url + path
    response = requests.post(
        url, data=json.dumps(data), headers=folio_client.okapi_headers
    )
    if response.status_code != 201:
        # print("Error Posting Batch")
        print(response.status_code)
        print(response.text)
        resp = json.loads(response.text)
        for error in resp["errors"]:
            failed_ids.append(error["parameters"][0]["value"])
        print(f"Error Posting Batch {len(failed_ids)}")
        if not repost:
            handle_failed_batch(folio_client, batch, i, failed_ids)
    else:
        print(
            f"Posting successfull! {i} {response.elapsed.total_seconds()}s", flush=True,
        )


if __name__ == "__main__":
    main()
