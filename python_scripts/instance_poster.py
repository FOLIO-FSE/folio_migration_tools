"""Posts a file of Instances to OKAPI"""
import argparse
import json
import time
import traceback

import requests
from folioclient.FolioClient import FolioClient


def main():
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
            json_rec = json.loads(row.split("\t")[-1])
            i += 1
            try:
                batch.append(json_rec)
                if len(batch) == int(args.batch_size):
                    post_batch(folio_client, batch, i)
                    batch = []
            except Exception as exception:
                print(exception, flush=True)
                traceback.print_exc()
        print("Posting last batch")
        post_batch(folio_client, batch, i)


def post_batch(folio_client, batch, i):
    batch_size = len(batch)
    data = {"instances": batch}
    path = "/instance-storage/batch/synchronous"
    url = folio_client.okapi_url + path
    response = requests.post(
        url, data=json.dumps(data), headers=folio_client.okapi_headers
    )
    if response.status_code == 422:
        print(f"Error Posting Batch {i}")
        ex = json.loads(response.text)
        new_batch = []
        for error in ex["errors"]:
            print(error)
            if "instance_hrid_idx_unique" in error["message"]:
                hrid = error["parameters"][0]["value"]
                for rec in batch:
                    if "hrid" in rec and rec["hrid"] != hrid:
                        new_batch.append(rec)
                    else:
                        print(f"removed error record with hrid {hrid}")
        if len(new_batch) != len(batch) and len(new_batch) > 0:
            print("reposting batch with error records removed")
            post_batch(folio_client, new_batch, 0)
    elif response.status_code != 201:
        print(f"Error Posting Batch {i}")
        print(response.status_code)
        print(response.text)
        if batch_size > 1:
            handle_failed_batch(folio_client, batch)
    else:
        print(
            f"Posting successfull! {i} {response.elapsed.total_seconds()}s", flush=True
        )


def handle_failed_batch(folio_client, batch):
    i = 0
    for item in batch:
        i += 1
        post_batch(folio_client, [item], i)


if __name__ == "__main__":
    main()
