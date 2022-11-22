import json
import logging
import os
import sys

import requests

super_schema = {
    "/alternative-title-types": {},
    "/authority-note-types": {},
    "/call-number-types": {},
    "/classification-types": {},
    "/contributor-name-types": {},
    "/contributor-types": {},
    "/electronic-access-relationships": {},
    "/holdings-note-types": {},
    "/holdings-sources": {},
    "/holdings-types": {},
    "/identifier-types": {},
    "/ill-policies": {},
    "/instance-formats": {},
    "/instance-note-types": {},
    "/instance-relationship-types": {},
    "/instance-statuses": {},
    "/instance-types": {},
    "/item-damaged-statuses": {},
    "/item-note-types": {},
    "/loan-types": {},
    "/location-units": {},
    "/locations": {},
    "/material-types": {},
    "/modes-of-issuance": {},
    "/nature-of-content-terms": {},
    "/service-points": {},
    "/statistical-code-types": {},
    "/statistical-codes": {},
    "/templates": {},
}


def main():
    if not os.environ.get("GITHUB_TOKEN"):
        sys.exit(0)
    for entry in super_schema:
        res = get_ref_data_from_github_folder(
            "folio-org", "mod-inventory-storage", f"/reference-data{entry}"
        )
        print(f"fetched {entry}")
        super_schema[entry] = res
    rules = get_ref_data_from_github_folder(
        "folio-org",
        "mod-source-record-manager",
        "/mod-source-record-manager-server/src/main/resources/rules/",
    )
    super_schema["/mapping-rules/marc-authority"] = rules[0]
    super_schema["/mapping-rules/marc-bib"] = rules[1]
    super_schema["/mapping-rules/marc-holdings"] = rules[2]
    print(json.dumps(super_schema, indent=4))
    with open("./static/reference_data.json", "w") as write_file:
        json.dump(super_schema, write_file, indent=4, sort_keys=True)
    print("done fetching ref data")

    sys.exit(0)


def get_ref_data_from_github_folder(owner, repo, folder_path: str):
    ret_arr = []
    logging.info("Using GITHB_TOKEN environment variable for Gihub API Access")
    github_headers = {
        "content-type": "application/json",
        "User-Agent": "Folio Client (https://github.com/FOLIO-FSE/FolioClient)",
        "authorization": f"token {os.environ.get('GITHUB_TOKEN')}",
    }

    latest_path = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
    req = requests.get(latest_path, headers=github_headers)
    req.raise_for_status()
    latest = json.loads(req.text)
    # print(json.dumps(latest, indent=4))
    latest_tag = latest["tag_name"]
    path = f"https://api.github.com/repos/{owner}/{repo}/contents{folder_path}?ref={latest_tag}"
    # print(latest_path)
    req = requests.get(path, headers=github_headers)
    req.raise_for_status()
    file_list = [x["name"] for x in json.loads(req.text) if x["type"] == "file"]
    github_path = "https://raw.githubusercontent.com"
    for file_name in file_list:
        file_path = f"{github_path}/{owner}/{repo}/{latest_tag}{folder_path}/{file_name}"
        resp = requests.get(file_path, headers=github_headers)
        resp.raise_for_status()
        ret_arr.append(json.loads(resp.text))
    logging.debug(folder_path)
    return ret_arr


if __name__ == "__main__":
    main()
