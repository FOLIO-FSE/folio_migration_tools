"""Utility script to fetch default reference data from FOLIO instances.

Retrieves common reference data (locations, material types, loan types, etc.) from
a FOLIO module GitHub repo and stores it in a JSON file for offline use. Useful for development
and testing without requiring a live FOLIO connection.
"""

import json
import logging
import os
import sys

import httpx

super_schema: dict = {
    "/location-units/campuses": {},
    "/location-units/institutions": {},
    "/location-units/libraries": {},
    "/locations": {},
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
        if not res:
            res = get_ref_data_from_github_folder(
                "folio-org", "mod-inventory-storage", f"/sample-data{entry}"
            )
        print(f"fetched {entry}: {any(res)}")
        if entry == "/holdings-types":
            res.append(
                {
                    "id": "b92480eb-210d-442e-a6f7-4043fe7f0f24",
                    "name": "Unknown",
                    "source": "local",
                    "metadata": {
                        "createdDate": "2022-12-08T09:55:58.067+00:00",
                        "createdByUserId": "7cee216a-b298-4e7c-b678-8917e6b8edd3",
                        "updatedDate": "2022-12-08T09:55:58.067+00:00",
                        "updatedByUserId": "7cee216a-b298-4e7c-b678-8917e6b8edd3",
                    },
                }
            )

        super_schema[entry] = res
    rules = get_ref_data_from_github_folder(
        "folio-org",
        "mod-source-record-manager",
        "/mod-source-record-manager-server/src/main/resources/rules/",
    )
    super_schema["/mapping-rules/marc-authority"] = rules[0]
    super_schema["/mapping-rules/marc-bib"] = rules[1]
    super_schema["/mapping-rules/marc-holdings"] = rules[2]
    # print(json.dumps(super_schema, indent=4))
    with open("./static/reference_data.json", "w") as write_file:
        json.dump(super_schema, write_file, indent=4, sort_keys=True)
    print("done fetching ref data")

    sys.exit(0)


def get_ref_data_from_github_folder(owner, repo, folder_path: str):
    ret_arr = []
    try:
        logging.info("Using GITHUB_TOKEN environment variable for GitHub API Access")
        github_headers = {
            "content-type": "application/json",
            "User-Agent": "Folio Client (https://github.com/FOLIO-FSE/FolioClient)",
            "authorization": f"token {os.environ.get('GITHUB_TOKEN')}",
        }

        latest_path = f"https://api.github.com/repos/{owner}/{repo}/releases/latest"
        req = httpx.get(latest_path, headers=github_headers)
        req.raise_for_status()
        latest = json.loads(req.text)
        # print(json.dumps(latest, indent=4))
        latest_tag = latest["tag_name"]
        path = (
            f"https://api.github.com/repos/{owner}/{repo}/contents{folder_path}?ref={latest_tag}"
        )
        # print(latest_path)
        req = httpx.get(path, headers=github_headers)
        req.raise_for_status()
        file_list = [x["name"] for x in json.loads(req.text) if x["type"] == "file"]
        github_path = "https://raw.githubusercontent.com"
        for file_name in file_list:
            file_path = f"{github_path}/{owner}/{repo}/{latest_tag}{folder_path}/{file_name}"
            resp = httpx.get(file_path, headers=github_headers)
            resp.raise_for_status()
            ret_arr.append(json.loads(resp.text))
        logging.debug(folder_path)
    except Exception as ee:
        logging.error("Something went wrong: %s", ee)
    return ret_arr


if __name__ == "__main__":
    main()
