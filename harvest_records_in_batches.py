"""Harvest records from the Deutsche Digitale Bibliothek OAI-PMH endpoint using GetRecords."""

import os
import time
import xml.etree.ElementTree as ET

import requests
from lxml import etree


OAI_URL = "https://oai.deutsche-digitale-bibliothek.de/oai"
METADATA_PREFIX = "ddb"
SAVE_DIR = ""
MAX_RETRIES = 10


def list_sets() -> requests.Response:
    """
    List all sets available in the DDB OAI-PMH endpoint.

    Returns:
        requests.Response: The response object from the OAI-PMH endpoint.

    """

    params = {"verb": "ListSets"}
    result = requests.get(OAI_URL, params=params, timeout=None)

    if result.status_code != 200:
        print(f"Error {result.status_code}: {result.text}")
        return None

    return result


def parse_sets(xml_data: str) -> list[str]:
    """
    Parse the XML data to extract the set specifications (set-ids).

    Args:
        xml_data (str): The XML response from the ListSets call on the OAI-PMH endpoint.

    Returns:
        list: A list of set ids.

    """

    sets = []
    root = ET.fromstring(xml_data)
    for set_element in root.findall(".//{http://www.openarchives.org/OAI/2.0/}set"):
        set_spec = set_element.find(
            "{http://www.openarchives.org/OAI/2.0/}setSpec"
        ).text
        if ":" not in set_spec:
            sets.append(set_spec)
    return sets


def make_request(
    params: dict, session: requests.Session, retries=0
) -> requests.Response:
    """
    Make a request to the OAI-PMH endpoint.

    Args:
        params (dict): The parameters to send with the request.
        session (requests.Session): The requests session object to use for the request.
        retries (int): The number of retries attempted so far.

    Returns:
        requests.Response: The response object from the OAI-PMH endpoint.

    """

    try:
        response = session.get(OAI_URL, params=params, timeout=(20, 80))
        response.raise_for_status()
        return response

    except requests.exceptions.RequestException as e:
        if retries < MAX_RETRIES:
            wait_time = 2**retries
            time.sleep(wait_time)
            return make_request(params, session, retries + 1)

        raise e


def list_records(set_spec: str, session: requests.Session) -> list[str]:
    """
    Get all records for a given set.

    Args:
        set_spec (str): The set specification to list records for.
        session (requests.Session): The requests session object.

    Returns:
        list: A list of record XML strings.

    """

    records = []
    expected_records_in_set = 0
    resumption_token = None

    params = {
        "verb": "ListRecords",
        "metadataPrefix": METADATA_PREFIX,
        "set": set_spec,
    }

    while True:
        if resumption_token:
            params = {
                "verb": "ListRecords",
                "resumptionToken": resumption_token,
            }

        response = make_request(params, session)

        if response.status_code == 200:
            records.extend(parse_records_list(response.text))

            if expected_records_in_set == 0:
                try:
                    expected_records_in_set = int(
                        ET.fromstring(response.text)
                        .find(
                            ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
                        )
                        .attrib["completeListSize"]
                    )

                except AttributeError:
                    print("No resumption token found.")
                    expected_records_in_set = len(records)

            print(f"Records harvested: {len(records)}/{expected_records_in_set}")

            root = ET.fromstring(response.text)
            resumption_token = root.findtext(
                ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
            )

            if not resumption_token:
                break
        else:
            break

    return records


def parse_records_list(xml_data: str) -> list[str]:
    """
    Parse the XML data to extract the records as XML-strings.

    Args:
        xml_data (str): The XML response from the ListRecords call on the OAI-PMH endpoint.

    Returns:
        list: A list of record XML strings.

    """

    records = []

    root = etree.fromstring(xml_data.encode(encoding="utf-8"))

    for record in root.findall(".//{http://www.openarchives.org/OAI/2.0/}record"):
        record_xml = etree.tostring(record, encoding="unicode", pretty_print=True)
        records.append(record_xml)

    return records


def save_record(record_xml: str, dataset_id: str):
    """
    Save the record to a file.

    Args:
        record_xml (str): The record XML as a string.
        dataset_id (str): The dataset identifier.

    """

    output_dir = os.path.join(SAVE_DIR, dataset_id)
    os.makedirs(output_dir, exist_ok=True)

    record = etree.fromstring(record_xml.encode(encoding="utf-8"))
    identifier = record.find(".//{http://www.openarchives.org/OAI/2.0/}identifier").text

    record_file_path = os.path.join(output_dir, f"{identifier}.xml")
    with open(record_file_path, "w", encoding="utf-8") as file:
        file.write(record_xml)


def harvest_ddb_data():
    """
    Harvest records from the DDB OAI-PMH endpoint in batches.

    """

    response = list_sets()

    if response:
        sets = parse_sets(response.text)

        print(f"Found {len(sets)} unique sets.")

        session = requests.Session()

        for set_spec in sets:
            print(f"Processing set: {set_spec}")
            records = list_records(set_spec, session)

            if records:
                for record in records:
                    save_record(record, set_spec)

                print(f"Collected {len(records)} records for set {set_spec}")

            else:
                print(f"No records found for set {set_spec}")


if __name__ == "__main__":
    harvest_ddb_data()
