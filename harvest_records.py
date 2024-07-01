"""Download metadata from the Deutsche Digitale Bibliothek using the OAI-PMH protocol."""

import concurrent.futures
import os
import time
import xml.etree.ElementTree as ET

import requests

OAI_URL = "https://oai.deutsche-digitale-bibliothek.de/oai"
METADATA_PREFIX = "ddb"
SAVE_DIR = "/home/tim/ddb_metadata2"
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


def parse_sets(xml_data: str) -> list:
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
        params (dict): The parameters to be passed to the OAI-PMH endpoint.
        session (requests.Session): The requests session object.
        retries (int): The number of retries attempted.

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

        else:
            raise e


def list_identifiers(set_spec: str, session: requests.Session) -> list:
    """
    List all identifiers for a given set. Collects all identifiers across paginated results.

    Args:
        set_spec (str): The set specification (set-id).
        session (requests.Session): The requests session object.

    Returns:
        list: A list of found identifiers.

    """

    identifiers = []
    expected_identifiers_in_set = 0
    resumption_token = None

    params = {
        "verb": "ListIdentifiers",
        "metadataPrefix": METADATA_PREFIX,
        "set": set_spec,
    }

    while True:
        if resumption_token:
            params = {"verb": "ListIdentifiers", "resumptionToken": resumption_token}

        response = make_request(params, session)

        if response.status_code == 200:
            identifiers.extend(parse_identifiers(response.text))

            if expected_identifiers_in_set == 0:
                try:
                    expected_identifiers_in_set = int(
                        ET.fromstring(response.text)
                        .find(
                            ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
                        )
                        .attrib["completeListSize"]
                    )

                except AttributeError:
                    print("No resumption token found.")

                    expected_identifiers_in_set = len(identifiers)

            print(
                f"Identifiers found: {len(identifiers)}/{expected_identifiers_in_set}"
            )

            root = ET.fromstring(response.text)
            resumption_token = root.findtext(
                ".//{http://www.openarchives.org/OAI/2.0/}resumptionToken"
            )

            if not resumption_token:
                break

        else:
            break

    if len(identifiers) != expected_identifiers_in_set:
        print("-" * 60)
        print(
            f"Error: Expected {expected_identifiers_in_set} identifiers, found {len(identifiers)}."
        )
        print("-" * 60)

    return identifiers


def parse_identifiers(xml_data: str) -> list:
    """
    Parse the XML data to extract the identifiers.

    Args:
        xml_data (str): The XML response from the ListIdentifiers call on the OAI-PMH endpoint.

    Returns:
        list: A list of identifiers.
    """

    identifiers = []
    root = ET.fromstring(xml_data)

    for identifier in root.findall(
        ".//{http://www.openarchives.org/OAI/2.0/}identifier"
    ):
        identifiers.append(identifier.text)

    return identifiers


def get_record_information(
    identifier: str, session: requests.Session
) -> requests.Response:
    """
    Get the record information for a given identifier.

    Args:
        identifier (str): The identifier of the record.
        session (requests.Session): The requests session object.

    Returns:
        requests.Response: The metadata for a record as xml response.

    """

    params = {
        "verb": "GetRecord",
        "metadataPrefix": METADATA_PREFIX,
        "identifier": identifier,
    }

    retries = 0

    while retries < MAX_RETRIES:
        response = make_request(params, session)
        if (
            response.status_code != 200
            or '<error code="cannotDisseminateFormat">' in response.text
            or '<error code="idDoesNotExist">' in response.text
        ):
            retries += 1
            time.sleep(5 * retries)
            print(f"Error: {response.text}")
            print(f"Retrying in {5 * retries} seconds ...")
        else:
            break

    return response


def process_record(identifier: str, session, set_spec: str) -> None:
    """
    Process a single identifier and save the metadata to a file.

    Args:
        identifier (str): The identifier of the record.
        session: The requests session object.
        set_spec (str): The set specification (set-id).

    Returns:
        None
    """

    print(f"Processing record: {identifier}")

    result = get_record_information(identifier, session)

    if result.status_code == 200:
        save_metadata(result.text, identifier, set_spec)

    return None


def save_metadata(record_xml: str, identifier: str, dataset_id: str) -> None:
    """
    Save the metadata to a file.

    Args:
        record_xml (str): The metadata as XML.
        identifier (str): The identifier of the record.
        dataset_id (str): The set specification (set-id).

    Returns:
        None

    """

    directory_path = os.path.join(SAVE_DIR, dataset_id)
    os.makedirs(directory_path, exist_ok=True)

    file_path = os.path.join(directory_path, identifier + ".xml")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write(record_xml)

    return None


def harvest_ddb_data():
    """
    Harvest metadata from the DDB OAI-PMH endpoint.
    """

    response = list_sets()

    if response:
        sets = parse_sets(response.text)
        print(f"Found {len(sets)} unique sets.")

        session = requests.Session()

        for set_spec in sets:
            print(f"Processing set: {set_spec}")

            identifiers = list_identifiers(set_spec, session)

            if identifiers:
                print(f"Found {len(identifiers)} identifiers for set {set_spec}")

                with concurrent.futures.ThreadPoolExecutor() as executor:
                    futures = [
                        executor.submit(process_record, identifier, session, set_spec)
                        for identifier in identifiers
                    ]

            else:
                print(f"No identifiers found for set {set_spec}")


if __name__ == "__main__":
    harvest_ddb_data()
