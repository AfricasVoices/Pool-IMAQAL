import argparse
import csv
import json
import sys

from core_data_modules.cleaners import Codes
from core_data_modules.logging import Logger
from core_data_modules.traced_data.io import TracedDataJsonIO
from id_infrastructure.firestore_uuid_table import FirestoreUuidTable
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

sys.setrecursionlimit(50000)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exports weekly ad contacts from analysis Traced Data")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket"),
    parser.add_argument("uuid_table_credentials_file_url", metavar="uuid-table-credentials-file-url",
                        help="GS URL to the Firebase credentials file to use for the uuid table")
    parser.add_argument("uuid_table_name", metavar="uuid-table-name",
                        help="Name of the uuid table to use to re-identify the participants")
    parser.add_argument("traced_data_paths", metavar="traced-data-paths", nargs="+",
                        help="Paths to the traced data files (either messages or individuals) to extract phone "
                             "numbers from")
    parser.add_argument("csv_output_file_path", metavar="csv-output-file-path",
                        help="Path to a CSV file to write the contacts from the locations of interest to. "
                             "Exported file is in a format suitable for direct upload to Rapid Pro")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    uuid_table_credentials_file_url = args.uuid_table_credentials_file_url
    uuid_table_name = args.uuid_table_name
    traced_data_paths = args.traced_data_paths
    csv_output_file_path = args.csv_output_file_path

    log.info("Initialising uuid table client...")
    credentials = json.loads(google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path,
        uuid_table_credentials_file_url
    ))

    uuid_table = FirestoreUuidTable.init_from_credentials(
        credentials,
        uuid_table_name,
        None  # We can use None here because we're not going to create any ids
    )
    log.info("Initialised uuid table client")

    uuids = set()
    opt_out_uuids = set()

    for path in traced_data_paths:
        log.info(f"Loading previous traced data from file '{path}'...")
        with open(path) as f:
            data = TracedDataJsonIO.import_jsonl_to_traced_data_iterable(f)
        log.info(f"Loaded {len(data)} traced data objects")

        for td in data:
            if td["consent_withdrawn"] == Codes.TRUE:
                opt_out_uuids.add(td["participant_uuid"])

            uuids.add(td["participant_uuid"])
    log.info(f"Loaded {len(uuids)} uuids from TracedData (of which {len(opt_out_uuids)} uuids withdrew consent)")
    uuids = uuids - opt_out_uuids
    log.info(f"Proceeding with {len(uuids)} opt-in uuids")

    log.info(f"Converting {len(uuids)} uuids to urns...")
    urn_lut = uuid_table.uuid_to_data_batch(uuids)
    urns = {urn_lut[uuid] for uuid in uuids}
    log.info(f"Converted {len(uuids)} to {len(urns)}")

    # Export contacts CSV
    log.warning(f"Exporting {len(urns)} urns to {csv_output_file_path}...")
    with open(csv_output_file_path, "w") as f:
        urn_namespaces = {urn.split(":")[0] for urn in urns}
        headers = [f"URN:{namespace}" for namespace in urn_namespaces]

        writer = csv.DictWriter(f, fieldnames=headers, lineterminator="\n")
        writer.writeheader()
        for urn in urns:
            namespace = urn.split(":")[0]
            value = urn.split(":")[1]
            writer.writerow({
                f"URN:{namespace}": value
            })
        log.info(f"Wrote {len(urns)} urns to {csv_output_file_path}")
