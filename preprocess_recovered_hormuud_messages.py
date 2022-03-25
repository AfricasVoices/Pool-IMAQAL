import argparse
import csv
import re
from datetime import datetime, timedelta
from decimal import Decimal

import pytz
from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils

log = Logger(__name__)

TARGET_SHORTCODE = "359"


def get_incoming_hormuud_messages_from_rapid_pro(google_cloud_credentials_file_path, rapid_pro_domain,
                                                 rapid_pro_token_file_url,
                                                 created_after_inclusive=None, created_before_exclusive=None):
    log.info("Downloading Rapid Pro access token...")
    rapid_pro_token = google_cloud_utils.download_blob_to_string(
        google_cloud_credentials_file_path, rapid_pro_token_file_url).strip()

    rapid_pro = RapidProClient(rapid_pro_domain, rapid_pro_token)

    all_messages = rapid_pro.get_raw_messages(
        created_after_inclusive=created_after_inclusive,
        created_before_exclusive=created_before_exclusive,
        ignore_archives=True
    )
    log.info(f"Downloaded {len(all_messages)} messages")

    log.info(f"Filtering for messages from URNs on Hormuud's networks")
    hormuud_messages = [msg for msg in all_messages if msg.urn.startswith("tel:+25261") or msg.urn.startswith("tel:+25268")]
    log.info(f"Filtered for messages from URNs on Hormuud's networks: {len(hormuud_messages)} messages remain")

    log.info(f"Filtering for incoming messages")
    incoming_hormuud_messages = [msg for msg in hormuud_messages if msg.direction == "in"]
    log.info(f"Filtered for incoming messages: {len(incoming_hormuud_messages)} remain")

    return incoming_hormuud_messages


def get_incoming_hormuud_messages_from_recovery_csv(csv_path,
                                                    received_after_inclusive=None, received_before_exclusive=None):
    log.info(f"Loading recovered messages from Hormuud csv at {csv_path}...")
    all_recovered_messages = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for line in reader:
            all_recovered_messages.append(line)
    log.info(f"Loaded {len(all_recovered_messages)} messages")

    log.info(f"Filtering for messages sent to the target short code {TARGET_SHORTCODE}...")
    incoming_recovered_messages = [msg for msg in all_recovered_messages if msg["Receiver"] == TARGET_SHORTCODE]
    log.info(f"Filtered for messages sent to the target short code {TARGET_SHORTCODE}: "
             f"{len(incoming_recovered_messages)} recovered messages remain")

    log.info(f"Standardising fieldnames")
    for msg in incoming_recovered_messages:
        msg["Sender"] = "tel:+" + msg["Sender"]
        # Convert times with a try/catch because there are two possible formats due to the omission of ms when ms == 000
        try:
            msg["timestamp"] = pytz.timezone("Africa/Mogadishu").localize(
                datetime.strptime(msg["ReceivedOn"], "%d/%m/%Y %H:%M:%S.%f")
            )
        except ValueError:
            msg["timestamp"] = pytz.timezone("Africa/Mogadishu").localize(
                datetime.strptime(msg["ReceivedOn"], "%d/%m/%Y %H:%M:%S")
            )

    if received_after_inclusive is not None:
        log.info(f"Filtering out messages sent before {received_after_inclusive}...")
        incoming_recovered_messages = [msg for msg in incoming_recovered_messages
                                       if msg["timestamp"] >= received_after_inclusive]
        log.info(f"Filtered out messages sent before {received_after_inclusive}: "
                 f"{len(incoming_recovered_messages)} messages remain")
    if received_before_exclusive is not None:
        log.info(f"Filtering out messages sent after {received_before_exclusive}...")
        incoming_recovered_messages = [msg for msg in incoming_recovered_messages
                                       if msg["timestamp"] < received_before_exclusive]
        log.info(f"Filtered out messages sent after {received_before_exclusive}: "
                 f"{len(incoming_recovered_messages)} messages remain")

    return incoming_recovered_messages


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Uses Rapid Pro's message logs to filter a Hormuud recovery csv for incoming messages on this "
                    "short code that aren't in Rapid Pro. Attempts to identify messages that have already been "
                    "received in Rapid Pro by (i) looking for exact text matches, then (ii) looking for matches after "
                    "applying Excel's data-mangling algorithms, then (iii) matching by timestamp. "
                    "Matches made by method (iii) are exported for manual review")

    parser.add_argument("google_cloud_credentials_file_path", metavar="google-cloud-credentials-file-path",
                        help="Path to a Google Cloud service account credentials file to use to access the "
                             "credentials bucket")
    parser.add_argument("rapid_pro_domain", metavar="rapid-pro-domain",
                        help="URL of the Rapid Pro server to download data from")
    parser.add_argument("rapid_pro_token_file_url", metavar="rapid-pro-token-file-url",
                        help="GS URL of a text file containing the authorisation token for the Rapid Pro server")
    parser.add_argument("start_date", metavar="start-date",
                        help="Timestamp to filter both datasets by (inclusive), as an ISO8601 str")
    parser.add_argument("end_date", metavar="end-date",
                        help="Timestamp to filter both datasets by (exclusive), as an ISO8601 str")
    parser.add_argument("hormuud_csv_input_path", metavar="hormuud-csv-input-path",
                        help="Path to a CSV file issued by Hormuud to recover messages from")
    parser.add_argument("timestamp_matches_log_output_csv_path", metavar="timestamp-matches-log-output-csv-path",
                        help="File to log the matches made between the Rapid Pro and recovery datasets by timestamp, "
                             "for manual review and approval")
    parser.add_argument("output_csv_path", metavar="output-csv-path",
                        help="File to write the filtered, recovered data to, in a format ready for de-identification "
                             "and integration into the pipeline")

    args = parser.parse_args()

    google_cloud_credentials_file_path = args.google_cloud_credentials_file_path
    rapid_pro_domain = args.rapid_pro_domain
    rapid_pro_token_file_url = args.rapid_pro_token_file_url
    start_date = isoparse(args.start_date)
    end_date = isoparse(args.end_date)
    hormuud_csv_input_path = args.hormuud_csv_input_path
    timestamp_matches_log_output_csv_path = args.timestamp_matches_log_output_csv_path
    output_csv_path = args.output_csv_path

    # Get messages from Rapid Pro and from the recovery csv
    rapid_pro_messages = get_incoming_hormuud_messages_from_rapid_pro(
        google_cloud_credentials_file_path, rapid_pro_domain, rapid_pro_token_file_url,
        created_after_inclusive=start_date,
        created_before_exclusive=end_date,
    )
    all_rapid_pro_messages = rapid_pro_messages

    recovered_messages = get_incoming_hormuud_messages_from_recovery_csv(
        hormuud_csv_input_path, received_after_inclusive=start_date, received_before_exclusive=end_date
    )

    # Group the messages by the sender's urn, and store in container dicts where we can write the best matching Rapid
    # Pro message to when we find it.
    recovered_lut = dict()  # of urn -> list of recovered message dict
    recovered_messages.sort(key=lambda msg: msg["timestamp"])
    for msg in recovered_messages:
        urn = msg["Sender"]
        if urn not in recovered_lut:
            recovered_lut[urn] = []
        recovered_lut[urn].append({
            "recovered_message": msg,
            "rapid_pro_message": None
        })

    # Search the recovered messages for exact text matches to each of the Rapid Pro messages.
    # A Rapid Pro message matches a message in the recovery csv if:
    # (i)   the recovery csv message has no match yet,
    # (ii)  the text exactly matches, and
    # (iii) the time at Hormuud differs from the time at Rapid Pro by < 5 minutes (experimental analysis of this
    # dataset showed the mean lag to be roughly 3-4 mins, with >99.99% of messages received within 4 minutes)
    log.info(f"Attempting to match the Rapid Pro messages with the recovered messages...")
    rapid_pro_messages.sort(key=lambda msg: msg.sent_on)
    unmatched_messages = []
    skipped_messages = []
    for rapid_pro_msg in rapid_pro_messages:
        rapid_pro_text = rapid_pro_msg.text

        if rapid_pro_msg.urn not in recovered_lut:
            log.warning(f"URN {rapid_pro_msg.urn} not found in the recovered_lut")
            skipped_messages.append(rapid_pro_msg)
            continue

        for recovery_item in recovered_lut[rapid_pro_msg.urn]:
            if recovery_item["rapid_pro_message"] is None and \
                    recovery_item["recovered_message"]["Message"] == rapid_pro_text and \
                    rapid_pro_msg.sent_on - recovery_item["recovered_message"]["timestamp"] < timedelta(minutes=5):
                recovery_item["rapid_pro_message"] = rapid_pro_msg
                break
        else:
            unmatched_messages.append(rapid_pro_msg)
    log.info(f"Attempted to perform exact matches for {len(rapid_pro_messages)} Rapid Pro messages: "
             f"{len(rapid_pro_messages) - len(unmatched_messages)} matched successfully, "
             f"{len(skipped_messages)} messages skipped due to their urns not being present in the recovery csv, "
             f"{len(unmatched_messages)} unmatched messages remain")

    # Attempt to find matches after simulating Excel-mangling of some of the data.
    rapid_pro_messages = unmatched_messages
    unmatched_messages = []
    for rapid_pro_msg in rapid_pro_messages:
        rapid_pro_text = rapid_pro_msg.text
        rapid_pro_text = rapid_pro_text.replace("\n", " ")  # newlines -> spaces
        if re.compile("^\\s*[0-9][0-9]*\\s*$").match(rapid_pro_text):
            rapid_pro_text = rapid_pro_text.strip()  # numbers with whitespace -> just the number
            if rapid_pro_text.startswith("0"):
                rapid_pro_text = rapid_pro_text[1:]  # replace leading 0
            if Decimal(rapid_pro_text) > 1000000000:
                rapid_pro_text = f"{Decimal(rapid_pro_text):.14E}"  # big numbers -> scientific notation
        if re.compile("^\".*\"$").match(rapid_pro_text):
            rapid_pro_text = rapid_pro_text.replace("\"", "")  # strictly quoted text -> just the text
        rapid_pro_text = rapid_pro_text.encode("ascii", "replace").decode("ascii")  # non-ascii characters -> '?'

        for recovery_item in recovered_lut[rapid_pro_msg.urn]:
            if recovery_item["rapid_pro_message"] is None and \
                    recovery_item["recovered_message"]["Message"] == rapid_pro_text and \
                    rapid_pro_msg.sent_on - recovery_item["recovered_message"]["timestamp"] < timedelta(minutes=5):
                recovery_item["rapid_pro_message"] = rapid_pro_msg
                break
        else:
            unmatched_messages.append(rapid_pro_msg)
    log.info(f"Attempted to perform Excel-mangled matches for {len(rapid_pro_messages)} Rapid Pro messages: "
             f"{len(rapid_pro_messages) - len(unmatched_messages)} matched successfully, "
             f"{len(unmatched_messages)} unmatched messages remain")

    # Finally, search by timestamp, and export these to a log file for manual review.
    # This covers all sorts of weird edge cases, mostly around Hormuud/Excel's handling of special characters.
    rapid_pro_messages = unmatched_messages
    unmatched_messages = []
    with open(timestamp_matches_log_output_csv_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["Rapid Pro", "Hormuud Recovery"])
        writer.writeheader()

        for rapid_pro_msg in rapid_pro_messages:
            for recovery_item in recovered_lut[rapid_pro_msg.urn]:
                if recovery_item["rapid_pro_message"] is None and \
                        rapid_pro_msg.sent_on - recovery_item["recovered_message"]["timestamp"] < timedelta(minutes=5):
                    writer.writerow({
                        "Rapid Pro": rapid_pro_msg.text,
                        "Hormuud Recovery": recovery_item["recovered_message"]["Message"]
                    })

                    recovery_item["rapid_pro_message"] = rapid_pro_msg
                    break
            else:
                unmatched_messages.append(rapid_pro_msg)
    log.info(f"Attempted to perform timestamp matching for {len(rapid_pro_messages)} Rapid Pro messages: "
             f"{len(rapid_pro_messages) - len(unmatched_messages)} matched successfully, "
             f"{len(unmatched_messages)} unmatched messages remain")
    log.info(f"Wrote the timestamp-based matches to {timestamp_matches_log_output_csv_path} for manual verification. " 
             f"Please check these carefully")

    if len(unmatched_messages) > 0:
        log.error(f"{len(unmatched_messages)} unmatched messages remain after attempting all automated matching "
                  f"techniques")
        print(unmatched_messages[0].serialize())
        exit(1)

    # Get the recovered messages that don't have a matching message from Rapid Pro
    unmatched_recovered_messages = []
    matched_recovered_messages = []
    for urn in recovered_lut:
        for recovery_item in recovered_lut[urn]:
            if recovery_item["rapid_pro_message"] is None:
                unmatched_recovered_messages.append(recovery_item["recovered_message"])
            else:
                matched_recovered_messages.append(recovery_item["recovered_message"])
    log.info(f"Found {len(unmatched_recovered_messages)} recovered messages that had no match in Rapid Pro "
             f"(and {len(matched_recovered_messages)} that did have a match)")
    expected_unmatched_messages_count = len(recovered_messages) - len(all_rapid_pro_messages) + len(skipped_messages)
    log.info(f"Total expected unmatched messages was {expected_unmatched_messages_count}")

    if expected_unmatched_messages_count != len(unmatched_recovered_messages):
        log.error("Number of unmatched messages != expected number of unmatched messages")
        exit(1)

    # Export to a csv that can be processed by de_identify_csv.py
    log.info(f"Exporting unmatched recovered messages to {output_csv_path}")
    with open(output_csv_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["Sender", "Receiver", "Message", "ReceivedOn"])
        writer.writeheader()

        for msg in unmatched_recovered_messages:
            writer.writerow({
                "Sender": msg["Sender"],
                "Receiver": msg["Receiver"],
                "Message": msg["Message"],
                "ReceivedOn": msg["ReceivedOn"]
            })
