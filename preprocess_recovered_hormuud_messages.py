import argparse
import csv
import re
from collections import defaultdict
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import pytz
from core_data_modules.logging import Logger
from dateutil.parser import isoparse
from rapid_pro_tools.rapid_pro_client import RapidProClient
from storage.google_cloud import google_cloud_utils
from temba_client.v2 import Message as RapidProMessage

log = Logger(__name__)

TARGET_SHORTCODE = "378"


class MatchedMessage:
    def __init__(self, rapid_pro_message, recovered_message):
        """
        Represents two message objects, one from Rapid Pro, and one from a recovery CSV, that have been identified as
        being the same message in each source.

        One or both arguments may be None, representing a message that is not currently matched.

        :param rapid_pro_message:
        :type rapid_pro_message: temba_client.v2.Message | None
        :param recovered_message:
        :type recovered_message: RecoveredMessage | None
        """
        self.rapid_pro_message = rapid_pro_message
        self.recovered_message = recovered_message


class RecoveredMessage:
    def __init__(self, sender, receiver, text, timestamp, raw_timestamp, message_id=None):
        if message_id is None:
            message_id = uuid.uuid4()

        self.message_id = message_id
        self.sender = sender
        self.receiver = receiver
        self.text = text
        self.timestamp = timestamp
        self.raw_timestamp = raw_timestamp

    @classmethod
    def from_hormuud_csv_row(cls, row):
        """
        Creates a `RecoveredMessage` from a row in a Hormuud recovery CSV.

        The row should contain the columns:
          - "Sender": a number with country code but no leading '+' or 'tel:'
          - "Receiver": short code to which the message was sent.
          - "Message": raw message text
          - "ReceivedOn": string representing the date in EAT.
        """
        try:
            timestamp = pytz.timezone("Africa/Mogadishu").localize(
                datetime.strptime(row["ReceivedOn"], "%d/%m/%Y %H:%M:%S.%f")
            )
        except ValueError:
            try:
                timestamp = pytz.timezone("Africa/Mogadishu").localize(
                    datetime.strptime(row["ReceivedOn"], "%d/%m/%Y %H:%M:%S")
                )
            except ValueError:
                timestamp = pytz.timezone("Africa/Mogadishu").localize(
                    datetime.strptime(row["ReceivedOn"], "%Y-%m-%d %H:%M:%S")
                )

        return RecoveredMessage(
            sender="tel:+" + row["Sender"],
            receiver=row["Receiver"],
            text=row["Message"],
            raw_timestamp=row["ReceivedOn"],
            timestamp=timestamp
        )


def get_incoming_hormuud_messages_from_rapid_pro(google_cloud_credentials_file_path, rapid_pro_domain,
                                                 rapid_pro_token_file_url,
                                                 created_after_inclusive=None, created_before_exclusive=None):
    """
    :rtype: list of RapidProMessage
    """
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
    """
    :rtype: list of RecoveredMessage
    """
    log.info(f"Loading recovered messages from Hormuud csv at {csv_path}...")
    all_recovered_messages = []
    with open(csv_path) as f:
        reader = csv.DictReader(f)
        for line in reader:
            all_recovered_messages.append(RecoveredMessage.from_hormuud_csv_row(line))
    log.info(f"Loaded {len(all_recovered_messages)} messages")

    log.info(f"Filtering for messages sent to the target short code {TARGET_SHORTCODE}...")
    incoming_recovered_messages = [msg for msg in all_recovered_messages if msg.receiver == TARGET_SHORTCODE]
    log.info(f"Filtered for messages sent to the target short code {TARGET_SHORTCODE}: "
             f"{len(incoming_recovered_messages)} recovered messages remain")

    if received_after_inclusive is not None:
        log.info(f"Filtering out messages sent before {received_after_inclusive}...")
        incoming_recovered_messages = [msg for msg in incoming_recovered_messages
                                       if msg.timestamp >= received_after_inclusive]
        log.info(f"Filtered out messages sent before {received_after_inclusive}: "
                 f"{len(incoming_recovered_messages)} messages remain")
    if received_before_exclusive is not None:
        log.info(f"Filtering out messages sent after {received_before_exclusive}...")
        incoming_recovered_messages = [msg for msg in incoming_recovered_messages
                                       if msg.timestamp < received_before_exclusive]
        log.info(f"Filtered out messages sent after {received_before_exclusive}: "
                 f"{len(incoming_recovered_messages)} messages remain")

    return incoming_recovered_messages


def filter_matched_messages_from_recovered(matched_messages, recovered_messages):
    """
    :type matched_messages: list of MatchedMessage
    :type recovered_messages: list of RecoveredMessage
    """
    matched_message_ids = {match.recovered_message.message_id for match in matched_messages}
    return [msg for msg in recovered_messages if msg.message_id not in matched_message_ids]


def group_recovered_messages_by_urn(recovered_messages):
    urn_to_recovered_messages = defaultdict(list)  # of urn -> list of recovered message from this urn.
    for recovered_msg in recovered_messages:
        urn = recovered_msg.sender
        urn_to_recovered_messages[urn].append(recovered_msg)
    return urn_to_recovered_messages


class MatchStrategy:
    def __init__(self, name, csv_log_file_path=None):
        """
        :param name: Name of this strategy, for console logs.
        :type name: str
        :param csv_log_file_path: Path to write a CSV log to. This CSV will contain the pairs of messages matched by
                                  this strategy.
                                  If None, no log file will be written for this strategy.
        :type csv_log_file_path: str | None
        """
        self.name = name
        self.csv_log_file_path = csv_log_file_path

    def messages_match(self, rapid_pro_message, recovered_message):
        """
        Whether the two given messages can be considered a match under this strategy.

        :type rapid_pro_message: RapidProMessage
        :type recovered_message: RecoveredMessage
        :rtype: boolean
        """
        return False

    def skip_message(self, rapid_pro_message, matched_messages):
        """
        Whether the given Rapid Pro message should be skipped without looking for a match e.g. because it is a duplicate

        :param rapid_pro_message: Rapid Pro message to test whether it should be skipped.
        :type rapid_pro_message: RapidProMessage
        :param matched_messages: Messages that have been matched so far.
        :type matched_messages: list of MatchedMessage
        """
        return False


class ExactMatch(MatchStrategy):
    def __init__(self, max_time_delta, csv_log_file_path=None):
        """
        Matches messages only if they have the same text and sender, and their timestamps differ by less than the
        `max_time_delta`.
        """
        super().__init__("Exact Match", csv_log_file_path)
        self.max_time_delta = max_time_delta

    def messages_match(self, rapid_pro_message, recovered_message):
        if rapid_pro_message.urn != recovered_message.sender:
            return False

        if recovered_message.timestamp - rapid_pro_message.sent_on > self.max_time_delta:
            return False

        if rapid_pro_message.text != recovered_message.text:
            return False

        return True


class ExcelMangledMatch(MatchStrategy):
    def __init__(self, max_time_delta, csv_log_file_path=None):
        """
        Matches messages if they have the same sender, the texts match after applying some common Excel-manipulations,
        and their timestamps differ by less than the `max_time_delta`
        """
        super().__init__("Excel-Mangled Match", csv_log_file_path)
        self.max_time_delta = max_time_delta

    def messages_match(self, rapid_pro_message, recovered_message):
        if rapid_pro_message.urn != recovered_message.sender:
            return False

        if recovered_message.timestamp - rapid_pro_message.sent_on > self.max_time_delta:
            return False

        rapid_pro_text = rapid_pro_message.text
        rapid_pro_text = rapid_pro_text.replace("\n", " ")  # newlines -> spaces
        rapid_pro_text = rapid_pro_text.strip()
        # Match numbers in whitespace, unless the number is zero.
        if re.compile("^\\s*[0-9]+\\s*$").fullmatch(rapid_pro_text) and not re.compile("\\s*0+\\s*").fullmatch(rapid_pro_text):
            rapid_pro_text = rapid_pro_text.strip()  # numbers with whitespace -> just the number
            if rapid_pro_text.startswith("0"):
                rapid_pro_text = rapid_pro_text[1:]  # replace leading 0
            if Decimal(rapid_pro_text) > 1000000000:
                rapid_pro_text = f"{Decimal(rapid_pro_text):.4E}"  # big numbers -> scientific notation
        if re.compile("^\".*\"$").match(rapid_pro_text):
            rapid_pro_text = rapid_pro_text.replace("\"", "")  # strictly quoted text -> just the text
        rapid_pro_text = rapid_pro_text.encode("ascii", "replace").decode("ascii")  # non-ascii characters -> '?'

        recovered_text = recovered_message.text.encode("ascii", "replace").decode("ascii")  # non-ascii characters -> '?'

        # Match messages erroneously interpreted as a formula
        if rapid_pro_text.startswith("=") and recovered_text == "#NAME?":
            return True

        if rapid_pro_text != recovered_text:
            return False

        return True


class Duplicates(MatchStrategy):
    def __init__(self, csv_log_file_path=None):
        """
        Skips messages if they have the same text and the same sender as a message that has already been matched.
        """
        super().__init__("Duplicates", csv_log_file_path)

    def skip_message(self, rapid_pro_message, matched_messages):
        # A Rapid Pro message is a duplicate if contains the same text and sender as another Rapid Pro message
        # that was successfully matched.
        matched_rapid_pro_messages = [match.rapid_pro_message for match in matched_messages
                                      if match.rapid_pro_message is not None]

        for msg in matched_rapid_pro_messages:
            if rapid_pro_message.urn == msg.urn and rapid_pro_message.text == msg.text:
                return True

        return False


class ClippedMatch(MatchStrategy):
    def __init__(self, max_time_delta, csv_log_file_path=None):
        """
        Matches messages only if they have the same sender, the text of the Rapid Pro message starts with the text of
        the recovered message, and their timestamps differ by less than the `max_time_delta`. This handles messages that
        have been clipped in the recovery dataset.
        """
        self.max_time_delta = max_time_delta
        super().__init__("Clipped", csv_log_file_path)

    def messages_match(self, rapid_pro_message, recovered_message):
        if rapid_pro_message.urn != recovered_message.sender:
            return False

        if recovered_message.text == "":
            return False

        if not rapid_pro_message.text.startswith(recovered_message.text):
            return False

        if recovered_message.timestamp - rapid_pro_message.sent_on > self.max_time_delta:
            return False

        return True


class TimestampMatch(MatchStrategy):
    def __init__(self, max_time_delta, csv_log_file_path=None):
        """
        Matches messages only if they have the same sender and their timestamps differ by less than the
        `max_time_delta`. Message texts are not considered here.
        """
        super().__init__("Timestamp Match", csv_log_file_path)
        self.max_time_delta = max_time_delta

    def messages_match(self, rapid_pro_message, recovered_message):
        if rapid_pro_message.urn != recovered_message.sender:
            return False

        if recovered_message.timestamp - rapid_pro_message.sent_on > self.max_time_delta:
            return False

        return True


def apply_match_strategy(match_strategy, rapid_pro_messages_to_match, urn_to_recovered_messages,
                         previously_matched_messages):
    """
    Applies a match strategy to find all the matches between sets of Rapid Pro messages and recovery messages.

    :param match_strategy: Match strategy to use to decide if pairs of messages match or not.
    :type match_strategy: MatchStrategy
    :param rapid_pro_messages_to_match: Rapid Pro messages to try to match using this strategy.
    :type rapid_pro_messages_to_match: list of RapidProMessage
    :param urn_to_recovered_messages: Recovery messages to try to match using this strategy, grouped by urn of the
                                      message sender.
    :type urn_to_recovered_messages: dict of str -> (list of RecoveredMessage)
    :param previously_matched_messages: Messages that were matched by previous match strategies.
    :type previously_matched_messages: list of MatchedMessage
    :return: Tuple of (messages that were matched, messages that were skipped, messages that were not matched).
    :rtype: (list of MatchedMessage, list of RapidProMessage, list of RapidProMessage)
    """
    matched_message_ids = set()  # of str
    matched_messages = []  # of MatchedMessage
    unmatched_rapid_pro_messages = []  # of RapidProMessage
    skipped_messages = []  # of RapidProMessage

    # For each Rapid Pro message to be matched:
    # Search all the recovered messages from the same urn for a message that matches, or check if this message should
    # be skipped
    for rapid_pro_msg in rapid_pro_messages_to_match:
        recovered_messages = urn_to_recovered_messages[rapid_pro_msg.urn]

        for recovered_msg in recovered_messages:
            if recovered_msg.message_id in matched_message_ids:
                continue

            if match_strategy.messages_match(rapid_pro_msg, recovered_msg):
                matched_message_ids.add(recovered_msg.message_id)
                matched_messages.append(MatchedMessage(
                    rapid_pro_message=rapid_pro_msg,
                    recovered_message=recovered_msg
                ))
                break

            if match_strategy.skip_message(rapid_pro_msg, previously_matched_messages):
                skipped_messages.append(rapid_pro_msg)
                break
        else:
            unmatched_rapid_pro_messages.append(rapid_pro_msg)

    if strategy.csv_log_file_path is not None:
        log.info(f"Logging matches to {strategy.csv_log_file_path}...")
        with open(strategy.csv_log_file_path, "w") as f:
            writer = csv.DictWriter(f, fieldnames=["URN", "Rapid Pro Text", "Recovered Text"])
            writer.writeheader()

            for match in matched_messages:
                writer.writerow({
                    "URN": match.rapid_pro_message.urn,
                    "Rapid Pro Text": match.rapid_pro_message.text,
                    "Recovered Text": match.recovered_message.text
                })

    return matched_messages, skipped_messages, unmatched_rapid_pro_messages


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
    parser.add_argument("log_dir_path", metavar="log-dir-path",
                        help="Directory to log the matched messages to")
    parser.add_argument("skipped_csv_path", metavar="skipped-csv-path",
                        help="Path to CSV to write the skipped messages to")
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
    log_dir_path = args.log_dir_path
    skipped_csv_path = args.skipped_csv_path
    output_csv_path = args.output_csv_path

    # Define the maximum time difference we can observe between a message in rapid pro and in the recovery csv for it
    # to count as a match.
    if end_date < isoparse("2022-04-03T00:00+03:00"):
        # During Pool-CSAP-Somalia projects that took place before April 3rd, the realtime connection was extremely
        # unreliable (typical message loss rate was 50%), but the delay was typically about 4 minutes, and all
        # less than 5.
        max_time_delta = timedelta(minutes=5)
    elif start_date >= isoparse("2022-04-03T00:00+03:00") and end_date < isoparse("2022-09-01T00:00+03:00"):
        # When the realtime connection was improved from April 3rd 2022, message loss rate decreased to 1-2% but
        # the maximum delay slightly increased. Use 7 minutes for messages received since that date.
        max_time_delta = timedelta(minutes=7)
    elif start_date >= isoparse("2022-09-01T00:00+03:00"):
        # Since at least September 1st 2022 (and possibly earlier, when there were no projects running on the short
        # code), loss-rate remains at ~2% but the maximum delay has increased significantly in a small number of cases.
        max_time_delta = timedelta(days=30)
    else:
        assert False, "Unsupported data-range due to data crossing a max_time_delta definition boundary. " \
                      "Either check the dates, update the date-ranges in the source code, or break the recovery " \
                      "dataset into a chunks for each supported time range."
    log.info(f"Using maximum message time delta of {max_time_delta}")

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
    all_recovered_messages = recovered_messages

    rapid_pro_messages.sort(key=lambda msg: msg.sent_on)
    recovered_messages.sort(key=lambda msg: msg.timestamp)

    # Group the messages by the sender's urn
    urn_to_recovered_messages = group_recovered_messages_by_urn(recovered_messages)

    match_strategies = [
        ExactMatch(max_time_delta, csv_log_file_path=f"{log_dir_path}/exact-match-log.csv"),
        ExcelMangledMatch(max_time_delta, csv_log_file_path=f"{log_dir_path}/excel-mangled-log.csv"),
        Duplicates(csv_log_file_path=f"{log_dir_path}/duplicates-log.csv"),
        ClippedMatch(max_time_delta, csv_log_file_path=f"{log_dir_path}/clipped-log.csv"),
        TimestampMatch(max_time_delta, csv_log_file_path=f"{log_dir_path}/timestamp-log.csv")
    ]

    # Apply all the match strategies in sequence
    all_matched_messages = []  # of MatchedMessage
    all_skipped_rapid_pro_messages = []  # of RapidProMessage
    for i, strategy in enumerate(match_strategies):
        log.info(f"Applying match strategy {i + 1}/{len(match_strategies)} '{strategy.name}' to "
                 f"{len(rapid_pro_messages)} Rapid Pro messages and {len(recovered_messages)} recovered messages")
        urn_to_recovered_messages = group_recovered_messages_by_urn(recovered_messages)
        matched_messages, skipped_rapid_pro_messages, unmatched_rapid_pro_messages = apply_match_strategy(
            strategy, rapid_pro_messages, urn_to_recovered_messages, all_matched_messages
        )
        all_matched_messages.extend(matched_messages)
        all_skipped_rapid_pro_messages.extend(skipped_rapid_pro_messages)
        rapid_pro_messages = unmatched_rapid_pro_messages
        recovered_messages = filter_matched_messages_from_recovered(matched_messages, recovered_messages)
        log.info(f"Applied match strategy '{strategy.name}'. {len(matched_messages)} Rapid Pro messages matched by "
                 f"this strategy, {len(skipped_rapid_pro_messages)} skipped, and {len(rapid_pro_messages)} "
                 f"remaining")

    # Ensure we matched all the Rapid Pro messages
    if len(rapid_pro_messages) > 0:
        log.error(f"{len(rapid_pro_messages)} unmatched Rapid Pro messages remain after attempting all automated "
                  f"matching techniques. The first remaining message is:")
        log.error(rapid_pro_messages[0].serialize())
        exit(1)

    # Get the recovered messages that weren't matched
    unmatched_recovered_messages = recovered_messages
    matched_recovered_messages = [match.recovered_message for match in all_matched_messages
                                  if match.recovered_message is not None]
    log.info(f"Found {len(unmatched_recovered_messages)} recovered messages that had no match in Rapid Pro "
             f"({len(matched_recovered_messages)} did have a match, {len(all_skipped_rapid_pro_messages)} were skipped)")
    expected_unmatched_messages_count = \
        len(all_recovered_messages) - len(all_rapid_pro_messages) + len(all_skipped_rapid_pro_messages)
    log.info(f"Total expected unmatched messages was {expected_unmatched_messages_count}")

    if expected_unmatched_messages_count != len(unmatched_recovered_messages):
        log.error("Number of unmatched messages != expected number of unmatched messages")
        exit(1)

    # Export skipped messages to a csv that can be used for further processing of duplicates.
    # The output is in the format used by tools/archive_matching_messages.py, as this is the most likely next step
    # for these messages.
    log.info(f"Exporting {len(all_skipped_rapid_pro_messages)} skipped messages to {skipped_csv_path}")
    with open(skipped_csv_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["urn", "text", "timestamp"])
        writer.writeheader()

        for msg in all_skipped_rapid_pro_messages:
            writer.writerow({
                "urn": msg.urn,
                "text": msg.text,
                "timestamp": msg.sent_on.isoformat()
            })

    # Export recovered messages to a csv that can be processed by de_identify_csv.py
    log.info(f"Exporting {len(unmatched_recovered_messages)} unmatched recovered messages to {output_csv_path}...")
    with open(output_csv_path, "w") as f:
        writer = csv.DictWriter(f, fieldnames=["Sender", "Receiver", "Message", "ReceivedOn"])
        writer.writeheader()

        for recovered_msg in unmatched_recovered_messages:
            writer.writerow({
                "Sender": recovered_msg.sender,
                "Receiver": recovered_msg.receiver,
                "Message": recovered_msg.text,
                "ReceivedOn": recovered_msg.raw_timestamp
            })
