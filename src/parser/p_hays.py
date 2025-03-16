from typing import Dict, List
from bs4 import BeautifulSoup
import traceback
from datetime import datetime
from src.parser.models import *
from sqlmodel import SQLModel, Field, Relationship, create_engine, Session, select
import xxhash
import os
import json
from dotenv import load_dotenv
import logging
from datetime import datetime as dt

CHARGE_SEVERITY = {
    "First Degree Felony": 1,
    "Second Degree Felony": 2,
    "Third Degree Felony": 3,
    "State Jail Felony": 4,
    "Misdemeanor A": 5,
    "Misdemeanor B": 6,
}

# List of motions identified as evidentiary.
# TODO: These should be moved to a separate JSON in resources
GOOD_MOTIONS = [
    "Motion To Suppress",
    "Motion to Reduce Bond",
    "Motion to Reduce Bond Hearing",
    "Motion for Production",
    "Motion For Speedy Trial",
    "Motion for Discovery",
    "Motion In Limine",
]


class ParserHays:

    def __init__(self):
        self.engine = self.create_postgres_engine()
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)
        self.logger = self.configure_logger()

        pass

    def configure_logger(self):
        # Configure the logger
        logger = logging.getLogger(name=f"parser: pid: {os.getpid()}")

        # Set up basic configuration for the logging system
        logging.basicConfig(level=logging.INFO)

        parser_log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
        now = datetime.now()
        # Format it as "DD-MM-YYYY - HH:MM"
        formatted_date_time = now.strftime("%d-%m-%Y-%H.%M")
        parser_log_name = formatted_date_time + "_newparser_logger_log.txt"

        file_handler = logging.FileHandler(
            os.path.join(parser_log_path, parser_log_name)
        )
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        return logger

    def create_postgres_engine(self):
        # Load environment variables from .env
        load_dotenv()

        # Fetch and create the url
        DATABASE_URL = os.getenv("URL")

        # Create the engine
        engine = create_engine(DATABASE_URL)
        return engine

    def extract_rows(self, table: BeautifulSoup, logger) -> List[List[str]]:
        try:
            rows = [
                [
                    tag.strip().replace("\xa0", "").replace("Â", "")
                    for tag in tr.find_all(text=True)
                    if tag.strip()
                ]
                for tr in table.select("tr")
            ]
            return [row for row in rows if row]
        except Exception as e:
            logger.info(f"Error extracting rows: {e}")
            return []

    def get_charge_severity(self, charge: str, logger) -> int:
        try:
            for charge_name, severity in CHARGE_SEVERITY.items():
                if charge_name in charge:
                    return severity
            return float("inf")
        except Exception as e:
            logger.info(f"Error getting charge severity: {e}")
            return float("inf")

    def count_dismissed_charges(self, dispositions: List[Dict], logger) -> int:
        try:
            return sum(
                1
                for disposition in dispositions
                for detail in disposition.get("details", [])
                if detail.get("outcome", "").lower() == "dismissed"
            )
        except Exception as e:
            logger.info(f"Error counting dismissed charges: {e}")
            return None

    def get_top_charge(
        self, dispositions: List[Dict], charge_information: List[Dict], logger
    ) -> Dict:
        try:
            top_charge = None
            min_severity = float("inf")

            charge_map = {info["charges"]: info["level"] for info in charge_information}

            for disposition in dispositions:
                if isinstance(disposition, dict):
                    for detail in disposition.get("details", []):
                        if isinstance(detail, dict):
                            charge_text = detail.get("charge", "").strip()
                            charge_name = (
                                charge_text.split(" >=")[0]
                                .strip()
                                .lstrip("0123456789. ")
                                .strip()
                            )
                            charge_level = charge_map.get(charge_name, None)

                            severity = self.get_charge_severity(charge_level, logger)
                            if severity < min_severity:
                                min_severity = severity
                                top_charge = {
                                    "charge_name": charge_name,
                                    "charge_level": charge_level,
                                }
                else:
                    logger.info(f"Unexpected type for disposition: {type(disposition)}")

            return top_charge
        except Exception as e:
            logger.info(f"Error getting top charge: {e}")
            return {"charge_name": None, "charge_level": None}

    def find_good_motions(
        self, events: list | str, good_motions: list[str]
    ) -> list[str]:
        """Finds motions in events based on list of good motions."""

        def contains_good_motion(motion: str, event: list | str) -> bool:
            """Recursively check if a motion exists in an event list or sublist."""
            if isinstance(event, list):
                return any(contains_good_motion(motion, item) for item in event)
            return motion.lower() in event.lower()

        return [
            motion for motion in good_motions if contains_good_motion(motion, events)
        ]

    def get_case_metadata(
        self,
        county: str,
        odyssey_id: str,
        case_soup: BeautifulSoup,
        logger,
        case_schema=CaseMetadata,
    ) -> Dict[str, str]:
        try:
            # logger.info(f"Getting case metadata for {county} case {odyssey_id}")
            return {
                "court_case_number": case_soup.select(
                    'div[class="ssCaseDetailCaseNbr"] > span'
                )[0].text,
                "odyssey_id": odyssey_id,
                "county_of_jurisdiction": county,
            }
        except Exception as e:
            logger.info(f"Error getting case metadata: {e}")
            return {
                "court_case_number": None,
                "odyssey_id": odyssey_id,
                "county_of_jurisdiction": county,
            }

    def get_case_details(self, table: BeautifulSoup, logger) -> Dict[str, str]:
        try:
            table_values = table.select("b")
            # logger.info(f"Getting case details")
            return {
                "case_name": table_values[0].text,
                "case_type": table_values[1].text,
                "date_filed": table_values[2].text,
                "location": table_values[3].text,
            }
        except Exception as e:
            logger.info(f"Error getting case details: {e}")
            return {
                "case_name": None,
                "case_type": None,
                "date_filed": None,
                "location": None,
            }

    def parse_defendant_rows(
        self, defendant_rows: List[List[str]], logger
    ) -> Dict[str, str]:
        try:
            # logger.info(f"Parsing defendant rows")
            return {
                "defendant": defendant_rows[1][1],
                "sex": defendant_rows[1][2].split(" ")[0],
                "race": defendant_rows[1][2].split(" ")[1],
                "date_of_birth": defendant_rows[1][3],
                "height": defendant_rows[1][4].split(" ")[0],
                "weight": defendant_rows[1][4].split(" ")[1],
                "defendant_address": defendant_rows[2][0] + " " + defendant_rows[2][1],
                "sid": defendant_rows[2][3],
            }
        except Exception as e:
            logger.info(f"Error parsing defendant rows: {e}")
            return {
                "defendant": None,
                "sex": None,
                "race": None,
                "date_of_birth": None,
                "height": None,
                "weight": None,
                "defendant_address": None,
                "sid": None,
            }

    def parse_defense_attorney_rows(
        self, defense_attorney_rows: List[List[str]], logger
    ) -> Dict[str, str]:
        try:
            # logger.info(f"Parsing defendant rows")
            return {
                "defense_attorney": defense_attorney_rows[1][5],
                "appointed_or_retained": defense_attorney_rows[1][6],
                "defense_attorney_phone_number": defense_attorney_rows[1][7],
            }
        except Exception as e:
            logger.info(f"Error parsing defendant rows: {e}")
            return {
                "defense_attorney": None,
                "appointed_or_retained": None,
                "defense_attorney_phone_number": None,
            }

    def parse_state_rows(self, state_rows: List[List[str]], logger) -> dict:
        try:
            prosecuting_attorney_dict = {
                "prosecuting_attorney": state_rows[3][2],
                "prosecuting_attorney_phone": state_rows[3][3],
            }
            return prosecuting_attorney_dict
        except Exception as e:
            logger.info(f"Error parsing state rows: {e}")
            prosecuting_attorney_dict = {
                "prosecuting_attorney": None,
                "prosecuting_attorney_phone": None,
            }
            return prosecuting_attorney_dict

    def get_charge_information(self, table: BeautifulSoup, logger) -> List[Dict]:
        try:
            # logger.info(f"Getting charge information")
            table_rows = [
                tag.strip().replace("\xa0", " ")
                for tag in table.find_all(text=True)
                if tag.strip()
            ]

            charge_information = []
            for i in range(5, len(table_rows), 5):
                charge_information.append(
                    {
                        k: v
                        for k, v in zip(
                            ["charges", "statute", "level", "date"],
                            table_rows[i + 1 : i + 5],
                        )
                    }
                )
            return charge_information
        except Exception as e:
            logger.info(f"Error getting charge information: {e}")
            return []

    def load_json_file(self, file_path: str) -> dict:
        """Loads a JSON file from a given file path and returns the data as an object"""
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error loading file at {file_path}: {e}")
            return {}

    def load_and_map_charge_names(self, file_path: str) -> dict:
        """Loads a JSON file and maps charge names to their corresponding UMich data."""
        charge_data = self.load_json_file(file_path)
        # Check if the file loaded successfully
        if not charge_data:
            self.logger.error(f"Failed to load charge data from {file_path}")
            raise FileNotFoundError(f"File not found or is empty: {file_path}")
        # Create dictionary mapping charge names
        try:
            return {item["charge_name"]: item for item in charge_data}
        except KeyError as e:
            self.logger.error(f"Error in mapping charge names: {e}")
            raise ValueError(f"Invalid data structure: {file_path}")

    def process_charges(
        self, charges: list[dict], charge_mapping: dict
    ) -> tuple[list[dict], str]:
        """
        Processes a list of charges by formatting charge details,
        mapping charges to UMich data, and finding the earliest charge date.

        Args:
            charges: A list of charges where each charge is a dictionary containing charge details.
            charge_mapping: A dictionary mapping charge names to corresponding UMich data.

        Returns:
            tuple: A list of processed charges and the earliest charge date.
        """
        charge_dates = []
        processed_charges = []

        for i, charge in enumerate(charges):
            charge_dict = {
                "charge_id": i,
                "charge_level": charge["charge_level"],
                "original_charge": charge["original_charge"],
                "statute": charge["statute"],
                "is_primary_charge": i == 0,
            }

            # Parse the charge date and append it to charge_dates

            try:
                charge_date = charge["charge_date"]
                charge_dates.append(charge_date)
                charge_dict["charge_date"] = dt.strftime(
                    charge_date, "%Y-%m-%d"
                )  # format for database.

            except Exception as e:
                self.logger.error(
                    f"Error processing date for charge: {charge}, error: {e}"
                )

            charge_date = charge["charge_date"]  # Get the date object

            # Try to map the charge to UMich data
            try:
                charge_dict.update(charge_mapping[charge["original_charge"]])
                processed_charges.append(charge_dict)
            except KeyError:
                self.logger.warning(
                    f"Couldn't find this charge: {charge['original_charge']}"
                )
                charge_dict.update(
                    {
                        "charge_name": charge["original_charge"],
                        "uccs_code": None,
                        "charge_desc": None,
                        "offense_category_desc": None,
                        "offense_type_desc": None,
                    }
                )
                processed_charges.append(charge_dict)

        # Find the earliest charge date
        if charge_dates:
            earliest_charge_date = dt.strftime(min(charge_dates), "%Y-%m-%d")
        else:
            self.logger.warning("No valid charge dates found.")
            earliest_charge_date = None

        return processed_charges, earliest_charge_date

    def format_events_and_orders_of_the_court(
        self, table: BeautifulSoup, case_soup: BeautifulSoup, logger
    ) -> List:
        try:
            # logger.info(f"Formatting events and orders of the court")
            table_rows = [
                [
                    tag.strip().replace("\xa0", " ")
                    for tag in tr.find_all(text=True)
                    if tag.strip()
                ]
                for tr in table.select("tr")
                if tr.select("th")
            ]
            table_rows = [
                [" ".join(word.strip() for word in text.split()) for text in sublist]
                for sublist in table_rows
                if sublist
            ]

            disposition_rows = []
            other_event_rows = []

            for row in table_rows:
                if len(row) >= 2:
                    if row[1] in ["Disposition", "Disposition:", "Amended Disposition"]:
                        disposition_rows.append(row)
                    else:
                        other_event_rows.append(row)

            # Reverse the order of the rows
            other_event_rows = other_event_rows[::-1]
            disposition_rows = disposition_rows[::-1]

            return (disposition_rows, other_event_rows)
        except Exception as e:
            logger.info(f"Error formatting events and orders of the court: {e}")
            return ([], [])

    def get_disposition_information(
        self, row, dispositions, case_data, table, county, case_soup, logger
    ) -> List[Dict]:
        try:
            if not row:
                # logger.info(f"No dispositions to process.")
                return dispositions

            if len(row) >= 5:
                # Extract judicial officer if present
                judicial_officer = ""
                if len(row[2]) > 18 and row[2].startswith("(Judicial Officer:"):
                    judicial_officer = row[2][18:-1].strip()

                # Create a disposition entry
                disposition = {
                    "date": row[0],
                    "event": row[1],
                    "judicial_officer": judicial_officer,
                    "details": [],
                }

                # Check if this row is a disposition
                if row[1].lower() in [
                    "disposition",
                    "amended disposition",
                    "deferred adjudication",
                    "punishment hearing",
                ]:
                    details = {"charge": row[3], "outcome": row[4]}
                    if len(row) > 5:
                        details["additional_info"] = row[5:]
                    disposition["details"].append(details)
                    dispositions.append(disposition)
                    dispositions.reverse()
                else:
                    # logger.info("Row is not a disposition: %s", row)
                    pass

            return dispositions
        except Exception as e:
            logger.info(f"Error getting disposition information: {e}")
            return dispositions

    def add_version(self, case_metadata: CaseMetadata) -> int:
        """
        Determines the version number for a CaseMetadata entry based on existing data.
        """

        html_hash = (
            case_metadata.html_hash
        )  # get the html hash from the case metadata instance.
        cause_number = case_metadata.court_case_number

        # 1. Duplicate HTML Hash Check
        existing_case_metadata = self.session.exec(
            select(CaseMetadata).where(CaseMetadata.html_hash == html_hash)
        ).first()

        if existing_case_metadata:
            self.logger.info(
                f"Version: Duplicate. Not adding. Case with matching HTML hash exists: {html_hash}"
            )
            return -1

        # 2. New Case Check
        existing_case_metadatas = self.session.exec(
            select(CaseMetadata).where(CaseMetadata.court_case_number == cause_number)
        ).all()

        if not existing_case_metadatas:
            version = 1
            self.logger.info(
                f"Version: New Case. Adding. No case with matching cause number exists: {cause_number}"
            )
            return version

        # 3. Existing Case Check
        else:
            versions = [
                cm.version for cm in existing_case_metadatas if cm.version is not None
            ]  # only add versions that are not None.
            if versions:
                highest_version = max(versions)
                version = highest_version + 1
            else:
                version = 1  # if there are no versions, set to one.
            self.logger.info(
                f"Version: Updated Case. Adding. {len(existing_case_metadatas)} cases with matching cause number exists: {cause_number}"
            )
            return version

    def parser_hays(
        self,
        county: str,
        odyssey_id: str,
        case_number,
        logger,
        case_soup: BeautifulSoup,
    ) -> Dict[str, Dict]:
        try:
            with self.session:

                body = case_soup.select("body")
                root_tables = case_soup.select("body>table")

                # Get fields related to the case
                case_metadata_data = self.get_case_metadata(
                    county, odyssey_id, case_soup, logger
                )

                # Create CaseMetadata

                case_metadata = CaseMetadata(
                    county_of_jurisdiction=case_metadata_data["county_of_jurisdiction"],
                    court_case_number=case_metadata_data["court_case_number"],
                    good_motions=None,
                    has_evidence_of_representation=None,
                    parsing_date=datetime.now().date(),
                    html_hash=xxhash.xxh64(str(body)).hexdigest(),
                    odyssey_id=odyssey_id,
                    court_case_number_hashed=xxhash.xxh64(
                        str(case_metadata_data["court_case_number"])
                    ).hexdigest(),
                    case_name=None,
                    case_type=None,
                    date_filed=None,
                    location=None,
                    version=None,
                )

                # Find the correct version number per this cause number
                case_metadata.version = self.add_version(CaseMetadata)

                self.session.add(case_metadata)
                self.session.commit()
                self.session.refresh(case_metadata)

                for table in root_tables:

                    if "Case Type:" in table.text and "Date Filed:" in table.text:
                        case_details = self.get_case_details(table, logger)

                        # Update CaseMetadata row with event-related field
                        case_metadata_row = self.session.get(
                            CaseMetadata, case_metadata.id
                        )
                        if case_metadata_row:
                            case_metadata_row.case_name = case_details["case_name"]
                            case_metadata_row.case_type = case_details["case_type"]
                            case_metadata_row.date_filed = case_details["date_filed"]
                            case_metadata_row.location = case_details["location"]
                            self.session.add(case_metadata_row)
                            self.session.commit()

                    elif "Related Case Information" in table.text:
                        related_cases_data = [
                            case.text.strip().replace("\xa0", " ")
                            for case in table.select("td")
                        ]
                        for related_case_text in related_cases_data:
                            related_case = RelatedCase(
                                case_id=case_metadata.id, related_case=related_case_text
                            )
                            self.session.add(related_case)

                    elif "Party Information" in table.text:
                        # Extract and add defendant row
                        defendant_data = self.parse_defendant_rows(
                            self.extract_rows(table, logger), logger
                        )
                        defendant = Defendant(
                            case_id=case_metadata.id, **defendant_data
                        )
                        self.session.add(defendant)

                        # Extract and add defense attorney row
                        defense_attorney_data = self.parse_defense_attorney_rows(
                            self.extract_rows(table, logger), logger
                        )
                        defense_attorney = DefenseAttorney(
                            case_id=case_metadata.id, **defense_attorney_data
                        )
                        self.session.add(defense_attorney)

                        # Extract and add state attorney row
                        state_data = self.parse_state_rows(
                            self.extract_rows(table, logger), logger
                        )
                        state_info = StateInformation(
                            case_id=case_metadata.id,
                            **state_data,
                        )
                        self.session.add(state_info)

                    elif "Charge Information" in table.text:

                        # Load charge database to categorize charge text
                        charge_name_to_umich_file = os.path.join(
                            os.path.dirname(__file__),
                            "..",
                            "..",
                            "resources",
                            "umich-uccs-database.json",
                        )
                        charges_mapped = self.load_and_map_charge_names(
                            charge_name_to_umich_file
                        )

                        # Get charge information from the HTML table
                        charge_information_data = self.get_charge_information(
                            table, logger
                        )
                        charges_dict = []

                        # Create preliminary dictionary with charge information
                        for i, charge_info in enumerate(charge_information_data):
                            charge_dict = {
                                "case_id": case_metadata.id,
                                "original_charge": charge_info["charges"],
                                "statute": charge_info["statute"],
                                "charge_level": charge_info["level"],
                                "charge_date": (
                                    datetime.strptime(
                                        charge_info["date"], "%m/%d/%Y"
                                    ).date()
                                    if charge_info["date"]
                                    else None
                                ),
                            }
                            charges_dict.append(charge_dict)

                        # Process the charge dictionary to add additional fields
                        charges_processed, earliest_charge_date = self.process_charges(
                            charges_dict, charges_mapped
                        )

                        # Add the final processes charges to the table
                        for charge_data in charges_processed:
                            charge = Charge(case_id=case_metadata.id, **charge_data)
                            self.session.add(charge)

                    elif "Events & Orders of the Court" in table.text:

                        # Extract dispositions and events
                        disposition_rows, other_event_rows = (
                            self.format_events_and_orders_of_the_court(
                                table, case_soup, logger
                            )
                        )

                        # Parse dispositions and add them as rows
                        dispositions = []
                        for row in disposition_rows:
                            disposition_data = self.get_disposition_information(
                                row,
                                dispositions,
                                {},  # case_data not used here
                                table,
                                county,
                                case_soup,
                                logger,
                            )
                            if disposition_data and disposition_data != dispositions:
                                dispositions = disposition_data

                        if dispositions:
                            for disp in dispositions:
                                disposition_model = Disposition(
                                    case_id=case_metadata.id,
                                    date=(
                                        datetime.strptime(
                                            disp["date"], "%m/%d/%Y"
                                        ).date()
                                        if disp["date"]
                                        else None
                                    ),
                                    event=disp["event"],
                                    judicial_officer=disp.get("judicial_officer"),
                                )
                                self.session.add(disposition_model)
                                self.session.commit()
                                self.session.refresh(disposition_model)

                                for detail in disp.get("details", []):
                                    disp_detail = DispositionDetail(
                                        disposition_id=disposition_model.id,
                                        charge=detail.get("charge"),
                                        outcome=detail.get("outcome"),
                                    )
                                    self.session.add(disp_detail)

                        # Parse and write the event rows to the database
                        for event_row in other_event_rows:
                            event_date = (
                                datetime.strptime(event_row[0], "%m/%d/%Y").date()
                                if event_row[0]
                                else None
                            )
                            event_model = Event(
                                case_id=case_metadata.id,
                                date=event_date,
                                event=event_row[1],
                                details=" ".join(event_row[2:]),
                            )
                            self.session.add(event_model)

                        # Update CaseMetadata row with disposition and event-related field
                        case_metadata_row = self.session.get(
                            CaseMetadata, case_metadata.id
                        )
                        if case_metadata_row:
                            case_metadata_row.good_motions = self.find_good_motions(
                                other_event_rows, GOOD_MOTIONS
                            )
                            case_metadata_row.has_evidence_of_representation = (
                                len(case_metadata_row.good_motions) > 0
                            )

                            top_charge_data = self.get_top_charge(
                                dispositions,
                                charge_information_data,
                                logger,
                            )
                            case_metadata_row.top_charge_name = top_charge_data[
                                "charge_name"
                            ]
                            case_metadata_row.top_charge_level = top_charge_data[
                                "charge_level"
                            ]

                            case_metadata_row.dismissed_charges_count = (
                                self.count_dismissed_charges(dispositions, logger)
                            )

                            self.session.add(case_metadata_row)
                            self.session.commit()

                self.session.commit()
                return {"status": "success"}  # Return a success status
        except Exception as e:
            logger.error(f"Unexpected error while parsing Hays case: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            self.session.rollback()
            return {"status": "error", "error": str(e)}
