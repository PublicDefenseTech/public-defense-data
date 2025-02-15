import json
import os
import datetime as dt
import xxhash
import logging
import traceback
from datetime import datetime

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

class Cleaner:
    def __init__(self):
        self.logger = self.configure_logger()
        pass

    def configure_logger(self):
        # Configure logging
        logger = logging.getLogger(name=f"cleaner: pid: {os.getpid()}")
        # Set up basic configuration for the logging system
        logging.basicConfig(level=logging.INFO)

        cleaner_log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
        now = datetime.now()
        # Format it as "DD-MM-YYYY - HH:MM"
        formatted_date_time = now.strftime("%d-%m-%Y-%H.%M")
        cleaner_log_name = formatted_date_time + '_cleaner_logger_log.txt'

        file_handler = logging.FileHandler(os.path.join(cleaner_log_path, cleaner_log_name))
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger
    
    def redact_cause_number(self, input_dict: dict) -> str:
        # This will hash and redact the cause number and then add it to the output file.
        cause_number_hash = xxhash.xxh64(str(input_dict["Case Metadata"]["cause_number"])).hexdigest()
        return cause_number_hash

    def get_or_create_folder_path(self, county: str, folder_type: str) -> str:
        """Returns and ensures the existence of the folder path."""
        folder_path = os.path.join(
            os.path.dirname(__file__), "..", "..", "data", county.lower(), folder_type
        )
        try:
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
                self.logger.info(f"Folder '{folder_path}' created successfully.")
            else:
                self.logger.info(f"Folder '{folder_path}' already exists.")
        except OSError as e:
            self.logger.error(f"Error creating folder '{folder_path}': {e}")
        return folder_path

    def load_json_file(self, file_path: str) -> dict:
        """Loads a JSON file from a given file path and returns the data as an object"""
        try:
            with open(file_path, "r") as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            self.logger.error(f"Error loading file at {file_path}: {e}")
            return {}

    """def remove_judicial_officer(self, data):
        # Check if data is a dictionary
        if isinstance(data, dict):
            # Remove 'judicial officer' if it exists in this dictionary
            if "judicial officer" in data:
                del data["judicial officer"]
            # Recursively check each value in the dictionary
            for key, value in data.items():
                self.remove_judicial_officer(value)
        # Check if data is a list
        elif isinstance(data, list):
            for item in data:
                self.remove_judicial_officer(item)"""

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
                "ChargeId": i,
                "ChargeLevel": charge["level"],
                "OrignalCharge": charge["charges"],
                "Statute": charge["statute"],
                "IsPrimaryCharge": i == 0,
            }

            # Parse the charge date and append it to charge_dates
            try:
                charge_datetime = dt.datetime.strptime(charge["date"], "%m/%d/%Y")
                charge_dates.append(charge_datetime)
                charge_dict["ChargeDate"] = dt.datetime.strftime(
                    charge_datetime, "%Y-%m-%d"
                )
            except ValueError:
                self.logger.error(f"Error parsing date for charge: {charge}")
                continue

            # Try to map the charge to UMich data
            try:
                charge_dict.update(charge_mapping[charge["charges"]])
            except KeyError:
                self.logger.warning(f"Couldn't find this charge: {charge['charges']}")
                continue

            processed_charges.append(charge_dict)

        # Find the earliest charge date
        if charge_dates:
            earliest_charge_date = dt.datetime.strftime(min(charge_dates), "%Y-%m-%d")
        else:
            self.logger.warning("No valid charge dates found.")
            earliest_charge_date = ""

        return processed_charges, earliest_charge_date

    def contains_good_motion(self, motion: str, event: list | str) -> bool:
        """Recursively check if a motion exists in an event list or sublist."""
        if isinstance(event, list):
            return any(self.contains_good_motion(motion, item) for item in event)
        return motion.lower() in event.lower()

    def find_good_motions(
        self, events: list | str, good_motions: list[str]
    ) -> list[str]:
        """Finds motions in events based on list of good motions."""
        return [
            motion
            for motion in good_motions
            if self.contains_good_motion(motion, events)
        ]

    def hash_defense_attorney(self, input_dict: dict) -> str:
        """Hashes the defense attorney info to anonymize it."""
        try:
            def_atty_unique_str = f'{input_dict["Defendent Information"]["defense attorney"]}:{input_dict["Defendent Information"]["defense attorney phone number"]}'
            return xxhash.xxh64(def_atty_unique_str).hexdigest()
        except KeyError as e:
            self.logger.error(f"Missing defense attorney data: {e}")
            return ""

    def write_json_output(self, file_path: str, data: dict) -> None:
        """Writes the given data to a JSON file at the specified file path."""
        try:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=4)
            self.logger.info(f"cleaner: Successfully wrote cleaned data to {file_path}")
        except OSError as e:
            self.logger.error(f"cleaner: Failed to write JSON output to {file_path}: {e}")

    def process_single_case(
        self,
        case_json_folder_path: str,
        case_json_filename: str,
        cleaned_folder_path: str,
    ) -> None:
        """Process a single case JSON file."""
        input_json_path = os.path.join(case_json_folder_path, case_json_filename)
        input_dict = self.load_json_file(input_json_path)

        if not input_dict:
            self.logger.error(f"Failed to load case data from {input_json_path}")
            return

        # Initialize cleaned output data
        output_json_data = {
            "ParseMetadata": {
                "ParsingDate": dt.datetime.today().strftime("%Y-%m-%d"),
                "HTMLHash": input_dict["html_hash"],
                "OdysseyID":input_dict['Case Metadata']['odyssey id']
            },
            "CaseMetadata": {
                "County": input_dict["Case Metadata"]["county"],
                "CauseNumber": input_dict["Case Metadata"]["cause_number"],
                "EarliestChargeDate": "",
                "HasEvidenceOfRepresentation": False,
            },
            "DefendantInformation": {
                "DefendantName": input_dict["Defendent Information"]["defendant"],
                "Sex": input_dict["Defendent Information"]["sex"],
                "Race": input_dict["Defendent Information"]["race"],
                "DateOfBirth": input_dict["Defendent Information"]["date of birth"],
                "Height": input_dict["Defendent Information"]["height"],
                "Weight": input_dict["Defendent Information"]["weight"],
                "DefendantAddress": input_dict["Defendent Information"]["defendant address"],
                "SID": input_dict["Defendent Information"]["SID"],
            },
            "DefenseAttorneyInformation":{
                "DefenseAttorney": input_dict["Defendent Information"]["defense attorney"],
                "DefenseAttorneyPhoneNumber": input_dict["Defendent Information"]["defense attorney phone number"],
                "AppointedOrRetained": input_dict["Defendent Information"]["appointed or retained"],
                "DefenseAttorneyHash": self.hash_defense_attorney(input_dict),
            },
            "StateInformation": {
                "ProsecutingAttorney": input_dict["State Information"]["prosecuting attorney"],
                "ProsecutingAttorneyPhoneNumber": input_dict["State Information"]["prosectuing attorney phone number"]
            },

            "ChargeInformation": [],
        }
        if "Disposition Information" in input_dict:
            output_json_data["DispositionInformation"] = input_dict["Disposition Information"]
        else: 
            output_json_data["DispositionInformation"] = None

        if "Related Cases" in input_dict:
            output_json_data['CaseMetadata']['RelatedCases'] = input_dict['Related Cases']
        else:
            output_json_data['CaseMetadata']['RelatedCases'] = None


        # Removing judicial office name from data
        #self.remove_judicial_officer(output_json_data["Disposition_Information"])

        # Load charge mappings
        charge_name_to_umich_file = os.path.join(
            os.path.dirname(__file__),
            "..",
            "..",
            "resources",
            "umich-uccs-database.json",
        )
        charges_mapped = self.load_and_map_charge_names(charge_name_to_umich_file)

        # Process charges and motions
        output_json_data["ChargeInformation"], output_json_data['CaseMetadata']["EarliestChargeDate"] = (
            self.process_charges(input_dict['Charge Information'], charges_mapped)
        )
        output_json_data['CaseMetadata']['GoodMotions'] = self.find_good_motions(
            input_dict["Other Events and Hearings"], GOOD_MOTIONS
        )
        output_json_data['CaseMetadata']["HasEvidenceOfRepresentation"] = (
            len(output_json_data['CaseMetadata']["GoodMotions"]) > 0
        )
        output_json_data['ParseMetadata']["CauseNumberHashed"] = self.redact_cause_number(input_dict)

        output_json_data['EventsAndHearings'] = input_dict["Other Events and Hearings"]

        # Write output to file
        output_filepath = os.path.join(cleaned_folder_path, case_json_filename)
        self.write_json_output(output_filepath, output_json_data)

    def process_json_files(self, county: str, case_json_folder_path: str) -> None:
        """Processes all JSON files in the specified folder."""
        try:
            list_case_json_files = os.listdir(case_json_folder_path)
        except (FileNotFoundError, Exception) as e:
            self.logger.error(f"Error reading directory {case_json_folder_path}: {e}")
            return

        # Ensure the case_json_cleaned folder exists
        cleaned_folder_path = self.get_or_create_folder_path(
            county, "case_json_cleaned"
        )

        for case_json_filename in list_case_json_files:
            try:
                self.process_single_case(
                    case_json_folder_path, case_json_filename, cleaned_folder_path
                )
            except Exception as e:
                self.logger.error(f"Unexpected error while cleaning case {case_json_filename}: {e}")
                self.logger.error(f"Traceback: {traceback.format_exc()}")

    def clean(self, county: str = 'hays') -> None:
        """
        Cleans and processes case data for a given county.
        This method performs the following steps:
        1. Loads raw JSON case data from the 'case_json' folder for the specified county.
        2. Processes and maps charges using an external UMich data source.
        3. Identifies relevant motions from a predefined list of good motions.
        4. Hashes defense attorney information to anonymize but uniquely identify the attorney.
        5. Adds metadata, such as parsing date and case number, to the cleaned data.
        6. Writes the cleaned data to the 'case_json_cleaned' folder for the specified county.
        """
        try:
            case_json_folder_path = self.get_or_create_folder_path(county, "case_json")
            self.logger.info(f"cleaner: Cleaning data for county: {county}")
            self.process_json_files(county, case_json_folder_path)
            self.logger.info(f"cleaner: Completed cleaning for county: {county}")
        except Exception as e:
            self.logger.error(f"Unexpected error while cleaning case: {e}")
            self.logger.error(f"Traceback: {traceback.format_exc()}")

if __name__ == '__main__':
    Cleaner().clean()
