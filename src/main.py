import os
import csv
import sys
import logging
import shutil
from datetime import datetime
import argparse

# Import all of the programs modules
import scraper
import parser
import cleaner
import updater

class Orchestrator:
    def __init__(self, 
                 counties = None, 
                 start_date = None, 
                 end_date = None,
                 court_calendar_link_text = None,
                 case_number = None,
                 case_html_path = None,
                 judicial_officers = None,
                 ms_wait = None,
                 parse_single_file = False,
                 test = False):

        self.create_logs_folder()
        self.logger = self.configure_logger()
        now = datetime.now()
        formatted_date_time = now.strftime("%d-%m-%Y-%H.%M")
        self.logger.info("~~~~~~~Starting TDD Program~~~~~~~")
        self.logger.info(f"Starting date and time is {formatted_date_time}.")

        # Handle counties, start_date, and end_date from arguments or defaults
        if counties is None:
            self.counties = []
            with open(
                os.path.join(
                    os.path.dirname(__file__), "..", "resources", "texas_county_data.csv"
                ),
                mode="r",
            ) as file_handle:
                csv_file = csv.DictReader(file_handle)
                for row in csv_file:
                    if row["scrape"].lower() == "yes":
                        self.counties.append(row["county"])
        else:
            self.counties = counties

        if start_date is None:
            self.start_date = '2024-01-01'
        else:
            self.start_date = start_date

        if end_date is None:
            self.end_date = '2024-01-31'
        else:
            self.end_date = end_date

        self.court_calendar_link_text = court_calendar_link_text
        self.case_number = case_number
        self.case_html_path = case_html_path
        self.judicial_officers = judicial_officers
        self.ms_wait = ms_wait
        self.parse_single_file = parse_single_file
        self.test = test

        self.logger.info(f"Scraping Start Date: {self.start_date}.")
        self.logger.info(f"Scraping End Date: {self.end_date}.")

    def configure_logger(self):
        # Configure logging (same as before)
        logger = logging.getLogger(name=f"orchestrator: pid: {os.getpid()}")
        logging.basicConfig(level=logging.INFO)
        orchestrator_log_path = os.path.join(os.path.dirname(__file__), "..", "logs")
        now = datetime.now()
        formatted_date_time = now.strftime("%d-%m-%Y-%H.%M")
        orchestrator_log_name = formatted_date_time + '_orchestrator_logger_log.txt'
        file_handler = logging.FileHandler(os.path.join(orchestrator_log_path, orchestrator_log_name))
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def create_logs_folder(self):
        # Create logs folder (same as before)
        logs_folder = os.path.join(os.path.dirname(__file__), "..", "logs")
        if not os.path.exists(logs_folder):
            os.makedirs(logs_folder)

    def file_reset(self, county):
        # File reset functionality (same as before)
        root_folder = os.path.join(os.path.dirname(__file__), "..", "data", county)
        subfolders = ['case_html', 'case_json', 'case_json_cleaned']
        for subfolder in subfolders:
            folder_path = os.path.join(root_folder, subfolder)
            if os.path.exists(folder_path) and os.path.isdir(folder_path):
                for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
            else:
                self.logger.error(f"{subfolder} folder not found here: {folder_path}")
        self.logger.info("Finished removing files.")

    def orchestrate(self):
        # Orchestration logic (same as before)
        for c in self.counties:
            c = c.lower()
            self.logger.info(f"Starting to scrape, parse, clean, and update this county: {c}")
            scraper.Scraper().scrape(county = c,
                                    start_date = self.start_date,
                                    end_date = self.end_date,
                                    court_calendar_link_text = self.court_calendar_link_text,
                                    case_number = self.case_number,
                                    case_html_path = self.case_html_path,
                                    judicial_officers = self.judicial_officers,
                                    ms_wait = self.ms_wait)
            parser.Parser().parse(county = c,
                            case_number = self.case_number,
                            parse_single_file = self.parse_single_file,
                            test=self.test)
            cleaner.Cleaner().clean(county = c)
            updater.Updater(c).update()
            self.logger.info(f"Completed with scraping, parsing, cleaning, and updating of this county: {c}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Public Defense Data Orchestrator")
    parser.add_argument("--counties", nargs="*", help="Counties to process (space-separated)")
    parser.add_argument("--start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", help="End date (YYYY-MM-DD)")
    parser.add_argument("--court_calendar_link_text", help="Court calendar link text")
    parser.add_argument("--case_number", help="Case number")
    parser.add_argument("--case_html_path", help="Case HTML path")
    parser.add_argument("--judicial_officers", help="Judicial officers")
    parser.add_argument("--ms_wait", type=int, help="Milliseconds wait")
    parser.add_argument("--parse_single_file", action="store_true", help="Parse single file")
    parser.add_argument("--test", action="store_true", help="Test mode")

    args = parser.parse_args()

    # Create Orchestrator instance with parsed arguments
    Orchestrator(
        counties=args.counties,
        start_date=args.start_date,
        end_date=args.end_date,
        court_calendar_link_text=args.court_calendar_link_text,
        case_number=args.case_number,
        case_html_path=args.case_html_path,
        judicial_officers=args.judicial_officers,
        ms_wait=args.ms_wait,
        parse_single_file=args.parse_single_file,
        test=args.test
    ).orchestrate()