from typing import Dict, List
from bs4 import BeautifulSoup
import traceback
from datetime import datetime
from sqlmodel import SQLModel, Field, Relationship, create_engine, Session, select
import xxhash
import os
import json
from dotenv import load_dotenv
import logging
from datetime import datetime as dt


class Updater:

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

    def update(self, parsed_data):
        CaseMetadataInstance = parsed_data["CaseMetadata"]
        EventInstance = parsed_data["Event"]
        ChargeInstance = parsed_data["Charge"]
        DefendantInstance = parsed_data["Defendant"]
        DefenseAttorneyInstance = parsed_data["DefenseAttorney"]
        DispositionInstance = parsed_data["Disposition"]
        DispositionDetailInstance = None
        EventInstance = parsed_data["Event"]
        RelatedCaseInstance = parsed_data["RelatedCase"]
        StateInformationInstance = parsed_data["StateInformation"]

        self.session.add(CaseMetadataInstance)
        self.session.commit()
        self.session.refresh(CaseMetadataInstance)

        CaseMetadata_id = self.session.get(
            CaseMetadataInstance, CaseMetadataInstance.id
        )

        DefendantInstance.case_id = CaseMetadata_id
        self.session.add(DefendantInstance)

        for Event in EventInstance:
            Event.case_id = CaseMetadata_id
            self.session.add(Event)

        for Charge in ChargeInstance:
            Charge.case_id = CaseMetadata_id
            self.session.add(Charge)

        for Disposition in DispositionInstance:
            Disposition.case_id = CaseMetadata_id
            self.session.add(Disposition)
            self.session.commit()
            self.session.refresh(Disposition)

            Disposition_id = self.session.get(
                DispositionInstance, DispositionInstance.id
            )
            for DispositionDetail in Disposition:
                DispositionDetail.disposition_id = Disposition_id
                DispositionDetailInstance.append(DispositionDetail)
                self.session.add(DispositionDetail)
                self.session.commit()
                self.session.refresh(DispositionDetail)
