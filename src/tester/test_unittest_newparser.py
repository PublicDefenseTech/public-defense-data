# tests.py
import sqlite3

from sqlmodel import Session, create_engine, SQLModel
from datetime import date
import unittest
import parser.p_hays


class TestModels(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        SQLModel.metadata.create_all(self.engine)
        self.session = Session(self.engine)

    def tearDown(self):
        self.session.close()

    def test_newparser_end_to_end(
        self, county="hays", odyssey_id="123456", case_number="123456"
    ):
        self.parser_instance = parser.Parser()
        self.parser_instance.parse(
            county=county,
            odyssey_id=odyssey_id,
            case_number=case_number,
            parse_single_file=True,
            test=True,
        )

    def test_case_metadata(self):
        case_data = {
            "county_of_jurisdiction": "hays",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        retrieved_case = self.session.get(CaseMetadata, 1)
        self.assertEqual(retrieved_case.county_of_jurisdiction, "hays")

    def test_related_case(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        related_case_data = {"case_id": 1, "related_case": "Related1"}
        related_case = RelatedCase(**related_case_data)
        self.session.add(related_case)
        self.session.commit()
        self.assertEqual(self.session.get(RelatedCase, 1).related_case, "Related1")

    # Add test methods for other models similarly...
    def test_defendant(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        defendant_data = {"case_id": 1, "name": "John Doe"}
        defendant = Defendant(**defendant_data)
        self.session.add(defendant)
        self.session.commit()
        self.assertEqual(self.session.get(Defendant, 1).name, "John Doe")

    def test_defense_attorney(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        attorney_data = {"case_id": 1, "name": "Jane Smith"}
        attorney = DefenseAttorney(**attorney_data)
        self.session.add(attorney)
        self.session.commit()
        self.assertEqual(self.session.get(DefenseAttorney, 1).name, "Jane Smith")

    def test_state_information(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        state_data = {"case_id": 1, "prosecuting_attorney": "State Attorney"}
        state = StateInformation(**state_data)
        self.session.add(state)
        self.session.commit()
        self.assertEqual(
            self.session.get(StateInformation, 1).prosecuting_attorney, "State Attorney"
        )

    def test_charge(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        charge_data = {"case_id": 1, "charge_name": "Test Charge"}
        charge = Charge(**charge_data)
        self.session.add(charge)
        self.session.commit()
        self.assertEqual(self.session.get(Charge, 1).charge_name, "Test Charge")

    def test_disposition(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        disposition_data = {"case_id": 1, "event": "Test Event"}
        disposition = Disposition(**disposition_data)
        self.session.add(disposition)
        self.session.commit()
        self.assertEqual(self.session.get(Disposition, 1).event, "Test Event")

    def test_disposition_detail(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        disposition_data = {"case_id": 1, "event": "Test Event"}
        disposition = Disposition(**disposition_data)
        self.session.add(disposition)
        self.session.commit()
        detail_data = {"disposition_id": 1, "charge": "Charge Detail"}
        detail = DispositionDetail(**detail_data)
        self.session.add(detail)
        self.session.commit()
        self.assertEqual(self.session.get(DispositionDetail, 1).charge, "Charge Detail")

    def test_event(self):
        case_data = {
            "parse_id": 1,
            "county_of_jurisdiction": "Test",
            "court_case_number": "1",
            "parsing_date": date(2023, 1, 1),
        }
        case = CaseMetadata(**case_data)
        self.session.add(case)
        self.session.commit()
        event_data = {"case_id": 1, "event": "Court Event"}
        event = Event(**event_data)
        self.session.add(event)
        self.session.commit()
        self.assertEqual(self.session.get(Event, 1).event, "Court Event")
