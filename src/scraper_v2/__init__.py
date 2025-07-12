import requests
from bs4 import BeautifulSoup
import urllib.parse
import time
from datetime import datetime
import logging
import os

class HaysCountyScraper:
    def __init__(self, base_url="https://public.co.hays.tx.us/"):
        self.session = requests.Session()
        self.session.verify = True
        self.base_url = base_url
        self.search_page_id = "100" # ID for Criminal Search
        self.case_html_path = os.path.join(os.path.dirname(__file__), "..", "..", "data", "hays", "case_html")

    def configure_logger(self) -> logging.Logger:
        logger = logging.getLogger(name=f"scraper: pid: {os.getpid()}")        
        logging.basicConfig(level=logging.INFO)
        scraper_log_path = os.path.join(os.path.dirname(__file__), "..", "..", "logs")
        now = datetime.now()
        formatted_date_time = now.strftime("%d-%m-%Y-%H.%M")
        scraper_log_name = formatted_date_time + '_scraper_logger_log.txt'
        file_handler = logging.FileHandler(os.path.join(scraper_log_path, scraper_log_name))
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        return logger

    def make_directories(self, case_html_path: str, logger):
        """Looks for a directory at the case_html_path location or creates it if it doesn't exist."""
        try:
            if not os.path.exists(case_html_path):
                os.makedirs(case_html_path)
                logger.info(f"Directory '{case_html_path}' created successfully.")
            else:
                logger.info(f"Directory '{case_html_path}' already exists.")
        except OSError as e:
            logger.error(f"Error creating directory '{case_html_path}': {e}")

    def _get_initial_page_elements(self):
        initial_page_url = urllib.parse.urljoin(self.base_url, f"Search.aspx?ID={self.search_page_id}")
        response_initial = self.session.get(initial_page_url)
        initial_soup = BeautifulSoup(response_initial.text, "html.parser")

        node_id = ""
        node_desc = ""
        sbx_control_id2 = initial_soup.find("select", id="sbxControlID2")
        if sbx_control_id2:
            selected_option = sbx_control_id2.find("option", selected=True) or sbx_control_id2.find("option")
            if selected_option:
                node_id = selected_option.get("value")
                node_desc = selected_option.text
        return node_id, node_desc

    def _get_search_form(self, node_id, node_desc):
        target_post_url = urllib.parse.urljoin(self.base_url, f"Search.aspx?ID={self.search_page_id}")
        post_data = {"NodeID": node_id, "NodeDesc": node_desc}
        
        response_search_page = self.session.post(target_post_url, data=post_data)
        search_page_soup = BeautifulSoup(response_search_page.text, "html.parser")
        
        search_form = search_page_soup.find("form")
        if not search_form:
            raise Exception("Could not find the main search form.")
            
        hidden_inputs = {inp.get("name"): inp.get("value") for inp in search_form.find_all("input", type="hidden") if inp.get("name")}
        
        form_action_url = search_form.get("action")
        if not form_action_url:
            form_action_url = response_search_page.url
        elif not form_action_url.startswith("http"):
            form_action_url = urllib.parse.urljoin(response_search_page.url, form_action_url)
            
        return hidden_inputs, form_action_url

    def search_case(self, case_number):
        """Performs the case search and returns parsed results."""
        node_id, node_desc = self._get_initial_page_elements()
        hidden_inputs, form_action_url = self._get_search_form(node_id, node_desc)

        search_params = {
            **hidden_inputs,
            "__EVENTTARGET": "",
            "__EVENTARGUMENT": "",
            "NodeID": node_id,
            "NodeDesc": node_desc,
            "SearchBy": "0", # Case Number radio button
            "CaseSearchMode": "CaseNumber",
            "CaseSearchValue": case_number,
            "CitationSearchValue": "",
            "CourtCaseSearchValue": "",
            "PartySearchMode": "Name",
            "AttorneySearchMode": "Name",
            "LastName": "",
            "FirstName": "",
            "cboState": "AA",
            "MiddleName": "",
            "DateOfBirth": "",
            "DriverLicNum": "",
            "CaseStatusType": "0", # All statuses
            "DateFiledOnAfter": "",
            "DateFiledOnBefore": "",
            "chkCriminal": "on", "chkFamily": "on", "chkCivil": "on",
            "chkProbate": "on", "chkDtRangeCriminal": "on", "chkDtRangeFamily": "on",
            "chkDtRangeCivil": "on", "chkDtRangeProbate": "on",
            "chkCriminalMagist": "on", "chkFamilyMagist": "on",
            "chkCivilMagist": "on", "chkProbateMagist": "on",
            "DateSettingOnAfter": "",
            "DateSettingOnBefore": "",
            "SortBy": "fileddate",
            "SearchSubmit": "Search",
            "SearchType": "CASE",
            "SearchMode": "CASENUMBER",
            "NameTypeKy": "", "BaseConnKy": "",
            "StatusType": "true", "ShowInactive": "", "AllStatusTypes": "true",
            "CaseCategories": "", "RequireFirstName": "",
            "CaseTypeIDs": "", "HearingTypeIDs": "",
            "SearchParams": f"SearchBy~~Search By:~~Case~~Case||CaseNumberOption~~Case Search Mode:~~CaseNumber~~Number||CaseSearchValue~~Case Number:~~{case_number}~~{case_number}||AllOption~~All~~0~~All||selectSortBy~~Sort By:~~Filed Date~~Filed Date"
        }

        final_search_response = self.session.post(form_action_url, data=search_params)
        results_soup = BeautifulSoup(final_search_response.text, "html.parser")
        return results_soup

    def parse_search_results(self, results_soup):
        """Parses the search results page and extracts case URLs."""
        results_table = results_soup.find(lambda tag: tag.name == 'table' and tag.find('th', class_='ssSearchResultHeader'))
        
        parsed_cases = []
        if results_table:
            for tr in results_table.find_all("tr"):
                cells = tr.find_all("td")
                
                if len(cells) >= 6: # Ensure enough columns for a data row
                    row_data = [td.get_text(strip=True) for td in cells]
                    
                    case_number_link = cells[0].find('a', href=True)
                    if case_number_link:
                        row_data[0] = {
                            'text': case_number_link.get_text(strip=True),
                            'href': urllib.parse.urljoin(self.base_url, case_number_link['href'])
                        }
                    
                    defendant_info_divs = cells[2].find_all('div')
                    if defendant_info_divs: row_data[2] = [d.get_text(strip=True) for d in defendant_info_divs]
                    else: row_data[2] = []

                    filed_location_divs = cells[3].find_all('div')
                    if filed_location_divs: row_data[3] = [d.get_text(strip=True) for d in filed_location_divs]
                    else: row_data[3] = []

                    type_status_divs = cells[4].find_all('div')
                    if type_status_divs: row_data[4] = [d.get_text(strip=True) for d in type_status_divs]
                    else: row_data[4] = []

                    charge_table_td = cells[5].find('td')
                    if charge_table_td: row_data[5] = charge_table_td.get_text(strip=True)

                    parsed_cases.append(row_data)
        return parsed_cases

    def get_case_detail_page(self, detail_url):
        """Fetches the HTML of a specific case detail page."""
        time.sleep(1)
        response_detail_page = self.session.get(detail_url)
        return response_detail_page.text

if __name__ == "__main__":
    scraper = HaysCountyScraper()
    case_number_to_search = "18-1415CR-2"

    print(f"Searching for case: {case_number_to_search}")
    results_soup = scraper.search_case(case_number_to_search)
    
    cases_found = scraper.parse_search_results(results_soup)

    if cases_found:
        print(f"Found {len(cases_found)} case(s) in search results.")
        for case_info in cases_found:
            print(f"Search Result: {case_info}")
            
            case_detail_link = case_info[0].get('href') if isinstance(case_info[0], dict) else None
            
            if case_detail_link:
                print(f"\nFetching detail page for: {case_detail_link}")
                case_html = scraper.get_case_detail_page(case_detail_link)
                print(case_html)
            else:
                print("No detail link found for this search result.")


            with open(
                os.path.join(scraper.case_html_path, f"{case_number_to_search}.html"), "w"
            ) as file_handle:
                file_handle.write(case_html)            
    else:
        print(f"No search results found for case number: {case_number_to_search}")