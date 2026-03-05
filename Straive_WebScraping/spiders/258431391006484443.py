import time
import scrapy
import random
import string
import requests
import pandas as pd
from ..utils import *
from parsel import Selector
from inline_requests import inline_requests
from playwright.sync_api import sync_playwright


class HonoluluSpider(scrapy.Spider):

    name ="hono"
    institution_id =258431391006484443

    course_url = "https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/classSearch/classSearch"

    calendar_url = "https://www.honolulu.hawaii.edu/academic-calendar/"

    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            self.parse_course()
            
        elif mode == "directory":
            self.parse_directory()

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            self.parse_directory()

        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

        else:
            self.parse_course()
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar)

            

    def parse_course(self):
        """
            Scrape course offerings from the University of Hawaiʻi Banner (SSB) system
        by combining browser automation with authenticated API requests.

        This method uses Playwright to:
            - Open the Banner term selection interface.
            - Iterate through all available academic terms for 2025 and 2026.
            - Apply advanced search filters (Campus = MAU).
            - Establish and maintain a valid session and cookies required for
            Banner's internal search APIs.

        Once a session is established, the method switches to direct HTTP requests
        against Banner JSON and HTML endpoints to efficiently retrieve course data.

        Term selection and search workflow:
            - Opens the term selector and iterates through each 2025–2026 term.
            - Submits the selected term and navigates to advanced search.
            - Applies campus filters and executes the course search.
            - Extracts session cookies from the browser context.

        API-driven course extraction:
            - Generates a unique session ID required by Banner search endpoints.
            - Uses paginated API requests (`pageOffset`, `pageMaxSize`) to retrieve
            all courses for the selected term.
            - Tracks total record count and iterates until all results are fetched.

        Session refresh logic:
            - Periodically reloads the Banner page after processing a fixed number
            of courses to prevent session expiration.
            - Re-applies search filters and refreshes cookies as needed.

        Per-course data enrichment:
            - Retrieves the course description via a POST request to the
            `getCourseDescription` endpoint.
            - Retrieves bookstore / textbook availability via the
            `getSectionBookstoreDetails` endpoint.
            - Retrieves meeting times and instructor assignments via the
            `getFacultyMeetingTimes` endpoint.
            - Normalizes instructor names, meeting dates, locations, and enrollment
            information.

        Data normalization:
            - Combines subject, course number, and title into a unified course name.
            - Formats enrollment as "enrolled / maximum".
            - Formats course dates as a start–end range.
            - Normalizes instructor names and meeting locations.

        Navigation reset:
            - After completing a term, navigates back to the term selector to
            continue processing remaining terms.

        Data persistence:
            - Aggregates all course records across terms into a single list.
            - Converts the results into a pandas DataFrame.
            - Saves the dataset using `save_df`, keyed by institution ID.

        Returns:
            None
        """
        rows = []

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            start_url = "https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/term/termSelection?mode=search"

            context = browser.new_context()
            page = context.new_page()

            page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
            time.sleep(5)

            page.locator('//span[@class="select2-arrow"]').click()
            time.sleep(1)
            page.locator('//li[@class="select2-more-results"]').scroll_into_view_if_needed()
            time.sleep(3)
            terms = page.locator(
                '//ul[@class="select2-results"]/li/div/div[contains(text(),"2025") or contains(text(),"2026")]'
            )

            for i in range(0, terms.count()):
                #  SELECT TERM 
                
                terms.nth(i).scroll_into_view_if_needed()
                terms.nth(i).click()
                time.sleep(1)

                term_code = page.locator('//input[@id="txt_term"]').get_attribute("value")
                page.locator('//button[@id="term-go"]').click()
                time.sleep(2)

                #  SEARCH 
                page.locator('//a[@id="advanced-search-link"]').click()
                time.sleep(2)
                page.locator('//label[contains(text(),"Campus")]/parent::li/parent::ul').click()
                time.sleep(2)
                page.locator('//div[@id="HON"]').click()
                time.sleep(2)
                page.locator('//button[@id="search-go"]').click()
                time.sleep(3)

                #  EXTRACT COOKIES 
                cookies = context.cookies()
                cookies_dict = {c["name"]: c["value"] for c in cookies}

                #  GENERATE uniqueSessionId 
                
                unique_session_id = (
                    ''.join(random.choices(string.ascii_lowercase, k=5))
                    + str(int(time.time() * 1000))
                )

                page_offset = 0
                page_size = 50
                total_count = None
                course_counter = 0
                while True:
                    
                    api_url = (
                        "https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/searchResults/searchResults"
                        f"?txt_campus=WOA"
                        f"&startDatepicker=&endDatepicker="
                        f"&uniqueSessionId={unique_session_id}"
                        f"&pageOffset={page_offset}&pageMaxSize={page_size}"
                        f"&sortColumn=subjectDescription&sortDirection=asc"
                    )

                    headers = {
                        'Accept': 'application/json, text/javascript, */*; q=0.01',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'Referer': 'https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/classSearch/classSearch',
                        'User-Agent': 'Mozilla/5.0',
                        'X-Requested-With': 'XMLHttpRequest'
                    }

                    response = requests.get(
                        api_url,
                        headers=headers,
                        cookies=cookies_dict,
                        timeout=30
                    )

                    api_json = response.json()
                    print("page_offset ------->",page_offset)
                    print("i>>>>>",i)
                    if total_count is None:
                        total_count = api_json.get("totalCount", 0)
                        print(f"Total records for term: {total_count}")

                    datas = api_json.get("data", [])

                    for data in datas:
                        course_counter += 1
                        if course_counter % 150 == 0:
                            print("🔄 Refreshing Banner session")
                            page.reload(wait_until="networkidle")
                            time.sleep(3)
                            page.locator('//a[@id="advanced-search-link"]').click()
                            time.sleep(2)
                            page.locator('//label[contains(text(),"Campus")]/parent::li/parent::ul').click()
                            time.sleep(3)
                            page.locator('//div[@id="HON"]').click()
                            time.sleep(3)
                            page.locator('//button[@id="search-go"]').click()
                            time.sleep(3)
                            cookies = context.cookies()
                            cookies_dict = {c["name"]: c["value"] for c in cookies}

                        faculty_list = data.get("faculty", [])
                        term_code = data.get("term","") 
                        course_reference = data.get("courseReferenceNumber","") 
                        url = "https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"

                        payload = f"term={term_code}&courseReferenceNumber={course_reference}"
                        headers = {
                        'Accept': 'text/html, */*; q=0.01',
                        'Accept-Language': 'en-GB,en-US;q=0.9,en;q=0.8',
                        'Connection': 'keep-alive',
                        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
                        'Origin': 'https://www.sis.hawaii.edu:9234',
                        'Referer': 'https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/classSearch/classSearch',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                        }

                        response = requests.request("POST", url, headers=headers, data=payload)
                        response = Selector(text=response.text)
                        desc = response.xpath('//section[@aria-labelledby="courseDescription"]/text()').getall()

                        textbook_url = "https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/searchResults/getSectionBookstoreDetails"

                        textbook_payload = f"term={term_code}&courseReferenceNumber={course_reference}"
                        textbook_response = requests.request("POST", textbook_url, headers=headers, data=textbook_payload)
                        textbook_response = Selector(text=textbook_response.text)

                        meeting_date_url = f"https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/searchResults/getFacultyMeetingTimes?term={term_code}&courseReferenceNumber={course_reference}"
                        meeting_date_url_response = requests.request("GET", meeting_date_url, headers=headers)
                        meeting_json_datas = meeting_date_url_response.json()
                        fmt_list = meeting_json_datas.get("fmt", [])
                        for meeting_json_data in fmt_list:
                            meetng_faculty_list = meeting_json_data.get("faculty", [])
                            instructor = ", ".join(f.get("displayName", "").strip() for f in meetng_faculty_list if f.get("displayName") )
                            mt = meeting_json_data.get("meetingTime", {})
                            rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": "https://www.sis.hawaii.edu:9234/StudentRegistrationSsb/ssb/classSearch/classSearch",
                                "Course Name": f"{data.get('subject','')} {data.get('courseDisplay','')} {data.get('courseTitle','')}".strip(),
                                "Course Description": "".join([p.strip() for p in desc if p.strip()]),
                                "Class Number": faculty_list[0].get("courseReferenceNumber", "") if faculty_list else "",
                                "Section": data.get("sequenceNumber","").strip(),
                                "Instructor": instructor,
                                "Enrollment": f"{data.get('enrollment','')} / {data.get('maximumEnrollment','')}",
                                "Course Dates": f"{mt.get('startDate','')} - {mt.get('endDate','')}",
                                "Location": f"{mt.get('buildingDescription','')} {mt.get('room','')}".strip(),
                                "Textbook/Course Materials": "https://hawaii-westoahu.verbacompare.com/"
                            })

                    page_offset += page_size
                    if page_offset >= total_count:
                        break

                # ⬅️ BACK TO TERM SELECTOR
                page.goto(start_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(3)
                page.locator('//span[@class="select2-arrow"]').click()
                time.sleep(1)

            browser.close()

        #  SAVE DATA 
        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")

    

    def parse_calendar(self,response):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
                
        blocks = response.xpath('//table/tbody/tr')
        rows=[]
        for block in blocks:
            desc = block.xpath('./td[2]/text() | ./td[2]/a/text() | ./td[2]/strong/text() | ./td[2]/strong/a/text()').getall()
            term_desc = [p.strip() for p in desc if p.strip()]
            rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": response.xpath('//h2[@class="wp-block-heading"]/text()').get('').strip(),
                    "Term Date": block.xpath('./td[1]/text()').get('').strip(),
                    "Term Date Description": " ".join(term_desc),
                })
        if rows:
            calendar_df = pd.DataFrame(rows)  # load to dataframe
            save_df(calendar_df, self.institution_id, "calendar") 