import re
import json
import scrapy
import pandas as pd
from ..utils import *
from email.mime import text
from urllib import response
from datetime import datetime
from urllib.parse import quote


class CatalogSpider(scrapy.Spider):
    # Spider name used by Scrapy CLI
    name = "catalog"

    # Unique institution ID used for all datasets
    institution_id = 258417421382084561 

    # Base URL for course search (dynamic content)
    course_url = "https://catalog.spu.edu/course-search/?"

    # Faculty / Staff directory page (currently empty)
    directory_url = ""

    # Academic calendar page (static HTML)
    calendar_url = "https://catalog.spu.edu/undergraduate/academic-calendar/"

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store scraped directory data
        self.directory_rows = []

        # Store scraped calendar data
        self.calendar_rows = []

        # Store scraped course data
        self.course_rows = []

    # Entry Point – Select Scrape Mode
    def start_requests(self):
        # NOTE: Playwright is used later because course data is dynamically loaded via JS

        # Read scrape mode from settings (course / directory / calendar / combinations)
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---- Single Mode Execution ----
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, dont_filter=True, callback=self.parse_course)

        elif mode == 'directory':
            # Custom headers to mimic browser AJAX request
            headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'referer': 'https://nwc.edu/directory/index.html',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }
            yield scrapy.Request(url=self.directory_url, headers=headers, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            # Static HTML page – simple GET request
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        # ---- Combined Modes (Order Independent) ----
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)

        # ---- Default: Scrape Everything ----
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

    # Utility function to safely clean string values
    def clean(self, value):
        return (value or "").strip()

    def parse_course(self, response):
        """
        Scrapes all course schedule data.
        The initial HTML is used only to extract session values.
        Actual data is retrieved via API POST requests.
        """

        # Extract available academic sessions from dropdown
        option = response.xpath('//select[@id="crit-session"]//option/@value').getall()

        # Skip the first option (usually "All")
        for val in option[1:]:
            # API endpoint for course search
            url = f"https://catalog.spu.edu/course-search/api/?page=fose&route=search&session={val}"

            # Payload defining search criteria
            payload = {
                    "other": {
                        "srcdb": "20256"
                    },
                    "criteria": [
                        {
                            "field": "session",
                            "value": f"{val}"
                        }
                    ]
                }

            # Headers required for API request
            headers = {
                'Accept': 'application/json, text/javascript, */*; q=0.01',
                'Content-Type': 'application/json',
                'Origin': 'https://catalog.spu.edu',
                'Referer': 'https://catalog.spu.edu/course-search/?',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }

            # POST request to retrieve course list
            yield scrapy.Request(
                url=url,
                method='POST',
                body=json.dumps(payload),
                headers=headers,
                dont_filter=True,
                callback=self.parse_course_list
            )

    def parse_course_list(self, response):
        # Parse JSON response containing course listings
        json_data = json.loads(response.text)

        for item in json_data["results"]:
            # Extract identifiers needed for course details
            srcdb = item.get("srcdb", "")
            code = item.get("code", "")
            crn = item.get("crn", "")

            # API endpoint for course details
            url = "https://catalog.spu.edu/course-search/api/?page=fose&route=details"

            # Payload for course detail lookup
            payload = {
                    "group": f"code:{code}",
                    "key": "",
                    "srcdb": srcdb,
                    "matched": f"crn:{crn}"
                }

            # Headers for detail request
            headers = {
                    'Accept': 'application/json, text/javascript, */*; q=0.01',
                    'Origin': 'https://catalog.spu.edu',
                    'Referer': 'https://catalog.spu.edu/course-search/?',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    }

            # Request detailed course information
            yield scrapy.Request(
                url=url,
                method='POST',
                body=json.dumps(payload),
                headers=headers,
                dont_filter=True,
                callback=self.parse_course_details
            )

    def parse_course_details(self, response):
        # Parse JSON response for course and section details
        json_data = json.loads(response.text)

        # Course-level data (shared across sections)
        title1 = json_data.get("code", "")
        title2 = json_data.get("title", "")
        course_title = f"{title1} - {title2}".strip()

        # Course description may contain HTML
        course_description = json_data.get("description", "")
        clean_description = re.sub(r'<[^>]+>', '', course_description)

        # Extract enrollment HTML snippet
        enrollment = json_data.get("seats", "")
        enrollment_clean = ""

        # Extract max and available seats using regex
        max_match = re.search(r'seats_max">(\d+)<', enrollment)
        avail_match = re.search(r'seats_avail">(\d+)<', enrollment)

        if max_match and avail_match:
            enrollment_clean = f"{avail_match.group(1)}/{max_match.group(1)}"
        else:
            enrollment_clean = ""

        # Iterate over all sections (CRNs)
        for sec in json_data.get("allInGroup", []):
            class_number = sec.get("crn", "")
            section = sec.get("section", "")
            instructor_name = sec.get("instr", "")

            start_date = sec.get("start_date", "")
            end_date = sec.get("end_date", "")
            date_only = f"{start_date} - {end_date}" if start_date and end_date else ""

            # Append structured course record
            self.course_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": "https://catalog.spu.edu/course-search/?",
                    "Course Name": re.sub(r'\s+', ' ', course_title),
                    "Course Description": re.sub(r'\s+', ' ', clean_description),
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": instructor_name,
                    "Enrollment": enrollment_clean,
                    "Course Dates": date_only,
                    "Location": '',
                    "Textbook/Course Materials": '',
                }
            )

            # Save course data incrementally
            course_df = pd.DataFrame(self.course_rows)
            save_df(course_df, self.institution_id, "course")

    def parse_directory(self, response):
        # This method handles cases where directory data is unavailable
        # It inserts a default placeholder record instead of scraped data
        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": "",  
            "Name": "Data not found",  
            "Title": "Data not found", 
            "Email":"Data not found" ,  
            "Phone Number": "Data not found",  
        })

        # Convert directory records into a DataFrame
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")
        
    def parse_calendar(self, response):
        # Each term is represented by an <h2> tag
        terms = response.xpath('//h2[@name="text"]')

        for term in terms:
            name = term.xpath('normalize-space(text())').get()

            # Each term has a table immediately following it
            table = term.xpath('following-sibling::table[1]')
            rows = table.xpath('.//tbody/tr')

            for row in rows:
                event = row.xpath('normalize-space(td[@class="column0"]//text())').get()
                date = row.xpath('normalize-space(td[@class="column1"]//text())').get()

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": "https://catalog.spu.edu/undergraduate/academic-calendar/",
                    "Term Name": name,
                    "Term Date": date,
                    "Term Date Description": event
                })

        # Save academic calendar data
        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
