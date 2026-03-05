import re
import json
import scrapy
import string
import pandas as pd
from ..utils import save_df


class UndergraduateSpider(scrapy.Spider):
    name = "undergraduate_lasell"

    # Unique institution ID
    institution_id = 258443775913781212

    # URLs
    course_url = "https://undergraduate.catalog.lasell.edu/courses"
    directory_url = "https://www.lasell.edu/staff-directory.html"
    calendar_url = "https://www.lasell.edu/academics/academic-catalog-and-calendar/academic-calendar.html"

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # Start Requests (Mode Based)
    def start_requests(self):
        """
        SCRAPE_MODE can be:
        - course
        - directory
        - calendar
        - all (default)
        """
        mode = self.settings.get("SCRAPE_MODE", "all").lower()

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default: scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # Utility Cleaner
    def clean(self, text):
        if not text:
            return ""
        if isinstance(text, list):
            text = " ".join(text)
        return re.sub(r"\s+", " ", text).strip()

    # COURSE SCRAPER (Coursedog API)
    def parse_course(self, response):
        """
        Coursedog API call for all courses (JSON API, no Playwright needed)
        """

        self.base_url = "https://app.coursedog.com/api/v1/cm/lasell/courses/search/%24filters"
        self.limit = 50

        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "origin": "https://undergraduate.catalog.lasell.edu",
            "referer": "https://undergraduate.catalog.lasell.edu/",
            "user-agent": "Mozilla/5.0",
        }

        self.payload = {
            "condition": "AND",
            "filters": [
                {
                    "filters": [
                        {
                            "condition": "and",
                            "filters": [
                                {"name": "status", "type": "is", "value": "Active"},
                                {"name": "courseNumber", "type": "greaterThan", "value": "1"},
                                {"name": "courseNumber", "type": "lessThan", "value": "500"},
                            ],
                        }
                    ],
                    "condition": "or",
                }
            ],
        }

        yield from self.course_request_data(skip=0)

    def build_url(self, skip):
        return (
            f"{self.base_url}"
            f"?catalogId=0Py3M38bTZ1I8mJU55Ic"
            f"&skip={skip}&limit={self.limit}"
        )

    def course_request_data(self, skip):
        url = self.build_url(skip)
        yield scrapy.Request(
            url=url,
            method="POST",
            headers=self.headers,
            body=json.dumps(self.payload),
            callback=self.parse_course_data,
            meta={"skip": skip},
        )

    def parse_course_data(self, response):
        data = json.loads(response.text)

        for item in data.get("data", []):
            course_name = f"{item.get('subjectCode')} {item.get('courseNumber')} - {item.get('longName')}"
            description = item.get("description", "")

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": course_name,
                "Course Description": self.clean(description),
                "Class Number": "",
                "Section": "",
                "Instructor": "",
                "Enrollment": "",
                "Course Dates": "",
                "Location": "",
                "Textbook/Course Materials": "",
            })

        # Pagination
        skip = response.meta["skip"] + self.limit
        if len(data.get("data", [])) == self.limit:
            yield from self.request_courses(skip)

        # Save
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        base_url = "https://www.lasell.edu/staff-directory.html?alpha={}"

        for letter in string.ascii_uppercase:
            yield scrapy.Request(base_url.format(letter), callback=self.parse_directory_details)

    def parse_directory_details(self, response):
        staff_items = response.xpath('//div[@class="staff-item"]')

        for item in staff_items:
            name = self.clean(item.xpath(".//h3/text()").get())
            title = self.clean(item.xpath('.//p[@class="position"]/text()').get())
            phone = self.clean(item.xpath('.//p[@class="phone"]/text()').get()).replace("Tel:", "").strip()

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": "",
                "Phone Number": phone,
            })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPER
    def parse_calendar(self, response):
        rows = response.xpath("//table//tr")
        current_month = ""

        for row in rows:
            # Month header
            month = row.xpath(".//h3/text()").get()
            if month:
                current_month = self.clean(month)
                continue

            date = self.clean(row.xpath("./td[1]//text()").getall())
            desc_td = row.xpath("./td[3]")

            if not date or not desc_td:
                continue

            # Multiple events
            events = desc_td.xpath(".//li/text()").getall()
            if events:
                for event in events:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": current_month,
                        "Term Date": date,
                        "Term Date Description": self.clean(event),
                    })
            else:
                desc = self.clean(desc_td.xpath(".//text()").getall())
                if desc:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": current_month,
                        "Term Date": date,
                        "Term Date Description": desc,
                    })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")
