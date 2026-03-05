import re
import scrapy
import pandas as pd
import urllib.parse
from ..utils import save_df


class RoswellSpider(scrapy.Spider):
    name = "roswell"

    # Institution ID (Static for all datasets)
    institution_id = 258444127673280468

    # URLs
    course_url = "https://ssb.enmu.edu:8911/ROSW/schedule.p_Classes?TRM=202521&SBJ=&INS=&CTY="
    directory_url = "https://www.roswell.enmu.edu/directory/"
    calendar_url = "https://www.roswell.enmu.edu/upcoming-events/"

    # Initialize Storage Containers
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # Entry Point – Control Scrape Mode
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-site': 'none',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            yield scrapy.Request(self.directory_url, headers=headers, callback=self.parse_directory)

        elif mode == "calendar":
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-US,en;q=0.9',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            yield scrapy.Request(self.calendar_url, headers=headers, callback=self.parse_calendar)

        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, headers=headers, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.calendar_url, headers=headers, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, headers=headers, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, headers=headers, callback=self.parse_calendar)

        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, headers=headers, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, headers=headers, callback=self.parse_calendar)

    # Utility: Clean Text
    def clean(self, text):
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    # COURSE SCRAPER
    def parse_course(self, response):

        rows = response.xpath("""
            //table//tr[
                position()>1
                and td[1]//b
                and normalize-space(td[1])!='CRN'
                and not(td[@colspan])
            ]
        """)

        for row in rows:

            course_code = row.xpath("normalize-space(td[2]//b)").get()
            course_title = row.xpath("normalize-space(td[5]//a)").get()

            # Skip invalid rows
            if not course_code or "Special Instructions" in course_code:
                continue

            crn = row.xpath("normalize-space(td[1]//b)").get()
            section = row.xpath("normalize-space(td[3]//b)").get()
            instructor = row.xpath("normalize-space(td[7])").get()
            location = row.xpath("normalize-space(td[11]//b)").get()

            enroll_cap = row.xpath("normalize-space(td[14])").get()
            enrolled = row.xpath("normalize-space(td[15])").get()

            term = row.xpath("normalize-space(td[12])").get()

            # Format enrollment
            enrollment = f"{enrolled}/{enroll_cap}" if enrolled and enroll_cap else ""

            # Extract term dates from parentheses
            term_dates = ""
            if term:
                match = re.search(r"\((.*?)\)", term)
                if match:
                    term_dates = match.group(1)

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": f"{course_code} {course_title}".strip(),
                "Course Description": "",
                "Class Number": crn,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": term_dates,
                "Location": location,
                "Textbook/Course Materials": "",
            })

        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):

        for row in response.xpath("//table[@id='tel-directory']//tbody//tr"):

            email = row.xpath("./@onclick").re_first(
                r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
            )

            if not email:
                continue

            encoded_email = urllib.parse.quote(email)
            ajax_url = (
                f"https://www.roswell.enmu.edu/emp-dir-ajax-handler.php?email={encoded_email}"
            )

            yield scrapy.Request(
                ajax_url,
                callback=self.parse_employee,
                meta={"source_url": response.url},
            )

    def parse_employee(self, response):

        data = response.json()

        name = f"{data.get('firstname','')} {data.get('lastname','')}"
        title = f"{data.get('position','')}, {data.get('department','')}"

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.meta.get("source_url"),
            "Name": self.clean(name),
            "Title": self.clean(title),
            "Email": data.get("emailaddress"),
            "Phone Number": data.get("telephonenumber"),
        })

        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):

        events = response.xpath(
            "//div[contains(@class,'tribe-events-widget-events-list__event-row')]"
        )

        for row in events:

            month = row.xpath(".//span[contains(@class,'event-date-tag-month')]/text()").get()
            day = row.xpath(".//span[contains(@class,'event-date-tag-daynum')]/text()").get()

            term_date = self.clean(f"{month} {day}") if month and day else None

            description = row.xpath(
                "normalize-space(.//h3[contains(@class,'event-title')]/a)"
            ).get()

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": "",
                "Term Date": term_date,
                "Term Date Description": self.clean(description),
            })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")