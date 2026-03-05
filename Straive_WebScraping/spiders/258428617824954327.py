import re
import json
import scrapy
import pandas as pd
from ..utils import save_df


class EkuSpider(scrapy.Spider):
    name = "eku"

    # Unique institution ID used for all datasets
    institution_id = 258428617824954327

    # Base URLs
    course_url = "https://catalogs.eku.edu/course-search/"
    directory_url = "https://www.eku.edu/people/"
    calendar_url = ""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT – Decide what to scrape
    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Step 1:
        Loop through known term codes and available subjects.
        For each combination → call course search API.
        """

        subjects = response.xpath('//select[@id="crit-coursetype"]/option/@value').getall()
        terms = ['202610', '202615', '202650', '202620', '202550', '202520']

        # Loop through each term
        for term in terms:

            # Skip first subject (usually empty option)
            for subject in subjects[1:]:

                url = "https://catalogs.eku.edu/course-search/api/?page=fose&route=search"

                payload = {
                    "other": {"srcdb": term},
                    "criteria": [{"field": subject, "value": "Y"}]
                }

                yield scrapy.Request(
                    url=url,
                    method="POST",
                    body=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                    callback=self.parse_course_list
                )

    def parse_course_list(self, response):
        """
        Step 2:
        For each course result → request detailed section data.
        """

        data = json.loads(response.text)

        for item in data.get("results", []):
            srcdb = item.get("srcdb")
            code = item.get("code")
            crn = item.get("crn")

            detail_url = "https://catalogs.eku.edu/course-search/api/?page=fose&route=details"

            payload = {
                "group": f"code:{code}",
                "key": f"crn:{crn}",
                "srcdb": srcdb,
                "matched": f"crn:{crn}"
            }

            yield scrapy.Request(
                url=detail_url,
                method="POST",
                body=json.dumps(payload),
                headers={"Content-Type": "application/json"},
                callback=self.parse_course_details
            )

    def parse_course_details(self, response):
        """
        Step 3:
        Extract course-level and section-level details.
        """

        data = json.loads(response.text)

        course_code = data.get("code", "")
        course_title = data.get("title", "")
        full_title = f"{course_code} - {course_title}".strip()

        # Clean HTML from description
        description_html = data.get("description", "")
        description = re.sub(r"<[^>]+>", "", description_html)

        # Extract seat availability using regex
        seats_html = data.get("seats", "")
        max_match = re.search(r'seats_max">(\d+)<', seats_html)
        avail_match = re.search(r'seats_avail">(\d+)<', seats_html)

        enrollment = ""
        if max_match and avail_match:
            enrollment = f"{avail_match.group(1)}/{max_match.group(1)}"

        # Loop through each section under this course
        for section in data.get("allInGroup", []):

            start_date = section.get("start_date")
            end_date = section.get("end_date")
            date_range = f"{start_date} - {end_date}" if start_date and end_date else ""

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": re.sub(r"\s+", " ", full_title),
                "Course Description": re.sub(r"\s+", " ", description).strip(),
                "Class Number": section.get("crn"),
                "Section": section.get("no"),
                "Instructor": section.get("instr"),
                "Enrollment": enrollment,
                "Course Dates": date_range,
                "Location": "",
                "Textbook/Course Materials": ""
            })

        # Save after processing response
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        EKU directory uses pagination.
        Loop through all known pages and request JSON data.
        """

        for page in range(1, 91):
            yield scrapy.Request(
                f"https://www.eku.edu/people/?page={page}",
                callback=self.parse_directory_data
            )

    def parse_directory_data(self, response):
        """
        Extract directory JSON embedded inside script tag.
        """

        script = response.xpath(
            "//script[contains(text(),'window.ekuDirectoryData')]/text()"
        ).get("")

        match = re.search(
            r'window\.ekuDirectoryData\s*=\s*(\[.*\]);',
            script,
            re.S
        )

        if not match:
            return

        data = json.loads(match.group(1))

        for person in data:

            first = person.get("prefFirstname") or person.get("firstname") or ""
            last = person.get("lastname") or ""
            name = f"{first} {last}".strip()

            profile_url = f"https://www.eku.edu/people/{first.lower()}-{last.lower()}/"

            title = person.get("jobTitle")
            department = person.get("department")
            email = person.get("email")
            phone = person.get("officePhone")

            full_title = f"{title} | {department}" if title and department else title

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": profile_url,
                "Name": name,
                "Title": full_title.strip() if full_title else None,
                "Email": email,
                "Phone Number": phone
            })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # CALENDAR SCRAPER (No Data Available)
    def parse_calendar(self, response):

        self.calendar_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": "Data not available",
            "Term Name": "Data not available",
            "Term Date": "Data not available",
            "Term Date Description": "Data not available"
        })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")