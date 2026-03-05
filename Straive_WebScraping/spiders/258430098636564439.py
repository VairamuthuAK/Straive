import re
import json
import scrapy
import pandas as pd
import pdfplumber
from io import BytesIO
from inline_requests import inline_requests
from ..utils import save_df   # Custom helper for saving data


class WestHillsSpider(scrapy.Spider):
    """
    Scrapes:
    1. Course schedule data
    2. Faculty/Staff directory
    3. Academic calendar PDF
    """

    name = "westhills"

    # Unique institution ID for datasets
    institution_id = 258430098636564439

    # URLs
    course_url = "https://westhillscollege.com/schedule/"
    directory_url = "https://westhillscollege.com/contact-us/directory/"
    calendar_url = "https://westhillscollege.com/documents/instructional_calendars_2025-26.pdf"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT - SELECT SCRAPE MODE
    def start_requests(self):
        """
        SCRAPE_MODE options:
        course, directory, calendar,
        course_directory, course_calendar, directory_calendar,
        all (default)
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:  # Default: scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COURSE SCRAPER
    @inline_requests
    def parse_course(self, response):
        """
        Extract course table and fetch section details via Ellucian API.
        """

        rows = response.xpath('//table[@id="ActiveSectionsTable"]/tbody/tr')

        for row in rows:
            campus = row.xpath('./td[1]/text()').get(default='').strip()

            # Only scrape Coalinga campus
            if campus != "Coalinga":
                continue

            title_text = row.xpath('./td[3]/a/text()').get(default='').strip()
            title_href = row.xpath('./td[3]/a/@href').get(default='').strip()
            section_id = title_href.split("/")[-1]

            # Parse course title and section
            # Example: (ENG-1A-A01) English Composition
            m = re.search(r"\((.*?)\)\s*(.*)", title_text)
            if not m:
                continue

            full_section = m.group(1)
            course_name = m.group(2)

            sec_match = re.search(r"(.*?)-([A-Z]\d+)$", full_section)
            class_code = sec_match.group(1) if sec_match else full_section
            section_code = sec_match.group(2) if sec_match else ""

            class_name = f"{class_code} - {course_name}"

            # Step 1: Get token and cookies
            url_id = f"https://ellucianssui.whccd.edu/Student/Courses/Search?sectionids={section_id}&"
            page = yield scrapy.Request(url=url_id, dont_filter=True)

            token = page.xpath('//input[@name="__RequestVerificationToken"]/@value').get('')
            cookie = b'; '.join(page.headers.getlist('Set-Cookie')).decode()

            # Step 2: POST search API
            search_url = "https://ellucianssui.whccd.edu/Student/Courses/PostSearchCriteria"

            payload = {
                "keyword": None,
                "terms": [],
                "sectionIds": [section_id],
                "pageNumber": 1,
                "quantityPerPage": 30,
                "searchResultsView": "CatalogListing"
            }

            headers = {
                "__isguestuser": "true",
                "__requestverificationtoken": token,
                "content-type": "application/json",
                "x-requested-with": "XMLHttpRequest",
                "user-agent": "Mozilla/5.0",
                "Cookie": cookie,
                "referer": url_id,
                "origin": "https://ellucianssui.whccd.edu"
            }

            search_resp = yield scrapy.Request(
                url=search_url,
                method="POST",
                headers=headers,
                body=json.dumps(payload),
                dont_filter=True
            )

            data = json.loads(search_resp.text)
            course = data["Courses"][0]
            course_id = course["Id"]
            matching_section_ids = course["MatchingSectionIds"]

            # Step 3: Fetch section details
            section_url = "https://ellucianssui.whccd.edu/Student/Courses/Sections"

            payload2 = {
                "courseId": course_id,
                "sectionIds": matching_section_ids
            }

            section_resp = yield scrapy.Request(
                url=section_url,
                method="POST",
                headers=headers,
                body=json.dumps(payload2),
                dont_filter=True
            )

            data2 = json.loads(section_resp.text)

            # Course description
            description = data2.get("SectionsRetrieved", {}).get("Course", {}).get("Description", "")

            # Section info
            term = data2.get("SectionsRetrieved", {}).get("TermsAndSections", [{}])[0]
            sec = term.get("Sections", [{}])[0]
            section_data = sec.get("Section", {})

            # Seats
            seats_raw = section_data.get("AvailabilityDisplay", "")
            seats = "/".join(seats_raw.replace(" ", "").split("/")[:2]) if seats_raw else ""

            # Meeting info
            meeting = section_data.get("FormattedMeetingTimes", [{}])[0]
            instruction_method = meeting.get("InstructionalMethodDisplay", "")
            location = section_data.get("LocationDisplay", "")
            date = meeting.get("DatesDisplay", "")

            if location:
                location = f"{location} ({instruction_method})"

            # Instructor
            faculty = sec.get("FacultyDisplay", "")

            # Save row
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": url_id,
                "Course Name": class_name,
                "Course Description": re.sub(r"\s+", " ", description),
                "Class Number": class_code,
                "Section": section_code,
                "Instructor": faculty,
                "Enrollment": seats,
                "Course Dates": date,
                "Location": location,
                "Textbook/Course Materials": ""
            })

        # Save course data
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrapes faculty and staff directory profiles.
        """

        for person in response.xpath('//ul[@class="block-listing__items"]/li'):
            name = person.xpath('.//div[@class="block-listing__title header6"]/text()').get(default='').strip()
            title = person.xpath('.//div[@class="block-listing__info"][1]/text()').get(default='').strip()
            department = person.xpath('.//div[@class="block-listing__sub-title"]/text()').get(default='').strip()
            email = person.xpath('.//div[contains(text(),"@")]/text()').get(default='').strip()

            # Extract phone number
            phone = ""
            for p in person.xpath('.//div/text()').getall():
                p = p.strip()
                if re.search(r"\d{3}", p) and "@" not in p:
                    phone = p
                    break

            full_title = f"{title}, {department}" if department else title

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_url,
                "Name": name,
                "Title": full_title,
                "Email": email,
                "Phone Number": phone,
            })

        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Extracts term dates from academic calendar PDF.
        """

        pdf = pdfplumber.open(BytesIO(response.body))

        # Extract all text
        full_text = "\n".join([page.extract_text() or "" for page in pdf.pages])

        # Term pattern
        term_pattern = r"(\d{4}\s+SUMMER SESSION|\d{4}\s+FALL SEMESTER|\d{4}\s+SPRING SEMESTER)"
        blocks = re.split(term_pattern, full_text)

        for i in range(1, len(blocks), 2):
            term_name = blocks[i].strip()
            block_text = blocks[i + 1]

            for line in block_text.split("\n"):
                line = line.strip()
                if not line:
                    continue

                match = re.match(r"([A-Za-z]+\s+\d+(?:\s*(?:–|-)\s*[A-Za-z]*\s*\d+)?)\s+(.*)", line)
                if not match:
                    continue

                term_date = match.group(1)
                desc = match.group(2)

                # Remove weekday prefixes
                desc = re.sub(r"^(M|T|W|TH|F|SA|SU|M-F|T-TH|TH-F)\s+", "", desc)

                # Skip junk lines
                if "Total Instruction Days" in desc or "Approved by" in desc:
                    continue

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": desc
                })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
