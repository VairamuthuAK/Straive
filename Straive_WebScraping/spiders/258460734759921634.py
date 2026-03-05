import re
import json
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *          
from io import BytesIO


class CampuswebSpider(scrapy.Spider):
    """
    Spider to scrape:
    1. Course data
    2. Faculty / Staff directory
    3. Academic calendar (PDF)
    """

    name = "campusweb"

    # Unique institution ID (used across all outputs)
    institution_id = 258460734759921634


    # Course search page
    course_url = 'https://campusweb.capecod.edu/ics/ClientConfig/CustomContent/coursesearch.html'

    # Faculty / Staff directory page
    directory_url = 'https://www.capecod.edu/directory/'

    # Academic calendar PDF
    calendar_url ='https://www.capecod.edu/media/capecodedu/content-assets/documents/human-resources/Academic-Calendar-2025-2026.pdf'


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store directory records
        self.directory_rows = []

        # Store calendar records
        self.calendar_rows = []

        # Store course records
        self.course_rows = []


    def start_requests(self):
        # Read scrape mode from settings
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # ---------- COURSE ONLY ----------
        if mode == 'course':
            urls = [
                'https://campusweb.capecod.edu/ICS/ClientConfig/CustomContent/CourseSearch.aspx?year=2024&term=F',
                'https://campusweb.capecod.edu/ICS/ClientConfig/CustomContent/CourseSearch.aspx?year=2024&term=I',
                'https://campusweb.capecod.edu/ICS/ClientConfig/CustomContent/CourseSearch.aspx?year=2024&term=S',
                'https://campusweb.capecod.edu/ICS/ClientConfig/CustomContent/CourseSearch.aspx?year=2024&term=M'
            ]
            for url in urls:
                yield scrapy.Request(
                    url=url,
                    dont_filter=True,
                    callback=self.parse_course
                )

        # ---------- DIRECTORY ONLY ----------
        elif mode == 'directory':
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True
            )

        # ---------- CALENDAR ONLY ----------
        elif mode == 'calendar':
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar
            )

        # ---------- COMBINED MODES ----------
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url, self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url, self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(self.calendar_url, self.parse_calendar)
            yield scrapy.Request(self.directory_url, self.parse_directory, dont_filter=True)

        # ---------- DEFAULT: RUN ALL ----------
        else:
            yield scrapy.Request(self.course_url, self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, self.parse_calendar)

    def clean(self, value):
        """Safely strip text values"""
        return (value or "").strip()

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Parses course JSON response and saves course data
        """

        # Load JSON response
        json_data = json.loads(response.text)

        # Extract course blocks
        blocks = json_data.get("resultset", [])

        for block in blocks:
            # Handle enrollment value
            enroll = block.get("seatsavail", "")
            if isinstance(enroll, str) and "-" in enroll:
                enrollment = 0
            else:
                enrollment = enroll or ""

            # Extract section info
            section = block.get("section", "").strip()
            parts = section.split("-")
            if len(parts) >= 3:
                section_value = "-".join(parts[-2:])
            else:
                section_value = parts[-1]

            # Append course row
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": f"{self.clean(block.get('course'))} {self.clean(block.get('title'))}",
                "Course Description": self.clean(block.get("course_text")),
                "Class Number": self.clean(block.get("course")),
                "Section": section_value,
                "Instructor": self.clean(block.get("instructor")),
                "Enrollment": enrollment,
                "Course Dates": f"{block.get('start_dte','')} - {block.get('end_dte','')}",
                "Location": self.clean(block.get("location")),
                "Textbook/Course Materials": self.clean(block.get("bookurl")),
            })

        # Save course output
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Scrapes faculty and staff directory data
        """

        for row in response.xpath('//tr'):
            # Extract name and email
            name = row.xpath(
                './/td[@class="directory__results__name"]/a/text()'
            ).get(default='').strip()

            email = row.xpath(
                './/td[@class="directory__results__name"]/a/@href'
            ).get(default='').replace('mailto:', '').strip()

            # Extract title and departments
            title = row.xpath(
                './/td[@class="directory__results__title"]/p/text()'
            ).get(default='').strip()

            departments = row.xpath(
                './/td[@class="directory__results__department"]//a/text()'
            ).getall()
            departments = ", ".join(d.strip() for d in departments)

            if departments:
                title = f"{title}, {departments}"
            elif not title and not departments:
                title = name
                name = ""

            # Extract phone
            phone = row.xpath(
                './/td[@class="directory__results__phone"]/a/text()'
            ).get('').replace('(no internal extension)', '').strip()
            phone = phone.lstrip('/').strip()

            # Skip empty rows
            if not any([name, email, phone]):
                continue

            # Append directory row
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        # Pagination handling
        next_page = response.xpath(
            '//div[@class="program-pagination"]//a[text()=">"]/@href'
        ).get()

        if next_page:
            yield response.follow(next_page, callback=self.parse_directory)

        # Save directory output
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER (PDF)
    def parse_calendar(self, response):
        """
        Extracts academic calendar data from PDF
        """

        calendar_data = []

        # Load PDF from response
        pdf_bytes = BytesIO(response.body)

        with pdfplumber.open(pdf_bytes) as pdf:
            text = ""
            for page in pdf.pages:
                text += page.extract_text() + "\n"

        # Split and clean lines
        lines = [line.strip() for line in text.split("\n") if line.strip()]

        term = None

        for line in lines:
            # Detect term headers
            term_match = re.match(r"^(Fall|Spring|Summer|Winter)", line, re.IGNORECASE)
            if term_match:
                term = term_match.group(1).title()
                continue

            # Match date and description
            match = re.match(r"^([A-Za-z]+\s\d{1,2}(?:-\d{1,2})?)\s+(.*)", line)
            if match and term:
                calendar_data.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term,
                    "Term Date": match.group(1),
                    "Term Date Description": match.group(2),
                })

        # Save calendar output
        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")
