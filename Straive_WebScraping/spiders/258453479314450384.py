import io
import re
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from inline_requests import inline_requests


class NavajotechSpider(scrapy.Spider):
    name = "navajotech"
    institution_id = 258453479314450384

    # URLs
    course_url = "https://www.navajotech.edu/academics/course-schedules/"
    directory_url = "https://www.navajotech.edu/faculty-staff/staff-directory/"
    calendar_pdf_url = "https://www.navajotech.edu/wp-content/uploads/2025/03/NTU-Academic-Calendar-2025-2026.pdf"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # START REQUESTS BASED ON MODE
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Single mode scraping
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_pdf_url, callback=self.parse_calendar)

        # Two mode combinations
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_pdf_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_pdf_url, callback=self.parse_calendar)

        # Default: scrape everything
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_pdf_url, callback=self.parse_calendar)

    # COURSE PARSER
    @inline_requests
    def parse_course(self, response):
        term_links = response.xpath('//div[@class="et_pb_text_inner"]//tbody//td[@style="height: 24px;"]//a/@href').getall()

        # Loop through each term PDF link
        for link in term_links:
            term_url = response.urljoin(link)
            term_response = yield scrapy.Request(term_url)

            global_last_course_date = ""
            current_course = None

            # Open PDF
            with pdfplumber.open(io.BytesIO(term_response.body)) as pdf:
                for page in pdf.pages:

                    # Extract tables using line detection
                    tables = page.extract_tables(
                        table_settings={
                            "vertical_strategy": "lines",
                            "horizontal_strategy": "lines",
                            "intersection_tolerance": 5,
                        }
                    )

                    # Loop through tables
                    for table in tables:
                        last_instructor = ""
                        last_enrollment = ""
                        last_course_date = ""
                        last_section = ""
                        title = ""
                        current_course = None

                        # Loop through each row in table
                        for row in table:
                            row = [(cell.strip() if isinstance(cell, str) else "") for cell in row]
                            row += [""] * (9 - len(row))  # Ensure min columns

                            # Skip empty rows
                            if not any(row):
                                continue

                            # Skip header rows
                            if "NAVAJO TECHNICAL UNIVERSITY" in row[0]:
                                continue

                            # Extract course dates (Format 1)
                            if "Students can register either" in row[0]:
                                match = re.search(r"Instruction\s*Begins\:\s*(.*?)\s*Tuition", row[0])
                                if match:
                                    course_dates = match.group(1).split("-")
                                    last_course_date = " - ".join(
                                        part.replace("ENDS:", "").strip()
                                        for part in course_dates
                                        if part.strip()
                                    )
                                    global_last_course_date = last_course_date

                            # Extract course dates (Format 2)
                            if "All outstanding accounts" in row[0]:
                                match = re.search(r"Instruction\s*Begins\s*\-\-\s*(.*?)\s*Tuition", row[0])
                                match_2 = re.search(r"Instruction\s*Ends\s*\-\-\s*(.*?)\s*", row[0])
                                if match and match_2:
                                    last_course_date = f"{match.group(1)} - {match_2.group(1)}"
                                    global_last_course_date = last_course_date
                                continue

                            effective_course_date = last_course_date or global_last_course_date

                            # Skip column headers
                            if row[0] == "NUMBER":
                                continue

                            # Title only row
                            if row[0] and not any(row[1:]):
                                title = row[0]
                                continue

                            # Main course row
                            if row[0] and row[1]:
                                course_name = f"{row[0]} {row[1]} {title}".strip()
                                last_instructor = row[6] or last_instructor
                                last_enrollment = int(row[8]) if row[8].isdigit() else last_enrollment
                                last_section = row[1].split("-")[-1] or last_section

                                current_course = {
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": term_url,
                                    "Course Name": course_name,
                                    "Course Description": row[2],
                                    "Class Number": row[1].split("-")[0],
                                    "Section": last_section,
                                    "Instructor": last_instructor,
                                    "Enrollment": last_enrollment,
                                    "Course Dates": effective_course_date,
                                    "Location": row[5],
                                    "Textbook/Course Materials": "",
                                }

                                self.course_rows.append(current_course)

                            # Continuation row
                            elif current_course and row[2]:
                                class_number = row[1].split("-")[0] if row[1] else current_course["Class Number"]
                                last_instructor = row[6] or last_instructor
                                last_enrollment = int(row[8]) if row[8].isdigit() else last_enrollment
                                last_section = row[1].split("-")[-1] or last_section

                                current_course = {
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": term_url,
                                    "Course Name": current_course["Course Name"],
                                    "Course Description": row[2],
                                    "Class Number": class_number,
                                    "Section": last_section,
                                    "Instructor": last_instructor,
                                    "Enrollment": last_enrollment,
                                    "Course Dates": effective_course_date,
                                    "Location": row[5],
                                    "Textbook/Course Materials": "",
                                }

                                self.course_rows.append(current_course)

        # Save course data
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY PARSER
    def parse_directory(self, response):
        staff_links = response.xpath(
            "//th[normalize-space()='Staff Directory']/ancestor::tr/following-sibling::tr/td/a/@href"
        ).getall()

        # Loop staff profile pages
        for link in staff_links:
            link_url = response.urljoin(link)
            yield scrapy.Request(link_url, callback=self.parse_directory_details)

    def parse_directory_details(self, response):
        staff_blocks = response.xpath("//div[contains(@class,'et_pb_team_member_description')]")

        # Loop staff blocks
        for staff in staff_blocks:
            name = staff.xpath(".//h4[@class='et_pb_module_header']/text()").get(default="").strip()
            title = staff.xpath(".//p[contains(@class,'et_pb_member_position')]/text()").get(default="").strip()
            text_block = " ".join(staff.xpath(".//text()").getall())

            email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text_block)

            # Phone regex
            pattern = re.compile(r"(?P<phone>\d{3}[.\-\s]\d{3}[.\-\s]\d{4})(?:.*?(?:ext|extension)\s*(?P<ext>\d+))?", re.I)
            match = pattern.search(text_block)

            phone = match.group("phone") if match else ""
            extension = match.group("ext") if match else ""

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email_match.group(0) if email_match else None,
                "Phone Number": f"{phone}, {extension}" if extension else phone,
            })

    def closed(self, reason):
        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

    # CALENDAR PARSER
    def parse_calendar(self, response):
        source_url = response.url
        pdf_file = BytesIO(response.body)
        raw_lines = []

        # Extract text lines from PDF
        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    raw_lines.extend(text.split("\n"))

        # Clean OCR issues
        def clean_ocr(text):
            text = text.replace(" l", " 1").replace(" I", " 1").replace("Day", "")
            return re.sub(r"\s+", " ", text).strip()

        cleaned_lines = [clean_ocr(line) for line in raw_lines]
        date_pattern = re.compile(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}(-\d{1,2})?")

        current_term = None

        # Loop through calendar lines
        for line in cleaned_lines:

            if "Fall Semester" in line:
                current_term = "Fall Semester 2025"
                continue

            if "Spring Semester" in line:
                current_term = "Spring Semester 2026"
                continue

            match = date_pattern.search(line)
            if match and current_term:
                event = line.replace(match.group(), "").strip(":- ")
                date = match.group()

                if len(event) < 3:
                    continue

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": source_url,
                    "Term Name": current_term,
                    "Term Date": date,
                    "Term Date Description": event,
                })

        # Save calendar data
        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
