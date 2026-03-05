import re
import scrapy
import pandas as pd
import pdfplumber
from io import BytesIO
from ..utils import save_df


class HiuSpider(scrapy.Spider):
    name = "hiu"

    # Unique institution ID for all datasets
    institution_id = 258432349954402275

    # Source URLs
    course_url = "https://www.hiu.edu/pdf/41189217_tug_spring_2026_course_schedule_by_course.pdf"
    directory_url = "https://www.hiu.edu/about-hiu/directory.php"
    calendar_url = "https://www.hiu.edu/undergraduate-online/academics/academic-calendar.php"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Store scraped data
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # Entry Point – Decide what to scrape based on settings
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Single mode execution
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Combined modes
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Default: scrape everything
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # Utility function to clean text
    def clean(self, text):
        return re.sub(r"\s+", " ", text).strip() if text else ""

    # COURSE PDF PARSER
    def parse_course(self, response):
        pdf_bytes = BytesIO(response.body)
        all_tables = []

        # Extract tables from PDF pages
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                for table in page.extract_tables():
                    all_tables.append(pd.DataFrame(table))

        if not all_tables:
            self.logger.warning("No tables found in course PDF")
            return

        final_df = pd.concat(all_tables, ignore_index=True)

        # Rename columns to standard format
        final_df.columns = [
            "Number", "Sec", "Course Title", "Days",
            "Start Time", "End Time", "Units", "Professor", "Prereq"
        ]

        # Filter valid course codes (e.g., ABC123)
        final_df = final_df[final_df["Number"].str.match(r"^[A-Z]{3}\d+", na=False)]

        # Clean text fields
        final_df["Course Title"] = final_df["Course Title"].astype(str).str.replace("\n", " ").str.replace("FULL", "").str.strip()
        final_df["Professor"] = final_df["Professor"].astype(str).str.replace("\n", " ").str.strip()

        # Add static fields
        final_df["institution_id"] = self.institution_id
        final_df["Source URL"] = self.course_url

        # Map to required schema
        mapped_df = pd.DataFrame({
            "Cengage Master Institution ID": final_df["institution_id"],
            "Source URL": final_df["Source URL"],
            "Course Name": final_df["Number"] + " - " + final_df["Course Title"],
            "Course Description": "",
            "Class Number": final_df["Number"],
            "Section": final_df["Sec"],
            "Instructor": final_df["Professor"],
            "Enrollment": "",
            "Course Dates": "",
            "Location": "",
            "Textbook/Course Materials": ""
        })

        # Store rows
        self.course_rows.extend(mapped_df.to_dict("records"))

        # Save to CSV
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    # FACULTY DIRECTORY SCRAPER
    def parse_directory(self, response):

        # Decode obfuscated email script
        def decode_email(script_text):
            coded = re.search(r'coded\s*=\s*"([^"]+)"', script_text)
            cipher = re.search(r'cipher\s*=\s*"([^"]+)"', script_text)
            if not coded or not cipher:
                return None

            coded, cipher = coded.group(1), cipher.group(1)
            shift = len(coded)

            return "".join(
                ch if ch not in cipher else cipher[(cipher.index(ch) - shift) % len(cipher)]
                for ch in coded
            )

        faculty_blocks = response.xpath('//div[@class="span9"]')

        for block in faculty_blocks:
            name = block.xpath('.//h5/text() | .//h5/a/text()').get()
            title = block.xpath('.//p[@class="bio-title"]/text()').get()
            phone = block.xpath('.//a[contains(@href,"tel:")]/text()').get()
            script_text = block.xpath('.//script[contains(text(),"coded")]/text()').get()
            email = decode_email(script_text) if script_text else None

            if not name and not email:
                continue

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": self.clean(name),
                "Title": self.clean(title),
                "Email": email,
                "Phone Number": phone
            })

        # Save directory data
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPER
    def parse_calendar(self, response):
        tables = response.xpath('//table[contains(@class,"table-striped")]')

        for table in tables:
            # Extract term name
            term_name = self.clean(table.xpath('.//tr[@class="table-head"]/th/strong/text()').get())

            # Loop through date rows
            for row in table.xpath('.//tr[not(contains(@class,"table-head"))]'):
                desc = self.clean(" ".join(row.xpath('./td[1]//text()').getall()))
                date = self.clean(" ".join(row.xpath('./td[2]//text()').getall()))

                if not desc or not date:
                    continue

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": date,
                    "Term Date Description": desc
                })

        # Save calendar data
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
