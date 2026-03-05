import re
import json
import scrapy
import pandas as pd
from ..utils import save_df


class EtownSpider(scrapy.Spider):
    name = "etown"

    # Unique institution ID for Cengage
    institution_id = 258460947356608476

    # URLs
    course_url = "https://elizabethtown.kctcs.edu/class-search.aspx"
    directory_url = "https://www.etown.edu/directory/"
    calendar_url = "https://www.etown.edu/offices/registration-records/academic-calendar-2026-27.aspx"

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT - Decide which scraper to run
    def start_requests(self):
        """
        SCRAPE_MODE can be set in settings.py
        Options: course, directory, calendar, all (default)
        """

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Run everything by default
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # Utility function to clean text
    def clean(self, text):
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        API pagination loop to fetch all course pages
        """

        base_url = "https://class-search.kctcsweb.com/api/search?college=elizabethtown&term=4256&page={}"

        # Loop through all pages (1 to 51)
        for page in range(1, 52):
            url = base_url.format(page)
            self.logger.info(f"Fetching course page: {url}")
            yield scrapy.Request(url=url, callback=self.course_details)

    def course_details(self, response):
        """
        Parse JSON API course response
        """

        data = json.loads(response.text).get("results", {}).get("data", {})

        # Loop through subjects and courses
        for subject, courses in data.items():
            for item in courses:

                subject = item.get("subject", "")
                catalog_number = item.get("catalog_number", "")
                title = item.get("title", "")
                course_name = f"{subject}-{catalog_number} {title}"

                course_number = item.get("number", "")
                section = item.get("section", "")
                instructor = item.get("instructor", "")

                start_date = item.get("starts_on", "")
                end_date = item.get("ends_on", "")
                course_dates = f"{start_date} to {end_date}" if start_date and end_date else ""

                location = item.get("building_description", "")
                enrolled = item.get("enrolled", "")
                max_enrolled = item.get("max_enrollment", "")
                enrollment = f"{enrolled} of {max_enrolled}"

                # Store course record
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": self.clean(course_name),
                    "Course Description": "",
                    "Class Number": course_number,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": enrollment,
                    "Course Dates": course_dates,
                    "Location": location,
                    "Textbook/Course Materials": ""
                })

        # Save after each API batch
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Parse faculty/staff directory table
        """

        rows = response.xpath('//table[@id="S1_grdDir"]/tbody/tr')

        for row in rows:
            cols = row.xpath('./td/text()').getall()

            # Extract columns safely
            dept_email = self.clean(cols[2]) if len(cols) > 2 else ""
            name = self.clean(cols[4]) if len(cols) > 4 else ""
            title = self.clean(cols[5]) if len(cols) > 5 else ""
            phone = self.clean(cols[6]) if len(cols) > 6 else ""
            dept = self.clean(cols[8]) if len(cols) > 8 else ""

            # Append department to title
            if dept:
                title = f"{title} | {dept}"

            # Store directory record
            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": dept_email,
                "Phone Number": phone
            })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Extract academic calendar term dates
        """

        academic_year = "2026 - 2027"
        terms = response.xpath("//h2[.//strong]")

        for term in terms:
            term_title = self.clean(" ".join(term.xpath(".//strong//text()").getall()))

            # Only required terms
            if term_title not in ["Fall Semester", "Winter Term", "Spring Term"]:
                continue

            term_name = f"{academic_year} {term_title}"
            table_rows = term.xpath("following::table[1]//tbody/tr")

            current_month = ""

            for row in table_rows:
                month = self.clean(" ".join(row.xpath("./td[1]//text()").getall()))
                day = self.clean(" ".join(row.xpath("./td[2]//text()").getall()))
                desc = self.clean(" ".join(row.xpath("./td[4]//text()").getall()))

                # If month cell empty, reuse previous month
                if month:
                    current_month = month
                else:
                    month = current_month

                if not day or not desc:
                    continue

                term_date = f"{month} {day}"

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": desc
                })

        self.logger.info(f"TOTAL CALENDAR ROWS: {len(self.calendar_rows)}")

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")
