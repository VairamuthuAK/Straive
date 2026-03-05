import re
import json
import scrapy
import pandas as pd
from urllib.parse import quote
from ..utils import save_df


class NorthwestSpider(scrapy.Spider):
    """
    Spider to scrape:
    1. Course schedules
    2. Faculty / staff directory
    3. Academic calendar
    """

    name = "northwest"

    # Unique Institution Identifier
    institution_id = 258450867898836950

    # URLs
    course_url = "https://area10.nwc.edu/nwcforms/Syllabi/GetScheduleDownload?term=25/SP"
    directory_url = (
        "https://nwc.edu/_resources/dmc/php/faculty.php"
        "?datasource=faculty&xpath=items/item&returntype=json"
    )
    calendar_url = (
        "https://api.calendar.moderncampus.net/pubcalendar/"
        "b13d7430-4fd2-48c8-ba66-158a73becf19/events"
        "?start=2026-01-01&end=2026-09-29"
    )

    def __init__(self, *args, **kwargs):
        """Initialize storage containers"""
        super().__init__(*args, **kwargs)
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    def start_requests(self):
        """
        Entry point.
        Decides which dataset to scrape based on SCRAPE_MODE setting.
        """
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode in ["course", "all"]:
            yield scrapy.Request(
                url=self.course_url,
                callback=self.parse_course,
                dont_filter=True,
            )

        if mode in ["directory", "all"]:
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True,
            )

        if mode in ["calendar", "all"]:
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar,
            )

    def parse_course(self, response):
        """
        Parse course schedule JSON data.
        Extracts course title, instructor, enrollment,
        dates, location, and syllabus PDF link.
        """
        data = json.loads(response.text)

        for item in data:
            # Construct course title
            short_title = item.get("SEC_SHORT_TITLE", "")
            full_title = item.get("SEC_NAME", "")
            base_title = full_title.rsplit("-", 1)[0]
            title = f"{base_title} - {short_title}"

            description = item.get("CRS_DESC") or ""

            # Extract meeting date and location from string
            meeting_info = item.get("SEC_MEETING_INFO", "")
            date_match = re.search(
                r"\d{2}/\d{2}/\d{4}-\d{2}/\d{2}/\d{4}", meeting_info
            )
            location_match = re.search(
                r",\s*([^,]+,\s*Room\s*[A-Za-z0-9]+)", meeting_info
            )

            enrollment = f"{item.get('ACTIVE_COUNT', '')}/{item.get('SEC_CAPACITY', '')}"

            # PDF syllabus link
            pdf_id = item.get("SEC_SYNONYM", "")
            pdf_link = f"https://area10.nwc.edu/syllabi/pdf/all/{pdf_id}.pdf"

            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": "https://area10.nwc.edu/nwcforms/syllabi/?term=25/SP",
                "Course Name": re.sub(r"\s+", " ", title),
                "Course Description": re.sub(r"\s+", " ", description),
                "Class Number": item.get("COURSE_SECTIONS_ID", ""),
                "Section": item.get("SEC_NO", ""),
                "Instructor": item.get("FAC1", ""),
                "Enrollment": enrollment,
                "Course Dates": date_match.group(0) if date_match else "",
                "Location": location_match.group(1) if location_match else "",
                "Textbook/Course Materials": pdf_link,
            })

        # Save course data
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self, response):
        """
        Parse faculty and staff directory JSON.
        Extracts name, title, email, phone, and profile URL.
        """
        data = json.loads(response.text)

        for row in data:
            name = f"{row.get('FIRST', '').strip()} {row.get('LAST', '').strip()}".strip()

            phone = (
                row.get("OFFICE_PHONE")
                or row.get("CELL_PHONE")
                or ""
            ).strip()

            profile_link = row.get("link", "")
            source_url = f"https://www.nwc.edu{profile_link}" if profile_link else ""

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": source_url,
                "Name": name,
                "Title": row.get("TITLE", "").strip(),
                "Email": row.get("EMAIL", "").strip(),
                "Phone Number": phone,
            })

        # Save directory data
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parse academic calendar events.
        Extracts term name, date, and description.
        """
        data = json.loads(response.text)

        for row in data:
            date = row.get("startDate") or row.get("startDatetime", "").split("T")[0]

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": "https://nwc.edu/events/index.html",
                "Term Name": row.get("categoryName", ""),
                "Term Date": date,
                "Term Date Description": row.get("title", ""),
            })

        # Save calendar data
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
