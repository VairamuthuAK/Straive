import re
import scrapy
import pandas as pd
from io import BytesIO
from datetime import datetime
import PyPDF2

from ..utils import save_df

class BelhavenSpider(scrapy.Spider):
    """
    Spider to scrape:
    1. Course Catalogue
    2. Faculty/Staff Directory
    3. Academic Calendar (PDF)
    """
    name = "belhaven"

    # Unique Institution ID
    institution_id = 258450314712082386

    # URLs
    course_url = "https://catalogue.belhaven.edu/content.php?catoid=1&navoid=36&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D=1"
    directory_url = "https://www.belhaven.edu/belhaven/staff-dir.asp"
    calendar_url = "https://www.belhaven.edu/pdfs/registrar/ago-2025-2026-academic-calendar.pdf"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ==========================================================
    # ENTRY POINT
    # ==========================================================
    def start_requests(self):
        """
        Controls which datasets to scrape based on SCRAPE_MODE setting.
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode in ["course", "course_directory", "course_calendar", "all"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        if mode in ["directory", "course_directory", "directory_calendar", "all"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        if mode in ["calendar", "course_calendar", "directory_calendar", "all"]:
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar, dont_filter=True)

    # ==========================================================
    # COURSE SCRAPING
    # ==========================================================
    def parse_course(self, response):
        """
        Iterates through all pagination pages (1–18)
        and sends requests to extract course links.
        """
        for page in range(1, 19):
            url = (
                "https://catalogue.belhaven.edu/content.php?"
                f"catoid=1&navoid=36&filter%5Bitem_type%5D=3&"
                f"filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D={page}"
            )
            yield scrapy.Request(url=url, callback=self.parse_course_links)

    def parse_course_links(self, response):
        """
        Extracts individual course detail page links.
        """
        links = response.xpath("//td[@class='width']//a/@href").getall()

        for link in links:
            yield scrapy.Request(
                url=response.urljoin(link),
                callback=self.parse_course_details
            )

    def parse_course_details(self, response):
        """
        Extracts course name, class number, and description.
        """

        name = response.xpath(
            "normalize-space(//h1[@id='course_preview_title'])"
        ).get("")

        name = name.replace("\xa0", " ").strip()
        classnum = name.split("-")[0].strip() if "-" in name else ""

        desc_list = response.xpath(
            "//h1[@id='course_preview_title']/following-sibling::text()"
        ).getall()

        description = " ".join(
            d.strip() for d in desc_list if d.strip() and d.strip() != "UG"
        )

        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": name,
            "Course Description": description,
            "Class Number": classnum,
            "Section": "",
            "Instructor": "",
            "Enrollment": "",
            "Course Dates": "",
            "Location": "",
            "Textbook/Course Materials": "",
        })

        # Save after each append (can be optimized to save in closed())
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    # ==========================================================
    # DIRECTORY SCRAPING
    # ==========================================================
    def parse_directory(self, response):
        """
        Extracts faculty/staff directory table data.
        """

        rows = response.xpath("//table[@id='myTable']//tr[td]")

        for row in rows:
            first_name = row.xpath("./td[2]/text()").get("")
            last_name = row.xpath("./td[@class='lastName']/text()").get("")
            department = row.xpath("./td[3]/text()").get("")
            title = row.xpath("./td[4]/text()").get("")
            phone = row.xpath("./td[5]/a/text()").get("")
            ext = row.xpath("./td[5]/text()").get("").strip()

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": f"{first_name} {last_name}".strip(),
                "Title": f"{title}, {department}".strip(", "),
                "Email": "",
                "Phone Number": f"{phone} {ext}".strip()
            })

        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    # ==========================================================
    # CALENDAR (PDF) SCRAPING
    # ==========================================================
    def parse_calendar(self, response):
        """
        Extracts academic calendar data from PDF.
        """

        reader = PyPDF2.PdfReader(BytesIO(response.body))
        full_text = ""

        for page in reader.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n"

        # Extract term (Spring 2026, Fall 2025, etc.)
        term_match = re.search(
            r"(Spring|Summer|Fall|Winter)\s+20\d{2}",
            full_text,
            re.IGNORECASE
        )
        term = term_match.group(0).title() if term_match else ""

        date_pattern = re.compile(
            r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\s+"
            r"(\d{1,2}(?:\s*&\s*\d{1,2})?)\s+(.*)",
            re.IGNORECASE
        )

        for line in full_text.split("\n"):
            line = line.strip()
            match = date_pattern.match(line)

            if not match:
                continue

            month, day, description = match.groups()

            term_date = f"{month} {day}"

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": term,
                "Term Date": term_date,
                "Term Date Description": re.sub(r"\s+", " ", description)
            })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")