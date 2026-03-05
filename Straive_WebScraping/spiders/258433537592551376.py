import re
import scrapy
import pandas as pd
from urllib.parse import quote
from ..utils import save_df


class RegistrarSpider(scrapy.Spider):
    name = "registrar"

    # Unique institution identifier
    institution_id = 258433537592551376

    # URLs
    course_url = "https://registrar.washu.edu/classes-registration/class-schedule-search/"
    directory_url = ""   # Add directory URL if required
    calendar_url = "https://registrar.washu.edu/calendars-exams/academic-calendar/"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT – Select scraping mode from settings
    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

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

    
    # Utility: Clean text
    
    def clean(self, text):
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    
    # COURSE SCRAPING
    
    def parse_course(self, response):

        terms = [t.strip() for t in response.xpath("//select[@id='termselect']/option/@value").getall() if t.strip()]
        schools = [s.strip() for s in response.xpath("//select[@id='schoolselect']/option/@value").getall() if s.strip()]

        for term in terms:
            for school in schools:

                payload = {
                    "term": term,
                    "school": school,
                    "department": "",
                    "level": "",
                    "instructor": "",
                    "mode": "",
                    "courses_search": "",
                    "paged": "1",
                    "pagination-submit": ""
                }

                yield scrapy.FormRequest(
                    url=self.course_url,
                    method="POST",
                    formdata=payload,
                    callback=self.parse_classes
                )

    def parse_classes(self, response):

        for row in response.xpath("//div[contains(@class,'scpi__classes--row')]"):

            title = self.clean(row.xpath(".//div[contains(@class,'wide')]/text()").get())
            code = self.clean(row.xpath(".//div[contains(@class,'middle')]/text()").get())

            desc_list = row.xpath(".//div[contains(@class,'scpi-class__details--content')]//text()").getall()
            description = self.clean(" ".join(desc_list))

            # Loop each section inside course
            for section_block in row.xpath(".//div[contains(@class,'scpi-class__data')]"):

                section = self.clean(section_block.xpath(".//div[contains(@class,'data-section')]//div/text()").get())
                instructor = self.clean(section_block.xpath(".//div[contains(@class,'data-instructor')]//div/text()").get())
                seats = self.clean(section_block.xpath(".//div[contains(@class,'data-seats')]//div/text()").get())

                # Skip invalid sections
                if not section:
                    continue

                # Clean section format (extract numeric part if needed)
                if not re.fullmatch(r"[A-Z]|\d{1,2}", section):
                    match = re.match(r"(\d{2})", section)
                    section = match.group(1) if match else None

                if not section:
                    continue

                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": f"{code} {title}",
                    "Course Description": description,
                    "Class Number": code,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": seats,
                    "Course Dates": "",
                    "Location": "",
                    "Textbook/Course Materials": ""
                })

        # Save once per response
        if self.course_rows:
            save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    
    # DIRECTORY SCRAPING
    
    def parse_directory(self, response):

        for row in response.xpath("//table[@id='tel-directory']//tbody//tr"):

            email = row.xpath("./@onClick | ./@onclick").re_first(
                r"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"
            )

            if not email:
                continue

            ajax_url = f"https://www.roswell.enmu.edu/emp-dir-ajax-handler.php?email={quote(email)}"

            yield scrapy.Request(
                ajax_url,
                callback=self.parse_employee,
                meta={"source_url": response.url}
            )

    def parse_employee(self, response):

        data = response.json()

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.meta.get("source_url"),
            "Name": f"{data.get('firstname')} {data.get('lastname')}",
            "Title": self.clean(f"{data.get('department')} | {data.get('position')}"),
            "Email": data.get("emailaddress"),
            "Phone Number": data.get("telephonenumber"),
        })

        if self.directory_rows:
            save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    
    # CALENDAR SCRAPING
    
    def parse_calendar(self, response):

        values = [
            v.strip() for v in
            response.xpath('//select[@name="calendar"]//option/@value').getall()
            if v.strip() and not v.startswith("2024")
        ]

        for cal in sorted(values):
            url = f"{self.calendar_url}?calendar={quote(cal)}"
            yield scrapy.Request(url, callback=self.parse_calendar_details)

    def parse_calendar_details(self, response):

        current_term = None

        nodes = response.xpath(
            "//div[@class='academic-calendar__term'] | "
            "//div[contains(@class,'academic-calendar__row')]"
        )

        for node in nodes:

            class_name = node.attrib.get("class", "")

            # Capture term header
            if "academic-calendar__term" in class_name:
                current_term = self.clean(node.xpath("text()").get())
                continue

            term_date = self.clean(node.xpath(".//div[@class='academic-calendar__date']/text()").get())

            description = node.xpath(
                ".//div[@class='academic-calendar__title']//div[contains(@class,'tooltip-a')]/text()"
            ).get()

            if not description:
                description = node.xpath(
                    ".//div[@class='academic-calendar__title']/text()"
                ).get()

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": current_term,
                "Term Date": term_date,
                "Term Date Description": self.clean(description)
            })

        if self.calendar_rows:
            save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")