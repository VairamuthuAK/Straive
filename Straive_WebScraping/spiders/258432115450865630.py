import re
import scrapy
import pandas as pd
from ..utils import save_df


class SouthsideSpider(scrapy.Spider):
    name = "south"

    # Unique institution ID
    institution_id = 258432115450865630

    # URLs
    course_url = "https://southside.edu/class-schedule/current-semester"
    directory_url = "https://southside.edu/directory"
    calendar_url = "https://southside.edu/academic-calendar"

    # INITIALIZATION
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT – SCRAPE MODE CONTROLLER
    def start_requests(self):
        """
        Controls scrape mode:
        course / directory / calendar / combinations / all
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            # Default → Scrape everything
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # COMMON CLEAN FUNCTION
    def clean(self, text):
        """Normalize whitespace."""
        return re.sub(r"\s+", " ", text).strip() if text else None

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Pagination handler for course listing.
        Loops through multiple pages of schedule.
        """

        # Maximum page assumption (safe upper bound)
        for page in range(0, 30):
            page_url = f"https://southside.edu/class-schedule/current-semester?page={page}"
            yield scrapy.Request(page_url, callback=self.parse_course_details)

    def parse_course_details(self, response):
        """
        Extract individual course rows from schedule table.
        """

        # Loop through each course block
        for course_block in response.xpath('//div[@class="schedule-wrapper"]'):

            course_name = self.clean(
                course_block.xpath('.//caption/h6/text()').get()
            )

            course_desc = self.clean(
                course_block.xpath('.//caption/div[@class="course_description"]/text()').get()
            )

            # Loop through each section row
            for row in course_block.xpath('.//tbody/tr[td[@headers]]'):

                class_number = self.clean(
                    row.xpath('.//td[contains(@class,"Class-Nbr")]/text()').get()
                )

                section = self.clean(
                    row.xpath('.//td[contains(@class,"Class-Section")]/text()').get()
                )

                instructor = self.clean(
                    row.xpath('.//td[contains(@class,"Name")]/text()').get()
                )

                dates = self.clean(
                    row.xpath('.//td[contains(@class,"End-Dt")]/text()').get()
                )

                location = self.clean(
                    row.xpath('.//td[contains(@class,"Location")]/text()').get()
                )

                textbook = row.xpath(
                    './/a[contains(@href,"booklookServlet")]/@href'
                ).get()

                # Append structured record
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": course_name,
                    "Course Description": course_desc,
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": None,  # Seat data not available in HTML
                    "Course Dates": dates,
                    "Location": location,
                    "Textbook/Course Materials": response.urljoin(textbook) if textbook else None,
                })

        # Save extracted course data
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Pagination handler for directory listing.
        """

        # Iterate through known directory pages
        for page in range(0, 16):
            page_url = f"https://southside.edu/directory?page={page}"
            yield scrapy.Request(page_url, callback=self.parse_employee)

    def decode_cf_email(self, encoded):
        """
        Decode Cloudflare protected email addresses.
        """
        r = int(encoded[:2], 16)
        return ''.join(
            chr(int(encoded[i:i+2], 16) ^ r)
            for i in range(2, len(encoded), 2)
        )

    def parse_employee(self, response):
        """
        Extract individual employee details.
        """

        for person in response.xpath('//div[@class="row ml-0 mr-0"]'):

            name = self.clean(
                person.xpath('.//div[@class="faculty-staff-name"]//text()').get()
            )

            position = self.clean(
                person.xpath('.//div[@class="faculty-position"]//text()').get()
            )

            department = self.clean(
                person.xpath('.//div[@class="faculty-department"]//text()').get()
            )

            phone = self.clean(
                person.xpath('.//div[@class="phone-numbers"]//text()').get()
            )

            # Decode protected email
            email_hex = person.xpath(
                './/span[@class="__cf_email__"]/@data-cfemail'
            ).get()

            email = self.decode_cf_email(email_hex) if email_hex else None

            title = f"{position} | {department}" if position or department else None

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        # Save directory data
        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Extract academic calendar term-wise data.
        """

        # Loop through each term tab
        for term in response.xpath("//div[contains(@class,'tab-pane')]"):

            term_name = self.clean(
                term.xpath(".//h2/span/text()").get()
            )

            # Loop through each section inside term
            for section in term.xpath(
                ".//article[contains(@class,'node--type-academic-calendar-section')]"
            ):

                # Loop through each calendar row
                for row in section.xpath(".//tbody/tr"):

                    date = self.clean(
                        row.xpath(".//td[1]//div/text()").get()
                    )

                    title = self.clean(
                        row.xpath(".//td[2]//div/text()").get()
                    )

                    if not date or not title:
                        continue

                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": date,
                        "Term Date Description": title,
                    })

        # Save calendar data
        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")