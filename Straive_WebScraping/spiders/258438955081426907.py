import re
import scrapy
import pandas as pd
from inline_requests import inline_requests
from ..utils import save_df   # assuming save_df is your custom util


class RhodesSpider(scrapy.Spider):
    name = "rhodes"

    # Unique institution ID (fixed for this client)
    institution_id = 258438955081426907

    # Target URLs
    course_url = "https://catalog.rhodes.edu/courses"
    directory_url = "https://sites.rhodes.edu/academic-affairs/department-chairs-program-chairs"
    calendar_url = "https://catalog.rhodes.edu/general-information/academic-calendar"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT - MODE BASED SCRAPING
    def start_requests(self):
        """
        SCRAPE_MODE in settings:
        course | directory | calendar | all (default)
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar, dont_filter=True)

        # Combined modes
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar, dont_filter=True)

        # Default → scrape everything
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar, dont_filter=True)

    # CLEAN HELPER FUNCTION
    def clean(self, value):
        return (value or "").strip()

    # COURSE SCRAPER
    @inline_requests
    def parse_course(self, response):
        """
        Scrapes all course listings and visits each course page
        to extract description.
        """

        courses = response.xpath('//div[@class="field-content"]')

        for c in courses:
            # Extract course title text
            name_parts = c.xpath(".//a//text()").getall()
            name = " ".join([x.strip() for x in name_parts if x.strip()])
            class_num = name.split(":")[0].strip()

            # Course detail page URL
            url = c.xpath(".//a[last()]/@href").get()
            url = response.urljoin(url)

            # Visit course page
            course_page = yield scrapy.Request(url=url, dont_filter=True)

            # Extract description text
            description = course_page.xpath(
                '//div[contains(@class,"course__body")]//p//text()'
            ).getall()
            description = re.sub(r"\s+", " ", " ".join(description))

            # Append data
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": course_page.url,
                "Course Name": name,
                "Course Description": description,
                "Class Number": class_num,
                "Section": "",
                "Instructor": "",
                "Enrollment": "",
                "Course Dates": "",
                "Location": "",
                "Textbook/Course Materials": "",
            })

        # Save course data
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER (Faculty / Chairs)
    def parse_directory(self, response):
        """
        Scrapes department chairs directory table.
        """

        rows = response.xpath('//table//tr[td and td[a[starts-with(@href,"mailto:")]]]')

        for row in rows:
            department = self.clean(row.xpath("./td[1]//text()").get())
            name = self.clean(row.xpath("./td[2]//a/text()").get())
            email = row.xpath("./td[2]//a/@href").get()

            if department and name and email:
                email = email.replace("mailto:", "").strip()

                self.directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name,
                    "Title": f"CHAIR, {department}",
                    "Email": email,
                    "Phone Number": "",
                })

        # Save directory data
        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # ACADEMIC CALENDAR SCRAPER
    @inline_requests
    def parse_calendar(self, response):
        """
        Scrapes academic calendar events for all terms.
        """

        calendar_links = response.xpath(
            '//nav[@id="book-navigation-52"]//a[contains(@href,"academic-calendar")]/@href'
        ).getall()

        for link in calendar_links[1:]:
            link = response.urljoin(link)
            calendar_page = yield scrapy.Request(url=link, dont_filter=True)

            rows = calendar_page.xpath('//div[@class="field book__body"]//tr')
            current_term = None

            for row in rows:
                # Detect term name row
                term = row.xpath("./td/strong/text()").get()
                if term:
                    current_term = term.strip()
                    continue

                # Event + date
                event = self.clean(row.xpath("./td[1]//text()").get())
                date = self.clean(row.xpath("./td[2]//text()").get())

                if current_term and event and date:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": calendar_page.url,
                        "Term Name": current_term,
                        "Term Date": date,
                        "Term Date Description": event,
                    })

        # Save calendar data
        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
