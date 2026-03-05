import re
import math
import scrapy
import pandas as pd
from ..utils import save_df


class StmartinSpider(scrapy.Spider):
    name = "stmart"

    # Unique institution ID
    institution_id = 258423193264482266

    # URLs
    course_url = "https://selfservice.stmartin.edu/SelfService/Search/sectionsearch.aspx"
    directory_url = "https://www.stmartin.edu/directory/faculty-staff-directory"
    calendar_url = "https://www.stmartin.edu/academics/academic-calendar-catalog/academic-calendar"

    # ---------------- INIT ----------------
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.directory_rows = []
        self.calendar_rows = []
        self.course_rows = []

    # ---------------- START REQUESTS ----------------
    def start_requests(self):
        """
        Select scraping mode from settings:
        course / directory / calendar / all / combinations
        """

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        # Single modes
        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Combined modes
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # Default → scrape everything
        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # ---------------- UTILITY CLEANER ----------------
    def clean(self, text):
        """Normalize whitespace"""
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    #  COURSE SCRAPER 
    def parse_course(self, response):
        """
        Extract academic periods and trigger course pagination
        """

        periods = response.xpath('//select[@id="ctl00_mainContentZone_ucCriteria_ddlbPeriods"]/option/@value').getall()

        for period in periods[1:]:
            year, term = period.split("|")

            # Skip old years
            if year == "2024":
                continue

            url = f"{self.course_url}?sort=CourseId&year={year}&term={term}&num=100"
            yield scrapy.Request(url, callback=self.parse_course_profile, cb_kwargs={"year": year, "term": term})

    def parse_course_profile(self, response, year, term):
        """
        Handle pagination for course listing
        """

        total_count = response.xpath('//p[@class="searchBreadCrumb"]/strong/text()').get("0")
        total = int(total_count)
        per_page = 100
        total_pages = math.ceil(total / per_page)

        for page in range(1, total_pages):
            start = page * per_page
            url = f"{self.course_url}?sort=CourseId&year={year}&term={term}&num=100&start={start}"
            yield scrapy.Request(url, callback=self.parse_course_links)

    def parse_course_links(self, response):
        """
        Extract course detail page links
        """

        links = response.xpath("//a[contains(@href,'sectiondetailsdialog.aspx')]/@href").getall()
        for link in links:
            yield scrapy.Request(response.urljoin(link), callback=self.parse_course_details)

    def parse_course_details(self, response):
        """
        Extract detailed course data
        """

        # -------- Course Name --------
        raw_name = response.xpath("normalize-space(//span[@class='leveloneheader'])").get("")
        course_name = self.clean(re.sub(r'/[A-Za-z\s]+/\w+\s*-\s*', ' - ', raw_name))

        # -------- Class Number & Section --------
        if "/Course/" in raw_name:
            class_num, rest = raw_name.split("/Course/", 1)
            section_id = rest.split(" - ")[0]
        else:
            parts = raw_name.split(" - ")
            class_num = parts[0].split("/")[0]
            section_id = parts[0].split("/")[-1]

        # -------- Description --------
        description = response.xpath(
            '//table[@id="ctl00_mainContent_ucSectionDetail_HeaderFormView"]//td'
        ).xpath("normalize-space(string())").get("")

        # Remove header junk
        description = re.sub(r'^\d{4}\s+\w+\s+\w+\s+\S+.*?Credits\s+\d+\.\d+\s*', '', description)
        description = re.sub(r'Students\s*\|\s*Credits.*?\d+\.\d+\s*', '', description).strip()

        # -------- Instructor --------
        instructor = self.clean(response.xpath(
            "//span[@id='ctl00_mainContent_ucSectionDetail_lblInstructors']/parent::td/following-sibling::td//text()"
        ).get())

        # -------- Course Dates --------
        course_dates = self.clean(response.xpath(
            "//span[@id='ctl00_mainContent_ucSectionDetail_lblDuration']/parent::td/following-sibling::td/text()"
        ).get())

        # -------- Enrollment --------
        enroll_parts = response.xpath(
            "//span[@id='ctl00_mainContent_ucSectionDetail_lblClass']/parent::td/following-sibling::td//text()"
        ).getall()
        enrollment = self.clean(" ".join(enroll_parts))

        nums = re.findall(r"\d+", enrollment)
        if len(nums) >= 2:
            enrollment = f"{nums[1]}/{nums[0]}"

        # -------- Save Row --------
        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": course_name,
            "Course Description": description,
            "Class Number": class_num,
            "Section": section_id,
            "Instructor": instructor,
            "Enrollment": enrollment,
            "Course Dates": course_dates,
            "Location": "",
            "Textbook/Course Materials": "",
        })

        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    #  DIRECTORY SCRAPER 
    def parse_directory(self, response):
        """
        Extract faculty profile links + pagination
        """

        profiles = response.xpath('//article//h3[@class="results__heading"]/a/@href').getall()
        for url in profiles:
            yield scrapy.Request(response.urljoin(url), callback=self.parse_directory_details)

        # Pagination
        next_page = response.xpath('//li[@class="pager__item pager__item--next"]/a/@href').get()
        if next_page:
            yield scrapy.Request(response.urljoin(next_page), callback=self.parse_directory)

    def parse_directory_details(self, response):
        """
        Extract faculty profile details
        """

        name = self.clean(response.xpath("//h1/span/text()").get())
        title = self.clean(response.xpath('//div[@class="profile__cards--info-card"]//h2/text()').get())
        email = self.clean(response.xpath('//a[starts-with(@href,"mailto:")]/text()').get())
        phone = self.clean(response.xpath('//a[starts-with(@href,"tel:")]/text()').get())

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    #  CALENDAR SCRAPER 
    def parse_calendar(self, response):
        """
        Extract academic calendar events
        """

        terms = response.xpath('//div[starts-with(@id,"fall-") or starts-with(@id,"spring-") or starts-with(@id,"summer-")]')

        for term in terms:
            term_name = self.clean(term.xpath('.//span[@class="accordion__button-text"]/text()').get())
            rows = term.xpath(".//table//tbody/tr")

            for row in rows:
                event = self.clean(" ".join(row.xpath("./td[1]//text()").getall()))
                date = self.clean(" ".join(row.xpath("./td[2]//text()").getall()))

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": date,
                    "Term Date Description": event,
                })

        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
