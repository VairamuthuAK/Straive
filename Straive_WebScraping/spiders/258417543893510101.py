import scrapy
import re
import pandas as pd
from ..utils import save_df


class StmartinSpider(scrapy.Spider):
    name = "stmartin"

    # Unique institution ID
    institution_id = 258417543893510101

    # URLs
    course_url = "https://selfservice.stmartin.edu/SelfService/Search/SectionSearch.aspx?sort=CourseId&year=2026&term=SPR&num=100"
    directory_url = "https://www.stmartin.edu/directory/faculty-staff-directory"
    calendar_url = "https://www.stmartin.edu/academics/academic-calendar-catalog/academic-calendar"


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Storage containers
        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # ENTRY POINT – SCRAPE MODE CONTROLLER
    def start_requests(self):
        """
        Controls scraping mode:
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

    # COURSE SCRAPER
    def parse_course(self, response):
        """
        Step 1:
        Extract all department course detail links from listing page.
        """

        # Extract all section detail links
        department_links = response.xpath(
            '//a[contains(@href,"sectiondetailsdialog.aspx")]/@href'
        ).getall()

        # Loop through each course detail page
        for link in department_links:
            yield response.follow(link, callback=self.parse_department_courses)

        # Pagination Logic (Execute only on first page)
        if "start=" not in response.url:

            total_text = response.xpath(
                '//p[@class="searchBreadCrumb"]//strong/text()'
            ).get("")

            try:
                total_records = int(total_text)
            except:
                total_records = 0

            num = 100  # records per page

            # Generate pagination URLs
            for start in range(num, total_records, num):
                next_page = (
                    "https://selfservice.stmartin.edu/SelfService/Search/"
                    f"SectionSearch.aspx?sort=CourseId&year=2026&term=SPR&num=100&start={start}"
                )
                yield scrapy.Request(next_page, callback=self.parse_course)

    def parse_department_courses(self, response):
        """
        Extract individual course detail information.
        """

        # Course Header Parsing
        raw = response.xpath(
            'normalize-space(//table[@id="ctl00_mainContent_ucSectionDetail_HeaderFormView"]//span[@class="leveloneheader"])'
        ).get("").replace("\xa0", "")

        if " - " not in raw:
            return

        left_part, course_title = raw.split(" - ", 1)

        # Extract class number & section
        classnum, _, section = left_part.split("/")

        title = f"{classnum} - {course_title}"

        # Description Extraction
        texts = response.xpath(
            "//table[@id='ctl00_mainContent_ucSectionDetail_HeaderFormView']//td/text()"
        ).getall()

        texts = [t.strip() for t in texts if t.strip()]
        description = " ".join(texts[2:])

        # Instructor
        instructor = response.xpath(
            "//span[@id='ctl00_mainContent_ucSectionDetail_lblInstructors']"
            "/parent::td/following-sibling::td/text()[normalize-space()]"
        ).get("")

        # Course Dates
        course_dates = response.xpath(
            "//span[@id='ctl00_mainContent_ucSectionDetail_lblDuration']"
            "/parent::td/following-sibling::td/text()"
        ).get("")

        # Enrollment Logic
        enroll_text = " ".join(
            response.xpath(
                "//span[@id='ctl00_mainContent_ucSectionDetail_lblClass']"
                "/parent::td/following-sibling::td/text()[normalize-space()]"
            ).getall()
        )

        match = re.search(r"(\d+)\s*\|\s*(\d+)", enroll_text)

        if match:
            seats = int(match.group(1))
            available = int(match.group(2))
            enrollment = f"{available} of {seats} Available"
        else:
            enrollment = None

        # Append Final Structured Row
        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": re.sub(r"\s+", " ", title),
            "Course Description": re.sub(r"\s+", " ", description),
            "Class Number": classnum,
            "Section": section,
            "Instructor": instructor.strip(),
            "Enrollment": enrollment,
            "Course Dates": course_dates.strip(),
            "Location": "",
            "Textbook/Course Materials": "",
        })

        # Save after processing
        df = pd.DataFrame(self.course_rows)
        save_df(df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):
        """
        Extract profile links from faculty/staff directory listing page.
        """

        profile_links = response.xpath(
            '//h3[@class="results__heading"]/a/@href'
        ).getall()

        # Loop through each profile
        for link in profile_links:
            yield response.follow(link, callback=self.parse_directory_details)

        # Pagination Handling
        last_page_href = response.xpath(
            '//li[contains(@class,"pager__item--last")]/a/@href'
        ).get()

        if last_page_href:
            total_pages = int(last_page_href.split("=")[-1]) + 1

            for page in range(total_pages):
                next_page = f"{self.directory_url}?page={page}"
                yield scrapy.Request(next_page, callback=self.parse_directory)

    def parse_directory_details(self, response):
        """
        Extract detailed faculty/staff profile information.
        """

        name = response.xpath('//h1/span/text()').get("").strip()

        titles = response.xpath(
            '//div[contains(@class,"profile__cards--info-card")]'
            '//h2[@class="h5"]/text()'
        ).getall()

        titles = [t.strip() for t in titles]
        final_title = " | ".join(titles)

        department = response.xpath(
            "//h2[normalize-space()='School or Department']"
            "/following-sibling::ul[1]/li/a/text()"
        ).getall()

        department = " | ".join(department).strip()

        title = f"{final_title} | {department}" if department else final_title

        email = response.xpath(
            "//ul[@class='contact-set']//a[starts-with(@href,'mailto:')]/text()"
        ).get("").strip()

        phone = response.xpath(
            "//ul[@class='contact-set']//a[starts-with(@href,'tel:')]/text()"
        ).get("").strip()

        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": re.sub(r"\s+", " ", name),
            "Title": re.sub(r"\s+", " ", title),
            "Email": email,
            "Phone Number": phone,
        })

        df = pd.DataFrame(self.directory_rows)
        save_df(df, self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):
        """
        Extract Academic Calendar data from accordion sections.
        """

        # Loop through each term accordion block
        for term_block in response.xpath("//div[contains(@class,'accordion')]"):

            term_name = term_block.xpath(
                ".//span[@class='accordion__button-text']/text()"
            ).get()

            if not term_name:
                term_name = term_block.xpath("./@id").get("")

            term_name = term_name.replace("-", " ").title()

            # Loop through tables inside each term
            for table in term_block.xpath(".//table"):
                for row in table.xpath(".//tbody/tr"):

                    event = " ".join(row.xpath("./td[1]//text()").getall()).strip()
                    term_date = " ".join(row.xpath("./td[2]//text()").getall()).strip()

                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": term_date,
                        "Term Date Description": event,
                    })

        df = pd.DataFrame(self.calendar_rows)
        save_df(df, self.institution_id, "calendar")