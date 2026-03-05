import json                      
import scrapy                       
import PyPDF2                     
import pandas as pd                
from ..utils import *               
from io import BytesIO             
from parsel import Selector         
from playwright.sync_api import sync_playwright  


class BristolccSpider(scrapy.Spider):
    """
    Scrapy spider to scrape:
    Course data, Faculty directory, and Academic calendar
    """

    name = "bristolcc"  # Spider name

    institution_id = 258417159162587097  # Unique institution ID

    course_url = 'https://webapp.bristolcc.edu/rpc/coursesearch'  # Course API URL
    directory_url = 'https://bristolcc.edu/faculty/'             # Faculty directory URL
    calendar_url = "https://bristolcc.edu/learnatbristol/academicresources/academiccalendar.html"  # Calendar URL

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.directory_rows = []  # Stores faculty and staff data
        self.calendar_rows = []   # Stores academic calendar data
        self.course_rows = []     # Stores course data

    def start_requests(self):
        """
        Controls which scraper runs based on SCRAPE_MODE
        """

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')  # Read scrape mode

        if mode == 'course':
            # Payload to fetch course list
            formdata = {
                "request": json.dumps({
                    "func": "CourseSearch.getClasses",
                    "arg": {
                        "term": "202601",
                        "ptrm": "",
                        "type": "CR"
                    }
                })
            }

            headers = {  # Headers required for AJAX API call
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                "Origin": "https://webapp.bristolcc.edu",
                "Referer": "https://webapp.bristolcc.edu/coursesearch/",
                "User-Agent": "Mozilla/5.0",
                "X-Requested-With": "XMLHttpRequest",
            }

            yield scrapy.FormRequest(
                url=self.course_url,
                formdata=formdata,
                headers=headers,
                callback=self.parse_course
            )

        elif mode == 'directory':
            yield scrapy.Request(
                url=self.directory_url,
                callback=self.parse_directory,
                dont_filter=True
            )

        elif mode == 'calendar':
            yield scrapy.Request(
                url=self.calendar_url,
                callback=self.parse_calendar
            )

        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    def parse_course(self, response):
        """
        Parses course list response and filters online courses
        """

        data = response.json()

        for block in data.get("result", []):
            crn = block.get("crn", "")
            location = block.get("camp_desc", "")

            if location == 'Online':  # Only online courses are captured
                instructor = f"{block.get('instr_last_name', '')},{block.get('instr_first_name', '')}".strip()
                enrollment = f"{block.get('seats_avail', '')}/{block.get('max_enrl', '')}"

                course_item = {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": "https://webapp.bristolcc.edu/coursesearch/",
                    "Course Name": f"{block.get('subj_code','')} {block.get('crse_numb','')} {block.get('crse_title','')}".strip(),
                    "Course Description": "",
                    "Class Number": crn,
                    "Section": block.get("section", ""),
                    "Instructor": instructor,
                    "Enrollment": enrollment,
                    "Course Dates": f"{block.get('start_date','')} - {block.get('end_date','')}",
                    "Location": location,
                    "Textbook/Course Materials": (
                        "https://www.bkstr.com/bristolccstore/follett-discover-view/"
                        "booklook?shopBy=discoverViewCourseRefId"
                        "&bookstoreId=988"
                        f"&courseRefId={crn}"
                        "&termId=202601"
                    ),
                }

                self.course_rows.append(course_item)  # Store course record

                formdata = {  # Payload to fetch course description
                    "request": json.dumps({
                        "func": "CourseSearch.getClassInfo",
                        "arg": {
                            "term": "202601",
                            "crn": crn
                        }
                    })
                }

                headers = {
                    "Accept": "application/json, text/javascript, */*; q=0.01",
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "Origin": "https://webapp.bristolcc.edu",
                    "Referer": "https://webapp.bristolcc.edu/coursesearch/",
                    "User-Agent": "Mozilla/5.0",
                    "X-Requested-With": "XMLHttpRequest",
                }

                yield scrapy.FormRequest(
                    url=self.course_url,
                    formdata=formdata,
                    headers=headers,
                    callback=self.parse_course_details,
                    meta={"crn": crn}
                )

    def parse_course_details(self, response):
        """
        Extracts course description and updates the course record
        """

        data = response.json()
        crn = response.meta["crn"]
        description = ""

        for item in data.get("result", []):  # Find narrative description
            if item.get("type") == "NARRATIVE":
                description = item.get("text", "").strip()
                break

        for row in self.course_rows:  # Update matching course
            if row["Class Number"] == crn:
                row["Course Description"] = description
                break

        course_df = pd.DataFrame(self.course_rows)  # Save course data
        save_df(course_df, self.institution_id, "course")

    def parse_directory(self, response):
        """
        Scrapes faculty directory using Playwright pagination
        """

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context()
            page = context.new_page()

            page.goto(self.directory_url, wait_until="domcontentloaded", timeout=60000)
            page.wait_for_selector('//select[@name="directory-list_length"]')
            page.select_option('//select[@name="directory-list_length"]', value='100')
            page.wait_for_timeout(3000)

            while True:
                html = page.content()
                response = Selector(text=html)

                rows = response.xpath('//table[@id="directory-list"]//tbody/tr')

                for lis in rows:  # Extract faculty details
                    name1 = lis.xpath('./td[@headers="fname"]//strong/text() | ./td[@headers="fname"]//strong/a/text()').get(default='').strip()
                    name2 = lis.xpath('./td[@headers="lname"]//strong/a/text() | ./td[@headers="lname"]//strong/text()').get(default='').strip()
                    title = lis.xpath('./td[@headers="title"]//text()').get(default='').strip()
                    department = lis.xpath('./td[@headers="program"]//text()').get(default='').strip()

                    self.directory_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_url,
                        "Name": f"{name1} {name2}".strip(),
                        "Title": f'{title}, {department}',
                        "Email": lis.xpath('./td[@headers="email"]/a/text()').get(default='').strip(),
                        "Phone Number": lis.xpath('./td[@headers="phone"]/a/text()').get(default='').strip(),
                    })

                next_btn = page.locator('#directory-list_next')  # Pagination check
                if "disabled" in next_btn.get_attribute("class"):
                    break

                page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                page.wait_for_timeout(1000)
                next_btn.click()
                page.wait_for_timeout(3000)

            directory_df = pd.DataFrame(self.directory_rows)  # Save directory data
            save_df(directory_df, self.institution_id, "campus")
            browser.close()

    def parse_calendar(self, response):
        """
        Scrapes academic calendar data
        """

        cards = response.xpath('//div[@class="card-header"]')

        for card in cards:
            heading = card.xpath('.//button/text()').get()
            heading = heading.strip() if heading else ""

            tables = card.xpath('.//table[@class="table table-striped"]//tbody/tr')

            for table in tables:
                date = table.xpath('./td[1]//text()').get()
                description = table.xpath('./td[2]//text()').get()

                if not date or not description:
                    continue

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": heading,
                    "Term Date": date.strip(),
                    "Term Date Description": description.strip(),
                })

    def closed(self, reason):
        """
        Saves calendar data after spider completes
        """

        if not self.calendar_rows:
            self.logger.warning("No calendar data collected")
            return

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
        self.logger.info(f"Saved {len(calendar_df)} calendar rows")
