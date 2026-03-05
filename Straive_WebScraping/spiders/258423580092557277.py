import re
import time
import random
import string
import requests
import scrapy
import pandas as pd
from parsel import Selector
from playwright.sync_api import sync_playwright
from ..utils import save_df


class CochiseSpider(scrapy.Spider):
    name = "cochis"

    # Unique Institution ID
    institution_id = 258423580092557277

    # URLs
    course_url = "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/term/termSelection?mode=search"
    directory_url = "https://www.cochise.edu/about/directory/faculty-directory.html"
    calendar_url = "https://cochise.smartcatalogiq.com/en/2025-2026/2025-2026-catalog/academic-calendar"

    # Initialize storage lists
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.course_rows = []
        self.directory_rows = []
        self.calendar_rows = []

    # Entry point – controls scrape mode
    def start_requests(self):

        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        if mode == "course":
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        else:
            yield scrapy.Request(self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

    # Utility – Clean text
    def clean(self, text):
        if not text:
            return ""
        return re.sub(r"\s+", " ", text).strip()

    # COURSE SCRAPER (Playwright + Banner API)
    def parse_course(self, response):

        PAGE_SIZE = 50

        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            try:
                # Open term selection page
                page.goto(self.course_url, wait_until="domcontentloaded", timeout=60000)
                time.sleep(3)

                # Open term dropdown
                page.locator('//span[@class="select2-arrow"]').click()
                time.sleep(2)

                terms = page.locator('//ul[@class="select2-results"]//li/div[@class="select2-result-label"]/div')

                # Loop through each term
                for i in range(terms.count()):

                    try:
                        terms.nth(i).click()
                        time.sleep(1)

                        term_code = page.locator('//input[@id="txt_term"]').get_attribute("value")

                        page.locator('//button[@id="term-go"]').click()
                        time.sleep(2)

                        page.locator('//a[@id="advanced-search-link"]').click()
                        time.sleep(2)

                        # Select campus
                        page.locator('//div[@id="select2-result-label-9"]//div').click()
                        time.sleep(2)

                        page.locator('//button[@id="search-go"]').click()
                        time.sleep(3)

                        # Extract session cookies
                        cookies = context.cookies()
                        cookies_dict = {c["name"]: c["value"] for c in cookies}

                        unique_session_id = ''.join(random.choices(string.ascii_lowercase, k=5)) + str(int(time.time() * 1000))

                        page_offset = 0
                        total_count = None

                        # Pagination Loop – Banner API
                        while True:

                            api_url = (
                                "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/searchResults?"
                                f"txt_campus=2&txt_term={term_code}"
                                f"&uniqueSessionId={unique_session_id}"
                                f"&pageOffset={page_offset}&pageMaxSize=187"
                                f"&sortColumn=subjectDescription&sortDirection=asc"
                            )

                            api_response = requests.get(api_url, cookies=cookies_dict, timeout=30)
                            api_json = api_response.json()

                            if total_count is None:
                                total_count = api_json.get("totalCount", 0)

                            datas = api_json.get("data", [])

                            # Loop courses inside API response
                            for data in datas:

                                term = data.get("term", "")
                                crn = data.get("courseReferenceNumber", "")

                                # ----- Course Description -----
                                desc = ""
                                try:
                                    desc_url = "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/getCourseDescription"
                                    payload = f"term={term}&courseReferenceNumber={crn}&first=first"

                                    desc_resp = requests.post(desc_url, data=payload, cookies=cookies_dict, timeout=30)
                                    desc_sel = Selector(text=desc_resp.text)

                                    desc = " ".join(
                                        desc_sel.xpath('//section[@aria-labelledby="courseDescription"]/text()').getall()
                                    )

                                except Exception:
                                    pass

                                # ----- Meeting & Instructor -----
                                instructor = ""
                                course_dates = ""

                                try:
                                    meet_url = (
                                        "https://ssb.cochise.edu/StudentRegistrationSsb/ssb/searchResults/getFacultyMeetingTimes"
                                        f"?term={term}&courseReferenceNumber={crn}"
                                    )

                                    meet_resp = requests.get(meet_url, cookies=cookies_dict, timeout=30)
                                    meet_json = meet_resp.json()

                                    for m in meet_json.get("fmt", []):
                                        faculty = m.get("faculty", [])
                                        instructor = ", ".join(f.get("displayName", "") for f in faculty)

                                        mt = m.get("meetingTime", {})
                                        course_dates = f"{mt.get('startDate','')} - {mt.get('endDate','')}"

                                except Exception:
                                    pass

                                # ----- Store Course Row -----
                                self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": self.course_url,
                                    "Course Name": f"{data.get('subject','')} {data.get('courseDisplay','')} {data.get('courseTitle','')}".strip(),
                                    "Course Description": self.clean(desc),
                                    "Class Number": crn,
                                    "Section": data.get("sequenceNumber", ""),
                                    "Instructor": instructor,
                                    "Enrollment": f"{data.get('enrollment','')} / {data.get('maximumEnrollment','')}",
                                    "Course Dates": course_dates,
                                    "Location": data.get("campusDescription", ""),
                                    "Textbook/Course Materials": "https://www.cochise.edu/bookstore",
                                })

                            page_offset += PAGE_SIZE
                            if page_offset >= total_count:
                                break

                        # Return to term selection
                        page.goto(self.course_url, wait_until="domcontentloaded")

                    except Exception:
                        continue

            finally:
                browser.close()

        # Save final course dataframe
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

    # DIRECTORY SCRAPER
    def parse_directory(self, response):

        for row in response.xpath("//div[@class='regular-text desc']//table//tr"):

            name = self.clean(row.xpath("./td[1]//a/text()").get())
            email = row.xpath("./td[1]//a/@href").get()
            title = self.clean(row.xpath("./td[2]//p/text()").get())

            if email:
                email = email.replace("mailto:", "").strip()

            if not name and not title:
                continue

            self.directory_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": None
            })

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")

    # CALENDAR SCRAPER
    def parse_calendar(self, response):

        for term_block in response.xpath("//h2"):

            term_name = self.clean(term_block.xpath("normalize-space(.)").get())
            table = term_block.xpath("following-sibling::table[1]")

            for row in table.xpath(".//tr"):

                title = self.clean(" ".join(row.xpath("./td[1]//text()").getall()))
                date = self.clean(" ".join(row.xpath("./td[2]//text()").getall()))

                if not title or not date:
                    continue

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": date,
                    "Term Date Description": title,
                })

        calendar_df = pd.DataFrame(self.calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")