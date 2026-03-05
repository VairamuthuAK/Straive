import re
import html
import scrapy
import requests
import pandas as pd
from ..utils import *
from parsel import Selector
from playwright.sync_api import sync_playwright


class HawccSpider(scrapy.Spider):

    name = "hawcc"
    institution_id = 258437238700926931
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://ce.uhcc.hawaii.edu/search/publicCourseAdvancedSearch.do?method=load"
    
    # DIRECTORY CONFIG
    directory_source_url = "https://hawaii.hawaii.edu/about/directory/faculty-staff"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://hawaii.hawaii.edu/academic-calendar"
    calendar_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is extracted using Pdfplumber

        - Directory and Calendar data is available as static HTML pages and is scraped
        using normal Scrapy requests.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

    # PARSE COURSE
    def parse_course(self):
        """
        Parse course data using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Course Name"                   : str
        - "Course Description"            : str
        - "Class Number"                  : str
        - "Section"                       : str
        - "Instructor"                    : str
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0"
        })

        with sync_playwright() as p:

            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            url = self.course_url

            page.goto(url, wait_until="networkidle")

            # Open advanced search
            page.locator('//span[contains(text(),"Advanced Search Options")]').click()

            # Click all category links
            links = page.locator('//ul[@class="dynatree-container"]/li//a')

            count = links.count()

            for i in range(count):
                links = page.locator('//ul[@class="dynatree-container"]/li//a')
                links.nth(i).click()

            # Select "Search all courses"
            page.locator('//label[normalize-space()="Search all courses"]').click()

            # Submit search
            with page.expect_navigation():
                page.locator('//button[@type="submit"]').click()

            page.wait_for_load_state("networkidle")

            # ---------------- Helper Function ----------------
            def fetch_course(u):
                try:
                    full_url = f'https://ce.uhcc.hawaii.edu/{u}'
                    res = session.get(full_url, timeout=15)
                    sel = Selector(text=res.text)
                    name = sel.xpath('//h1/span[@class="title"]/text()').get('').strip()
                    class_num = sel.xpath('//h1/span[@class="courseCode"]/span/text()').get('').strip()
                    desc = re.sub(r'\s+',' ', html.unescape(re.sub(r'<!--.*?-->', '', ' '.join(sel.xpath('//h2[contains(text(),"Course Description")]/parent::div//text()').getall()), flags=re.S))).encode('latin1','ignore').decode('utf-8','ignore').replace('Course Description','').strip()
                    location = sel.xpath('//div[@class="orgUnitPublicName"]/text()').get('').strip()
                    name = f"{class_num} - {name}".strip()

                    if name != '-':
                        return {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": full_url,
                            "Course Name": re.sub(r'\s+',' ',name),
                            "Course Description": re.sub(r'\s+',' ',desc),
                            "Class Number": re.sub(r'\s+',' ',class_num),
                            "Section": '',
                            "Instructor": '',
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": re.sub(r'\s+',' ',location),
                            "Textbook/Course Materials": ""
                        }

                except Exception as e:
                    return None

            page_no = 1

            while True:

                response = Selector(text=page.content())
                urls = response.xpath('//span[@class="courseName"]/a/@href').getall()
                for u in urls:
                    row = fetch_course(u)

                    if row:
                        self.course_rows.append(row)

                next_btn = page.locator('//a[@title="Next page"]')

                if next_btn.count() == 0:
                    break

                with page.expect_navigation():
                    next_btn.click()

                page.wait_for_load_state("networkidle")

                page_no += 1

            browser.close()

    # PARSE DIRECTORY
    def parse_directory(self,response):
        """
        Parse directory using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        blocks = response.xpath('//p[@class="namecard"]')
        for block in blocks:
            title = block.xpath('.//strong/following-sibling::text()[1]').get('').strip()
            email = block.xpath('.//a/text()').get('').strip()
            name = ' '.join(block.xpath('.//strong/text()').getall()).strip()
            if name == 'Yoshida, Sara':
                title = ''
            if name == 'Grube, Talon':
                email = 'grube26@hawaii.edu'
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.directory_source_url,
            "Name": re.sub(r'\s+',' ',name),
            "Title": re.sub(r'\s+',' ',title),
            "Email": email,
            "Phone": block.xpath(
                './/text()'
            ).re_first(r'\(\d{3}\)\s*\d{3}-\d{4}') or '',
            })

    # PARSE CALENDAR
    def parse_calendar(self,response):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """  

        blocks = response.xpath('//tbody/tr')
        for block in blocks:
            description = ' '.join(block.xpath('.//td[2]//text()').getall()).strip()

            term_name = ''.join(block.xpath('.//parent::tbody/parent::table//parent::div/parent::div/parent::div//h4/a/text()').getall()).replace('Calendar','').strip()
            if term_name == '':
                term_name = ''.join(block.xpath('.//parent::tbody/parent::table/parent::figure/parent::div/parent::div/parent::div//h4/a/text()').getall()).replace('Calendar','').strip()
            if "Final Exam" in term_name:
                    continue
            
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_source_url,
                "Term Name": re.sub(r'\s+',' ',term_name),
                "Term Date": block.xpath('.//td[1]/text()').get('').split('(')[0].strip(),
                "Term Date Description": re.sub(r'\s+',' ',description),
            })

    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")
        