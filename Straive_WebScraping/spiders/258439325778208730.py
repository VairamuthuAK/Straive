import re
import time
import scrapy
import pandas as pd
from ..utils import *
from parsel import Selector
from playwright.sync_api import sync_playwright


class SticcSpider(scrapy.Spider):

    name = "sticc"
    institution_id = 258439325778208730
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://applications.stlcc.edu/ClassSchedule/Term_Courses.asp"
    course_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://applications.stlcc.edu',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = "https://ssb.stlcc.edu:4444/BannerExtensibility/customPage/page/STLCCEmployeeDirectory"

    # CALENDAR CONFIG
    calendar_source_url = "https://stlcc.edu/office-of-the-registrar/academic-calendar.aspx"
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

        - Course and Calendar data is extracted using Scrapy

        - Directory data is extracted using playwright.

        """

        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            # self.parse_course()
        elif mode == "directory":
            self.parse_directory()

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            self.parse_directory()

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            self.parse_directory()
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

    # PARSE COURSE
    def parse_course(self,response):
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

        terms = response.xpath('//select[@name="lstTerms"]/option')
        for term in terms:
            term_name = term.xpath('.//text()').get('').split('*')[-1].replace(' ', '%20').strip()
            if term_name == 'Pick%20One':
                continue
            term_value = term.xpath('.//@value').get('').strip()
            locations = ['SCC','2','3','4','5','6']
            for loc in locations:
                course_url = "https://applications.stlcc.edu/ClassSchedule/Course_Info.asp"

                course_payload = f'Location_ANY=All%20Locations&Location_5=Florissant%20Valley&Location_4=Forest%20Park&Location_6=Meramec&Location_2=Online&Location_SCC=South%20County%20Educ%20Ctr&Location_3=Wildwood&lstTerms={term_value}&campus={loc}&hidDisplay=ALL&lstBeginHour=00&lstBeginMin=00&lstEndHour=00&lstEndMin=00&lstSessionTypes=All&lstSchedTypes=All&lstCharacteristics=All&lstInstructor=ANY&lstLocation=All&lstPartofTerm=All&hidTermDescrip={term_name}%20&hidTerm={term_value}&txtCampus1={loc}&txtOnline='
                course_headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                'Cache-Control': 'max-age=0',
                'Connection': 'keep-alive',
                'Content-Type': 'application/x-www-form-urlencoded',
                'Origin': 'https://applications.stlcc.edu',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
                yield scrapy.Request(url = course_url,method='POST',body=course_payload,headers = course_headers, callback=self.parse_course_final, dont_filter=True)

    def parse_course_final(self,response):
        checkboxes = response.xpath(
            "//input[@type='checkbox'] | "
            "//span[@title='Closed. No enrollment.']//parent::div/parent::td | "
            "//span[@title='Restricted enrollment.']//parent::div/parent::td"
        )
        for cb in checkboxes:
            # Main row
            main_row = cb.xpath("./ancestor::tr[1]")

            # ---------- Basic Info ----------

            class_number = main_row.xpath(
                ".//span[@class='colvalue3']/text()"
            ).get("").strip()

            course_code = main_row.xpath(
                ".//td[contains(@class,'colvalue2')]/text()"
            ).get("").strip()

            title = main_row.xpath(
                ".//b/text()"
            ).get("").strip()

            # Example: ART 109 501 → ART 109 Drawing I
            parts = course_code.split()

            if len(parts) >= 2:
                course_name = f"{parts[0]} {parts[1]} {title}"
            else:
                course_name = f"{course_code} {title}"

            # Section = last part (501)
            section = parts[-1] if parts else ""

            # ---------- Instructor / Enrollment ----------

            info_row = main_row.xpath("following-sibling::tr[1]")

            instructor = info_row.xpath(
                ".//span[@class='colvalue4']/text()"
            ).get("").strip()

            capacity = info_row.xpath(
                ".//td[@class='colvalue5'][1]/text() |"
                ".//td[@class='colvalue6'][1]/text()"
            ).get("").strip()

            available = info_row.xpath(
                ".//span[@class='colvalue5']/text() |"
                ".//span[@class='colvalue6']/text()"
            ).get("").strip()
            if available and capacity:
                
                available = int(capacity) - int(available)
                enrollment = f"{str(available)} of {capacity}"
            else:
                enrollment = ''

            # ---------- Schedule Rows ----------

            next_rows = info_row.xpath("following-sibling::tr")

            for row in next_rows:

                # Stop when next course starts
                if row.xpath("./td/div/input[@type='checkbox']"):
                    break

                # Stop at "Combined with"
                if row.xpath(".//text()[contains(.,'Combined')]"):
                    break

                # Date (mandatory)
                course_date = row.xpath(
                    ".//span[contains(., '-') and contains(., '/')]/text()"
                ).get("").strip()

                if not course_date:
                    continue

                # Time
                time_ = row.xpath(
                    ".//td[contains(@class,'colvalue4') and contains(.,'AM')]/text()"
                ).get("").strip()

                # Location
                location = row.xpath(
                    ".//div[@class='colvalue4']/text()"
                ).get("").strip()

                # Combine
                course_date = course_date.strip()

                # Save
                if location:
                    self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": re.sub(r'\s+',' ',course_name),
                        "Course Description": "",
                        "Class Number": re.sub(r'\s+',' ',class_number),
                        "Section": section,
                        "Instructor": re.sub(r'\s+',' ',instructor),
                        "Enrollment": enrollment,
                        "Course Dates": re.sub(r'\s+',' ',course_date),
                        "Location": re.sub(r'\s+',' ',location),
                        "Textbook/Course Materials": ""
                    })
    
    # PARSE DIRECTORY
    def parse_directory(self):
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
        url = self.directory_source_url


        with sync_playwright() as p:

            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            page.goto(url, timeout=60000)
            page.wait_for_load_state("networkidle")

            page_no = 1

            while True:

                # Parse page
                response = Selector(text=page.content())
                blocks = response.xpath('//div[@class="card-body"]')

                for block in blocks:

                    name = block.xpath('.//h3/text()').get('').strip()
                    dep = block.xpath('.//h6/text()').get('').strip()
                    tit = block.xpath('.//p[@class="card-text card-department"]/text()').get('').strip()

                    if dep and tit:
                        title = f"{dep}, {tit}"
                    elif dep:
                        title = dep
                    elif tit:
                        title = tit
                    else:
                        title = ""

                    phone = block.xpath(
                        './/i[@class="fa-solid fa-phone card-text-icons"]/parent::p/text()'
                    ).get('').replace('Not Available', '').strip()

                    email = block.xpath(
                        './/span[@ng-bind-html="item.EMAIL"]/a/text()'
                    ).get('').replace('Not Available', '').strip()
                    if name:
                        self.directory_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": url,
                            "Name": re.sub(r'\s+', ' ', name),
                            "Title": re.sub(r'\s+', ' ', title),
                            "Email": email,
                            "Phone Number": phone
                        })

                next_btn = page.locator('//button[@title="Next Page"]')

                # Stop if no more pages
                if next_btn.count() == 0 or next_btn.is_disabled():
                    break

                # Click next page
                next_btn.click()
                time.sleep(3)
                page.wait_for_load_state("networkidle")

                page_no += 1

            browser.close()
            
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
            description = ' '.join(block.xpath('.//td[1]//text()').getall()).strip()
            term_name = block.xpath('.//parent::tbody/parent::table/parent::div/parent::div//a/text()').get('').strip()
            term_date = block.xpath('.//td[5]//text()').get('').strip()
            if term_date == '':
                term_date = 'Jan. 20'
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_source_url,
                "Term Name": re.sub(r'\s+',' ',term_name),
                "Term Date": re.sub(r'\s+',' ',term_date),
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
        