import time
import scrapy
import requests
import pandas as pd
import cloudscraper
from ..utils import *
from ..utils import save_df
from parsel import Selector
from urllib.parse import urljoin

class LynnSpider(scrapy.Spider):
    """
    Spider for Lynn University to extract Courses, Campus Directory, and Academic Calendars.
    
    Attributes:
        institution_id (int): Unique identifier for the institution.
        course_url (str): Entry point for course catalog.
        directory_url (str): Entry point for staff/faculty directory.
        calendar_url (str): URL for academic dates and deadlines.
    """
    name = "lynn"
    institution_id = 258457375256635349

    # Storage for extracted data
    calendar_rows = []
    course_rows = []
    campus_row = []

    course_url = "https://www.lynn.edu/academics/catalog/courses/"
    directory_url = "https://www.lynn.edu/campus-directory/people"
    calendar_url = 'https://www.lynn.edu/academics/academic-calendar'
    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
           self.parse_course()
        elif mode == 'directory':
           self.parse_directory()
        elif mode == 'calendar':
            self.parse_calendar()
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            self.parse_directory()

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            self.parse_calendar()

        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_calendar()
            self.parse_directory()
        
        # All three (default)
        else:
            self.parse_course()
            self.parse_directory()
            self.parse_calendar()
       

    # Parse methods UNCHANGED from your original
    def parse_course(self):
        """
        Parse course data using Scrapy response and SSCC PDF.
        Outputs:
            - Cengage Master Institution ID
            - Source URL
            - Course Name
            - Course Description
            - Class Number
            - Section
            - Instructor
            - Enrollment
            - Course Dates
            - Location
            - Textbook/Course Materials
        """
        """
        Scrapes course details including descriptions and locations.
        Uses cloudscraper to bypass potential Cloudflare challenges on catalog pages.
        """
        scraper = cloudscraper.create_scraper()
        MAX_RETRIES = 3   # number of retries per page

        # Iterating through 74 pages of course listings
        for i in range(1, 75):
            url = f"https://www.lynn.edu/academics/catalog/courses/p{i}"
            for attempt in range(1, MAX_RETRIES + 1):

                # Throttling to avoid rate limiting
                time.sleep(4)
                response = scraper.get(url)
                sel = Selector(text=response.text)

                # Extracting individual course links from the table
                blocks = sel.xpath('//tr[@class="lynn-richtext"]/td/a/@href').getall()
                if blocks:
                    print(f"Found {len(blocks)} links")
                    for block in blocks:
                        product_res = scraper.get(block)
                        product_response = Selector(text=product_res.text)

                        # Data Extraction logic
                        class_number = product_response.xpath('//span[@class="lynn-heading-pretitle"]/text()').get(default="").strip()
                        title = product_response.xpath('//span[@class="lynn-type--2xl"]/text()').get(default="").strip()
                        course_name = f"{class_number} {title}".strip()
                        course_description = product_response.xpath('//div[@class="lynn-richtext"]/text()').get(default="").strip()
                        location = product_response.xpath('//div[@class="lynn-table-wrapper--scroll"]//td[4]/text()').get(default="").strip()

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": block,
                            "Course Name": course_name,
                            "Course Description": course_description,
                            "Class Number": class_number,
                            "Section": "",
                            "Instructor": "",
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": location,
                            "Textbook/Course Materials": ""
                        })

                    break   # stop retry loop if success

                else:
                    print(f"Retry {attempt}/{MAX_RETRIES} - No blocks found")
                    time.sleep(2)

        # Exporting collected data
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "campus")
         
    def parse_directory(self):
        """
        Parse directory using requests.

        Output columns:
        - Cengage Master Institution ID
        - Source URL
        - Name
        - Title
        - Email
        - Phone Number
        """

        BASE_URL = "https://www.lynn.edu"
        LIST_URL = "https://www.lynn.edu/campus-directory/people/p{}"

        MAX_RETRIES = 3
        RETRY_DELAY = 3

        # requests-impersonate helps bypass TLS fingerprintin
        session = requests.Session(impersonate="chrome120")

    
        def decode_cloudflare_email(encoded_string):
            """De-obfuscates Cloudflare email protection via XOR logic."""
            if not encoded_string:
                return ""
            key = int(encoded_string[:2], 16)
            email = ""
            for i in range(2, len(encoded_string), 2):
                char_code = int(encoded_string[i:i+2], 16) ^ key
                email += chr(char_code)
            return email

        def get_with_retry(url):
            """Wrapper for session.get with built-in retry logic."""
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    r = session.get(url, timeout=30)
                    if r.status_code == 200:
                        return r
                    else:
                        print(f"Attempt {attempt} | Status {r.status_code} -> {url}")
                except Exception as e:
                    print(f"Attempt {attempt} | Error: {e} -> {url}")

                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)

            return None

        # Iterate through directory pages
        for i in range(1, 29):
            list_url = LIST_URL.format(i)
            print(f"\nFetching list page: {list_url}")

            response = get_with_retry(list_url)
            if not response:
                continue

            selector = Selector(text=response.text)
            profile_links = selector.xpath('//table//tbody//tr//td[1]/a/@href').getall()
            for link in profile_links:
                profile_url = urljoin(BASE_URL, link)
                res = get_with_retry(profile_url)
                if not res:
                    continue

                sel = Selector(text=res.text)

                # Scraping individual profile fields
                name = sel.xpath('//h1/text()').get(default='').strip()
                title = sel.xpath('(//p/text())[1]').get(default='').strip()

                encoded = sel.xpath("//span[@class='__cf_email__']/@data-cfemail").get()
                email = decode_cloudflare_email(encoded)
                phone = sel.xpath('//main[@id="page-maincontent"]//a[starts-with(@href,"tel:")]/@href').get(default="")
                phone = phone.replace("tel:", "")

                #Append dictionary
                self.campus_row.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone
                })

        directory_df = pd.DataFrame(campus_row)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self):
  
        session = requests.Session(impersonate="chrome120")
        response = session.get(self.calendar_url, timeout=30)
        sel = Selector(text=response.text)

        # Loop all tab panels (day ug, online, day grad)
        panels = sel.xpath('//div[@role="tabpanel"]')
        for panel in panels:
            term_name = panel.xpath('./p[@class="mb-7"]/text()').get('')

            # Each date block
            blocks = panel.xpath('.//div[@class="font-bold text-blue"]')
            for block in blocks:
                month = block.xpath('preceding::h2[1]/text()').get('')
                term_dates = block.xpath('.//text()').getall()
                term_dates = " ".join(t.strip() for t in term_dates if t.strip())
                term_date = f"{month} {term_dates}"

                descriptions = block.xpath('following-sibling::p/text()').getall()
                term_description = " ".join(d.strip() for d in descriptions)

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,  
                    "Term Date": term_date,
                    "Term Date Description": term_description,
                })

        calendar_df = pd.DataFrame(self.calendar_rows)
        # Persist calendar data
        save_df(calendar_df, self.institution_id, "calendar")


 