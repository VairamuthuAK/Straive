import time
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from parsel import Selector
from playwright.sync_api import sync_playwright


class CloudSpider(scrapy.Spider):
    
    name = "cloud"
    institution_id = 258425153359210453
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://icloud.cloud.edu/ICS/Portal_Homepage.jnz?portlet=Course_Schedules&screen=Advanced+Course+Search&screenType=next'

    # DIRECTORY CONFIG
    directory_source_url = "https://www.cloud.edu/academics/faculty/"
    directory_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.cloud.edu/Assets/pdfs/academics/schedules-and-calendars/2025-2026/25-26-Calendar.pdf"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is extracted using Playwright, as the course listing
        and detail pages are dynamically rendered and cannot be reliably
        accessed using standard Scrapy requests.

        - Directory data is available as static HTML pages and is scraped
        using normal Scrapy requests in the `parse_directory` callback.

        - Calendar data is provided as PDF files.
        These PDFs are downloaded using HTTP requests and parsed using
        the pdfplumber library to extract academic calendar information.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

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

        """
        Automates course data extraction from a dynamic course search portal.

        This logic uses browser automation to navigate a term-based course
        search interface, apply filters, and traverse paginated course results.
        For each available academic term, the workflow performs the following:

        - Selects valid academic terms based on predefined criteria
        - Submits search requests for undergraduate course listings
        - Iterates through paginated course result pages
        - Opens individual course detail pages
        - Extracts detailed course information including:
            - Course name and description
            - Class number and section
            - Instructor details
            - Course dates and location
            - Enrollment capacity and availability
        - Normalizes extracted values and stores them as structured
        course records

        This approach is required for content that is rendered dynamically
        and cannot be accessed through static HTTP responses alone.
        """

        # Launch Playwright and open a Chromium browser session
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False) # Launch browser in visible mode
            page = browser.new_page()
            page.set_default_timeout(60000) 
            page.goto(self.course_sourse_url)
            # Wait for the term dropdown to load
            page.wait_for_selector('select[name="pg0$V$ddlTerm"]')
            # Get all available terms from the dropdown
            terms = page.locator('select[name="pg0$V$ddlTerm"] option').all_text_contents()
            for term in terms:
                term_l = term.lower()
                if not ("2025" in term_l or ("2026" in term_l and ("spring" in term_l or "summer" in term_l))):
                    continue # Skip terms not in 2025 or 2026 spring/summer
                
                # Select the term in the dropdown
                page.select_option('select[name="pg0$V$ddlTerm"]', label=term)
                page.wait_for_load_state("networkidle")
                # Click the search button and wait for navigation
                with page.expect_navigation(wait_until="networkidle"):
                    page.click('//input[@value="Search"]')

                # Select undergraduate courses
                page.select_option('select[name="pg0$V$ddlDivision"]', value="UG")
                page.wait_for_load_state("networkidle")
                # Click search again to filter
                with page.expect_navigation(wait_until="networkidle"):
                    page.click('//input[@value="Search"]')

                # Loop through all course results pages
                while True:
                    page.wait_for_selector('//table//td/a') # Wait for course links to appear
                    course_links = page.locator('//table//td/a')
                    count = course_links.count()  # Number of courses on the page
                    # Loop through each course link
                    for page in range(count):
                        course_links = page.locator('//table//td/a')
                        link = course_links.nth(page)
                        time.sleep(2)
                        # Click the course link and wait for page load
                        with page.expect_navigation(wait_until="networkidle"):
                            link.click()

                        response = Selector(text=page.content())
                        course_name = response.xpath('//div[@class="col-xs-8"]/b/text()').get(default="").strip()
                        section = response.xpath('//span[contains(text(),"Section")]/following-sibling::span/text()').get(default="").strip()
                        instructor = ''.join(response.xpath('//span[contains(text(),"Instructor")]/parent::div/text()').getall()).strip()
                        course_dates = response.xpath('//td[@class="BorderLeftBottom"][2]/text()').get(default="").strip()
                        location = response.xpath('//td[@class="BorderLeftRightBottom"]/text()').get(default="").strip()
                        inside = course_name.rsplit("(", 1)[-1].replace(")", "").strip()
                        parts = inside.split()
                        class_num = " ".join(parts[:2])
                        section = " ".join(parts[2:]) 
                        cap = response.xpath('//table[@class="groupedGrid table-bordered"]/tbody/tr[2]/td[4]/text()').get('').strip()
                        rem = response.xpath('//table[@class="groupedGrid table-bordered"]/tbody/tr[2]/td[5]/text()').get('').strip()
                        enrollment = f'{rem} of {cap}'
                        description = response.xpath('//span[contains(@id,"CourseDescValue")]/text()').get(default="").strip()
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": page.url,
                            "Course Name": re.sub(r'\s+',' ', course_name),
                            "Course Description": re.sub(r'\s+',' ', description),
                            "Class Number": class_num,
                            "Section": section,
                            "Instructor": re.sub(r'\s+',' ', instructor),
                            "Enrollment": re.sub(r'\s+',' ', enrollment),
                            "Course Dates": re.sub(r'\s+',' ', course_dates),
                            "Location": re.sub(r'\s+',' ', location),
                            "Textbook/Course Materials": ""
                        })
                        # Click the back button to return to the results page
                        with page.expect_navigation(wait_until="networkidle"):
                            page.locator('//a[@id="pg0_V_lnkBack"]').click()
                        page.wait_for_selector('//table//td/a')

                    # Check for "Next page" link to paginate
                    next_page = page.locator('//a[contains(text(),"Next page")]')
                    if next_page.count() == 0:
                        break # No more pages, exit loop
                    
                    # Click next page and wait for navigation
                    with page.expect_navigation(wait_until="networkidle"):
                        next_page.first.click()
                    time.sleep(1)

            # Close the browser after all terms and courses are processed
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
        blocks = response.xpath('//li[@class="perc-person"]')
        for block in blocks:
            dept = block.xpath('.//div[@class="perc-person-dpt"]/text()').get('').strip()
            position = block.xpath('.//div[@class="perc-person-title"]/text()').get('').strip()
            if dept:
                title = f'{dept}, {position}'
            else:
                title = position

            self.directory_rows.append( {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_source_url,
                "Name": f'{block.xpath('.//span[@class="perc-person-first-name"]/text()').get('').strip()} {block.xpath('.//span[@class="perc-person-last-name"]/text()').get('').strip()}',
                "Title": re.sub(r'\s+',' ',title),
                "Email":"",
                "Phone Number": block.xpath('.//div[@class="perc-person-phone"]/text()').get('').strip(),
            })

    # PARSE CALENDAR
    def parse_calendar(self):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """     

        """
        Extracts academic calendar information from a PDF file.

        The calendar PDF is downloaded and parsed using pdfplumber. Text is
        extracted from a cropped region of each page where calendar data
        is displayed. The parser scans the content line by line to identify
        academic terms, dates, and their corresponding descriptions.

        The logic handles:
        - Detection of academic term headers (Fall, Winter, Spring, Summer)
        - Identification of month and day values in multiple formats
        - Inference of term names when explicit headers are not present
        - Skipping of weekday-only rows used for calendar layout
        - Association of multi-line descriptions with the correct calendar date

        Each valid calendar entry is normalized and stored as a structured
        record containing the term name, date, and description.
        """
        # Download the calendar PDF and load it into memory
        pdf_file = BytesIO(requests.get(self.calendar_source_url).content)
        # Define month abbreviations for date detection
        MONTHS = "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec"
        # Regex to detect dates like "Jan 15" or "Feb 3-5"
        date_pattern = re.compile(rf"\b({MONTHS})\s+(\d{{1,2}}(-\d{{1,2}})?)")
        # Regex to detect lines starting with just a day number
        leading_day = re.compile(r"^(\d{1,2})\s+(.*)")
        # Regex to detect weekday header rows (S M T W T F S)
        weekday_row = re.compile(r"^S\s*M\s*T\s*W\s*T\s*F\s*S$")
        # Recognized term headers in the calendar
        TERM_HEADERS = {
            "FALL SEMESTER",
            "WINTER TERM",
            "SPRING TERM",
            "SUMMER TERM"
        }
        # Function to infer term based on month if header not explicitly given
        def infer_term(month):
            if month in ["Aug", "Sep", "Oct", "Nov"]:
                return "FALL SEMESTER"
            if month in ["Dec", "Jan"]:
                return "WINTER TERM"
            if month in ["Feb", "Mar", "Apr", "May"]:
                return "SPRING TERM"
            return "SUMMER TERM"

        # Initialize current tracking variables
        current_date = None
        current_month = None
        current_term = None

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                # Crop the page to the column where calendar data appears
                cropped = page.crop((170, 0, 360, page.height - 120))
                text = cropped.extract_text()
                if not text:
                    continue # Skip empty pages

                # Split the text into lines, stripping whitespace and removing empty lines
                lines = [l.strip() for l in text.split("\n") if l.strip()]
                for line in lines:
                    # Skip lines that are weekday headers
                    if weekday_row.match(line):
                        continue

                    if line.upper() in TERM_HEADERS:
                        current_term = line.upper()
                        continue  # DO NOT ADD ROW

                    month_pattern = date_pattern.search(line)
                    if month_pattern:
                        month = month_pattern.group(1)
                        day = month_pattern.group(2)
                        current_date = f"{month} {day}"
                        current_month = month

                        # fallback term if header not yet seen
                        if not current_term:
                            current_term = infer_term(month)

                        desc = line[month_pattern.end():].strip()
                        if desc:
                            self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_source_url,
                            "Term Name": current_term,
                            "Term Date": current_date,
                            "Term Date Description": desc,
                        })
                        continue
                    
                    # Handle lines that start with a day number only
                    current_month_pattern = leading_day.match(line)
                    if current_month_pattern and current_month:
                        day = current_month_pattern.group(1)
                        desc = current_month_pattern.group(2).strip()
                        current_date = f"{current_month} {day}"
                        if desc:
                            self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_source_url,
                            "Term Name": current_term,
                            "Term Date": current_date,
                            "Term Date Description": desc,
                        })
                        continue
                    if current_date and re.search(r"[A-Za-z]", line):
                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_source_url,
                            "Term Name": current_term,
                            "Term Date": current_date,
                            "Term Date Description": desc,
                        })

    #Called automatically when the Scrapy spider finishes scraping.
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
        