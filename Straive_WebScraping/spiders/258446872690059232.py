import re
import io
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector
from datetime import datetime



class JwccSpider(scrapy.Spider):

    """
    Scrapy spider for John Wood Community College (JWCC).

    This spider supports scraping:
    1. Course catalog (PDF-based)
    2. Employee directory (AJAX endpoint)
    3. Academic calendar (HTML pages with pagination)

    Scrape behavior is controlled by the SCRAPE_MODE setting.
    """

    name = "jwcc"

    # Unique institution identifier used by downstream pipelines
    institution_id = 258446872690059232

    # Accumulator for calendar rows across paginated pages
    calendar_rows = []

    # COURSE DATA
    # Course catalog PDF (2025–2026)
    course_url = "https://www.jwcc.edu/wp-content/uploads/2025/11/2025-26-Course-Catalog.pdf"
    
    # DIRECTORY DATA 
    # AJAX endpoint powering the JWCC employee directory
    directory_api_url = (
        "https://www.jwcc.edu/wp-admin/admin-ajax.php"
    )

    # Headers required for successful AJAX request
    directory_headers = {
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'origin': 'https://www.jwcc.edu',
        'referer': 'https://www.jwcc.edu/directory/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }
    
    # POST payload required by the directory endpoint
    directory_payload = {'first-name': '',
        'last-name': '',
        'search-job': '',
        'department': '',
        'action': 'fetch_employees'}
    
    # Academic calendar page URL
    calendar_url = 'https://www.jwcc.edu/events/category/academic/page/5/?eventDisplay=past'
    
    def start_requests(self):
        """
        Entry point for the spider.

        The SCRAPE_MODE setting determines which sections are executed.
        Supported values:
            - course
            - directory
            - calendar
            - any combination (course_directory, course_calendar, etc.)
            - default: all three
        """

        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            
        elif mode == 'directory':
           self.parse_directory()
           
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()


        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            self.parse_directory()
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

    # Parse methods UNCHANGED from your original
    def parse_course(self, response):
        """
        Parse course data from the JWCC course catalog PDF.

        PDF pages 140–221 contain course descriptions.
        Extracted fields:
            - Course Name
            - Course Description
            - Class Number

        Output is normalized to IC-CMS schema.

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Course Name"                   : str
        - "Course Description"            : str
        - "Class Number"                  : str
        - "Section"                       : str
        - "Instructor"                    : str
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        course_rows = []

        # Convert Scrapy response bytes to file-like object
        pdf_bytes = io.BytesIO(response.body)

        with pdfplumber.open(pdf_bytes) as pdf:
            all_text = []
 
            # Pages 140–221 (pdfplumber is 0-indexed)
            for page_num in range(139, 221): 
                page = pdf.pages[page_num]
                text = page.extract_text()
                if text:
                    all_text.append(text)

        # Combine all pages into a single searchable string
        full_text = "\n".join(all_text)

        # TEXT NORMALIZATION

        # Remove standalone page numbers
        full_text = re.sub(r"\n\d+\n", "\n", full_text)

        # Fix hyphenated line breaks: "infor- mation" → "information"
        full_text = re.sub(r"-\s*\n\s*", "", full_text)

        # Normalize spacing but preserve logical line breaks
        full_text = re.sub(r"[ \t]+", " ", full_text)
        full_text = re.sub(r"\n{2,}", "\n", full_text)

        # COURSE REGEX
        """
        Regex captures:
            subject      -> Department code (e.g., ENG, BIO)
            number       -> Course number (e.g., 101)
            title        -> Course title
            credits      -> Credit hours
            description  -> Full paragraph description
        """

        course_pattern = re.compile(
            r"""
            (?P<subject>[A-Z]{2,4})\s+
            (?P<number>\d{3})\s+
            (?P<title>[A-Za-z0-9&(),’'\/\- ]+?)
            (?:\s*\.{3,}\s*|\s+)
            (?P<credits>(?:variable\s*)?\d+(?:-\d+)?)
            \s*cr\.?\s*hrs\.?
            \s*
            (?P<description>.*?)
            (?=
                \n[A-Z]{2,4}\s+\d{3}\s+
                |
                \Z
            )
            """,
            re.DOTALL | re.VERBOSE
        )

        #PARSE COURSES
        for match in course_pattern.finditer(full_text):

            subject = match.group("subject")
            number = match.group("number")
            title = match.group("title").strip()
            description = match.group("description").strip()

            course_rows.append(
                {
                    "Cengage Master Institution ID": 258425407433369553,
                    "Source URL": self.course_url,
                    "Course Name": f"{subject} {number} {title}",
                    "Course Description": description,
                    "Class Number": f"{subject} {number}",
                    "Section": "",
                    "Instructor": "",
                    "Enrollment": "",
                    "Course Dates": "",
                    "Location": "",
                    "Textbook/Course Materials": "",
                }
            )

            #SAVE OUTPUT CSV FILE
            course_df = pd.DataFrame(course_rows)
            save_df(course_df, self.institution_id, "course")
            
    def parse_directory(self):
        """
            Parse the JWCC employee directory.

            Data source:
                - AJAX endpoint exposed via WordPress admin-ajax.php
                - Returns HTML fragments inside a JSON response

            Output schema:
                - "Cengage Master Institution ID" : int
                - "Source URL"                    : str
                - "Name"                          : str
                - "Title"                         : str
                - "Email"                         : str
                - "Phone Number"                  : str

            IMPORTANT:
                This method uses the `requests` library instead of Scrapy's
                request/response cycle. As a result:
                    - Scrapy middlewares are bypassed
                    - Retries, proxies, throttling are NOT applied
                    - Errors must be handled manually if needed
            """
        # Perform POST request to directory AJAX endpoint
        res = requests.post(self.directory_api_url, headers=self.directory_headers, data=self.directory_payload)

        # Parse JSON response
        data = json.loads(res.text)

        # HTML snippet containing employee cards
        html = data.get("html", "")

        # Create a Parsel Selector from raw HTML
        response = Selector(text=html)

        # Select all employee blocks
        blocks = response.xpath('//div[@class="employee-card"]')
        
        rows = []

        for block in blocks:
            # NAME
            name = block.xpath('.//h2/text()').get().strip()

            #TITLE
            # Extract all visible text nodes under title container
            title = block.xpath('.//div[@class="employee-card-left"]/div//text()[normalize-space()]').getall()

            # Normalize whitespace and deduplicate titles
            title = [
                x for i, x in enumerate(dict.fromkeys(" ".join(t.split()) for t in title if t.strip()))
                if not any(x != y and x in y for y in dict.fromkeys(" ".join(t.split()) for t in title if t.strip()))
            ]
            title = ' , '.join(t.replace('\n', '').strip() for t in title)

            # PHONE
            phone = block.xpath('.//a[@class="phone-url"]/text()').get('')

            # ---------------- STORE ROW ----------------
            rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": "https://www.jwcc.edu/directory/",
                        "Name": name,
                        "Title": title,
                        "Email": '',
                        "Phone Number": phone,
                    }
                )

        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
            Parse JWCC academic calendar events.

            Source:
                - Paginated HTML event listing pages

            Extracted fields:
                - Term Name
                - Term Date
                - Term Date Description

            Behavior:
                - Accumulates rows across pagination
                - Saves data only after final page
            """

        # TERM NAME EXTRACTION
        # Extract mobile datepicker text (may include ranges)
        raw_name = response.xpath('//span[@class="tribe-events-c-top-bar__datepicker-mobile"]/text()').getall()
        raw_name = " - ".join([n.strip() for n in raw_name if n.strip()])

        # Normalize term name formats
        if raw_name.lower() == "upcoming":
            formatted_name = "Upcoming"
        elif raw_name.lower().startswith("now"):
            parts = raw_name.split(" - ", 1)
            if len(parts) == 2:
                dt = datetime.strptime(parts[1], "%Y-%m-%d")
                formatted_name = f"Now - {dt.strftime('%B')} {dt.day}, {dt.year}"
            else:
                formatted_name = "Now"
        elif " - " in raw_name and raw_name[:4].isdigit():
            start, end = raw_name.split(" - ", 1)
            d1 = datetime.strptime(start, "%Y-%m-%d")
            d2 = datetime.strptime(end, "%Y-%m-%d")
            formatted_name = (
                f"{d1.strftime('%B')} {d1.day}, {d1.year} - "
                f"{d2.strftime('%B')} {d2.day}, {d2.year}"
            )
        elif raw_name[:4].isdigit():
            d = datetime.strptime(raw_name, "%Y-%m-%d")
            formatted_name = f"{d.strftime('%B')} {d.day}, {d.year}"
        else:
            formatted_name = raw_name

        # EVENT BLOCKS
        blocks = response.xpath('//ul[@class="tribe-events-calendar-list"]/li')
        prev_month = ""

        for block in blocks:
            # Month label appears intermittently
            month = block.xpath('.//time[contains(@class,"month-separator-text")]/text()').get('').strip()

            if month:
                prev_month = month
            else:
                month = prev_month

            day = block.xpath('.//span[contains(@class,"event-date-tag-daynum")]/text()').get('').strip()

            desc = block.xpath('.//a/text()').get('').strip()
            if not desc:
                continue

            #DATE NORMALIZATION
            term_date_raw = f"{day} {month}".strip()

            try:
                dt = datetime.strptime(term_date_raw, "%d %B %Y")
                term_date = f"{dt.strftime('%B')} {dt.day} {dt.year}"
            except ValueError:
                try:
                    dt = datetime.strptime(term_date_raw, "%B %Y")
                    term_date = f"{dt.strftime('%B')} {dt.year}"
                except ValueError:
                    term_date = term_date_raw

            # STORE EVENT
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": formatted_name,
                "Term Date": term_date,
                "Term Date Description": desc,
            })

        #  PAGINATION
        next_page = response.xpath('//a[contains(.,"Next")]/@href').get()
        if next_page:
            yield scrapy.Request(next_page, callback=self.parse_calendar)
        else:
            # Save only once after last page
            calendar_df = pd.DataFrame(self.calendar_rows)
            save_df(calendar_df, self.institution_id, "calendar")





 