import io
import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector


class SheridanSpiderMixed(scrapy.Spider):
    """
    Scrapy spider for scraping Course, Directory, and Calendar data
    from Sheridan College (https://www.sheridan.edu).

    Modes supported via SCRAPE_MODE setting:
        - course
        - directory
        - calendar
        - course_directory
        - course_calendar
        - directory_calendar
        - all (default)

    Outputs:
        - Course data (PDF schedules)
        - Directory placeholder data
        - Calendar event data
    """

    name = "sheridan"
    institution_id = 258439721619843035

    # Containers for scraped data
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_urls = [
            'https://www.sheridan.edu/app/uploads/2025/03/SheridanSchedule_25FA_20250310.pdf',
            'https://www.sheridan.edu/app/uploads/2025/10/SheridanSchedule_26SP_20251014.pdf',
            'https://www.sheridan.edu/app/uploads/2025/10/SheridanSchedule_26SU_20251014.pdf',
            'https://www.sheridan.edu/app/uploads/2025/03/OnlineSchedule_25FA_20250310.pdf',
            'https://www.sheridan.edu/app/uploads/2025/10/OnlineSchedule_26SP_20251014.pdf',
            'https://www.sheridan.edu/app/uploads/2025/10/OnlineSchedule_26SU_20251014.pdf',
            'https://www.sheridan.edu/app/uploads/2025/03/BuffaloSchedule_25FA_20250310.pdf',
            'https://www.sheridan.edu/app/uploads/2025/10/BuffaloSchedule_26SP_20251014.pdf',
            'https://www.sheridan.edu/app/uploads/2025/10/BuffaloSchedule_26SU_20251014.pdf'
        ]
    
    # DIRECTORY CONFIG
    directory_url = "https://www.sheridan.edu/about/department-directory/"

    # CALENDAR CONFIG
    calendar_source_url = "https://www.sheridan.edu/events/list/page/1/"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            self.parse_directory()

        elif mode == "calendar":
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            self.parse_directory()

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

        #  All three (default)
        else:
            self.parse_course()
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

    # PARSE COURSE
    def parse_course(self):
        """
        Extracts course schedule data from PDF files.

        - Detects block date legends (Block A/B/C or WW1/WW2).
        - Maps block codes to actual date ranges.
        - Extracts course information from table rows.
        """

        def clean_text(text):
            """Normalize whitespace and safely clean text values."""
            if text:
                return re.sub(r'\s+', ' ', str(text)).strip()
            return ""

        def extract_block_dates(pdf):
            """
            Extract block-to-date mappings from the first 2 pages of the PDF.
            Returns dictionary like:
                {'A': '01/01/2026 - 02/01/2026'}
            """
            date_map = {}
            combined_text = ""

            # First 2 pages usually contain block date legends
            for i in range(min(2, len(pdf.pages))):
                page_text = pdf.pages[i].extract_text()
                if page_text:
                    combined_text += page_text + "\n"

            # Capture Block A/B/C or WW1/WW2 patterns with date ranges
            patterns = re.findall(
                r'(Block\s+([A-Z])|WW(\d+)).*?'
                r'(\d{1,2}/\d{1,2}/\d{2,4}\s*-\s*\d{1,2}/\d{1,2}/\d{2,4})',
                combined_text
            )

            for match in patterns:
                block_letter = match[1]
                ww_num = match[2]
                key = block_letter.strip().upper() if block_letter else f"WW{ww_num}"
                date_map[key] = match[3].strip()

            return date_map

        #MAIN LOOP
        for url in self.course_urls:
            response = requests.get(url, timeout=30)
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:

                block_date_map = extract_block_dates(pdf)

                for page in pdf.pages:
                    table = page.extract_table()
                    if not table:
                        continue

                    for i, row in enumerate(table):
                        if not row:
                            continue
                        
                        # Skip header rows
                        if "Start Time" in str(row) or "Syn" in str(row):
                            continue

                        loc_val = clean_text(row[0])
                        block = clean_text(row[1]) if len(row) > 1 else ""
                        class_info = clean_text(row[3])

                        # Skip invalid rows
                        if not class_info or class_info in ["Course", "None"]:
                            continue

                        course_dates = ""

                        # Match block from legend
                        for key in block_date_map:
                            if re.search(rf'\b{key}\b', block):
                                course_dates = block_date_map[key]
                                course_dates = re.sub(r'\s+', ' ', course_dates).replace('-', ' - ')
                                break

                        # Special Block O logic (date appears in next row)
                        if block == "O":
                            for offset in [1, 2]:
                                if i + offset < len(table):
                                    next_row_text = " ".join(
                                        [str(c) for c in table[i + offset] if c]
                                    )
                                    date_match = re.search(
                                        r'(\d{1,2}/\d{1,2}/\d{2,4}\s*-\s*'
                                        r'\d{1,2}/\d{1,2}/\d{2,4})',
                                        next_row_text
                                    )
                                    if date_match:
                                        course_dates = date_match.group(1)
                                        course_dates = re.sub(r'\s+', ' ', course_dates).replace('-', ' - ')
                                        break

                        # Fallback from map
                        if not course_dates and block in block_date_map:
                            course_dates = block_date_map[block]
                            course_dates = re.sub(r'\s+', ' ', course_dates).replace('-', ' - ')

                        # Course details
                        parts = class_info.split('*')
                        section = parts[-1] if len(parts) > 1 else ""
                        class_num = '*'.join(parts[:2])

                        title = clean_text(row[4]) if len(row) > 4 else ""
                        instructor = clean_text(row[6]) if len(row) > 6 else ""

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": url,
                            "Course Name": f"{class_num} {title}",
                            "Course Description": "",
                            "Class Number": class_num,
                            "Section": section,
                            "Instructor": instructor,
                            "Course Dates": clean_text(course_dates),
                            "Location": loc_val,
                            "Textbook/Course Materials": ""
                        })

    # PARSE DIRECTORY
    def parse_directory(self):
        """
        Currently returns placeholder data.
        Directory scraping not implemented for Sheridan.
        """

        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": self.directory_url,
        "Name": 'Data not found',
        "Title": 'Data not found',
        "Email": 'Data not found',
        "Phone Number": 'Data not found',
        })
        
    # PARSE CALENDAR
    def parse_calendar(self,response):
        """
        Scrapes event listings from Sheridan events pages.
        Follows pagination automatically.
        """
        urls = response.xpath('//h4/a/@href').getall()
        for url in urls:
            res = requests.get(url)
            page_response = Selector(text=res.text)
            term_date = page_response.xpath('//span[@class="tribe-event-date-start"]/text()').get('').split('@')[0].strip()
            term_name = page_response.xpath('//h1/text()').get('').strip()
            description = page_response.xpath('//h5/strong/text()').get('').strip()

            if description == '':
                description = ', '.join(t.strip() for t in page_response.xpath('//div[@class="mec-single-event-description mec-events-content"]/p/text()').getall() if t.strip())
            else:
                description = description

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": url,
                "Term Name": re.sub(r'\s+', ' ', term_name),
                "Term Date": term_date,
                "Term Date Description": re.sub(r'\s+',' ',description)
            })

        # Follow next page if exists
        next_page = response.xpath(
            '//li[contains(@class,"tribe-events-c-nav__list-item--next")]/a/@href'
        ).get()

        if next_page:
            yield scrapy.Request(
                url=response.urljoin(next_page),
                callback=self.parse_calendar
            )

    # CLOSED SPIDER
    # Saves all collected datasets to CSV
    def closed(self, reason):
        """
        Called automatically when spider finishes.
        Saves course, directory, and calendar datasets if populated.
        """
        
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")
