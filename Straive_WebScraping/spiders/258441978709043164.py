import io
import re
import scrapy
import urllib3
import requests
import pdfplumber
import cloudscraper
import pandas as pd
from ..utils import *
from parsel import Selector
from datetime import datetime


class ThomasSpider(scrapy.Spider):
    
    name = "thomas"
    institution_id = 258441978709043164
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://apps.thomasmore.edu/CourseSchedule/Home/"

    # DIRECTORY CONFIG
    directory_url = "https://apps.thomasmore.edu/WebApps/EmployeeDirectory/?_gl=1*1j85n6r*_gcl_au*MTA4NzAyODM1Ny4xNzY1MzQwMjgw"

    # CALENDAR CONFIG
    calendar_source_url = "https://www.thomasmore.edu/wp-content/uploads/2025-2026-Academic-Calendar-.pdf"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        yield from []
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and directory URLs are blocked by Cloudflare, so `cloudscraper`
        is used to make requests and retrieve the data safely.

        - Calendar data is provided as PDFs, so `parse_calendar` uses
        `pdfplumber` to extract the relevant information.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            self.parse_directory()

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            self.parse_directory()

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            self.parse_directory()
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            self.parse_directory()
            self.parse_calendar()

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

        scraper = cloudscraper.create_scraper() # Create a scraper that can bypass Cloudflare anti-bot protections
        response = scraper.get(self.course_url, timeout=10) # Send a GET request to the course URL with a 10-second timeout
        response = Selector(text=response.text)
        urls = response.xpath('//td/a/@href').getall()

        for url in urls:
            url = f'https://apps.thomasmore.edu{url}'
            resp = scraper.get(url, timeout=10)
            response = Selector(text=resp.text)
            blocks = response.xpath('//tr[@style="background-color:lightblue"]')

            for block in blocks:
                sub_blocks = block.xpath('.//following-sibling::tr[1]//td[4]')
                for sub_block in sub_blocks:
                    name = block.xpath('.//td[1]/a[1]/text()').get('').strip()
                    before_colon = name.split(':')[0]
                    parts = before_colon.split()
                    class_num = f"{parts[0]} {parts[1]}" 
                    section = parts[2] if len(parts) > 2 else ""
                    enroll_part = block.xpath('.//td[contains(text(),"/ Reg: ")]/text()').get('').strip()
                    parts = enroll_part.split('/')
                    reg = parts[1].split(':')[1].strip()
                    max = parts[2].split(':')[1].strip()
                    enrollment = f"{reg} of {max}"
                    instructor = block.xpath('.//td[2]/text()').get('').replace('Instructor:','').strip()
                    course_dates = sub_block.xpath('.//text()').get('').strip()
                    location = sub_block.xpath('.//preceding-sibling::td[2]/text()').get('').strip()

                    url = block.xpath('.//td[1]/a[1]/@href').get('')
                    url = f'https://apps.thomasmore.edu{url}'
                    text_book_link = block.xpath('.//td[1]/a[2]/@href').get('')
                    resp = scraper.get(url, timeout=10)
                    response = Selector(text=resp.text)
                    description = response.xpath('//h3[contains(text(),"Course Description:")]/following::p[1]/text()').get('').strip()
                    self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": url,
                        "Course Name": re.sub(r'\s+',' ',name),
                        "Course Description": re.sub(r'\s+',' ',description),
                        "Class Number": class_num,
                        "Section": section,
                        "Instructor": re.sub(r'\s+',' ',instructor),
                        "Enrollment": enrollment,
                        "Course Dates": re.sub(r'\s+',' ',course_dates),
                        "Location": re.sub(r'\s+',' ',location),
                        "Textbook/Course Materials": text_book_link
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
        scraper = cloudscraper.create_scraper() # Create a scraper that can bypass Cloudflare anti-bot protections
        response = scraper.get(self.directory_url, timeout=10) # Send a GET request to the course URL with a 10-second timeout
        response = Selector(text=response.text)
        blocks = response.xpath('//div[@class="col-sm-12 col-md-12 col-lg-12"]')
        for block in blocks:
            name = block.xpath('.//div/label[1]/a/text()').get('').replace('\xa0', ' ').strip()
            title = block.xpath('.//div/label[2]/text()').get('').strip()
            title = title.lstrip('|').strip()
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.directory_url,
            "Name": re.sub(r'\s+',' ',name),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.xpath('.//a[contains(@href,"mailto")]/text()').get('').strip(),
            "Phone Number": block.xpath('.//div/text()[3]').get('').replace('|', ' ').strip(),
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
        # Suppress warnings for insecure HTTPS requests
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)   
        # Headers to mimic a real browser request
        headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
    
        try:
            # Send GET request to the calendar PDF URL, ignoring SSL verification
            response = requests.get(self.calendar_source_url, headers=headers, verify=False, timeout=30)
            response.raise_for_status()
        except Exception as e:
            return

        # Open the PDF content in memory using pdfplumber
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                # Extract table data from the page
                table = page.extract_table()
                if not table: continue # Skip pages without tables

                for row in table:
                    # Clean up each cell in the row (strip whitespace and handle None)
                    raw_row = [str(item).strip() if item else "" for item in row]
                    # Skip empty rows or rows that contain header labels like "Date"
                    if not any(raw_row) or "Date" in "".join(raw_row): 
                        continue

                    if len(raw_row) >= 2:
                        date_raw = raw_row[0] # First column: raw date
                        desc_raw = raw_row[-1] # Last column: description
                        
                        # Normalize and store the last valid date
                        if date_raw:
                            temp_date = re.sub(r'(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|,)', '', date_raw, flags=re.IGNORECASE)
                            temp_date = re.sub(r'\(.*?\)', '', temp_date) # Remove text in parentheses
                            last_valid_date = temp_date.strip()
                        
                        current_term = "Summer Session 2026" # Default term
                        try:
                            # Try to parse month and day from the last valid date
                            date_match = re.search(r'([A-Z][a-z]{2})\.?\s+(\d{1,2})', last_valid_date)
                            if date_match:
                                m_str, d_num = date_match.group(1), int(date_match.group(2))
                                # Determine year based on month
                                year = 2025 if m_str in ["Aug", "Sep", "Oct", "Nov", "Dec"] else 2026
                                
                                # Parse datetime object
                                try:
                                    dt = datetime.strptime(f"{m_str} {d_num} {year}", "%b %d %Y")
                                except:
                                    dt = datetime.strptime(f"{m_str} {d_num} {year}", "%B %d %Y")

                                # Map date to academic term
                                if datetime(2025, 8, 11) <= dt <= datetime(2025, 12, 18):
                                    current_term = "Fall Session 2025"
                                elif datetime(2025, 12, 15) <= dt <= datetime(2026, 1, 20):
                                    current_term = "Winter Session 2026"
                                elif datetime(2026, 1, 5) <= dt <= datetime(2026, 5, 16):
                                    current_term = "Spring Session 2026"
                                else:
                                    current_term = "Summer Session 2026"
                        except:
                            current_term = "Summer Session 2026" # Fallback if date parsing fails

                        # Split multi-line descriptions into individual events
                        events = desc_raw.split('\n')
                        for event in events:
                            event = event.strip()
                            if not event: continue
                            
                            # If event continues from previous row, append to last entry
                            if (event.startswith('(') or (len(event) > 0 and event[0].islower())) and self.calendar_rows:
                                self.calendar_rows[-1]["Term Date Description"] += " " + event
                            else:
                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": self.calendar_source_url,
                                    "Term Name": current_term,
                                    "Term Date": last_valid_date,
                                    "Term Date Description": event,
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

        
