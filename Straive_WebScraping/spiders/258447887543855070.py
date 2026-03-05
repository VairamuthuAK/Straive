import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import save_df



class EwcSpider(scrapy.Spider):
    """
    Scrapy spider for scraping Eastern Wyoming College (EWC) data.

    This spider extracts:
    1. Course catalog data (from PDF)
    2. Faculty/Staff directory data (from website)
    3. Academic calendar data (from PDF)

    The scraping mode can be controlled using the SCRAPE_MODE setting:
        - 'course'
        - 'directory'
        - 'calendar'
        - 'all' (default)
    """
    name = "ewc"

    # Unique institution identifier used across all datasets
    institution_id = 258447887543855070

    # Base URLs
    course_url = "https://ewc.wy.edu/wp-content/uploads/2025/06/Catalog2025-2026-in-PDF-0425.pdf"
    directory_url = "https://ewc.wy.edu/our-family/directory/"
    calendar_url = "https://ewc.wy.edu/wp-content/uploads/2025/07/FA2025-Academic-Cal.pdf"

    def __init__(self, *args, **kwargs):
        """
        Initialize storage containers for scraped datasets.
        """
        super().__init__(*args, **kwargs)

        # Storage for scraped data
        self.course_rows = []       # Stores all course data
        self.directory_rows = []    # Stores all directory (faculty/staff) data
        self.calendar_rows = []     # Stores all calendar events data

    def start_requests(self):
        """
        Entry point for the spider.
        Scrape mode can be controlled using SCRAPE_MODE setting.
        """
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')  # Determine mode

        if mode == 'course':
            self.parse_course()

        elif mode == 'directory':
            # Only scrape directory data
            headers = {
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'x-requested-with': 'XMLHttpRequest'
            }
            yield scrapy.Request(
                self.directory_url,
                headers=headers,
                callback=self.parse_directory,
                dont_filter=True
            )

        elif mode == 'calendar':
            self.parse_calendar()

        else:
            # Default: scrape course, directory, and calendar
            self.parse_course()
            headers = {
                'accept': 'application/json, text/javascript, */*; q=0.01',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
                'x-requested-with': 'XMLHttpRequest'
            }
            yield scrapy.Request(
                self.directory_url,
                headers=headers,
                callback=self.parse_directory,
                dont_filter=True
            )

            self.parse_calendar()


    def parse_course(self):
        """
        Extracts course information from the catalog PDF.

        Logic:
        - Reads specific page range from PDF.
        - Detects course headers using regex.
        - Builds course title and description.
        - Cleans and saves structured output.
        """
        response = requests.get(self.course_url)
        pdf_file = io.BytesIO(response.content)
        course_list = []

        # Regex patterns
        header_pattern = re.compile(r'^([A-Z]{3,4}\s\d{4})\s*[–-]\s*(.*)')
        subject_header_pattern = re.compile(r'^[A-Z]{3,4}\s–\s[A-Z\s]+$')

        current_course = None

        with pdfplumber.open(pdf_file) as pdf:
            # Process only known course listing pages
            for i in range(174, 266):
                if i >= len(pdf.pages):
                    break

                page = pdf.pages[i]
                width, height = page.width, page.height
                mid = width / 2

                # Split into two columns
                columns = [
                    page.within_bbox((0, 0, mid, height)),
                    page.within_bbox((mid, 0, width, height))
                ]

                for col in columns:
                    text = col.extract_text()
                    if not text:
                        continue

                    lines = text.split('\n')

                    for line in lines:
                        line = line.strip()

                        # Skip unwanted noise
                        if not line or line.isdigit() or "COURSE LISTING" in line:
                            continue
                        if subject_header_pattern.match(line):
                            continue

                        header_match = header_pattern.match(line)

                        # If new course header found
                        if header_match:
                            # Save previous course
                            if current_course:
                                course_list.append(current_course)

                            class_num = header_match.group(1).strip()
                            title_start = header_match.group(2).strip()

                            current_course = {
                                "Class Number": class_num,
                                "Course Name": f"{class_num} – {title_start}",
                                "Course Description": "",
                                "_description_started": False
                            }
                        
                        # If continuation line
                        elif current_course:

                            # Description begins after "Credits:"
                            if "Credits:" in line:
                                current_course["_description_started"] = True
                                desc_part = re.sub(r'Credits:\s*[\d\.]+', '', line).strip()
                                current_course["Course Description"] += " " + desc_part
                            
                            # Title continuation
                            elif not current_course["_description_started"]:
                                current_course["Course Name"] += " " + line

                            # Description continuation
                            else:
                                line = re.sub(r'Course Fee:\s*\$[\d\.,]+', '', line)
                                line = re.sub(
                                    r'Course offered [A-Za-z\s]+ only\.?',
                                    '',
                                    line,
                                    flags=re.IGNORECASE
                                )
                                current_course["Course Description"] += " " + line

            # Append last course
            if current_course:
                course_list.append(current_course)

        # Clean and push to self.course_rows
        for course in course_list:
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": re.sub(r'\s+', ' ', course["Course Name"]).strip(),
                "Course Description": re.sub(r'\s+', ' ', course["Course Description"]).strip(),
                "Class Number": course["Class Number"],
            })

        # Save
        save_df(pd.DataFrame(self.course_rows), self.institution_id, "course")

    def parse_directory(self, response):
        """
        Extracts profile page URLs from the directory listing page.
        Sends requests to individual profile pages.
        """

        blocks = response.xpath('//div[@class="faculty-list-item"]/a[1]/@href').getall()
        for block in blocks:
            headers = {
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-language': 'en-US,en;q=0.9',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }
            yield scrapy.Request(block,headers=headers, callback=self.parse_directory_profile)

    def parse_directory_profile(self, response):
        """
        Extracts individual faculty/staff details:
        - Name
        - Title
        - Email
        - Phone
        """

        name = response.xpath('//div[@style="padding-top: 0;"]/h2/text()').get('').strip()
        title = response.xpath('//div[@style="padding-top: 0;"]//p[@class="title"]/text()').get('').strip()
        phone = response.xpath('//div[@style="padding-top: 0;"]//a[starts-with(@href, "tel:")]/text()').get('').strip()
        email = response.xpath('//div[@style="padding-top: 0;"]//a[starts-with(@href, "mailto:")]/text()').get('').strip()
       
        self.directory_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": name,
            "Title": title,
            "Email": email,
            "Phone Number": phone,
        })

        # # Save updated DataFrame
        save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

    def parse_calendar(self):
        """
        Extracts academic calendar events from PDF.

        Logic:
        - Identifies lines starting with date pattern.
        - Groups subsequent lines as event description.
        - Saves structured term date entries.
        """

        pdf_url = self.calendar_url
        response = requests.get(pdf_url)
        response.raise_for_status()

        date_pattern = re.compile(r"^[A-Z][a-z]+ \d{1,2} \([A-Z][a-z]+\)")

        current_date = None
        current_desc = ""

        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split('\n')

                for line in lines:
                    line = line.strip()
                    if not line:
                        continue

                    match = date_pattern.match(line)

                    if match:
                        # Save previous event
                        if current_date:
                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": pdf_url,
                                "Term Name": "Fall 2025",
                                "Term Date": current_date,
                                "Term Date Description": re.sub(r'\s+', ' ', current_desc).strip(),
                            })

                        date_string = match.group()
                        description_start = line[len(date_string):].strip()

                        current_date = date_string
                        current_desc = description_start

                    else:
                        if current_date:
                            current_desc += " " + line

            # Append last entry
            if current_date:
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": pdf_url,
                    "Term Name": "Fall 2025",
                    "Term Date": current_date,
                    "Term Date Description": re.sub(r'\s+', ' ', current_desc).strip(),
                })

        # Save
        save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")
