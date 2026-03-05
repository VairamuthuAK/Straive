import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector

def calendar_parse_normalize(text):
        """Helper to clean whitespace from strings."""
        return re.sub(r'\s+', ' ', text).strip() if text else ""

class NicolasSpider(scrapy.Spider):
    """
    Spider for Nichols College (ICCMS).
    
    This spider extracts three types of data:
    1. Course Catalog: Extracted from a PDF.
    2. Faculty Directory: Scraped from the institutional website.
    3. Academic Calendar: Extracted from multiple PDF schedules.
    """

    name = "nicolas"
    institution_id = 258457944432076757

    # Accumulator for calendar rows across paginated pages
    calendar_rows = []
    directory_rows = []

    # Target URLs
    course_url = "https://www.nichols.edu/wp-content/uploads/2025/12/Nichols-Catalog-25-26-WEB.pdf"
    directory_url = "https://www.nichols.edu/faculty/page/"
    calendar_urls = [
        "https://graduate.nichols.edu/wp-content/uploads/2024/11/FY26-semester-calendar.pdf",
        "https://www.nichols.edu/wp-content/uploads/2025/07/2025-2026-ACADEMIC-CALENDAR-rev-07.29.25.pdf"
                ]
    
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
            self.parse_course()
            
        elif mode == 'directory':
           pages = ['1','2','3','4','5','6','7','8']
           for page in pages:
               url = f'https://www.nichols.edu/faculty/page/{page}/'
               yield scrapy.Request(url=url, callback=self.parse_directory)
           
        elif mode == 'calendar':
            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()

            pages = ['1','2','3','4','5','6','7','8']
            for page in pages:
               url = f'https://www.nichols.edu/faculty/page/{page}/'
               yield scrapy.Request(url=url, callback=self.parse_directory)


        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()

            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)

            pages = ['1','2','3','4','5','6','7','8']
            for page in pages:
               url = f'https://www.nichols.edu/faculty/page/{page}/'
               yield scrapy.Request(url=url, callback=self.parse_directory)
        
        # All three (default)
        else:
            self.parse_course()

            for page in pages:
               url = f'https://www.nichols.edu/faculty/page/{page}/'
               yield scrapy.Request(url=url, callback=self.parse_directory)
            
            for calendar_url in self.calendar_urls:
                yield scrapy.Request(url=calendar_url, callback=self.parse_calendar)
       

    # Parse methods UNCHANGED from your original
    def parse_course(self):
        """
        Downloads and parses the Course Catalog PDF.
        
        Uses pdfplumber to extract text from specific page ranges and 
        regex to identify course codes (e.g., ACC 101) and descriptions.
        """
        
        course_rows = []
        response = requests.get(self.course_url)
        pdf_bytes = io.BytesIO(response.content)

        # Zero-indexed page ranges for course descriptions
        pages_to_extract = list(range(141, 184)) + list(range(185, 190))

        all_text = ""
        with pdfplumber.open(pdf_bytes) as pdf:
            for i in pages_to_extract:
                try:
                    page = pdf.pages[i]
                    text = page.extract_text()
                    if text:
                        all_text += text + "\n"
                except Exception as e:
                    print(f"Skipped page {i+1}: {e}")

        # Clean and split text into lines for processing
        lines = [l.strip() for l in all_text.splitlines() if l.strip()]

        # Regex: Matches patterns like "ACC 101 Financial Accounting"
        course_header = re.compile(r"^[A-Z]{2,4}\s?\d{3}(?:/\d{3})*\s+.+$")

        # Regex: Specific footer-like text in descriptions
        shared_desc_regex = re.compile(r"^See\s+.*?governing policies\.?$",re.IGNORECASE)

        # Department headers found in the PDF that should not be treated as courses
        skip_exact = {
            "Art",
            "Business Analytics",
            "Return to Table of Contents",
            "Communication",
            "Criminal Justice",
            "Criminal Psychology",
            "Critical Writing, Reading and Research",
            "Dance",
            "Economics",
            "English",
            "Entrepreneurship",
            "Environmental Science",
            "Finance",
            "General Business",
            "History",
            "Honors",
            "Hospitality, Events, & Tourism",
            "Human Resource Management",
            "Humanities",
            "Interdisciplinary Studies",
            "International Business",
            "Leadership",
            "Legal Studies",
            "Liberal Arts",
            "Management",
            "Marketing",
            "Mathematics",
            "Music",
            "Philosophy",
            "Political Science",
            "Psychology",
            "Real Estate Management",
            "Religion",
            "Seminar",
            "Sociology",
            "Spanish",
            "Sport Management",
            "WAY",
            "Business Core Courses",
            "Master of Business Administration (MBA & EMBA)",
            "Master of Organizational Leadership (MSOL)",
            "Master of Science in Accounting (MSA)",
            "Master of Science in Counterterrorism (MSC)",
            
        }

        courses = []
        i = 0

        while i < len(lines):
            line = lines[i]

            # Skip department headers & credit lines
            if (line in skip_exact or re.match(r"^\d+\s+Hours,", line) or line.startswith("Prerequisite:")):
                i += 1
                continue

            # Course detected
            if course_header.match(line):
                titles = []

                # Collect consecutive course titles
                while i < len(lines) and course_header.match(lines[i]):
                    titles.append(lines[i])
                    i += 1

                # Shared description (internships)
                if i < len(lines) and shared_desc_regex.match(lines[i]):
                    shared_desc = (lines[i].replace("â€œ", "“").replace("â€", "”").replace("â€™", "’"))
                    for t in titles:
                        class_number = ' '.join(t.split(' ')[0:2])
                        courses.append((t, shared_desc, class_number))
                        
                    i += 1
                    continue

                # Normal single course with description
                title = titles[0]
                desc_lines = []

                while i < len(lines) and not course_header.match(lines[i]):
                    if not re.match(r"^\d+\s+Hours,", lines[i]):
                        desc_lines.append(lines[i])
                    i += 1

                # Post-process description text
                description = " ".join(desc_lines).strip()
                description = re.sub(r'Nichols College 2025-2026 Catalog\s+\d+','',description).strip()
                description = description.replace('Return to Table of Contents','').strip()
                description = re.sub(r'\s*Prerequisite:.*$', '', description).strip()
                description = description.replace('Prerequisite:','').strip()
                class_number = ' '.join(title.split(' ')[0:2])
                courses.append((title, description ,class_number))
                continue

            i += 1

        # Format rows for DataFrame
        for name, desc , class_nu in courses:
            course_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": name,
                    "Course Description": desc,
                    "Class Number": class_nu,
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
            
    def parse_directory(self, response):
        """
        Parses the faculty directory web pages.
        
        Follows links to individual profile cards and extracts contact info.
        """

        # Identify links to individual faculty profiles
        product_links = response.xpath('//div[@class="card rounded-0 mb-3"]//div[@class="card-body text-center px-3 pt-3 pb-1"]/a/@href').getall()
        for product_link in product_links:
            res = requests.get(product_link)
            product_response = Selector(text=res.text)
            name = product_response.xpath('//h1/text()').get('').strip()
            title = product_response.xpath('//div[@class="card-text mb-3 font-italic smallish"]/p/text()').getall()

            # Clean and join titles if multiple exist
            if len(title) >1:
                title = [re.sub(r"\s+", " ", t).strip()for t in title]
                title = ', '.join(title)
            else:
                title = title[0]
            phone = product_response.xpath("//div[@class='card bg-success border-0 rounded-0']//a[starts-with(@href, 'tel:')]/text()").get('')
            email = product_response.xpath("//a[starts-with(@href, 'mailto:')]/text()").get('')

            #STORE ROW
            self.directory_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": res.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                    }
                )

        directory_df = pd.DataFrame(self.directory_rows)
        save_df(directory_df, self.institution_id, "campus")
    
    def parse_calendar(self, response):
        """
        Parses Academic Calendar PDFs.
        
        Handles two formats: 
        1. Tabular format (FY26 semester calendar).
        2. Two-column text format (Standard academic calendar).
        """

        # Format 1: Table-based PDF (FY26)  
        if "fy26" in response.url:
            pdf_bytes = response.body
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                page = pdf.pages[0]
                table = page.extract_table()

            # Identify sessions from headers (Summer I, Fall II, etc.)
            raw_headers = table[0][1:]
            sessions = [calendar_parse_normalize(h) for h in raw_headers if h]

            VALID_LABELS = {
                "Session dates",
                "Registration Add dates",
                "Registration Drop dates",
                "Classes Open PreAssignment work",
                "Payment Dates",
                "Withdrawal Dates"
            }

            for row in table[1:]:
                label = calendar_parse_normalize(row[0])

                if label not in VALID_LABELS:
                    continue

                for idx, cell in enumerate(row[1:]):
                    if not cell:
                        continue

                    term_date = cell.strip().replace("–", "-")
                    term_description = sessions[idx]

                    term_name = ("Summer 25 Session 1" if label == "Session dates" else label)

                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": term_name,
                        "Term Date": term_date,
                        "Term Date Description": term_description
                    })

        #Format 2: Columnar Text-based PDF
        else:
            date_pattern = r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[-\s]\d{1,2}(?:-\d{1,2})?"
            pdf_bytes = response.body
            with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
                for page in pdf.pages:

                    x0, y0, x1, y1 = page.bbox
                    mid_x = (x0 + x1) / 2

                    left_bbox = (x0, y0, mid_x, y1)
                    right_bbox = (mid_x, y0, x1, y1)

                    for bbox in (left_bbox, right_bbox):
                        text = page.crop(bbox).extract_text()
                        if not text:
                            continue

                        for line in text.split("\n"):
                            line = line.strip()
                            if not line:
                                continue

                            matches = list(re.finditer(date_pattern, line))

                            for i, match in enumerate(matches):
                                term_date = match.group()
                                start = match.end()
                                end = matches[i + 1].start() if i + 1 < len(matches) else len(line)
                                term_description = line[start:end].strip()

                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Term Name": "2025-2026 Academic Calendar",
                                    "Term Date": term_date,
                                    "Term Date Description": term_description
                                })

            # Save only once after last page
            calendar_df = pd.DataFrame(self.calendar_rows)
            save_df(calendar_df, self.institution_id, "calendar")





 