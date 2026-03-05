import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import save_df
from inline_requests import inline_requests


class MuskingumSpider(scrapy.Spider):
    """
    Spider for Muskingum University.
    Scrapes:
    1. Undergraduate Course Catalog (PDF)
    2. Academic Calendar (PDFs linked from registrar page)
    3. Faculty Directory (Search results + Profile pages)
    """

    name="muskingum"
    institution_id = 258459874877269973

    base_url = "http://www.muskingum.edu"
    calendar_url = "https://www.muskingum.edu/registrar/academic-calendar"
    course_url = "https://www.muskingum.edu/sites/default/files/media/Grad/2022-2033%20Undergraduate%20Catalog.pdf"
    directory_base_url = "https://www.muskingum.edu/directory/search"
    directory_url = "https://www.muskingum.edu/directory/search?title=&field_department_category_target_id=All"
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            self.parse_course()
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            self.parse_course()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
            # self.parse_calendar(self.calendar_url)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
    
    def parse_course(self):
        """
        Parses the large Undergraduate Catalog PDF for course descriptions.
        """

        course_data = []
        seen = set()

        #Regex & Skip Patterns
        course_title_pattern = re.compile(r'^([A-Z]{2,5})\s(\d{3}(?:-\d{3})?)\s-\s(.+)$')
        SKIP_PREFIXES = (
            "credit hours",
            "prerequisite",
            "co-requisite",
            "corequisite",
            "repeatability",
            "field hours",
            "note:",
            "map only",
            "cross listed",
            "restrictions"
        )

        resp = requests.get(self.course_url, timeout=60)
        resp.raise_for_status()
        pdf_bytes = io.BytesIO(resp.content)

        #Parse PDF
        with pdfplumber.open(pdf_bytes) as pdf:
            current_course = None
            current_desc = []

            # Define your page ranges (0-based indexing)
            page_ranges = [
                (0, 180),    # pages 0–179
                (408, 587)   # pages 408–586
            ]

            for start, stop in page_ranges:
                for page in pdf.pages[start:stop]:
                    text = page.extract_text()
                    if not text:
                        continue

                    for line in text.split("\n"):
                        line = line.strip()
                        if not line:
                            continue

                        #Detect Course Title
                        match = course_title_pattern.match(line)
                        if match:
                            # Save previous course if exists
                            if current_course:
                                key = current_course["class_number"]
                                if key not in seen:
                                    seen.add(key)
                                    course_data.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": resp.url,
                                        "Course Name": f'{current_course["class_number"]} {current_course["course_name"]}',
                                        "Course Description": " ".join(current_desc).strip(),
                                        "Class Number": current_course["class_number"],
                                        "Section": "",
                                        "Instructor": "",
                                        "Enrollment": "",
                                        "Course Dates": "",
                                        "Location": "",
                                        "Textbook/Course Materials": ""
                                    })
                            # Start new course
                            subject, number, name = match.group(1), match.group(2), match.group(3)
                            current_course = {
                                "class_number": f"{subject}-{number}",
                                "course_name": name
                            }
                            current_desc = []
                            continue

                        #Accumulate Description
                        if current_course:
                            low = line.lower()
                            if low.startswith(SKIP_PREFIXES):
                                continue
                            # Skip large junk uppercase blocks
                            if line.isupper() and len(line) > 25:
                                continue
                            current_desc.append(line)

            # Save last course in the page ranges
            if current_course:
                key = current_course["class_number"]
                if key not in seen:
                    seen.add(key)
                    course_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": resp.url,
                        "Class Number": current_course["class_number"],
                        "Course Name": f'{current_course["class_number"]} {current_course["course_name"]}',
                        "Course Description": " ".join(current_desc).strip(),
                        "Section": "",
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": ""
                    })

        df = pd.DataFrame(course_data)
        save_df(df, self.institution_id, "course")
        
     
    @inline_requests
    def parse_calendar(self, response):
        """
        Follows PDF links on the registrar page to extract key semester dates.
        """
        calendar_data = []
        pdf_links = response.xpath(
            '//article//div[contains(@class,"field--name-body")]//p/a[contains(@href,".pdf")]/@href'
        ).getall()
        
        for link in pdf_links:
            full_url = self.base_url + link
            pdf_res = yield scrapy.Request(full_url, headers=self.headers)

            pdf_bytes = io.BytesIO(pdf_res.body)
            with pdfplumber.open(pdf_bytes) as pdf:
                text = ""
                for page in pdf.pages:
                    page_text = page.extract_text()
                    if page_text:
                        text += page_text + "\n"

            lines = [l.strip() for l in text.split("\n") if l.strip()]
            current_term = None
            term_headers = [
                "Fall Semester",
                "Spring Semester",
                "May Term",
                "Special Dates"
            ]
            for line in lines:
                # Detect term headers
                for header in term_headers:
                    if line.startswith(header):
                        current_term = line.strip()
                        break

                # Handle TBD rows
                if line.startswith("TBD") and current_term:
                    calendar_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": current_term,
                        "Term Date": "TBD",
                        "Term Date Description": line.replace("TBD", "").strip()
                    })
                    continue

                # Skip bracket-only lines like "(Excluding Sat. & Sun., May 2 & 3)"
                if line.startswith("(") and line.endswith(")"):
                    continue

                # Skip broken lines like "May 2, & 3)"
                if "&" in line and not re.search(r'\d{4}', line):
                    continue

                # Match date + description
                match = re.match(r'^(.+?\d{1,2}(?:\s*\(.*?\))?)\s+(.*)$', line)

                if match and current_term:
                    raw_date = match.group(1).strip()

                    # Remove weekday
                    term_date = raw_date.split(",", 1)[1].strip() if "," in raw_date else raw_date.strip()

                    # Remove time like (5 p.m.), (noon)
                    term_date = re.sub(r'\s*\(.*?\)', '', term_date).strip()
                    desc = match.group(2).strip()
                    calendar_data.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": response.url,
                        "Term Name": current_term,
                        "Term Date": term_date,
                        "Term Date Description": desc
                    })

        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")

    def parse_directory(self, response):
        """
        Extracts faculty information from search results and profile pages.
        """

        directory_data = []
        links = response.xpath('//div[@class="views-row"]//h3/a/@href').getall()
        for link in links:
            if not link:
                continue
            if "vacant" in link.lower():
                continue
            full_url = self.base_url + link
            directory_res = yield scrapy.Request(url=full_url, headers=self.headers )

            name = directory_res.xpath('//div[contains(@class,"col-md-9")]//h3/text()').get(default='').strip()

            # ALL titles (dynamic: -1, -2, -3, etc.)
            titles = directory_res.xpath(
                '//div[contains(@class,"field--name-field-professional-title")]//text()'
            ).getall()
            titles = [t.strip() for t in titles if t.strip()]
            title = ", ".join(titles)

            # Phone
            phone = directory_res.xpath(
                '//div[contains(@class,"field--name-field-phone-number")]//div[@class="field__item"]/text()'
            ).get(default='').strip()

            # Email
            email = directory_res.xpath('//a[starts-with(@href,"mailto:")]/text()').get(default='').strip()
            
            if not name:
                return

            directory_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone,
            })

        df = pd.DataFrame(directory_data)
        save_df(df, self.institution_id, "campus")


