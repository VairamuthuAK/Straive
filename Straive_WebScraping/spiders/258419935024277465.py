import re
import io
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class CatholicSpider(scrapy.Spider):
    """
    Spider to scrape Course schedules, Faculty Directory, and Academic Calendars 
    from Catholic University of America (CUA).
    
    Data Sources:
    - Courses: Web-service API returning JSON.
    - Directory: Faculty bio JSON feeds with secondary scraping of profile links.
    - Calendar: PDF-based tables parsed using pdfplumber.
    """

    name = "catholic"
    institution_id = 258419935024277465

    # Target URLs and APIs
    course_api_url = "https://webservices.cua.edu/course-information/course-info.php?method=course_schedule_init&subjects=&order_by=asc"
    course_source_url = "https://arts-sciences.catholic.edu/academics/courses/course-schedules/index.html"
    directory_api_urls = ['https://communications.catholic.edu/feeds/faculty-bios-a-g.json','https://communications.catholic.edu/feeds/faculty-bios-h-m.json','https://communications.catholic.edu/feeds/faculty-bios-n-z.json']
    calendar_url = "https://enrollment-services.catholic.edu/academic-calendar/academiccalendarpdf.html" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')


        if mode == 'course':
            yield scrapy.Request(self.course_api_url,callback=self.parse_course)

        elif mode == 'directory':
            for directory_api_url in self.directory_api_urls:
                directory_api_headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://ancient-medieval.catholic.edu',
                    'Referer': 'https://ancient-medieval.catholic.edu/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                    }
                yield scrapy.Request(url=directory_api_url,headers=directory_api_headers,callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_api_url,callback=self.parse_course)
            for directory_api_url in self.directory_api_urls:
                directory_api_headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://ancient-medieval.catholic.edu',
                    'Referer': 'https://ancient-medieval.catholic.edu/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                    }
                yield scrapy.Request(url=directory_api_url,headers=directory_api_headers,callback=self.parse_directory, dont_filter=True)


            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_api_url,callback=self.parse_course)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            for directory_api_url in self.directory_api_urls:
                directory_api_headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://ancient-medieval.catholic.edu',
                    'Referer': 'https://ancient-medieval.catholic.edu/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                    }
                yield scrapy.Request(url=directory_api_url,headers=directory_api_headers,callback=self.parse_directory, dont_filter=True)

        
        # All three (default)
        else:
            yield scrapy.Request(self.course_api_url,callback=self.parse_course)
            for directory_api_url in self.directory_api_urls:
                directory_api_headers = {
                    'Accept': '*/*',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Origin': 'https://ancient-medieval.catholic.edu',
                    'Referer': 'https://ancient-medieval.catholic.edu/',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                    }
                yield scrapy.Request(url=directory_api_url,headers=directory_api_headers,callback=self.parse_directory, dont_filter=True)

            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       
    @inline_requests
    def parse_course(self,response):
        """
        Parses course metadata to find all available subjects and terms,
        then iteratively requests schedules for every combination.
        """

        course_data_url = "https://webservices.cua.edu/course-information/course-info.php"
        headers = {
            "accept": "*/*",
            "origin": "https://arts-sciences.catholic.edu",
            "referer": "https://arts-sciences.catholic.edu/",
            "user-agent": "Mozilla/5.0",
        }
        
        # Step 1: Initialize metadata (extract Subject Codes and Term IDs)
        init_data = response.json()
        subjects = [row[1] for row in init_data.get("SUBJECTNAME", {}).get("DATA", [])]
        terms = [
            row[0]
            for row in init_data.get("COURSE_TERMS", {}).get("DATA", [])
        ]
    
        if not subjects or not terms:
            self.logger.error("Subjects or terms missing in init API")
            return
    
        self.logger.warning(
            f"Found {len(subjects)} subjects and {len(terms)} terms"
        )
    
        rows = []
        
        #Loop through every term × subject combination
        for term in terms:
            for subject in subjects:
                # Build course schedule API URL
                api_url = (
                    f"{course_data_url}"
                    f"?method=course_schedule"
                    f"&subjects={subject}"
                    f"&course_number="
                    f"&career="
                    f"&strm={term}"
                    f"&order_by=asc"
                )
    
                # Make API request for the specific subject and term
                api_response = yield scrapy.Request(
                    url=api_url,
                    headers=headers,
                    method="GET",
                    dont_filter=True,
                )
                

                 # Parse JSON response
                data = api_response.json()
    
                #xtract course details from API payload
                for dat in data["QRYNEW_SCHEDULE"]["DATA"]:
                    
                    # Course number (e.g., "205")
                    catalog_nbr = dat[0]      
                    # Subject code (e.g., "ACCT")
                    catlog_id = dat[2]   
                    # Course title       
                    course  = dat[15] 
                    # Course date range
                    dates = f'{dat[6]} - {dat[8]} {dat[10]} - {dat[11]}'
                    # Course location
                    loca = dat[23]

                    if "MAIN" in loca:
                        loca = "Main Campus"

                    rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_source_url,
                        "Course Name": f"{catlog_id}{catalog_nbr} {course}",
                        "Course Description": dat[14],
                        "Class Number": f"{catlog_id}{catalog_nbr}",
                        "Section": dat[5],
                        "Instructor": dat[17],
                        "Enrollment": "",
                        "Course Dates": dates,
                        "Location": loca,
                        "Textbook/Course Materials": "",
                    })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")
       
       
    def parse_directory(self, response):
        """
        Parses faculty JSON feeds. For each person, follows their profile link
        to scrape detailed contact information and titles.
        """

        rows = []
        data = response.json()
        for dat in data:
            product_links = dat.get('link','')

            # Request profile page to get deep details (Phone/Email/Title)
            res = requests.get(product_links)
            product_response = scrapy.Selector(text=res.text)
            name = product_response.xpath('//h1[@class="page-title"]/text()').get("").strip()
            name = re.sub(r'\d+', '', name).replace('(', '').replace(')', '').strip()
            titles = product_response.xpath('//h1[@class="page-title"]/following-sibling::h2/text()'
                ' | //h3[normalize-space()="Department"]/following-sibling::li/text()'
            ).getall()

            # normalize + split by comma
            parts = []
            for t in titles:
                if t.strip():
                    for p in t.split(","):
                        p = " ".join(p.split()).strip()
                        if p.lower().startswith("and "):
                            p = p[4:].strip()
                        parts.append(p)

            # remove exact duplicates (keep order)
            parts = list(dict.fromkeys(parts))

            # remove subset duplicates (Economics inside Emeritus Professor of Economics)
            parts = [
                p for p in parts if not any(
                    p != q and p.lower() in q.lower()
                    for q in parts
                )
            ]

            title = ", ".join(parts)
            email = product_response.xpath(
                "//a[starts-with(@href, 'mailto:')]/text()"
            ).get("").strip()

            raw_phone = product_response.xpath(
                "//strong[contains(text(),'Office Phone:')]/parent::p/text()"
            ).get("").strip()

            phone = ""

            if raw_phone:
                # Sometimes email appears in phone field
                if "@" in raw_phone:
                    email = raw_phone
                else:
                    match = re.search(r'(\+?\d[\d\s().-]+)', raw_phone)
                    if match:
                        phone = match.group(1).strip()

                        # Fix missing opening bracket: 202) 319-5653 → (202) 319-5653
                        if ")" in phone and "(" not in phone:
                            phone = re.sub(r'^(\d+)\)', r'(\1)', phone)
            if name and "First name Last name" not in name:
                rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL":  response.url,
                    "Name": name,
                    "Title": title,
                    "Email": email,
                    "Phone Number": phone,
                }) 
        
        directory_df = pd.DataFrame(rows)
        save_df(directory_df, self.institution_id, "campus")

    @inline_requests
    def parse_calendar(self, response):
        """
        Identifies PDF calendar links from the registrar page and 
        extracts event rows using pdfplumber.
        """
        rows = []

        # Pattern to identify the start of a new calendar event line
        WEEKDAY_PATTERN = re.compile(
            r"^(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),\s+[A-Za-z]+\s+\d{1,2}"
        )
        # Helper function to normalize PDF text
        def normalize(text):
            return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()
        # Each table row contains a link to a calendar PDF
        table_rows = response.css("table tbody tr")
        
        # Loop through each table row
        for row in table_rows:
            pdf_links = row.css('a[href$=".pdf"]')

            for link in pdf_links:
                term_name = link.css("::text").get("").strip()
                pdf_url = response.urljoin(link.attrib.get("href"))
                # Skip PDFs older than 2025
                year_match = re.search(r"(20\d{2})", term_name)

                if not year_match or int(year_match.group(1)) < 2025:
                    continue

                pdf_response = yield scrapy.Request(pdf_url, dont_filter=True)
                is_summer = "summer" in pdf_url.lower()

                # Summer pdf
                # Identify summer calendars by URL
                if is_summer:
                    # Open PDF from response bytes
                    with pdfplumber.open(io.BytesIO(pdf_response.body)) as pdf:
                        # Iterate through every page in the PDF
                        for page in pdf.pages:
                            text = page.extract_text()
                            
                            if not text:
                                continue
                            
                             # Split text into lines and normalize each line
                            lines = [normalize(l) for l in text.split("\n") if normalize(l)]
                            # These variables track the current event
                            current_date, current_desc = None, []

                            for line in lines:
                                
                                # If line starts with weekday + date,
                                if WEEKDAY_PATTERN.match(line):

                                    if current_date and current_desc:
                                        rows.append({
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": pdf_url,
                                            "Term Name": term_name,
                                            "Term Date": current_date,
                                            "Term Date Description": " ".join(current_desc),
                                        })

                                    parts = line.split(" ", 3)
                                    current_date = " ".join(parts[:3])
                                    current_desc = [line[len(current_date):].strip()]

                                else:

                                    if current_date:
                                        current_desc.append(line)

                            if current_date and current_desc:

                                rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": pdf_url,
                                    "Term Name": term_name,
                                    "Term Date": current_date,
                                    "Term Date Description": " ".join(current_desc),
                                })

                 # SPRING / FALL CALENDAR
                else:

                    current_calendar_name = ""
                    last_row = None

                    # Open PDF from response bytes
                    with pdfplumber.open(io.BytesIO(pdf_response.body)) as pdf:

                        # Iterate through every page in the PDF
                        for page in pdf.pages:

                            text = page.extract_text()

                            if text:
                                match = re.search(
                                    r"(Academic Calendar:\s+[A-Za-z\s()]+Semester\s+\d{4})",
                                    text
                                )

                                if match:
                                    current_calendar_name = match.group(1).strip()

                            tables = page.extract_tables()

                            for table in tables:

                                for r in table:

                                    if not r or len(r) < 2:
                                        continue
                                    # Extract date and description
                                    date_text = normalize(r[0] or "")
                                    desc_text = normalize(r[1] or "")

                                    if WEEKDAY_PATTERN.match(date_text):
                                        last_row = {
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": pdf_url,
                                            "Term Name": current_calendar_name.replace("Academic Calendar:", "").strip(),
                                            "Term Date": date_text,
                                            "Term Date Description": desc_text,
                                        }
                                        rows.append(last_row)

                                    elif desc_text and last_row and not WEEKDAY_PATTERN.match(date_text):
                                        last_row["Term Date Description"] += " " + desc_text

        save_df(pd.DataFrame(rows), self.institution_id, "calendar")
    
        
            
        
            
                