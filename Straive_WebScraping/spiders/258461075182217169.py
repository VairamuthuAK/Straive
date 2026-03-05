import re
import io
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from pypdf import PdfReader


class MarianSpider(scrapy.Spider):
    
    name = "marian"
    institution_id = 258461075182217169
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://www.marianuniversity.edu/wp-content/uploads/2025/12/2025-2026-Academic-Catalog.pdf'

    # DIRECTORY CONFIG
    directory_source_url = "https://www.marianuniversity.edu/wp-content/themes/Avada-Child-Theme/staff-index.json"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.marianuniversity.edu/wp-content/uploads/2025/03/2025-2026-Academic-Calendar.pdf"
    
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
        junk_codes = [
        "A R T", "ASL AMERICAN SIGN LANGUAGE", "ATH ART THERAPY",
        "BAN BUSINESS ANALYTICS", "BIO BIOLOGY", "BUA BUSINESS ADMINISTRATION",
        "BUS BUSINESS", "CHE CHEMISTRY", "CMG CULTURAL, MEDIA, AND GENDER STUDIES",
        "COM COMMUNICATION", "CON CONSTRUCTION MANAGEMENT",
        "CRJ CRIMINAL JUSTICE", "CYB CYBERSECURITY", "CYT CYTOTECHNOLOGY",
        "DAT DATA SCIENCE", "DHY DENTAL HYGIENE",
        "DMS DIAGNOSTIC MEDICAL SONOGRAPHY",
        "ECE EARLY CHILDHOOD–ELEMENTARY EDUCATION",
        "EDR RELIGIOUS EDUCATION", "EDU PROFESSIONAL EDUCATION",
        "EME ELEMENTARY–MIDDLE EDUCATION", "ENG ENGLISH",
        "ESS EXERCISE AND SPORT SCIENCE", "FIN FINANCE",
        "FLA FOREIGN LANGUAGE", "FOS FORENSIC SCIENCE",
        "GEN GENERAL EDUCATION", "GEO GEOGRAPHY",
        "HCA HEALTH CARE ADMINISTRATION", "HIS HISTORY",
        "HOS HOMELAND SECURITY", "IDS INTERDISCIPLINARY STUDIES",
        "IPD INSTITUTE FOR PROFESSIONAL DEVELOPMENT",
        "JPN JAPANESE", "LDR LEADERSHIP", "MAT MATHEMATICS",
        "MGT MANAGEMENT", "MKT MARKETING", "NRS NURSING",
        "NTR NUTRITION", "NUR NURSING", "PBS PUBLIC SAFETY MANAGEMENT",
        "PHS PHYSICAL SCIENCE", "POS POLITICAL SCIENCE",
        "PSY PSYCHOLOGY", "RAD RADIOLOGIC TECHNOLOGY",
        "RDG READING TEACHER", "RMI RISK MANAGEMENT AND INSURANCE",
        "RST RESPIRATORY THERAPY", "SCI SCIENCE",
        "SCM SUPPLY CHAIN MANAGEMENT", "SCP SABRE FOUNDATIONS",
        "SEC MIDDLE–SECONDARY EDUCATION", "SOC SOCIOLOGY",
        "SOJ SOCIAL JUSTICE", "SPA SPANISH", "SPE SPECIAL EDUCATION",
        "SRM SPORT AND RECREATION MANAGEMENT", "SSS TRIO",
        "STA STUDY ABROAD", "SWK SOCIAL WORK",
        "TCH TEACHER EDUCATION",
        "TDE DIFFERENTIATED INSTRUCTION FOR ALL LEARNERS",
        "TEC INFORMATION TECHNOLOGY", "THA THANATOLOGY", "THE THEOLOGY"
        ]

        def clean_description(text):

            for code in junk_codes:
                text = re.sub(re.escape(code), '', text, flags=re.IGNORECASE)

            text = re.sub(r'\s+', ' ', text).strip()
            text = re.sub(r'^[,.\s]+', '', text)

            return text

        # Download PDF
        response = requests.get(self.course_sourse_url)
        response.raise_for_status()
        reader = PdfReader(io.BytesIO(response.content))

        # Pages to read
        page_ranges = [(157, 297)]

        target_pages = []
        for start, end in page_ranges:
            target_pages.extend(range(start - 1, end))

        for idx in target_pages:

            if idx >= len(reader.pages):
                continue

            page = reader.pages[idx]

            page_text = page.extract_text()

            if not page_text:
                continue

            lines = page_text.split('\n')
            current_title = ""
            current_description = []

            for line in lines:
                line = line.strip()

                if re.match(r'^[A-Z]{2,4}\s\d{3}(?:\s*,\s*\d{3})*', line):

                    if current_title:
                        description = clean_description(
                            " ".join(current_description)
                        )

                        class_match = re.match(
                            r'^([A-Z]{2,4}\s\d{3}(?:\s*,\s*\d{3})*)',
                            current_title
                        )
                        class_num = class_match.group(1) if class_match else ""


                        # Manual fixes
                        if current_title.startswith("FOS 101 is allowed"):
                            current_title = "FOS 101 Introduction to Forensic Science"

                        elif current_title.startswith("RAD 314 is the first"):
                            current_title = "RAD 314 Radiographic Practicum I"

                        elif current_title.startswith("RAD 334 is the third"):
                            current_title = "RAD 334 Radiographic Practicum III"

                        elif current_title.startswith("RAD 442 is the fourth"):
                            current_title = "RAD 442 Radiologic Practicum IV"

                        elif current_title.startswith("RAD 453 is the fifth"):
                            current_title = "RAD 453 Radiographic Practicum V"


                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_sourse_url,
                            "Course Name": re.sub(r'\s+', ' ', current_title),
                            "Course Description": description,
                            "Class Number": class_num,
                            "Section": "",
                            "Instructor": "",
                            "Enrollment": "",
                            "Course Dates": "",
                            "Location": "",
                            "Textbook/Course Materials": ""
                        })

                    current_title = line
                    current_description = []

                elif current_title:
                    if line and not line.isdigit():
                        current_description.append(line)


            # Save last record
            if current_title:
                description = clean_description(" ".join(current_description))
                class_match = re.match(
                    r'^([A-Z]{2,4}\s\d{3}(?:\s*,\s*\d{3})*)',
                    current_title
                )
                class_num = class_match.group(1) if class_match else ""
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_sourse_url,
                    "Course Name": re.sub(r'\s+', ' ', current_title),
                    "Course Description": description,
                    "Class Number": class_num,
                    "Section": "",
                    "Instructor": "",
                    "Enrollment": "",
                    "Course Dates": "",
                    "Location": "",
                    "Textbook/Course Materials": ""
                })

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

        json_data = json.loads(response.text)
        blocks = json_data['items']
        for block in blocks:
            url = block.get('permalink','')
            yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)
    
    def parse_directory_final(self,response):
        dept = response.xpath('//h4/text()').get('').strip()
        tit = response.xpath('//div[@class="title"]/div/text()').get('').strip()
        if dept and tit:
            title = f'{dept}, {tit}'
        elif dept:
            title = dept
        elif tit:
            title = tit
        else:
            title = ''
        name = response.xpath('//h2/text()').get('').strip()
        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": re.sub(r'\s+',' ',name),
        "Title": re.sub(r'\s+',' ',title),
        "Email": response.xpath('//div[@class="single-contact-email"]/a/text()').get('').strip(),
        "Phone Number": response.xpath('//div[@class="single-staff-contact-info"]/h3/text()').get('').strip(),
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

        calendar_source_url = self.calendar_source_url
        def clean_text(text):
            if text:
                text = re.sub(r'\s+', ' ', str(text)).strip()
                text = re.sub(r'([a-zA-Z]+)(\d+)', r'\1 \2', text)
                return text

            return ""

        def is_actual_date(text):
            months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
                    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
            pattern = r'(' + '|'.join(months) + r')'

            return (
                bool(re.search(pattern, text.upper()))
                and bool(re.search(r'\d', text))
            )

        try:
            response = requests.get(calendar_source_url)
            response.raise_for_status()
            pdf_file = BytesIO(response.content)

            with pdfplumber.open(pdf_file) as pdf:
                current_term = "FALL 2025 SEMESTER I"
                last_valid_date = ""
                for page in pdf.pages:
                    tables = page.extract_tables()
                    for table in tables:
                        for row in table:
                            clean_row = [
                                clean_text(cell)
                                for cell in row
                                if cell is not None
                            ]
                            if len(clean_row) < 2:
                                continue

                            full_row_text = " ".join(clean_row).upper()
                            # Detect term headers
                            if any(word in full_row_text for word in
                                ["SEMESTER", "SESSIONS", "WINTERIM"]):
                                if not is_actual_date(full_row_text):
                                    current_term = clean_text(clean_row[0])
                                    continue

                            # Skip internal headers
                            if "DATE:" in full_row_text or "DAY:" in full_row_text:
                                continue

                            description = clean_row[0]
                            potential_date = ""
                            for cell in clean_row[1:]:
                                if is_actual_date(cell):
                                    potential_date = cell

                                    break
                            if potential_date:
                                last_valid_date = potential_date

                            if description.upper() == current_term.upper():
                                continue

                            if not description:
                                continue

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": calendar_source_url,
                                "Term Name": current_term,
                                "Term Date": last_valid_date,
                                "Term Date Description": description,
                            })

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": calendar_source_url,
                "Term Name": "FALL 2025 SEMESTER I",
                "Term Date": "Oct. 31, 2025",
                "Term Date Description": "Freshmen (0-29 credits)",
            })


            self.calendar_rows.append({

                "Cengage Master Institution ID": self.institution_id,
                "Source URL": calendar_source_url,
                "Term Name": "SPRING 2026 SEMESTER II",
                "Term Date": "Mar. 20, 2026",
                "Term Date Description": "Freshmen (0-29 credits)",
            })

        except Exception as e:
            return

    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            weekdays = [
            "MONDAY", "TUESDAY", "WEDNESDAY",
            "THURSDAY", "FRIDAY", "SATURDAY", "SUNDAY"
            ]
            df = df[~df["Term Date"].str.upper().isin(weekdays)]
            df = df.drop_duplicates(
                subset=["Term Name", "Term Date", "Term Date Description"]
            )
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")
        