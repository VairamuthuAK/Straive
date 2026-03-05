import re
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO


class ColbySpider(scrapy.Spider):

    name = "colby"
    institution_id = 258441686684821462
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://www.colby.edu/registrar/CWcurricq23.html"
    course_url = 'https://cxweb.colby.edu/regist/CWcurricq/CWcurricq.cis'
    course_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Content-Type': 'application/x-www-form-urlencoded',
    'Origin': 'https://www.colby.edu',
    'Referer': 'https://www.colby.edu/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = 'https://www.colbycc.edu/faculty-staff/index.html'

    # CALENDAR CONFIG
    calendar_source_urls = ["https://www.colbycc.edu/academics/academic-calendar/2025-26.pdf",
                            "https://www.colbycc.edu/academics/academic-calendar/2026-27.pdf"]
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course url does not return proper response in scrapy so 
        using request for getting data.

        - Directory URL is return proper response so using scrapy
        for collecting data.

        - Calendar data is provided as PDFs, so `parse_calendar` uses
        `pdfplumber` to extract the relevant information.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
           yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)
            self.parse_calendar()

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)
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
        terms = ['JP','SP','FA']
        for term in terms:
            course_payload = f'acyr=2425&deptsel=LIKE%20\'%25\'&crssel=LIKE%20\'%25\'&statsel=LIKE%20\'%25\'&facsel=%3E%3D%200&areasel=LIKE%20\'%25\'&divsel=LIKE%20\'%25\'&bldgsel=\'%25\'&dayssel=LIKE%20\'%25\'&btimesel=%3E%3D%200&etimesel=%3E%3D%200&writingsel=LIKE%20\'%25\'&labscisel=LIKE%20\'%25\'&froshsel=LIKE%20\'%25\'&sess={term}&acadyr=2425&ccrs=&cdept=ANY&cfac=ANY&carea=*&cdiversity=*&cwriting=*&option=include&dayopts=%23%23%23%23%23%23%23&btopt=%3D%20&btime=%25&etopt=%3D%20&etime=%25&bldg='
            response = requests.request("POST", self.course_url, headers=self.course_headers, data=course_payload)

            if re.search(r'crs_no\[\d+\]\s*=\s*"[\s\S]*?faculty\[\d+\]\s*=\s*".*?";',response.text):
                blocks = re.findall(r'crs_no\[\d+\]\s*=\s*"[\s\S]*?faculty\[\d+\]\s*=\s*".*?";',response.text)

                for block in blocks:
                    if re.search(r'crs_no\[\d+\]\s\=\s\"(.*?)"',block):
                        class_num = re.findall(r'crs_no\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                    else:
                        class_num = ''

                    if re.search(r'sec_no\[\d+\]\s\=\s\"(.*?)"',block):
                        section = re.findall(r'sec_no\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                    else:
                        section = ''

                    if re.search(r'title\[\d+\]\s\=\s\"(.*?)"',block):
                        title = re.findall(r'title\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                    else:
                        title = ''


                    if re.search(r'room\[\d+\]\s\=\s\"(.*?)"',block):
                        room = re.findall(r'room\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                        if room == '&nbsp;':
                            room = ''
                    else:
                        room = ''

                    if re.search(r'no_reg\[\d+\]\s\=\s\"(.*?)"',block):
                        no_reg = re.findall(r'no_reg\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                        if no_reg == '&nbsp;':
                            no_reg = ''
                    else:
                        no_reg = ''

                    if re.search(r'max_reg\[\d+\]\s\=\s\"(.*?)"',block):
                        max_reg = re.findall(r'max_reg\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                        if max_reg == '&nbsp;':
                            max_reg = ''
                    else:
                        max_reg = ''

                    if re.search(r'faculty\[\d+\]\s\=\s\"(.*?)"',block):
                        faculty = re.findall(r'faculty\[\d+\]\s\=\s\"(.*?)"',block)[0].strip()
                        if faculty == '&nbsp;':
                            faculty = ''
                    else:
                        faculty = ''

                    name = f'{class_num} - {title}'
                    if max_reg == '':
                        enrollment = ''
                    else:
                        enrollment = f'{no_reg} of {max_reg}'
                    
                    self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_source_url,
                        "Course Name": re.sub(r'\s+',' ',name),
                        "Course Description": '',
                        "Class Number": class_num,
                        "Section": section,
                        "Instructor": re.sub(r'\s+',' ',faculty),
                        "Enrollment": enrollment,
                        "Course Dates": '',
                        "Location": room,
                        "Textbook/Course Materials": '',
                    })

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
        urls = response.xpath('//td[@headers="name"]/a/@href').getall()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url,callback=self.parse_directory_final)
    
    def parse_directory_final(self,response):
            tit = response.xpath('//h3/text()').get('').strip()
            dep = response.xpath('//h4/text()').get('').strip()

            if tit and dep:
                title = f'{tit}, {dep}'
            elif tit:
                title = tit
            elif dep:
                title = dep
            else:
                title = ''

            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": response.xpath('//h2/text()').get('').strip(),
            "Title": re.sub(r'\s+',' ',title),
            "Email": response.xpath('//div[contains(text(),"Email")]/following-sibling::div/a/text()').get('').strip(),
            "Phone Number": response.xpath('//div[contains(text(),"Phone #1")]/following-sibling::div/a/text()').get('').strip(),
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
        for pdf_url in self.calendar_source_urls:
            if '2025' in pdf_url:
                def clean_noise(text):
                    if not text:
                        return ""

                    # Remove footer noise
                    if "NITY COLLEGE" in text:
                        text = text.split("NITY COLLEGE")[0]

                    # Remove calendar grid noise
                    text = re.sub(r'T F S S M.*', '', text)

                    # Fix truncated "(final"
                    text = re.sub(
                        r'\(final$',
                        '(final exams) – Grades Due August 3',
                        text
                    )

                    # Normalize spaces
                    text = re.sub(r'\s+', ' ', text).strip()
                    return text

                response = requests.get(pdf_url)
                pdf_content = BytesIO(response.content)

                all_rows = []
                months = [
                    "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
                    "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY"
                ]

                current_term = ""
                current_month = ""

                with pdfplumber.open(pdf_content) as pdf:
                    for page in pdf.pages:
                        width, height = page.width, page.height

                        left_col = page.within_bbox((0, 0, width / 2, height))
                        right_col = page.within_bbox((width / 2, 0, width, height))

                        for column in [left_col, right_col]:
                            text = column.extract_text(x_tolerance=2, y_tolerance=3)
                            if not text:
                                continue

                            lines = text.split('\n')

                            for line in lines:
                                line = line.strip()
                                if not line:
                                    continue

                                # Skip junk lines
                                if line.startswith('(') or line.startswith('['):
                                    continue

                                # Term detection
                                term_match = re.search(r'(FALL|SPRING|SUMMER)\s+202\d', line, re.IGNORECASE)
                                if term_match:
                                    current_term = term_match.group(0).upper()
                                    continue

                                # Month detection
                                if line.upper() in months:
                                    current_month = line.capitalize()
                                    continue

                                # Date + Description
                                date_match = re.match(r'^(\d{1,2}(?:[-\d,\s/]*))\s+(.*)', line)
                                if date_match:
                                    date_val = date_match.group(1).strip()
                                    desc_val = clean_noise(date_match.group(2).strip())

                                    # Fix split descriptions
                                    if len(desc_val.split()) < 2 and desc_val.lower() in ["classes", "exams)"]:
                                        if all_rows:
                                            all_rows[-1]["Term Date Description"] += " " + date_val + " " + desc_val
                                            continue

                                    if len(date_val.split()) > 3:
                                        continue

                                    final_month = current_month

                                    # Spring December → January fix
                                    if current_term == "SPRING 2026" and current_month == "December":
                                        final_month = "January"

                                    # Auto Spring detection
                                    if current_month in ["March", "April", "May"]:
                                        current_term = "SPRING 2026"

                                    if not final_month:
                                        final_month = "August"

                                    
                                    if current_term:
                                        all_rows.append({
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": pdf_url,
                                            "Term Name": current_term,
                                            "Term Date": f"{final_month} {date_val}",
                                            "Term Date Description": desc_val
                                        })

                                # Continuation lines
                                elif all_rows and len(line) > 2:
                                    if not any(m in line.upper() for m in months) and not re.search(r'[SMTWTFS]{4,}', line):
                                        all_rows[-1]["Term Date Description"] += " " + clean_noise(line)

                for row in all_rows:
                    desc = row["Term Date Description"]

                    # Remove NITY COLLEGE footer fragments
                    desc = re.sub(r'NITY COLLEGE.*', '', desc, flags=re.IGNORECASE)

                    # Remove calendar grid symbols
                    desc = re.sub(r'T F S S M.*', '', desc)

                    # Normalize spaces
                    desc = re.sub(r'\s+', ' ', desc).strip()

                    # Fix truncated '(final' anywhere
                    desc = re.sub(r'\(final\)?', '(final exams) – Grades Due August 3', desc, flags=re.IGNORECASE)

                    row["Term Date Description"] = desc


            else:
                def clean_noise(text):
                    if not text:
                        return ""

                    # Remove footer / college name noise
                    if "NITY COLLEGE" in text:
                        text = text.split("NITY COLLEGE")[0]

                    # Remove calendar grid symbols
                    text = re.sub(r'T F S S M.*', '', text)

                    # Fix truncated "(final"
                    text = re.sub(
                        r'\(final$',
                        '(final exams) – Grades Due August 3',
                        text
                    )

                    # Normalize spaces
                    text = re.sub(r'\s+', ' ', text).strip()
                    return text

                response = requests.get(pdf_url)
                pdf_content = BytesIO(response.content)

                all_rows = []
                months = [
                    "AUGUST", "SEPTEMBER", "OCTOBER", "NOVEMBER", "DECEMBER",
                    "JANUARY", "FEBRUARY", "MARCH", "APRIL", "MAY", "JUNE", "JULY"
                ]

                current_term = ""
                current_month = ""

                with pdfplumber.open(pdf_content) as pdf:
                    for page in pdf.pages:
                        width, height = page.width, page.height

                        left_col = page.within_bbox((0, 0, width / 2, height))
                        right_col = page.within_bbox((width / 2, 0, width, height))

                        for column in [left_col, right_col]:
                            text = column.extract_text(x_tolerance=2, y_tolerance=3)
                            if not text:
                                continue

                            lines = text.split('\n')

                            for line in lines:
                                line = line.strip()
                                if not line:
                                    continue

                                # Skip bracket lines
                                if line.startswith('(') or line.startswith('['):
                                    continue

                                # Term detection
                                term_match = re.search(r'(FALL|SPRING|SUMMER)\s+202\d', line, re.IGNORECASE)
                                if term_match:
                                    current_term = term_match.group(0).upper()
                                    continue

                                # Month detection
                                if line.upper() in months:
                                    current_month = line.capitalize()
                                    continue

                                # Date + description
                                date_match = re.match(r'^(\d{1,2}(?:[-\d,\s/]*))\s+(.*)', line)
                                if date_match:
                                    date_val = date_match.group(1).strip()
                                    desc_val = clean_noise(date_match.group(2).strip())

                                    # Fix split descriptions like "Classes"
                                    if len(desc_val.split()) < 2 and desc_val.lower() in ["classes", "exams)"]:
                                        if all_rows:
                                            all_rows[-1]["Term Date Description"] += " " + date_val + " " + desc_val
                                            continue

                                    if len(date_val.split()) > 3:
                                        continue
                                    if desc_val.startswith('(') or desc_val.startswith('['):
                                        continue

                                    final_month = current_month

                                    # December → January fix for Spring
                                    if "SPRING" in current_term and current_month == "December":
                                        final_month = "January"

                                    # Auto Spring detection
                                    if final_month in ["March", "April", "May"]:
                                        current_term = "SPRING 2027"

                                    if not final_month:
                                        final_month = "August"

                

                                    if current_term:
                                        all_rows.append({
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": pdf_url,
                                            "Term Name": current_term,
                                            "Term Date": f"{final_month} {date_val}",
                                            "Term Date Description": desc_val
                                        })

                                # Continuation lines
                                elif all_rows and len(line) > 2:
                                    if not any(m in line.upper() for m in months) and not re.search(r'[SMTWTFS]{4,}', line):
                                        all_rows[-1]["Term Date Description"] += " " + clean_noise(line)

                for row in all_rows:
                    desc = row["Term Date Description"]

                    # Remove NITY COLLEGE footer fragments
                    desc = re.sub(r'NITY COLLEGE.*', '', desc, flags=re.IGNORECASE)

                    # Remove calendar grid symbols
                    desc = re.sub(r'T F S S M.*', '', desc)

                    # Normalize spaces
                    desc = re.sub(r'\s+', ' ', desc).strip()

                    # Fix truncated '(final' anywhere
                    desc = re.sub(r'\(final\)?', '(final exams) – Grades Due August 3', desc, flags=re.IGNORECASE)

                    row["Term Date Description"] = desc

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

        