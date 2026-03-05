import io
import re
import json
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *


class SetonHillSpider(scrapy.Spider):

    name = "setonhill"
    institution_id = 258441978851649502
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://www.setonhill.edu/resources/offices/registrar/catalog-calendars-course-schedules.html"
    
    # DIRECTORY CONFIG
    directory_source_url = 'https://www.setonhill.edu/academics/faculty/'
    directory_url = "https://www.setonhill.edu/academics/faculty/index.json"
    directory_headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://www.setonhill.edu/academics/faculty/',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36'
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.setonhill.edu/academics/academic-calendar/index.html"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is available in the pdf so using pdfplumber for extracting data.

        - Directory and Calendar data extracting using scarpy
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
           yield scrapy.Request(url=self.directory_url, headers=self.directory_headers, callback=self.parse_directory)

        elif mode == "calendar":
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, headers=self.directory_headers, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url=self.directory_url, headers=self.directory_headers, callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url=self.directory_url, headers=self.directory_headers, callback=self.parse_directory)
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

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
        url = 'https://setonhill.policytech.com/docview/?app=pt&source=unspecified&docid=5255&public=true&fileonly=true'
        response = requests.get(url)
        pdf_file = io.BytesIO(response.content)

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if not text:
                    continue

                lines = text.split('\n')
                for line in lines:
                    parts = line.split()
                    match = re.search(r'([A-Z]{3,4})\s+(\d{3})\s+([0-9A-Z]{2})', line)
                    
                    if match:
                        subj, num, section = match.groups()
                        class_num = f"{subj}-{num}"
                        if class_num.startswith("T"):
                            class_num = class_num[1:]
                        
                        try:
                            for i, part in enumerate(parts):
                                if ("." in part and part.replace(".","").isdigit()) or part == "V":
                                    full_instructor = " ".join(parts[i+1 : i+3])
                                    if '$' in full_instructor:
                                        instructor = ''
                                    elif '2.0' in full_instructor:
                                        instructor = 'Rinchuse'
                                    else:
                                        instructor = full_instructor.strip()
                                        
                                    course_name = f'{class_num} {" ".join(parts[3:i])}'
                                    break
                            if instructor == '':
                                for i, part in enumerate(parts):
                                    if "." in part and part.replace(".","").isdigit() or part == "V":
                                        instructor = parts[i+1]
                                        if '$' in instructor:
                                            instructor = ''
                                        elif instructor == '2.0':
                                            instructor = 'Rinchuse'
                                        else:
                                            instructor = instructor
                
                                        course_name = f'{class_num} {" ".join(parts[3:i])}'
                                        break
                        except:
                            course_name = "Check PDF"
                        if instructor == 'Van':
                            instructor = 'Van aken'
                        if instructor == 'Scott':
                            instructor = 'Scott Eshman'
                        class_num = course_name.split()
                        class_num = class_num[0]
                        try:
                            begin_date = parts[-2]
                            end_date = parts[-1].replace("-", "")
                            course_dates = f"{begin_date} - {end_date}"

                            room = parts[-3]
                            bldg = parts[-4]
                            if room == 'instructor':
                                location = bldg
                            else:
                                location = f"{bldg} - {room}"
                            if (location == 'with - instructor'or location == '- - 09:00p' or 'Online' in location or '$' in location or 'announced' in location or 'instructor' in location or 'course' in location or 'with' in location):
                                location = ""
                        
                        except:
                            course_dates = ""
                            location = ""

                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_source_url,
                            "Course Name": course_name,
                            "Course Description": "",
                            "Class Number": class_num,
                            "Section": section,
                            "Instructor": instructor,
                            "Enrollment": '',
                            "Course Dates": course_dates,
                            "Location": location,
                            "Textbook/Course Materials": "",
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
        json_data =  json.loads(response.text)
        blocks = json_data.get('faculty', [])
        for block in blocks:
            tit = block.get('title', '').strip()
            dep = ', '.join(block.get('departments', [])).strip()
            if dep and tit:
                title = f'{tit}, {dep}'
            elif dep:
                title = dep
            elif tit:
                title = tit
            else:
                title = ''
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.directory_source_url,
            "Name": block.get('fullName', '').strip(),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.get('email', '').strip(),
            "Phone Number": block.get('phone', '').replace('Office:','').replace('Mobile', '').strip(),
            })
        
    # PARSE CALENDAR
    def parse_calendar(self,response):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """     
        blocks = response.xpath('//table/tbody/tr')
        for block in blocks:
            description = block.xpath('.//td[2]/text()').get('').strip()
            if description:
                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_source_url,
                    "Term Name":block.xpath('(./ancestor::table/preceding-sibling::h4)[last()]/text()').get(default='').replace('J-Term','January Term').replace('M-Term','March Term').strip(),
                    "Term Date": block.xpath('.//td[1]/strong/text()').get('').strip(),
                    "Term Date Description": re.sub(r'\s+',' ',description)
                })

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

        