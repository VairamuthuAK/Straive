import re
import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *


class GraceSpider(scrapy.Spider):
    
    name = "grace"
    institution_id = 258452144888244181
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://www.grace.edu/academics/registrar/class-schedules/"
    course_headers ={
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'referer': 'https://ce.uhcc.hawaii.edu/search/publicCourseAdvancedSearch.do?method=doPaginatedSearch&showInternal=false&orgUnitFilterString=school&selectedOrgUnit=PO0001&courseSearch.courseDescriptionKeyword=&courseSearch.courseCategoryStringArray=0&courseSearch.deliveryMethodString=&courseSearch.campusStringArray=0&courseSearch.filterString=all',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = "https://www.grace.edu/about/grace-college/faculty-directory/"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.grace.edu/academics/grace-college-academic-calendar/"
    calendar_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Coourse and Calendar data is avaiable in the pdf so using pdfplumber
        for extacting data.

        - Directory data getting using scrapy 
       
        """

        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            # self.parse_course()
        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_url,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

    # PARSE COURSE
    def parse_course(self,response):
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

        urls = ['https://portal.grace.edu/ics/gra_public/class-schedule-undergraduate-fall-2025.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-undergraduate-spring-2026.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-undergraduate-summer-2026.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-graduate-fall-2025.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-graduate-spring-2026.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-graduate-summer-2026.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-seminary-fall-2025.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-seminary-spring-2026.pdf', 'https://portal.grace.edu/ics/gra_public/class-schedule-seminary-summer-2026.pdf']
        for url in urls:
            response = requests.get(url)
            pdf_content = io.BytesIO(response.content)

            with pdfplumber.open(pdf_content) as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    
                    if table:
                        for row in table[1:]:
                            if not row or len(row) < 5:
                                continue
                            
                            class_num = str(row[2]).strip() if row[2] else ""
                            parts = class_num.split()

                            try:
                                if parts:
                                    class_num = f'{parts[0]} {parts[1]}'
                                    section = parts[2]
                            except:
                                pass

                            name = str(row[3]).strip() if row[3] else ""
                            instructor = str(row[4]).strip() if row[4] else ""
                            start_date = str(row[5]).strip() if row[5] else ""
                            end_date = str(row[6]).strip() if row[6] else ""
                            course_name = f"{class_num} {name}".strip()
                            course_dates = f'{start_date} - {end_date}'
                            if 'Date' not in course_dates:
                                if class_num:
                                    self.course_rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": url,
                                    "Course Name": re.sub(r'\s+',' ',course_name),
                                    "Course Description": "",
                                    "Class Number": re.sub(r'\s+',' ',class_num),
                                    "Section": re.sub(r'\s+',' ',section),
                                    "Instructor": re.sub(r'\s+',' ',instructor),
                                    "Enrollment": "",
                                    "Course Dates": re.sub(r'\s+',' ',course_dates),
                                    "Location": '',
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
        urls = response.xpath('//div[@class="c-profile__meta"]/a/@href').getall()
        for url in urls:
            yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)

        next_page = response.xpath('//a[contains(text(),"Next Page »")]/@href').get()
        if next_page:
            yield response.follow(next_page, callback=self.parse_directory)
        
    def parse_directory_final(self,response):
        title = response.xpath('//h1/following-sibling::p/text()').get('').strip()
        email = response.xpath('//div[@class="c-details-list__content"]/a/text()').get('').strip()
        name = response.xpath('//h1/text()').get('').strip()
        phone = response.xpath('//p[contains(text(),"800")]/text()').get('').strip()
        if phone == '':
            phone = response.xpath('//p[contains(text(),"574")]/text()').get('').strip()
            if phone == '':
                phone = response.xpath('//p[contains(text(),"616")]/text()').get('').strip()

        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": re.sub(r'\s+',' ',name),
        "Title": re.sub(r'\s+',' ',title),
        "Email": email,
        "Phone Number":phone
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

        urls = [
        'https://www.grace.edu/wp-content/uploads/2025/03/25-26-Academic-Calendar.pdf',
        'https://www.grace.edu/wp-content/uploads/2025/10/26-27-academic-calendar-final.pdf'
        ]
        for url in urls:
            is_25_26 = '25-26' in url
            response = requests.get(url)
            
            with pdfplumber.open(io.BytesIO(response.content)) as pdf:
                current_month = ""
                
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text: continue
                    
                    for line in text.split('\n'):
                        line = line.strip()
                        
                        month_match = re.search(r'^(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}', line)
                        if month_match:
                            current_month = month_match.group(1)
                            continue
                        match = re.match(r'^([\d\–\-/]+)\s+([A-Z/a-z-]+\s+)?(.*)', line)
                        
                        if match and current_month:
                            day_val = match.group(1)
                            desc = match.group(3) if match.group(3) else match.group(2)
                            
                            if not desc or any(x in desc.upper() for x in ["CALENDAR FOR", "ACADEMIC CALENDAR"]):
                                continue

                            year_suffix = "2025" if (is_25_26 and current_month in ['August', 'September', 'October', 'November', 'December']) else \
                                        "2026" if (is_25_26 and current_month in ['January', 'February', 'March', 'April', 'May', 'June', 'July']) else \
                                        "2026" if (not is_25_26 and current_month in ['August', 'September', 'October', 'November', 'December']) else \
                                        "2027"
                            
                            if current_month in ['August', 'September', 'October', 'November', 'December']:
                                term_name = f"Fall Semester {year_suffix}"
                            elif current_month in ['January', 'February', 'March', 'April']:
                                term_name = f"Spring Semester {year_suffix}"
                            elif current_month == "May":
                                term_name = f"Spring Semester {year_suffix}"
                            elif current_month in ['June', 'July']:
                                term_name = f"Summer Semester {year_suffix}"
                            else:
                                term_name = "Academic Calendar"

                            if current_month == "May":
                                if any(x in day_val for x in ["10", "11", "20/21", "21/22", "20", "21", "22"]) and "May Term" in desc:
                                    term_name = f"Tentative May Term {year_suffix}"
                                elif ("24" in day_val or "25" in day_val) and "Summer" in desc:
                                    term_name = f"Summer Semester {year_suffix}"

                            if desc== 'as':
                                continue
                            if desc == 'Break' and current_month == 'December':
                                desc = 'Christmas Break'
                            if desc == 'Break' and current_month == 'March':
                                desc = 'Spring Break'
                            if current_month == 'May' and day_val == '26':
                                term_name = 'Summer Semester 2027'
                            if current_month == 'May' and day_val == '31':
                                term_name = 'Summer Semester 2027'
                            if current_month == 'May' and day_val == '27':
                                term_name = 'Summer Semester 2026'   

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": url,
                                "Term Name": re.sub(r'\s+',' ',term_name),
                                "Term Date": f"{current_month} {day_val}",
                                "Term Date Description": re.sub(r'\s+', ' ', desc).strip(),
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
        