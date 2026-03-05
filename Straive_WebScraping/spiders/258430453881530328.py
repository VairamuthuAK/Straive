import re
import io
import fitz
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from concurrent.futures import ThreadPoolExecutor


class PensacolastateSpider(scrapy.Spider):

    name = "pensacolastate"
    institution_id = 258430453881530328
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://pensacolastate.edu/course-search/"
    course_payload = 'course=&primary_instructor_last_name=&academic_period=Spring%202026%25&campus_locations=&academic_level=&delivery_mode=&academic_units=&course_section_start_time_military=0500&course_section_end_time_military=2359&submit-search='
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'cache-control': 'max-age=0',
    'content-type': 'application/x-www-form-urlencoded',
    'origin': 'https://pensacolastate.edu',
    'referer': 'https://pensacolastate.edu/course-search/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = "https://www.pensacolastate.edu/wp-content/themes/dt-the7-child/page-templates/custom/facultySearch2021.php"
    directory_payload = 'searchName=&searchDept='
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.pensacolastate.edu/docs/calendars_schedules/2025/Academic-Calendar-2025-2026-042325.pdf"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and Directory data scraped using Scrapy

        - Calendar data is available in the pdf so using pdfplumber

        """

        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_url,method='POST',body=self.course_payload,headers = self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,method='POST',body=self.directory_payload,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_url,method='POST',body=self.course_payload,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,method='POST',body=self.directory_payload,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            yield scrapy.Request(url = self.course_url,method='POST',body=self.course_payload,headers = self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,method='POST',body=self.directory_payload,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_url,method='POST',body=self.course_payload,headers = self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,method='POST',body=self.directory_payload,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

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
        
        blocks = response.xpath('//tr[@width="100%"]')
        temp_data = []
        pdf_urls = []
        for block in blocks:
            course_name = block.xpath('.//td[1]/p[1]/strong/a/text()').get('').strip()
            desc_url = block.xpath('.//td[1]/p[1]/strong/a/@href').get('').strip()

            if 'CIS-4358' in desc_url: continue
            item = {
                "course_name": course_name,
                "class_num": course_name.split('-')[0].strip(),
                "section": block.xpath('.//strong[contains(text(),"Section:")]/parent::p/text()').get('').strip(),
                "instructor": block.xpath('.//strong[contains(text(),"Instructor:")]/parent::p/text()').get('').strip(),
                "location": block.xpath('.//strong[contains(text(),"Room Number:")]/parent::p/text()').get('').strip(),
                "enrollment": '',
                "course_dates": block.xpath('.//td[1]/p[3]/strong/text()').get('').split('(')[-1].split(')')[0].strip(),
                "desc_url": desc_url
            }
            temp_data.append(item)
            if '.pdf' in desc_url: pdf_urls.append(desc_url)

        # Phase 2: High Speed PDF Extraction
        session = requests.Session()
        
        with ThreadPoolExecutor(max_workers=20) as executor:
            def _extract(url):
                try:   
                        r = session.get(url, timeout=10)
                        with fitz.open(stream=io.BytesIO(r.content), filetype="pdf") as doc:
                            full_text = doc[0].get_text("text")
                            description = re.split(r'Semester\s+O[ﬀf]+ered|Offered|Credits', re.split(r'Course\s+Descrip\S*\s*:*', full_text, flags=re.I)[1], flags=re.I)[0].replace('\r','').replace('\n','').replace('\t','').strip()
                            corrections = {
                                r'5c': 'tic',       
                                r'5onal': 'tional', 
                                r'5on': 'tion',     
                                r'5ca': 'tica',     
                                r'2ce': 'tice',     
                                r'2ca': 'tica',     
                                r'4ng': 'tting',    
                                r'2n': 'tin',
                                r'9cal': 'tical',                
                                r'9ng': 'ing',
                                r'2on': 'tion',  
                                r'9on': 'tion', 
                                r'ac3vi3es' : 'activities',
                                r'3ng' : 'ting',
                                r'9es': 'ties',
                                r'9v': 'iv',        
                                r'3on': 'tion',     
                                r'3iv': 'itiv',     
                                r'cra=': 'craft',   
                                r'mi<ed': 'mitted', 
                                r':on': 'tion',     
                                r':ng': 'ting',     
                            }
                            for pattern, replacement in corrections.items():
                                description = re.sub(pattern, replacement, description)

                            description = re.sub(r'\s+', ' ', description).strip()
                        
                        return (url, description)
                except: return (url, "")
            
            desc_map = dict(list(executor.map(_extract, pdf_urls)))

        # Phase 3: Final Append
        for item in temp_data:
            description = desc_map.get(item['desc_url'], "")
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": re.sub(r'\s+',' ', item['course_name']),
                "Course Description": re.sub(r'\s+',' ', description),
                "Class Number": item['class_num'],
                "Section": item['section'],
                "Instructor": re.sub(r'\s+',' ', item['instructor']),
                "Enrollment": '',
                "Course Dates": re.sub(r'\s+',' ', item['course_dates']),
                "Location": re.sub(r'\s+',' ', item['location']),
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
        blocks = response.xpath('//div[@class="s-resultPod"]')
        for block in blocks:
        
            title = ', '.join(block.xpath('.//div[@class="s-resultRow s-resultDept"]//text()').getall()).strip()
            name = block.xpath('.//div[@class="s-resultRow s-resultName"]/text()').get('').strip()
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": 'https://www.pensacolastate.edu/faculty-staff/facultystaffdept-directory/',
            "Name": re.sub(r'\s+',' ',name),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.xpath('.//div[@class="s-resultRow s-resultEmail"]/a/text()').get('').strip(),
            "Phone Number": block.xpath('.//div[@class="s-resultRow s-resultPhone"]/a/text()').get('').strip(),
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

        terms = ["Fall 2025", "Spring 2026", "Summer 2026"]
        sessions = ["Session A", "Session B", "Session C", "Session D"]
        headers = [f"{t} {s}" for t in terms for s in sessions]

        def clean_date(date):
            date = re.sub(r'\s+', ' ', date).strip()
            date = date.replace("Aug 19 Add Aug 22 Drop", "Aug 19 Add - Aug 22 Drop")
            date = date.replace("Jan 12 Add Jan 15 Drop", "Jan 12 Add - Jan 15 Drop")
            date = date.replace("Jul 28 Aug 11", "Jul 28 - Aug 11")
            date = date.replace("Apr 22 May 6", "Apr 22 - May 6")
            return date

        pdf_url = self.calendar_source_url
        response = requests.get(pdf_url)
        response.raise_for_status()


        with pdfplumber.open(io.BytesIO(response.content)) as pdf:

            table = pdf.pages[0].extract_table()
            for row in table:
                if not row or not row[0]:
                    continue

                description = re.sub(r'\s+', ' ', row[0]).strip()

                if any(x in description for x in
                    ["Academic Dates", "Commencement", "HOLIDAYS", "Final Exams"]):
                    continue

                date_cols = [
                    c for c in row[1:]
                    if c and str(c).strip().lower() != "none"
                    and str(c).strip() != ""
                ]

                if not date_cols:
                    continue

                if len(date_cols) == 3:

                    for i, date in enumerate(date_cols):

                        for s_idx in range(4):

                            clean = clean_date(date)
                            desc = description.replace(
                                "Deadlines to Pay Fees See Details Below",
                                "Deadlines to Pay Fees"
                            )

                            self.calendar_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": pdf_url,
                                "Term Name": headers[i * 4 + s_idx],
                                "Term Date": clean,
                                "Term Date Description": desc
                            })


                elif len(date_cols) == 12:
                    for i, date in enumerate(date_cols):

                        clean = clean_date(date)
                        desc = description.replace(
                            "Deadlines to Pay Fees See Details Below",
                            "Deadlines to Pay Fees"
                        )

                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": headers[i],
                            "Term Date": clean,
                            "Term Date Description": desc
                        })

                else:
                    for i, date in enumerate(row[1:]):

                        if i >= len(headers):
                            break

                        if not date or str(date).strip().lower() == "none":
                            continue
                        clean = clean_date(date)
                        desc = description.replace(
                            "Deadlines to Pay Fees See Details Below",
                            "Deadlines to Pay Fees"
                        )
                        self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": pdf_url,
                            "Term Name": headers[i],
                            "Term Date": clean,
                            "Term Date Description": desc
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
        