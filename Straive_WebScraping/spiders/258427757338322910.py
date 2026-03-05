import re
import json
import time
import pytz
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO
from datetime import datetime
from inline_requests import inline_requests
from dateutil.relativedelta import relativedelta


class NpSpider(scrapy.Spider):
    name = "np"
    institution_id = 258427757338322910

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://np.edu/admissions-aid/class-schedules"
    directory_source_url = "https://np.edu/about/faculty-staff-directory/"
    calendar_url = "https://cal.np.edu/"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)

    # Parse methods UNCHANGED from your original
    @inline_requests
    def parse_course(self, response):
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
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        # List to store parsed course records
        course_rows = []
        
        # Regex pattern 1:
        # Matches rows with class number, dates, room, instructor, status
        pattern1 = re.compile(
            r'^(\d+)\s+'                                # 1 class_no
            r'(?:\S+\s+)*?'                             # optional misc tokens
            r'(?:[MTWRFS\-]+\s+)?'                      # optional days
            r'(?:\d{2}:\d{2}[AP]M\s+\d{2}:\d{2}[AP]M\s+)?'  # optional time
            r'(\d{2}/\d{2}/\d{4})\s+'                   # 2 start_date
            r'(\d{2}/\d{2}/\d{4})\s+'                   # 3 end_date
            r'([\w\-]+)\s+'                             # 4 room
            r'([A-Za-z .]+)\s+'                         # 5 instructor
            r'(Open|Closed)$'                           # 6 status
        )

        # Regex pattern:
        # Full row with class number, delivery type, title, dates, instructor
        pattern = re.compile(
            r'^(\d+)\s+'                               # 1 class_no
            r'([A-Z\-]+)'                              # 2 type (INDEPEND, ONL, WEB-ENH, BLEND)
            r'(?:(\S+-\d+-\d+)\s+)?'                   # 3 optional course_id
            r'(.*?)\s+'                                # 4 optional title (non-greedy)
            r'(?:\d+\.\d+)?\s*'                        # optional credits (non-capturing)
            r'(?:ONL\s+)?'                             # optional ONL
            r'(?:[MTWRFS\-]+\s+)?'                     # optional days
            r'(?:\d{2}:\d{2}[AP]M\s+\d{2}:\d{2}[AP]M\s+)?'  # optional time
            r'(\d{2}/\d{2}/\d{4})\s+'                  # 5 start_date
            r'(\d{2}/\d{2}/\d{4})\s+'                  # 6 end_date
            r'(?:([\w\-]+)\s+)?'                       # 7 optional room
            r'(.+?)\s+'                                # 8 instructor
            r'(Open|Closed)$'                          # 9 status
        )

        # Regex pattern 2:
        # Simplified rows with dates, room, instructor, status
        pattern2 = re.compile(
            r'^'
            r'([A-Z\-]+)?\s*'                   # optional days (letters + dash)
            r'(?:'                               # optional times block
                r'(\d{2}:\d{2}[AP]M)\s+'
                r'(\d{2}:\d{2}[AP]M)\s+'
            r')?'
            r'(\d{2}/\d{2}/\d{4})\s+'           # start_date
            r'(\d{2}/\d{2}/\d{4})\s+'           # end_date
            r'([A-Z0-9-]+)\s+'                  # room (or ONLINE)
            r'([A-Za-z-]+)?\s*'                 # optional instructor
            r'(Open|Closed)'                     # status
            r'$'
        )
        
        # State variables reused while parsing PDF lines
        DEPT = ''
        COURSE_NAME = ''
        CLASS_NO = ''
        SEC = ''
        START_DATE = ''
        END_DATE = ''
        ROOM = ''
        INSTRUCTOR = ''
        
        # Extract course PDF URLs from the page
        course_pdf_urls = response.xpath('//div[@class="wysiwygContent"]/div//a/@href').getall()
        
        for course_pdf_url in course_pdf_urls:
            time.sleep(2)
            course_pdf_url = f"https://np.edu{course_pdf_url}"
            response = requests.get(course_pdf_url)
            response.raise_for_status()
            with pdfplumber.open(BytesIO(response.content)) as pdf:
                
                # Skip first page (usually headers)
                for page_no, page in enumerate(pdf.pages[1:None], start=1):
                    text = page.extract_text()
                    if not text:
                        continue
                    lines = [l.strip() for l in text.split("\n") if l.strip()]
                    dept_pattern = re.compile(r"^\S+\:\S+")
                    last_line_skip = re.compile(r'^As\s*of\s*\d+\/\d+\/\d+\s*')
    
                    for line in lines:
                        if dept_pattern.match(line):
                            if re.search(r'^\S+\:\S+',line):
                                DEPT = re.findall(r'^(\S+)\:\S+', line)[0]
                            
                            print("department mapping skipped")
                            continue
                        
                        elif last_line_skip.match(line):
                            if START_DATE and END_DATE:
                                sec = ''
                                if re.search(r'\d+\-(\d+)\s+', COURSE_NAME):
                                    sec = re.findall(r'\d+\-(\d+)\s+', COURSE_NAME)[0].strip()
                                
                                SEC = sec
                                course_rows.append(
                                    {
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": course_pdf_url,
                                        "Course Name": COURSE_NAME,
                                        "Course Description": '',
                                        "Class Number": CLASS_NO,
                                        "Section": SEC,
                                        "Instructor": INSTRUCTOR,
                                        "Enrollment": '',
                                        "Course Dates": f"{START_DATE} - {END_DATE}",
                                        "Location": ROOM,
                                        "Textbook/Course Materials": '',
                                    }
                                )
                            print("last line mapping skipped")
                            continue
                        
                        # Skip known headers and noise lines
                        elif "COURSE ID" in line:
                            print("second header")
                            continue
                        elif "MEETING START" in line:
                            print("first header")
                            continue
                        elif "Class Schedule -" in line:
                            print("term name")
                            continue
                        elif "PARK COLLEGE" in line :
                            print('University name')
                            continue
                        
                        else:
                            if re.search(r"(0[1-9]|1[0-2])\/(0[1-9]|[12][0-9]|3[01])\/\d{4}", line):
                                
                                print("Valid row:", line)
                                
                                if START_DATE and END_DATE:
                                    sec = ''
                                    if re.search(r'\d+\-(\d+)\s+', COURSE_NAME):
                                        sec = re.findall(r'\d+\-(\d+)\s+', COURSE_NAME)[0].strip()
                                    
                                    SEC = sec
                                    course_rows.append(
                                        {
                                            "Cengage Master Institution ID": self.institution_id,
                                            "Source URL": course_pdf_url,
                                            "Course Name": COURSE_NAME,
                                            "Course Description": '',
                                            "Class Number": CLASS_NO,
                                            "Section": SEC,
                                            "Instructor": INSTRUCTOR,
                                            "Enrollment": '',
                                            "Course Dates": f"{START_DATE} - {END_DATE}",
                                            "Location": ROOM,
                                            "Textbook/Course Materials": '',
                                        }
                                    )
                                
                                match = pattern.search(line)
                                match1 = pattern1.search(line)
                                match2 = pattern2.search(line)
                                if match:
                                    (
                                        class_no,
                                        type_,
                                        course_id,
                                        title,
                                        start_date,
                                        end_date,
                                        room,
                                        instructor,
                                        status
                                    ) = match.groups()
                                
                                    course_name = title.strip()
                                    if course_name and DEPT not in course_name:
                                        course_name = f"{DEPT}-{course_name}"
                                    if re.match(r'^\d', course_name):
                                        digits = re.findall(r'\d+', course_name)
                                        start_id = digits[0]
                                        course_name = re.sub(r'^\d+\s*', '',course_name)
                                        course_name = f"{DEPT}-{start_id} {course_name}"
                                    if course_name:
                                        COURSE_NAME = course_name
                                    
                                        
                                    CLASS_NO = class_no
                                    START_DATE = start_date
                                    END_DATE = end_date
                                    INSTRUCTOR = instructor
                                    ROOM = room
                                    
                                elif match1:
                                    (
                                        class_no,
                                        start_date,
                                        end_date,
                                        room,
                                        instructor,
                                        status
                                    ) = match1.groups()
                                    
                                    
                                    
                                    if class_no:
                                        CLASS_NO = class_no
                                    
                                    START_DATE = start_date
                                    END_DATE = end_date
                                    INSTRUCTOR = instructor
                                    ROOM = room
                                    
                                elif match2:
                                    (
                                        days,
                                        start_time,
                                        end_time,
                                        start_date,
                                        end_date,
                                        room,
                                        instructor,
                                        status
                                    ) = match2.groups()
                                
                                    START_DATE = start_date
                                    END_DATE = end_date
                                    if instructor:
                                        INSTRUCTOR = instructor
                                    ROOM = room
                                    
                                else:
                                    
                                    print("❌ Pattern not matched:", line)
                            else:
                                # Continuation lines → append to course name
                                COURSE_NAME = f"{course_name} {line}"
                                
        # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(course_rows)
        save_df(course_df,    self.institution_id, "course")
        

    def parse_directory(self, response):
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
        
        # List to store extracted staff records
        directory_rows = []
        
        blocks = response.xpath('//div[@class="cell department"]/following-sibling::div')
        for block in blocks:
            
            # Append extracted staff data to the results list
            directory_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_source_url,
                    "Name": block.xpath('.//div[@class="name"]/text()').get('').strip(),
                    "Title": block.xpath('.//span[@class="department"]/text()').get('').strip(),
                    "Email": block.xpath('.//span[@class="email"]/a/text()').get('').lower().strip(),
                    "Phone Number": block.xpath('.//span[@class="phone"]/a/text()').get('').strip(),
                }
            )
        # Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the DataFrame using a custom helper function
        save_df(directory_df, self.institution_id, "campus")
        
        
    @inline_requests
    def parse_calendar(self, response):
        """
        Parse calendar using Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        # Initialize list to collect all calendar rows
        calendar_rows = []
        
        utc = pytz.UTC
        central = pytz.timezone("US/Central")
        
        # Get current date for API range
        current_date = datetime.today()
        current_date_str = current_date.strftime("%Y-%m-%d")
        
        # Calculate end date 8 months from today
        end_date = current_date + relativedelta(months=8)
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        # Extract calendar ID from page HTML using regex
        if re.search(r'data-calendar-id\s*\=\s*\"(.*?)\"', response.text):
            cal_id = re.findall(r'data-calendar-id\s*\=\s*\"(.*?)\"', response.text)[0]
        else:
            cal_id = ''
        
        url = f"https://api.calendar.moderncampus.net/pubcalendar/{cal_id}/events?start={current_date_str}&end={end_date_str}"
        headers = {
        'accept': '*/*',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'origin': 'https://np.edu',
        'referer': 'https://np.edu/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }
        # Make GET request to fetch calendar events JSON
        cal_response = requests.request("GET", url, headers=headers)
        
        json_datas = json.loads(cal_response.text)
        
        for data in json_datas:
            start_date_main = data.get('startDatetime','').strip()
            
            start_date1 = data.get('startDate','').strip()
            start_date = start_date_main
            
            # Convert ISO datetime to US/Central if present
            if "T" in start_date_main:
                
                dt_utc = datetime.fromisoformat(start_date_main).replace(tzinfo=utc)
                dt_central = dt_utc.astimezone(central)
                start_date = dt_central.date().strftime("%Y-%m-%d")
               
            end_date_main = data.get('endDatetime','').strip()
            end_date1 = data.get('endDate','').strip()
            end_date = end_date_main
            if "T" in end_date_main:
                dt_utc = datetime.fromisoformat(end_date_main).replace(tzinfo=utc)
                dt_central = dt_utc.astimezone(central)
                end_date = dt_central.date().strftime("%Y-%m-%d")
                
            desc = data.get('title','')
            formatted_range = ''
            formatted_date =''
            date = ''
            if start_date and end_date and start_date == end_date:
                date = start_date
            elif start_date1 and end_date1 and start_date1 == end_date1:
                date = start_date1
            elif start_date and end_date and start_date != end_date:
                start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                
                # Compare the month of both dates
                if start_dt.month == end_dt.month:
                    formatted_range = f"{start_dt.strftime('%B')} {start_dt.day} – {end_dt.day}, {start_dt.year}"
                else:
                    formatted_range = f"{start_dt.strftime('%B')} {start_dt.day} – {end_dt.strftime('%B')} {end_dt.day}, {start_dt.year}"
            elif start_date1 and end_date1 and start_date1 != end_date1:
                start_dt = datetime.strptime(start_date1, "%Y-%m-%d")
                end_dt = datetime.strptime(end_date1, "%Y-%m-%d")
                
                # Compare the month of both dates
                if start_dt.month == end_dt.month:
                    formatted_range = f"{start_dt.strftime('%B')} {start_dt.day} – {end_dt.day}, {start_dt.year}"
                else:
                    formatted_range = f"{start_dt.strftime('%B')} {start_dt.day} – {end_dt.strftime('%B')} {end_dt.day}, {start_dt.year}"
            if date:
                # Parse the date
                dt = datetime.strptime(date, "%Y-%m-%d")
                formatted_date = dt.strftime("%A, %B %d, %Y")
            
            elif formatted_range:
                formatted_date = formatted_range
                
            # Append event data to calendar_rows list
            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": '',
                "Term Date": formatted_date,
                "Term Date Description": desc,
            })
        
        
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
