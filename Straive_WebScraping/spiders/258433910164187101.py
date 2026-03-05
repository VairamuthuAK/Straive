import io
import json
import scrapy
import requests
import pandas as pd
from ..utils import *
from pypdf import PdfReader


class UmoSpider(scrapy.Spider):

    name = "umo"
    institution_id = 258433910164187101
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://umo.bncollege.com/course-material/findCourse?courseFinderSuggestion=SCHOOL_DEPARTMENT&campus=567&term=567_1_26_W&oer=false"
    course_headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'content-type': 'application/json',
    'referer': 'https://umo.bncollege.com/course-material/course-finder',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
    }

    # DIRECTORY CONFIG
    directory_url = "https://umo.edu/wp-content/plugins/umo-directory/api/contacts.php"
    directory_payload = "api=contacts&jb_current_locale=en_US"
    directory_headers = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
    'origin': 'https://umo.edu',
    'referer': 'https://umo.edu/faculty-staff-directory/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_urls = [
                            "https://api.my.umo.edu/v1/pages/attachments/2070/share/",
                            "https://api.my.umo.edu/v1/pages/attachments/2071/share/",
                            "https://api.my.umo.edu/v1/pages/attachments/2072/share/",
                            "https://api.my.umo.edu/v1/pages/attachments/2073/share/",
                            "https://api.my.umo.edu/v1/pages/attachments/2260/share/",
                            "https://api.my.umo.edu/v1/pages/attachments/2261/share/"]
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and directory data are scraped using Scrapy in
        `parse_course` and `parse_directory`.

        - The calendar data is collected using PdfReader in `parse_calendar`,
        based on the calendar source format.

        - Each SCRAPE_MODE determines which parsing functions are executed.
        """

        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_url, headers=self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_url,method='POST',body=self.directory_payload, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            self.parse_calendar()

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_url, headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_url,method='POST',body=self.directory_payload, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            self.parse_calendar()
            yield scrapy.Request(url = self.course_url, headers=self.course_headers, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_url,method='POST',body=self.directory_payload, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            self.parse_calendar()

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_url, headers=self.course_headers, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_url,method='POST',body=self.directory_payload, headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
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

        json_data = json.loads(response.text)
        for block in json_data:
            department_code = block.get('code','')
            url = f"https://umo.bncollege.com/course-material/findCourse?courseFinderSuggestion=SCHOOL_COURSE&campus=567&term=567_1_26_W&department={department_code}&oer=false"
            yield scrapy.Request(url = url, headers=self.course_headers, callback=self.parse_course_department,cb_kwargs={'department_code': department_code}, dont_filter=True)

    def parse_course_department(self,response,department_code):
        json_data = json.loads(response.text)
        for block in json_data:
            course_code = block.get('code','')
            url = f'https://umo.bncollege.com/course-material/findCourse?courseFinderSuggestion=SCHOOL_COURSE_SECTION&campus=567&term=567_1_26_W&department={department_code}&course={course_code}&oer=false'
            yield scrapy.Request(url = url, headers=self.course_headers, callback=self.parse_course_section,cb_kwargs={'department_code': department_code, 'course_code': course_code}, dont_filter=True)   

    def parse_course_section(self,response,department_code,course_code):
        json_data = json.loads(response.text)
        for block in json_data:
            section_code = block.get('code','')
            department = department_code.split('_')[-1]
            url = f'https://umo.bncollege.com/course-material-caching/course?campus=567&term=567_1_26_W&course=567_1_26_W_{department}_{course_code}_1&section={section_code}&oer=false'
            yield scrapy.Request(url = url, headers=self.course_headers, callback=self.parse_course_section_details,cb_kwargs={'department_code': department_code, 'course_code': course_code, 'section_code': section_code}, dont_filter=True)

    def parse_course_section_details(self,response,department_code,course_code,section_code):
        blocks = response.xpath('//div[@class="bned-cm-item-main-container"]')
        for block in blocks:
            name = block.xpath('.//span[@class="js-bned-item-name-text"]/text()').get('').strip()
            cou = response.xpath('//h1/following-sibling::a/span[1]/text()').get('').strip()
            section = response.xpath('//input[@name="section"]/@value').get('').strip()
            location = response.xpath('//div[@role="listitem"]/@data-campus-name').get('').strip()
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": 'https://umo.bncollege.com/course-material/course-finder',
                "Course Name": f'{cou} {course_code} - {name}'.strip(),
                "Course Description": '',
                "Class Number": f'{cou} {course_code}',
                "Section":  section,
                "Instructor": block.xpath('.//span[@class="author"][1]//text()[2]').get('').strip(),
                "Enrollment": "",
                "Course Dates": '',
                "Location": re.sub(r'\s+',' ',location),
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
        json_data = response.json()
        for block in json_data:
            first_name = block.get('first','').strip()
            last_name = block.get('last','').strip()
            tit = block.get('title','').strip()
            dep = block.get('department','').strip()
            unit = block.get('unit','').strip()

            if tit and dep and unit:
                title = f'{tit}, {dep} / {unit}'.strip()
            elif tit and dep:
                title = f'{tit}, {dep}'.strip()
            elif tit:
                title = tit
            else:
                title = ""

            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": 'https://umo.edu/faculty-staff-directory/',
            "Name": f'{first_name} {last_name}'.strip(),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.get('email','').strip(),
            "Phone Number": block.get('phone','').strip(),
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
        for url in self.calendar_source_urls:
            # Map URL identifiers to their corresponding academic term names
            url_mapping = {
                "2070": "Fall Adult and Graduate",
                "2071": "Fall Traditional",
                "2072": "Spring Adult and Graduate",
                "2073": "Spring Traditional",
                "2260": "Summer Adult and Graduate",
                "2261": "Summer Traditional"
            }
            # List of weekdays used to identify calendar entries
            days_list = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            # Extract the calendar identifier from the URL
            url_id = url.split('/')[-3]
            # Get the academic term name using the URL identifier
            term_main_title = url_mapping.get(url_id, "Unknown Calendar")

            # Download the PDF calendar file
            response = requests.get(url)
            response.raise_for_status()
            
            # Load the PDF into memory
            pdf_file = io.BytesIO(response.content)
            reader = PdfReader(pdf_file)
            
            # Iterate through each page of the PDF
            for page in reader.pages:
                text = page.extract_text() # Extract text content from the page
                if not text: continue # Skip pages with no text
                lines = text.split('\n') # Split page text into individual lines
                
                for line in lines:
                    # Check if the line contains any weekday name
                    for day in days_list:
                        if day in line:
                            # Split the line into event description and date using the weekday
                            parts = line.split(day)
                            term_event = parts[0].strip()
                            term_date = parts[1].strip()
                            
                            if term_event and term_date:
                                self.calendar_rows.append({
                                    "Cengage Master Institution ID": 258433910164187101,
                                    "Source URL": url,
                                    "Term Name": term_main_title,
                                    "Term Date": term_date,
                                    "Term Date Description": term_event,
                                })
                            break 

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

        