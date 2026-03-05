import io
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *


class SonomaSpider(scrapy.Spider):

    name = "sonoma"
    institution_id = 258428237493856220
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://registrar.sonoma.edu/sites/registrar/files/2025-11/S26_Schedule_of_Classes.pdf'

    # DIRECTORY CONFIG
    directory_source_url = "https://ldaps.sonoma.edu/fasd/"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # CALENDAR CONFIG
    calendar_source_url = "https://www.sonoma.edu/academic/calendar"
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

        - Course data is extracted using Pdfplumber

        - Directory and Calendar data is available as static HTML pages and is scraped
        using normal Scrapy requests.
        """

        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

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

        url = self.course_sourse_url
        try:
            response = requests.get(url, timeout=15)
            pdf_file = io.BytesIO(response.content)
        except Exception as e:
            return []

        loc_markers = r'(STEV\w+|SALZ\w+|DARW\w+|ARTS\w+|WINE\w+|CARS\w+|SCHU\w+|PE\w+|IVRL\w+|ONLINE|SYNC|HYBRID|TBD|ARRANGE|UKIAH|ROOMTBD|ASYNC)'
        last_data = None

        with pdfplumber.open(pdf_file) as pdf:
            for page in pdf.pages:

                text = page.extract_text()
                if not text:
                    continue

                for line in text.split('\n'):

                    line = line.strip()
                    if not line or "---" in line or (line.isupper() and len(line.split()) < 4):
                        continue

                    main_match = re.search(r'^(\d{4,5})\s+[\d,]*\s*([A-Z]{2,4}\s+\d+[A-Z]*)\s+(\d{3})', line)

                    if main_match:

                        class_no = main_match.group(1)
                        course_id = main_match.group(2)
                        section = main_match.group(3)

                        title_match = re.search(fr'{section}\s+(?:(?:\*?[A-Z0-9]\s?[A-Z]?)\s+)?(.*?)\s+\d\.\d\s+(?:SUP|LEC|SEM|ACT|DIS|LAB)', line)
                        title = title_match.group(1).strip() if title_match else "Special Topic/Research"

                        last_data = {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": url,
                            "Course Name": f"{course_id} {title}",
                            "Course Description": "",
                            "Class Number": class_no,
                            "Section": section,
                            "Enrollment": "",
                            "Course Date": "",
                            "Textbook/Course Materials": ""
                        }

                        locs = list(re.finditer(loc_markers, line))

                        if locs:
                            loc = "ARRANGE ASYNC" if ("ARRANGE" in line and "ASYNC" in line) else locs[-1].group(0)
                            instructor = line[locs[-1].end():].strip() or "STAFF"
                            if instructor == "OFF-SITE STAFF": instructor = "STAFF"
                            if "OFFS " in instructor: instructor = instructor.split("OFFS ")[-1]
                        else:
                            instructor = "STAFF"

                        row = last_data.copy()
                        row.update({"Location": "", "Instructor": instructor})
                        self.course_rows.append(row)

                    else:

                        lab_match = re.search(r'([A-Z]{2,4}\s+\d+[A-Z]*)\s+(\d{3})\s+.*LAB', line)

                        if lab_match and last_data:

                            sec = lab_match.group(2)
                            loc_match = re.search(loc_markers, line)

                            if loc_match:
                                instructor = line.split(loc_match.group(0))[-1].strip()
                            else:
                                instructor = "STAFF"

                            if instructor == "OFF-SITE STAFF": instructor = "STAFF"
                            if "OFFS " in instructor: instructor = instructor.split("OFFS ")[-1]

                            row = last_data.copy()
                            row.update({"Section": sec, "Location": "", "Instructor": instructor})
                            self.course_rows.append(row)

                        elif last_data:

                            sec_loc = re.search(loc_markers, line)

                            if sec_loc:

                                instructor = line[sec_loc.end():].strip()

                                if instructor:
                                    if instructor == "OFF-SITE STAFF": instructor = "STAFF"
                                    if "OFFS " in instructor: instructor = instructor.split("OFFS ")[-1]

                                    row = last_data.copy()
                                    row.update({"Location": "", "Instructor": instructor})
                                    self.course_rows.append(row)

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
        department = response.xpath('//select[@name="dept"]/option/@value').getall()
        for dept in department:
            url = "https://ldaps.sonoma.edu/fasd/index.cgi"
            payload = f'action=search&pat=0&lname=&fname=&job_title=&phone=&email=&dept={dept}&dept2=&bldg=&room='
            headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Origin': 'https://ldaps.sonoma.edu',
            'Referer': 'https://ldaps.sonoma.edu/fasd/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }
            yield scrapy.Request(url,method='POST',body=payload,headers=headers,callback=self.parse_directory_dept)

    def parse_directory_dept(self,response):
        urls = response.xpath('//table/tr/td[1]/a/@href').getall()
        if urls:
            for url in urls:
                url = response.urljoin(url)
                yield scrapy.Request(url,headers=self.directory_headers,callback=self.parse_directory_final)
    
    def parse_directory_final(self,response):
        dept = response.xpath('//th[contains(text(),"Job Title:")]/following-sibling::td/text()').get('').strip()
        tit = response.xpath('//th[contains(text(),"Department:")]/following-sibling::td/text()').get('').strip()

        if dept and tit:
            title = f'{dept}, {tit}'
        elif dept:
            title = dept
        elif tit:
            title = tit
        else:
            title = ''

        name = response.xpath('//th[contains(text(),"Name:")]/following-sibling::td/text()').get('').strip()
        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": re.sub(r'\s+',' ',name),
        "Title": re.sub(r'\s+',' ',title),
        "Email": response.xpath('//th[contains(text(),"Email Address:")]/following-sibling::td/a/text()').get('').strip(),
        "Phone Number": response.xpath('//a[contains(text(),"Phone*")]/parent::th/following-sibling::td/text()').get('').replace('(email preferred)','').replace('n/a','').replace('(cell-leave message)','').strip(),
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
            description = block.xpath('.//td[1]/text()').get('').strip()
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_source_url,
                "Term Name": block.xpath('.//parent::tbody/parent::table/thead/tr/th[1]/text()').get('').strip(),
                "Term Date": block.xpath('.//td[2]/text()').get('').strip(),
                "Term Date Description": re.sub(r'\s+',' ',description),
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
        