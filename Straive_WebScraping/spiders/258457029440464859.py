import time
import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class PaceSpider(scrapy.Spider):
    
    name = "pace"
    institution_id = 258457029440464859

    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://appsrv.pace.edu/ScheduleExplorer/index.cfm?rst=1"

    # DIRECTORY CONFIG
    directory_urls = ['https://www.pace.edu/sands/faculty-and-staff/adjunct-faculty',
                    'https://www.pace.edu/sands/faculty-and-staff/staff-directory',
                    'https://www.pace.edu/sands/faculty-directory']
    
    # CALENDAR CONFIG
    calendar_source_url = "https://catalog.pace.edu/academic-calendar/2025-2026-grid/"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        all the datas are collecting using scrapy

        """
        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_source_url, callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            for directory_source_url in self.directory_urls:
                yield scrapy.Request(url = directory_source_url, callback=self.parse_directory,cb_kwargs={'directory_source_url':directory_source_url}, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_source_url, callback=self.parse_course, dont_filter=True)
            for directory_source_url in self.directory_urls:
                yield scrapy.Request(url = directory_source_url, callback=self.parse_directory,cb_kwargs={'directory_source_url':directory_source_url}, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url = self.course_source_url, callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            for directory_source_url in self.directory_urls:
                yield scrapy.Request(url = directory_source_url, callback=self.parse_directory,cb_kwargs={'directory_source_url':directory_source_url}, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_source_url, callback=self.parse_course, dont_filter=True)
            for directory_source_url in self.directory_urls:
                yield scrapy.Request(url = directory_source_url, callback=self.parse_directory,cb_kwargs={'directory_source_url':directory_source_url}, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

    # PARSE COURSE
    @inline_requests
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
        terms = response.xpath('//option[contains(text(),"Choose Term:")]/following-sibling::option/@value').getall()
        course = response.xpath('//option[contains(text(),"All Summer")]/@value').get('')
        terms.append(course)
        for term in terms:
            levels = response.xpath('//select[@aria-label="Choose Level"]/option/@value').getall()
            for level in levels:
                if level:
                    url = "https://appsrv.pace.edu/ScheduleExplorer/index.cfm"
                    payload = f'term={term}&level={level}&subject=all&coursenumberHidden=&day=&time=&prof=&location=&instrMode=&submit=Processing...'
                    headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                    'Cache-Control': 'max-age=0',
                    'Connection': 'keep-alive',
                    'Content-Type': 'application/x-www-form-urlencoded',
                    'Origin': 'https://appsrv.pace.edu',
                    'Referer': 'https://appsrv.pace.edu/ScheduleExplorer/index.cfm?rst=1',
                    'Upgrade-Insecure-Requests': '1',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                    }
                    course_response = yield scrapy.Request(url=url, method="POST", body=payload, headers=headers)
                    blocks = course_response.xpath('//table[@id="mainResultsTable"]/tbody/tr')
                    for block in blocks:
        
                        timestamp = int(time.time() * 1000)
                        crn_number = block.xpath('.//td[1]/a/text()').get('').strip()
                        url = f"https://appsrv.pace.edu/ScheduleExplorer/course_detailsModal_ajaxContent.cfm?r=683366&crn={crn_number}&term={term}&_={timestamp}"
                        payload = {}
                        headers = {
                        'Accept': '*/*',
                        'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
                        'Connection': 'keep-alive',
                        'Referer': 'https://appsrv.pace.edu/ScheduleExplorer/index.cfm',
                        'Sec-Fetch-Dest': 'empty',
                        'Sec-Fetch-Mode': 'cors',
                        'Sec-Fetch-Site': 'same-origin',
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                        }
                        yield scrapy.Request(url = url ,headers= headers, callback = self.course_final,cb_kwargs = {'crn_number':crn_number})
                        
    def course_final(self,response,crn_number):
            description = ' '.join(response.xpath('//b[contains(text(),"Course Description:")]/parent::td//text() | //b[contains(text(),"DESCRIPTION:")]/parent::td/parent::tr//text()').getall()).replace('Course Description:',' ').replace('DESCRIPTION:',' ').replace('\n',' ').replace('\r',' ').replace('\t',' ').strip()
            course_date = response.xpath('//td[contains(normalize-space(.), "to see campus and room assignments")]/following-sibling::td/text()').get('').replace('\n',' ').replace('\r',' ').replace('\t',' ').strip()
            capacity = response.xpath('//b[contains(text(),"CAPACITY:")]/parent::td/following-sibling::td/text()').get('').strip()
            fill = response.xpath('//b[contains(text(),"SEATS AVAILABLE:")]/parent::td/following-sibling::td/text()').get('').strip()
            course_name = response.xpath('//b[contains(text(),"COURSE TITLE:")]/parent::td/following-sibling::td/text()').get('').strip()
            sub = response.xpath('//b[contains(text(),"SUBJECT/COURSE#:")]/parent::td/following-sibling::td/text()').get('').strip()
            if fill == 'CLOSED':
                enroll = f'{capacity}/{capacity}'
            elif capacity == '':
                enroll = ''
            else:
                available = int(capacity)-int(fill)
                enroll = f'{available}/{capacity}'
            if course_name == '':
                self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_source_url,
                        "Course Name": f'{sub} - {course_name}',
                        "Course Description": re.sub(r'\s+',' ',description),
                        "Class Number": crn_number,
                        "Section": '',
                        "Instructor": '',
                        "Enrollment": enroll,
                        "Course Dates": re.sub(r'\s+',' ',course_date),
                        "Location": response.xpath('//b[contains(text(),"CAMPUS:")]/parent::td/following-sibling::td/text()').get('').strip(),
                        "Textbook/Course Materials": ""
                    })


    # PARSE DIRECTORY
    def parse_directory(self,response,directory_source_url):
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
        blocks = response.xpath(
            '//article[@class="cc--component-container cc--person-card "] | '
            '//div[@class="cc--component-container cc--profile-card "]'
        )
        for block in blocks:
            name = ' '.join(block.xpath('.//div[@class="f--field f--cta-title"]/h3//text()').getall()).strip()
            title = ' '.join(block.xpath('.//div[@class="f--field f--description"]/p/em/text() | .//div[@class="f--field f--description"]/p/em/text()').getall()).strip()
            if title:
                title = title
            elif 'adjunct-faculty' in directory_source_url:
                title = block.xpath('.//div[@class="f--field f--description"]/p/em/text() | .//div[@class="f--field f--description"]/p/text()').get('').strip()
            else:
                school = block.xpath('.//div[@class="f--field f--category field-school"]/text()').get('').strip()
                department = block.xpath('.//div[@class="f--field f--category field-departments"]/text()').get('').strip()
                title = f'{department}, {school} '
            self.directory_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Name": name ,
                    "Title": '' if title == 'Email:' else title,
                    "Email": block.xpath('.//a[contains(@href,"mailto:")]/text() | .//a[contains(@href,"mailto:")]/span/text() | .//a[contains(@href,"mailto:")]/@href').get('').replace('mailto:','').strip(),
                    "Phone Number": block.xpath('.//div[@class="f--field f--phone field-phone"]/a/text()').get('').strip(),
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
        blocks = response.xpath('//h3[@class="toggle"]/parent::div/parent::div//div[contains(@class,"client-calendar-list")]/ul')
        for block in blocks:
                sub_blocks = block.xpath('.//ul/li')
                for sub_block in sub_blocks:
                    term_date = block.xpath('.//li/div/div/text()').get('').strip()
                    match = re.search(r'([A-Za-z]{3})\s+(\d{1,2})', term_date)
                    if match:
                        month = match.group(1).lower()
                        day = int(match.group(2))

                        # FALL
                        if month in ("sep", "oct", "nov", "dec"):
                            term_name = "Fall 2025"

                        # SPRING
                        elif month in ("jan", "feb", "mar", "apr"):
                            term_name = "Spring 2026"

                        elif month == "may" and day <= 16:
                            term_name = "Spring 2026"

                        # SUMMER
                        elif month == "may" and day >= 18:
                            term_name = "Summer 2026"

                        elif month in ("jun", "jul", "aug"):
                            term_name = "Summer 2026"
                    self.calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_source_url,
                            "Term Name":term_name,
                            "Term Date": term_date,
                            "Term Date Description": sub_block.xpath('.//div/text()').get('').strip(),
                        })
                    
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
      