import math
import scrapy
import pandas as pd
from ..utils import *


class FlcSpider(scrapy.Spider):

    name = "flc"
    institution_id = 258453946404726737
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = "https://hub.losrios.edu/classSearch/getCourses.php?flcFilter=true&openFilter=true&waitlistFilter=true&strm=1263,Spring%202026&link=true&offset=0&first=1"
    course_headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Referer': 'https://flc.losrios.edu/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
    }

    # DIRECTORY CONFIG
    directory_source_url = "https://inside.flc.losrios.edu/collegewide-resources/employee-directory?college=FLC-Inside&sort=first&searchLocation=FLC-Inside&cmd=undefined&offset=0&type=undefined&=undefined&offset=0&offset=1&offset=2&offset=3&link=true"
    directory_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        }

    # CALENDAR CONFIG
    calender_urls = ['https://flc.losrios.edu/admissions/academic-calendar-and-deadlines/spring-2026-academic-calendar',
                    'https://flc.losrios.edu/admissions/academic-calendar-and-deadlines/fall-2026-academic-calendar',
                    'https://flc.losrios.edu/admissions/academic-calendar-and-deadlines/summer-2026-academic-calendar']
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        All data extraction in this spider is performed entirely using Scrapy.
        """
        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_sourse_url, headers =self.course_headers,callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            for calendar_url in self.calender_urls:
                yield scrapy.Request(url = calendar_url, callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_sourse_url, headers =self.course_headers,callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            for calendar_url in self.calender_urls:
                yield scrapy.Request(url = calendar_url, callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url = self.course_sourse_url, headers =self.course_headers,callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            for calendar_url in self.calender_urls:
                yield scrapy.Request(url = calendar_url, callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_sourse_url, headers =self.course_headers,callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            for calendar_url in self.calender_urls:
                yield scrapy.Request(url = calendar_url, callback=self.parse_calendar, dont_filter=True)

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
        # Get the total number of courses from the page
        count = int(response.xpath('//span[@id="totalResults"]/text()').get('').strip())
        # Calculate the number of pages needed, assuming 20 courses per page
        per_page = math.ceil(count/20)
        for page in range(0,per_page+1):
            url = f"https://hub.losrios.edu/classSearch/getCourses.php?flcFilter=true&openFilter=true&waitlistFilter=true&strm=1263,Spring%202026&link=true&offset={page}&first=2"
            headers = {
            'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'Referer': 'https://flc.losrios.edu/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            }
            yield scrapy.Request(url = url ,headers= headers,callback=self.parse_course_pagination, dont_filter=True)

    def parse_course_pagination(self,response):
        urls = response.xpath('//a[@class="more-info"]/@onclick').getall()
        for onclick_str in urls:
            params_str = onclick_str.replace("getModal(", "").replace(");", "")
            params_str = params_str.replace("'", "")
            params = params_str.split(",")
            class_section = params[0]
            course_id = params[1]
            college = params[2]
            modal_strm = params[3]
            url = f"https://hub.losrios.edu/classSearch/getModal.php?ClassSection={course_id}&CourseId={class_section}&college={college}&modalStrm={modal_strm}"
            yield scrapy.Request(url = url ,headers= self.course_headers,callback=self.parse_course_detail, dont_filter=True)
    
    def parse_course_detail(self,response):
        course_name = response.xpath('//div[@class="content"]/h2/text()').get('').strip()
        description = ' '.join(response.xpath('//div[@class="content"]/p/text()').getall()).strip()
        blocks = response.xpath('//div[@class="section-details"]')
        for block in blocks:
            class_number = ''.join(block.xpath('.//span[contains(text(),"Class Number:")]/parent::li/text()').getall()).strip()
            location = ''.join(block.xpath('.//span[contains(text(),"Building:")]/parent::li/text()').getall()).strip()
            course_dates = ''.join(block.xpath('.//span[contains(text(),"Term:")]/parent::li/text()').getall()).replace('Full Term,','').replace('Other Term,','').strip()
            self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Course Name": re.sub(r'\s+',' ',course_name),
                    "Course Description": re.sub(r'\s+',' ',description),
                    "Class Number": class_number,
                    "Section": '',
                    "Instructor": block.xpath('.//span[contains(text(),"Instructors:")]/parent::li/a/text()').get('').strip(),
                    "Enrollment": ''.join(block.xpath('.//span[contains(text(),"Enrollment Status:")]/parent::li/text()').getall()).strip(),
                    "Course Dates": course_dates,
                    "Location": re.sub(r'\s+',' ',location),
                    "Textbook/Course Materials":  ''.join(block.xpath('.//span[contains(text(),"Textbook:")]/parent::li/span/a/@href').getall()).strip()
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

        """
        Generates paginated directory URLs and sends Scrapy requests
        to extract directory profile data.
        """
        # Base URL for the campus directory search
        base_url = "https://hub.losrios.edu/directory/profiles/directoryResults.php"
        # Query parameters for filtering and sorting the directory
        params = (
            "college=FLC-Inside&sort=first&searchLocation=FLC-Inside&"
            "cmd=undefined&type=undefined&=undefined"
        )
        # Total number of directory pages to iterate through
        count = response.xpath('//p[@class="results-num"]/text()').get('').replace('results for:','').strip()
        num_pages = math.ceil(count/20)
        # Initial offsets required by the site for the first few entries
        first_offsets = [0, 1, 2, 3]
        for page in range(1, num_pages + 1):
            # Combine the first offsets with the page-based offsets
            offsets = first_offsets + list(range(page))
            # Convert offsets list into URL query string format
            offsets_str = "&".join([f"offset={i}" for i in offsets])
            # Build the full URL for the current page
            url = f"{base_url}?{params}&{offsets_str}"
            yield scrapy.Request(url = url ,callback=self.parse_directory_pagination, dont_filter=True)

    def parse_directory_pagination(self,response):
        detail_urls = response.xpath('//a[@class="more-info"]/@href').getall()
        for detail_url in detail_urls:
            id = detail_url.split('id=')[-1]
            url = f'https://hub.losrios.edu/directory/profiles/employeeProfile.php?wid={id}&college=inside.flc.losrios.edu'
            main_url = f'https://inside.flc.losrios.edu/collegewide-resources/employee-directory{detail_url}'
            yield scrapy.Request(url = url,callback=self.parse_directory_detail,cb_kwargs={'main_url':main_url}, dont_filter=True)
    
    def parse_directory_detail(self,response,main_url):
        name = response.xpath('//h1/text()').get('').strip()
        blocks = response.xpath('//div[@class="accordion-btn job-role"]')
        if blocks:
            for block in blocks:
                phone = block.xpath('.//li[@class="bull-phone"]/text()').get('').strip()
                if 'No' in phone:
                    phone = ''

                else:
                    phone = phone

                dept = ''.join(block.xpath('.//span[contains(text(),"See contact information for")]/parent::div/text()').getall()).strip()
                role = block.xpath('.//div[@class="job-role"]/p/text() | .//p/text()').get('').strip()
                if role and dept:
                    title = f'{dept}, {role}'
                else:
                    title = dept
                self.directory_rows.append( {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": main_url,
                "Name": name,
                "Title": re.sub(r'\s+',' ',title),
                "Email": block.xpath('.//li[@class="bull-email"]/a/text()').get('').strip(),
                "Phone Number": phone,
                })
        else:
            phone = response.xpath('//li[@class="bull-phone"]/text()').get('').strip()
            if 'No' in phone:
                phone = ''

            else:
                phone = phone

            dept = ''.join(response.xpath('//span[contains(text(),"See contact information for")]/parent::div/text()').getall()).strip()
            role = response.xpath('//div[@class="job-role"]/p/text()').get('').strip()
            if role and dept:
                title = f'{dept}, {role}'

            else:
                title = dept

            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": main_url,
            "Name": name,
            "Title": re.sub(r'\s+',' ',title),
            "Email": response.xpath('//li[@class="bull-email"]/a/text()').get('').strip(),
            "Phone Number": phone,
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

        blocks = response.xpath('//table[@class="table-wrap"]/tbody/tr')
        for block in blocks:
            term_date = block.xpath('.//th[1]/text()').get('').strip()
            desc = ' '.join(block.xpath('.//td//text()').getall()).strip()
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": response.url.split('/')[-1].replace('-',' ').title(),
                "Term Date": re.sub(r'\s+',' ',term_date),
                "Term Date Description": re.sub(r'\s+',' ',desc),
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
