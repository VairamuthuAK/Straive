import re
import scrapy
import pandas as pd
from ..utils import *


class PepperSpider(scrapy.Spider):

    name = "pepper"
    institution_id = 258438839096338392
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_url = "https://catalog.bschool.pepperdine.edu/content.php?catoid=19&catoid=19&navoid=969&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D=1"
    cours_headers = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://catalog.bschool.pepperdine.edu/content.php?catoid=19&catoid=19&navoid=969&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D=1',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    }

    # DIRECTORY CONFIG
    directory_source_url = 'https://seaver.pepperdine.edu/about/administration/dean/contact/staff.htm'

    # CALENDAR CONFIG
    calendar_source_url = "https://seaver.pepperdine.edu/academics/calendar/"
    calendar_headers = {
    'Accept': '*/*',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Connection': 'keep-alive',
    'Origin': 'https://seaver.pepperdine.edu',
    'Referer': 'https://seaver.pepperdine.edu/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        All three sites(course, campus and calendar) are scraped using Scrapy.
        """
        # Single functions
        if mode == "course":
            yield scrapy.Request(url=self.course_url, headers = self.cours_headers,callback=self.parse_course)

        elif mode == "directory":
           yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)

        elif mode == "calendar":
            terms = ['20260109','20280104']
            for term in terms:
                if term == '20280104':
                    calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&spudformat=xhr"
                    yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)
                else:
                    for index in range(-1,2):
                        calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&index={index}&spudformat=xhr"
                        yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url=self.course_url, headers = self.cours_headers,callback=self.parse_course)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)

        elif mode in ["course_calendar", "calendar_course"]:
            terms = ['20260109','20280104']
            for term in terms:
                if term == '20280104':
                    calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&spudformat=xhr"
                    yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)
                else:
                    for index in range(-1,2):
                        calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&index={index}&spudformat=xhr"
                        yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)
            yield scrapy.Request(url=self.course_url, headers = self.cours_headers,callback=self.parse_course)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)
            terms = ['20260109','20280104']
            for term in terms:
                if term == '20280104':
                    calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&spudformat=xhr"
                    yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)
                else:
                    for index in range(-1,2):
                        calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&index={index}&spudformat=xhr"
                        yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

        #  All three (default)
        else:
            yield scrapy.Request(url=self.course_url, headers = self.cours_headers,callback=self.parse_course)
            yield scrapy.Request(url=self.directory_source_url, callback=self.parse_directory)
            terms = ['20260109','20280104']
            for term in terms:
                if term == '20280104':
                    calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&spudformat=xhr"
                    yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)
                else:
                    for index in range(-1,2):
                        calendar_url = f"https://www.trumba.com/s.aspx?calendar=seaver-academics&widget=main&date={term}&index={index}&spudformat=xhr"
                        yield scrapy.Request(url=calendar_url,headers=self.calendar_headers, callback=self.parse_calendar)

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
        page_count = int(response.xpath('(//td[contains(text(),"Page:")]/a)[last()]/text()').get('').strip())
        for page in range(1,page_count+1):
            course_url = f"https://catalog.bschool.pepperdine.edu/content.php?catoid=19&catoid=19&navoid=969&filter%5Bitem_type%5D=3&filter%5Bonly_active%5D=1&filter%5B3%5D=1&filter%5Bcpage%5D={page}"
            yield scrapy.Request(url=course_url,headers=self.cours_headers,callback=self.parse_course_pagination)
    
    def parse_course_pagination(self,response):
        urls = response.xpath('//table[@class="table_default"]/tr/td/a/@href').getall()
        for url in urls:
            if 'preview_course' in url:
                    url = response.urljoin(url)
                    yield scrapy.Request(url=url,callback=self.parse_course_final)
                    
    def parse_course_final(self,response):
        title = response.xpath('//h1/text()').get('').strip()
        parts = title.split()
        class_num = f'{parts[0]} {parts[1]}'
        description_first = response.xpath("//h1[@id='course_preview_title'] /following-sibling::text()").get('').strip()
        description_second = ' '.join(response.xpath("//h1[@id='course_preview_title'] /following-sibling::node() [not(self::div) and not(preceding::strong[normalize-space()='Grading Basis:'])] //text()[normalize-space()]").getall()).replace('\xa0','').replace('Grading Basis:','').strip()

        if description_second:
            description = f'{description_first} {description_second}'
        else:
            description = description_first

        self.course_rows.append({
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Course Name": re.sub(r'\s+',' ',title),
            "Course Description": re.sub(r'\s+',' ',description),
            "Class Number": re.sub(r'\s+',' ',class_num),
            "Section": '',
            "Instructor": '',
            "Enrollment": '',
            "Course Dates": '',
            "Location": '',
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

        blocks = response.xpath('//div[@class="section-row grid_3col"]/div[2]')
        for block in blocks:
            title = block.xpath('.//p/em/text()').get('').replace('&nbsp;','').strip()
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": self.directory_source_url,
            "Name": block.xpath('.//p/strong/text()').get('').strip(),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.xpath('.//p/a/@href').get('').split('mailto:')[-1].strip(),
            "Phone Number": block.xpath('.//p/text()').get('').strip(),
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
        blocks = response.xpath('//table[@class="twSimpleTableTable"]/tbody/tr[@class="twSimpleTableEventRow0 ebg0"]')
        for block in blocks:
            date = block.xpath('.//span[@class="twStartDate"]/text()').get('').strip()
            description = block.xpath('.//span[@class="twDescription"]/a/text()').get('').strip()
            term_name = block.xpath('(./preceding-sibling::tr/td[@class="twSimpleTableGroup"]/div/text())[last()]').get(default='').strip()
            if '2024' not in term_name:
                if description:
                    self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_source_url,
                        "Term Name": re.sub(r'\s+',' ',term_name),
                        "Term Date": re.sub(r'\s+',' ',date),
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

        