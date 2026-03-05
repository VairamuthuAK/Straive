import json
import scrapy
import calendar
import pandas as pd
from ..utils import *
from datetime import datetime


class MayvillestateSpider(scrapy.Spider):

    name = "mayvillestate"
    institution_id = 258439849135073241
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_sourse_url = 'https://my.mayvillestate.edu/schedules/index.aspx?page=allcourses'
    course_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_source_url = "https://mayvillestate.edu/about-msu/more-info/contact-us/employee_directory/"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }


    # CALENDAR CONFIG
    calendar_source_url = "https://mayvillestate.edu/about-msu/campus-calendar/"
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

        -All three datas getting using scrapy

        """
        
        # Single functions
        if mode == "course":
            yield scrapy.Request(url = self.course_sourse_url, headers=self.course_headers,callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url = self.course_sourse_url, headers=self.course_headers,callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)
            yield scrapy.Request(url = self.course_sourse_url, headers=self.course_headers,callback=self.parse_course, dont_filter=True)

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, headers=self.calendar_headers,callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            yield scrapy.Request(url = self.course_sourse_url, headers=self.course_headers,callback=self.parse_course, dont_filter=True)
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

        terms = response.xpath('//select[@name="ctl03$drpTerms"]/option/@value').getall()
        for term in terms:
            url = f'https://my.mayvillestate.edu/schedules/index.aspx?page=allcourses&term={term}'
            yield scrapy.Request(url,headers=self.course_headers,callback=self.parse_course_final)
    
    def parse_course_final(self,response):
        blocks = response.xpath('//table[@id="courselist"]/tbody/tr')
        for block in blocks:
            sec = block.xpath('.//td[2]/label/text()').get('').strip()
            class_num = block.xpath('.//td[4]/label/text()').get('').strip()
            name = block.xpath('.//td[3]/label/text()').get('').strip()
            course_name = f'{sec} {name}'
            location = block.xpath('.//td[5]/text()').get('').strip()
            instructor = block.xpath('.//td[9]/label//text()').get('').strip()
            course_dates = block.xpath('.//td[6]/label//text()').get('').strip()
            description = ' '.join(block.xpath('.//td[11]//text()').getall()).replace('\n','').replace('\r','').replace('\t','').strip()
            self.course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Course Name": re.sub(r'\s+',' ', course_name),
                "Course Description": re.sub(r'\s+',' ', description),
                "Class Number": class_num,
                "Section": '',
                "Instructor": re.sub(r'\s+',' ', instructor),
                "Enrollment": '',
                "Course Dates": re.sub(r'\s+',' ', course_dates),
                "Location": re.sub(r'\s+',' ', location),
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
        
        blocks = response.xpath('//div[@class="flex-1"]')
        for block in blocks:
            title = ', '.join(t.strip() for t in block.xpath('.//div[@class="mt-2 text-sm text-gray-700 space-y-3"]//text()').getall() if t.strip())
            self.directory_rows.append( {
            "Cengage Master Institution ID": self.institution_id,
            "Source URL": response.url,
            "Name": block.xpath('.//h2/text()').get('').strip(),
            "Title": re.sub(r'\s+',' ',title),
            "Email": block.xpath('.//a[contains(@href,"mailto:")]/@href').get('').replace('mailto:','').strip(),
            "Phone Number": block.xpath('.//strong[contains(text(),"Phone:")]/parent::p/text()').get('').strip(),
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
        base_url = "https://teamup.com/ksk8mhddbc6pyj378f/events"

        start_year = 2025
        start_month = 1   # Jan

        end_year = 2026
        end_month = 7     # July

        year = start_year
        month = start_month

        while (year < end_year) or (year == end_year and month <= end_month):

            # Month first day
            start_date = datetime(year, month, 1).strftime("%Y-%m-%d")

            # Month last day
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day).strftime("%Y-%m-%d")

            # Term name (Month-Year)
            term_name = datetime(year, month, 1).strftime("%B %Y")

            # URL build
            url = f"{base_url}?startDate={start_date}&endDate={end_date}&tz=America%2FChicago"
            headers = {
            'accept': 'application/json',
            'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
            'priority': 'u=1, i',
            'referer': 'https://teamup.com/ksk8mhddbc6pyj378f',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            }
            yield scrapy.Request(url, headers=headers,callback=self.parse_calendar_final,cb_kwargs={'term_name':term_name})
            if month == 12:
                month = 1
                year += 1
            else:
                month += 1

    def parse_calendar_final(self,response,term_name):
            json_data = json.loads(response.text)
            blocks = json_data['events']
            for block in blocks:

                description = block.get('title','').strip()
                start_date = datetime.strptime(block.get('start_dt','').split('T')[0].strip(),"%Y-%m-%d").strftime("%B %d %Y")
                end_date = datetime.strptime(block.get('end_dt','').split('T')[0].strip(),"%Y-%m-%d").strftime("%B %d %Y")
                if start_date == end_date:
                    term_date = start_date
                else:
                    term_date = f'{start_date} - {end_date}'

                self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_source_url,
                    "Term Name": re.sub(r'\s+',' ',term_name),
                    "Term Date": re.sub(r'\s+',' ',term_date),
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
        