import re
import scrapy
import requests
import urllib.parse
import pandas as pd
from ..utils import *
from parsel import Selector
from datetime import datetime
from inline_requests import inline_requests


class SwtcSpider(scrapy.Spider):
    name = "swtc"
    institution_id = 258416185257781203
    
    # Indian region website is not opening, so I used a US region proxy.
    proxy ={'proxy':'Enter your proxy here'}
    proxies = {
        'http': 'Enter your proxy here',
        'https': 'Enter your proxy here',
    }
    
    # Course page URL
    course_url = "https://myswtc.swtc.edu/CMCPortal/Common/CourseSchedule.aspx"
   
   # Directory page URL
    directory_url = (
        "https://www.swtc.edu/about/staff-directory"
    )

    # Academic calendar page URL
    calendar_url = "https://www.swtc.edu/calendar-events/" 


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)
            
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, meta=self.proxy, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, meta=self.proxy, dont_filter=True)
       

    @inline_requests
    def parse_course(self, response):
        """
        Parse course schedule data using Scrapy response.

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
       
        # Container to store parsed course records
        course_rows =[]
        
        # Extract ASP.NET hidden form values required for POST calls
        VIEWSTATE_text_main  = urllib.parse.quote(response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
        VIEWSTATEGENERATOR_text_main  = urllib.parse.quote(response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
        EVENTVALIDATION_text_main  = urllib.parse.quote(response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
        
        
        # Extract available academic terms from the dropdown
        values = response.xpath('//select[@id="_ctl0_PlaceHolderMain__ctl0_cbTerm"]/option/@value').getall()
        terms = response.xpath('//select[@id="_ctl0_PlaceHolderMain__ctl0_cbTerm"]/option/text()').getall()
        
        # Loop through terms and process the selected term only
        for value, term in zip(values, terms):
            
            if "2025" in term or "2026" in term:
                
                # Build POST payload to fetch course list
                payload = f'__EVENTTARGET=&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={VIEWSTATE_text_main}&_ctl0%3Apagetitle%3AhfShowLinkText=Show%20Quick%20Links...&_ctl0%3Apagetitle%3AhfHideLinkText=Hide%20Quick%20Link...&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCampus=5&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbTerm={term}&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtKeyword=&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkMo=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkWe=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTh=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkFr=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSa=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtCode=&_ctl0%3APlaceHolderMain%3A_ctl0%3ASections=rbOC&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbLowTime=0&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbHighTime=23&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCourseType=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCourseAttribute=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtSearch=&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_0=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_1=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_2=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_3=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_4=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_5=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_6=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AbtnSearch=Search&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text_main}&__EVENTVALIDATION={EVENTVALIDATION_text_main}'
                
                # Headers required for ASP.NET form submission
                headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
                'content-type': 'application/x-www-form-urlencoded',
                'origin': 'https://myswtc.swtc.edu',
                'referer': 'https://myswtc.swtc.edu/CMCPortal/Common/CourseSchedule.aspx',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                }
                
                # Submit POST request to retrieve course list
                inner_response = yield scrapy.Request(url=self.course_url,method="POST",headers=headers,body=payload, meta=self.proxy, dont_filter=True)

                # Extract updated hidden fields from course list page
                VIEWSTATE_text  = urllib.parse.quote(inner_response.xpath('//input[@id="__VIEWSTATE"]/@value').get('')).strip()
                VIEWSTATEGENERATOR_text  = urllib.parse.quote(inner_response.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get('').strip())
                EVENTVALIDATION_text  = urllib.parse.quote(inner_response.xpath('//input[@id="__EVENTVALIDATION"]/@value').get('')).strip()
                
                # Select all course rows
                blocks = inner_response.xpath('//table[@id="CourseList"]/tbody/tr')
                counts = len(blocks)
                
                # Loop through each course row
                for i, block in enumerate(blocks, start=1):
                    print(f"{i}/{counts}")
                    
                    # Extract course detail link (JavaScript-based postback)
                    detail_url = block.xpath('./td[13]/a/@href').get('').strip()
                    detail_page = urllib.parse.quote(re.findall(r'\'([\w\W]*?)\'', detail_url)[0])
                    
                    # Build POST payload to load course detail page
                    details_payload = f'__EVENTTARGET={detail_page}&__EVENTARGUMENT=&__LASTFOCUS=&__VIEWSTATE={VIEWSTATE_text}&_ctl0%3Apagetitle%3AhfShowLinkText=Show%20Quick%20Links...&_ctl0%3Apagetitle%3AhfHideLinkText=Hide%20Quick%20Link...&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCampus=5&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbTerm={term}&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtKeyword=&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkMo=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkWe=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkTh=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkFr=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSa=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchkSu=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtCode=&_ctl0%3APlaceHolderMain%3A_ctl0%3ASections=rbOpen&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbLowTime=0&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbHighTime=23&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCourseType=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AcbCourseAttribute=-1&_ctl0%3APlaceHolderMain%3A_ctl0%3AtxtSearch=&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_0=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_1=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_2=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_3=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_4=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_5=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AchbDeliveryMethod%3AchbDeliveryMethod_6=on&_ctl0%3APlaceHolderMain%3A_ctl0%3AhdnCourseListPageIndex=&_ctl0%3APlaceHolderMain%3A_ctl0%3AhdnScrollPos=&CourseList_length=10&CourseList_length=10&__VIEWSTATEGENERATOR={VIEWSTATEGENERATOR_text}&__EVENTVALIDATION={EVENTVALIDATION_text}'
                    
                    # Request course detail page
                    details_response = yield scrapy.Request(url=self.course_url,method="POST",headers=headers,body=details_payload, meta=self.proxy, dont_filter=True)
                    
                    # Extract detailed course information
                    description = re.sub('\s+', ' ', details_response.xpath('//span[@id="_ctl0_PlaceHolderMain_lblComments"]/text()').get('')).strip()
                    location = re.sub('\s+', ' ', details_response.xpath('//span[@id="_ctl0_PlaceHolderMain_ucCourseSched_lblLocationDetails"]/text()').get('')).strip()
                    
                    # Append structured course record
                    course_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.course_url,
                            "Course Name": re.sub('\s+', ' ', block.xpath('./td[2]//text()').get('')).strip(),
                            "Course Description": description,
                            "Class Number": block.xpath('./td[1]/span/text()').get('').strip(),
                            "Section": block.xpath('./td[3]//text()').get('').strip(),
                            "Instructor": block.xpath('./td[7]/span/text()').get('').strip(),
                            "Enrollment": block.xpath('./td[12]/span/text()').get('').strip(),
                            "Course Dates": block.xpath('./td[4]/span/span/text()').get('').strip(),
                            "Location": location,
                            "Textbook/Course Materials": '',
                        }
                    )
                    
        # Convert collected records to DataFrame and save             
        course_df = pd.DataFrame(course_rows)  
        save_df(course_df, self.institution_id, "course")
        
        
    def parse_directory(self, response):
        """
        Parse staff directory data from the Scrapy response.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        
        # Container to store parsed directory records
        directory_rows = []
        
        # Select all staff rows from the directory table
        dir_blocks = response.xpath('//table[@class="staff-directory"]//tbody/tr')
        
        # Iterate through each staff row
        for dir_block in dir_blocks:
            
            # Append extracted data as a structured record
            directory_rows.append(
                {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.directory_url,
                    "Name": dir_block.xpath('./td[1]//text()').get('').strip(),
                    "Title": dir_block.xpath('./td[2]//text()').get('').strip(),
                    "Email": '',
                    "Phone Number": dir_block.xpath('./td[3]//text()').get('').strip(),
                }
            )
        # Convert collected records into a DataFrame
        directory_df = pd.DataFrame(directory_rows)
        
        # Save the parsed directory data
        save_df(directory_df, self.institution_id, "campus")


    def parse_calendar(self, response):
        """
        Parse academic calendar data and return structured calendar rows.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """

        # Container to store parsed calendar records
        calendar_rows = []
        
        calendar_url = "https://www.swtc.edu/calendar-events/calendar-data?type=0"
        
        # Headers added to mimic a real browser request
        headers = {
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'referer': 'https://www.swtc.edu/calendar-events/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }

        # Make HTTP request to fetch calendar data
        calendar_response = requests.request("GET", calendar_url, headers=headers, proxies=self.proxies)
        
        # Convert response text into a Scrapy Selector for XPath parsing
        cal_selector = Selector(text=calendar_response.text)
       
        # Select all table rows containing calendar entries
        blocks = cal_selector.xpath('//table//tr')
        
        # Skip the first row (header) and process remaining rows
        for block in blocks[1:]:
            
            date = block.xpath('./td[1]//text()').get('').strip()
            title = block.xpath('./td[3]//text()').get('').strip()
            
            # Convert the string date into a datetime object
            date_obj = datetime.strptime(date, "%a %b %d, %Y")
            
            # Format date as "Month Year" (e.g., "January 2026")
            formatted_date = date_obj.strftime("%B %Y")
            
            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": formatted_date,
                "Term Date": date,
                "Term Date Description": title,
            })

        # Convert list of dictionaries into a DataFrame
        calendar_df = pd.DataFrame(calendar_rows)
        
        # Save the parsed calendar data
        save_df(calendar_df, self.institution_id, "calendar")