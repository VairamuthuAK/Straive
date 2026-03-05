import re
import json
import scrapy
import pandas as pd
from ..utils import *
from datetime import datetime
from inline_requests import inline_requests


class FrostburgSpider(scrapy.Spider):
    name = "frostburg"
    institution_id = 258426281777981396

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://www.frostburg.edu/academics/academic-catalogs.php#/courses"
    directory_source_url = "https://www.frostburg.edu/departments/visual-arts/faculty-and-staff.php"
    calendar_url = "https://www.frostburg.edu/academics/calendar.php"

   
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
        # Initialize list to store all extracted course records
        course_rows = []
        
        #Get all available public catalogs
        catalogs_url = "https://frostburg.kuali.co/api/v1/catalog/public/catalogs"
        
        catalog_response = yield scrapy.Request(catalogs_url)
        catallogs = json.loads(catalog_response.text)
        
        #Filter the required catalog (2026 Catalog)
        for catallog in catallogs:
            id = catallog.get('_id','')
            term = catallog.get('title','')
            current_year = datetime.now().year
            match = re.search(r"\d+\-\b(20\d{2})\b", term)
            if match and int(match.group(1)) >= current_year and "Catalog" in term:
                
                #Get all courses under this catalog
                term_url = f"https://frostburg.kuali.co/api/v1/catalog/courses/{id}?q="
                term_response = yield scrapy.Request(term_url)
                
                datas = json.loads(term_response.text)
                for data in datas:
                    pid = data.get('pid','')
                    desc_url = f"https://frostburg.kuali.co/api/v1/catalog/course/{id}/{pid}"
                    desc_response = yield scrapy.Request(desc_url)
                    
                    datails = json.loads(desc_response.text)
                    
                    desc = datails.get('description','')
                    CourseId = datails.get('__catalogCourseId','')
                    title = datails.get('title','')
                    course_name = ''
                    if CourseId and title:
                        course_name = f"{CourseId} {title}"
                    elif title:
                        course_name = title
                        
                    course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": course_name,
                        "Course Description": desc,
                        "Class Number": CourseId,
                        "Section": '',
                        "Instructor": '',
                        "Enrollment": '',
                        "Course Dates": '',
                        "Location": '',   
                        "Textbook/Course Materials": '',
                    })
                    
        # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(course_rows)
        save_df(course_df,    self.institution_id, "course")
        
    @inline_requests
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
        
        blocks = response.xpath('//h1/following-sibling::div')
        for block in blocks:
            name = block.xpath('.//h2/text()').get('').strip()
            role = block.xpath('.//h3/text()').get('').strip()
            phone = block.xpath('.//a[contains(@href,"tel:")]/text()').get('').strip()
            email = block.xpath('.//a[contains(@href,"mailto:")]/text()').get('').strip()
            if name:
                # Append extracted staff data to the results list
                directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.directory_source_url,
                            "Name": name,
                            "Title": role,
                            "Email": email,
                            "Phone Number": phone,
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
        # Initialize list to store all calendar records
        calendar_rows = []
        
        term_name = response.xpath('//h1//following-sibling::p/strong/span/text()').get('').strip()
        
        # Select all table rows from the main calendar table
        blocks = response.xpath('//table/tbody/tr')
        for block in blocks:
            event = " ".join(block.xpath('./td[2]//text()').getall()).strip()
            date = " ".join(block.xpath('./td[1]//text()').getall()).replace('*','').strip()
            
            # Append structured calendar data
            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": term_name,
                "Term Date": date,
                "Term Date Description": event,
            })
            
        # Extract URLs under "FUTURE CALENDARS" section
        future_cal_urls = [f"https://www.frostburg.edu/academics/{url}" for url in response.xpath('//a[contains(text(),"FUTURE CALENDARS")]/following-sibling::div//a/@href').getall()]
        for future_cal_url in future_cal_urls:
            
            future_response = yield scrapy.Request(future_cal_url)
            
            term_name1 = future_response.xpath('//h1//following-sibling::p/strong/span/text()').get('').strip()
            blocks1 = future_response.xpath('//table[1]/tbody/tr')
            for block1 in blocks1:
                event1 = " ".join(block1.xpath('./td[2]//text()').getall()).strip()
                date1 = " ".join(block1.xpath('./td[1]//text()').getall()).replace('*','').strip()
                
                # Append future calendar data
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": future_cal_url,
                    "Term Name": term_name1,
                    "Term Date": date1,
                    "Term Date Description": event1,
                })
                
        # Extract URLs under "ARCHIVED CALENDARS" section
        archived_cal_urls = [f"https://www.frostburg.edu/academics/{url}" for url in response.xpath('//a[contains(text(),"ARCHIVED CALENDARS")]/following-sibling::div//a/@href').getall()]
        for archived_cal_url in archived_cal_urls:
            current_year = str(datetime.now().year)
            
            if current_year in archived_cal_url:
                
                arch_response = yield scrapy.Request(archived_cal_url)
                
                term_name1 = arch_response.xpath('//h1//following-sibling::p/strong/span/text()').get('').strip()
                blocks1 = arch_response.xpath('//table[1]/tbody/tr')
                for block1 in blocks1:
                    event1 = " ".join(block1.xpath('./td[2]//text()').getall()).strip()
                    date1 = " ".join(block1.xpath('./td[1]//text()').getall()).replace('*','').strip()
                    
                    # Append archived calendar data
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": archived_cal_url,
                        "Term Name": term_name1,
                        "Term Date": date1,
                        "Term Date Description": event1,
                    })
                    
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
