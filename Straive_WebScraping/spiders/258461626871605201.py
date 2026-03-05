import re
import json
import scrapy
import requests
import pandas as pd
from ..utils import *
from parsel import Selector
from datetime import datetime
from inline_requests import inline_requests


class BucknellSpider(scrapy.Spider):
    name = "bucknell"
    institution_id = 258461626871605201

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://pubapps.bucknell.edu/CourseInformation/"
    directory_source_url = "https://www.bucknell.edu/azdirectory?t=faculty_staff&s=All"
    calendar_url = "https://www.bucknell.edu/calendar-resources/academic-planning-calendar"

   
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
        # List to store all extracted course records
        course_rows = []
        
        # API endpoint to fetch available academic terms
        term_url = "https://pubapps.bucknell.edu/CourseInformation/data/term"
        
        # Headers required to access Bucknell course APIs
        headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'Referer': 'https://pubapps.bucknell.edu/CourseInformation/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        }
        # Request the term list
        term_response = yield scrapy.Request(term_url, headers=headers)
        
        terms = json.loads(term_response.text)
        for term in terms:
            term_code = term.get('CodeBanner','').strip()
            term_id = term.get('CodeBn','').strip()
            
            # API endpoint to fetch courses for a specific term
            course_data_url = f"https://pubapps.bucknell.edu/CourseInformation/data/course/term/{term_code}"
            term_data_response = yield scrapy.Request(course_data_url, headers=headers)
            term_datas = json.loads(term_data_response.text)
            
            # Loop through each course in the term
            for term_data in term_datas:
                Crn = term_data.get('Crn','')
                Subj = term_data.get('Subj','')
                sec = term_data.get('Section','').strip()
                Number = term_data.get('Number','')
                term = term_data.get('Term','')
                Title = term_data.get('Title','').strip()
                
                # Build full course title
                full_title = f"{Subj} {Number} {Title}"
                
                # Extract meeting locations
                locations = []
                meeting_blocks = term_data['Meetings']
                for meeting_block in meeting_blocks:
                    Loca = meeting_block.get('Location','')
                    locations.append(Loca)
                if locations:
                    Location = ", ".join(loc for loc in locations if loc)
                else:
                    Location = ''
                    
                # Extract instructor names  
                Instructors = []
                Instructor_blocks = term_data['Instructors']
                for Instructor_block in Instructor_blocks:
                    name = Instructor_block.get('Display','')
                    Instructors.append(name)
                if Instructors:
                    Instructor = ", ".join(ins for ins in Instructors if ins)
                else:
                    Instructor = ''
                
                # Request detailed course info
                details_url = f"https://pubapps.bucknell.edu/CourseInformation/framework/academic/course-guide-view/{term}/{Crn}?subject={Subj}&number={Number}"
                detail_response = yield scrapy.Request(details_url, headers=headers)
                
                detail_data = json.loads(detail_response.text)
                
                # Extract and clean course description
                desc = detail_data.get('Description', '')
                if isinstance(desc, (list, tuple)):
                    desc = ' '.join(desc)
                elif desc is None:
                    desc = ''
                Description = re.sub(r'\s+', ' ', str(desc)).strip()
                
                # Extract enrollment information
                MaxEnroll = detail_data['Enrollment'].get('MaxEnroll','')
                Enrolled = detail_data['Enrollment'].get('Enrolled','')
                cross_list = detail_data.get('CrossListEnrollment', [])
                if cross_list and isinstance(cross_list, list):
                    MaxEnroll1 = cross_list[0].get('MaxEnroll', '')
                    Enrolled1 = cross_list[0].get('Enrolled', '')
                else:
                    Enrolled1 = ''
                    MaxEnroll1 = ''
                
                enroll=''
                if MaxEnroll and Enrolled:
                    enroll = f"{Enrolled} of {MaxEnroll}"
                elif MaxEnroll:
                    enroll = f"0 of {MaxEnroll}"
                elif MaxEnroll1 and Enrolled1:
                    enroll = f"{Enrolled1} of {MaxEnroll1}"
                elif MaxEnroll1:
                    enroll = f"0 of {MaxEnroll1}"
                
                text_book = f"https://bucknell.bncollege.com/course-material-listing-page?utm_campaign=storeId=63056_langId=-1_courseData={Subj}_{Number}_{sec}_{term_id}&utm_source=wcs&utm_medium=registration_integration"
                course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": re.sub('\s+',' ',full_title),
                    "Course Description": Description,
                    "Class Number": Crn,
                    "Section": sec,
                    "Instructor": Instructor,
                    "Enrollment": enroll,
                    "Course Dates": '',
                    "Location": Location,   
                    "Textbook/Course Materials": text_book,
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
        
        # Select all staff information blocks on the page
        blocks = response.xpath('//section[@class="m-staff-information"]')
        for block in blocks:
            mail = block.xpath('.//h4/following-sibling::a[1]/text()').get('').strip()
            phone = ''
            if not mail:
                mail = block.xpath('.//h4/following-sibling::div[2]/a/text()').get('').strip()
                phone = block.xpath('.//h4/following-sibling::div[1]/a/text()').get('').strip()
            
            title = re.sub('\s+',' ',block.xpath('.//div[@class="m-staff-information__title u-mar-top-xxxs"][1]/text()').get('')).strip()
            if not title:
                title = re.sub('\s+',' ',block.xpath('.//div[@class="m-staff-information__title u-mar-top-xxxs"][2]/text()').get('')).strip()
            
            name = block.xpath('.//h3/a/text()').get('').strip()
            if not name:
                name = block.xpath('.//h3/text()').get('').strip()
            if not mail:
                mail = block.xpath('.//h4/following-sibling::div[3]/a/text()').get('').strip()
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_source_url,
                        "Name": name,
                        "Title": title,
                        "Email": mail,
                        "Phone Number": phone,
                    }
                )
        # Get URL for the next page (pagination)
        next_page_url = response.xpath('//span[contains(text(),"Next page")]/parent::a/@href').get('').strip()
        
        # Continue scraping while a next page exists
        while next_page_url:
            next_page_url = f"https://www.bucknell.edu/azdirectory{next_page_url}"
            page_response = yield scrapy.Request(next_page_url)
            blocks = page_response.xpath('//section[@class="m-staff-information"]')
            for block in blocks:
                mail = block.xpath('.//h4/following-sibling::a[1]/text()').get('').strip()
                phone = ''
                if not mail:
                    mail = block.xpath('.//h4/following-sibling::div[2]/a/text()').get('').strip()
                    phone = block.xpath('.//h4/following-sibling::div[1]/a/text()').get('').strip()
                    
                    # If phone field actually contains email, fix it
                    if "@" in phone:
                        mail = phone
                        phone = ''
                # If mail value is not an email, treat it as phone
                if mail and "@" not in mail:
                    phone = mail
                    mail = ''
                
                if not mail:
                    mail = block.xpath('.//h4/following-sibling::div[3]/a/text()').get('').strip()
                title = re.sub('\s+',' ',block.xpath('.//div[@class="m-staff-information__title u-mar-top-xxxs"][1]/text()').get('')).strip()
                if not title:
                    title = re.sub('\s+',' ',block.xpath('.//div[@class="m-staff-information__title u-mar-top-xxxs"][2]/text()').get('')).strip()
                    
                name = block.xpath('.//h3/a/text()').get('').strip()
                if not name:
                    name = block.xpath('.//h3/text()').get('').strip()
                    
                # Append extracted staff data to the results list
                directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.directory_source_url,
                            "Name": name,
                            "Title": title,
                            "Email": mail,
                            "Phone Number": phone,
                        }
                    )
            # Get the next page URL again (if exists)
            next_page_url = page_response.xpath('//span[contains(text(),"Next page")]/parent::a/@href').get('').strip()
            
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
        # List to store all scraped calendar rows
        calendar_rows = []
        
        cal_urls = response.xpath('//div[@id="block-bucknell-content"]//li/a/@href').getall()
        cal_terms = response.xpath('//div[@id="block-bucknell-content"]//li/a/text()').getall()
        
        # Loop through each calendar URL and its corresponding term name
        for cal_url, cal_term in zip(cal_urls, cal_terms):
            
            # Convert relative URL to absolute URL
            cal_url = f"https://www.bucknell.edu{cal_url}"
            
            # Send request to the calendar page and wait for response
            cal_response = yield scrapy.Request(cal_url)
            
            blocks = cal_response.xpath('//table[@class="table"]//tbody/tr')
            for block in blocks:
                
                # Append extracted calendar data as a dictionary
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": cal_url,
                    "Term Name": cal_term,
                    "Term Date": block.xpath('./td[2]/text()').get('').strip(),
                    "Term Date Description": block.xpath('./td[1]/text()').get('').strip(),
                })
    
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
