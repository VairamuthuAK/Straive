import re
import json
import scrapy
import requests
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class JcuSpider(scrapy.Spider):
    name = "jcu"
    institution_id = 258435014931277794

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://jcubulletin.coursedog.com/courses?cq=&page=1"
    directory_source_url = ""
    calendar_url = "https://www.jcu.edu/academics/academic-calendar/"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            self.parse_directory()
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar, dont_filter=True)
        
        # # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            self.parse_directory()
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
        # List to collect all parsed course records
        course_rows = []
 
        # Extract CourseDog course ID from embedded page JavaScript
        if re.search(r'id\:\"sisId\-course\"[\w\W]*?\,id\:\"(.*?)\"', response.text):
            course_id = re.findall(r'id\:\"sisId\-course\"[\w\W]*?\,id\:\"(.*?)\"', response.text)[0]
        else:
            course_id = ''
        
        # Extract active catalog ID
        if re.search(r'activeCatalog\:\"(.*?)\"', response.text):
            catalogid = re.findall(r'activeCatalog\:\"(.*?)\"', response.text)[0]
        else:
            catalogid = ''
            
        # Extract course effective date range (start & end)
        if re.search(r'\"course\"\,[\w\W]*?(\d{4}\-\d{1,2}\-\d{1,2})\"\,\"courseNumber\"', response.text):
            end_date = re.findall(r'\"course\"\,[\w\W]*?(\d{4}\-\d{1,2}\-\d{1,2})\"\,\"courseNumber\"', response.text)[0]
        else:
            end_date = ''
        if re.search(r'\"course\"\,[\w\W]*?(\d{4}\-\d{1,2}\-\d{1,2})\"\,\"courseNumber\"', response.text):
            start_date = re.findall(r'\"course\"\,[\w\W]*?(\d{4}\-\d{1,2}\-\d{1,2})\"\,\"courseNumber\"', response.text)[0]
        else:
            start_date = ''

        # Build CourseDog API endpoint with filters and date range
        url = f"https://app.coursedog.com/api/v1/cm/jcu/courses/search/%24filters?catalogId={catalogid}&skip=0&limit=2000&orderBy=catalogDisplayName%2CtranscriptDescription%2ClongName%2Cname&formatDependents=false&effectiveDatesRange={start_date}%2C{end_date}&ignoreEffectiveDating=false&columns=customFields.rawCourseId%2CcustomFields.crseOfferNbr%2CcustomFields.catalogAttributes%2CdisplayName%2Cdepartment%2Cdescription%2Cname%2CcourseNumber%2CsubjectCode%2Ccode%2CcourseGroupId%2Ccareer%2Ccollege%2ClongName%2Cstatus%2Cinstitution%2CinstitutionId%2Cattributes%2Ccredits"

        # POST payload defining course filters (active, exclusions)
        payload = json.dumps({
        "condition": "AND",
        "filters": [
            {
            "filters": [
                {
                "id": "status-course",
                "condition": "field",
                "name": "status",
                "inputType": "select",
                "group": "course",
                "type": "is",
                "value": "Active"
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "1XXX",
                "customField": False
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "2XXX",
                "customField": False
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "3XXX",
                "customField": False
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "4XXX",
                "customField": False
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "5XXX",
                "customField": False
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "0000",
                "customField": False
                },
                {
                "id": "courseNumber-course",
                "condition": "field",
                "name": "courseNumber",
                "inputType": "text",
                "group": "course",
                "type": "doesNotContain",
                "value": "ELEC",
                "customField": False
                },
                {
                "id": "subjectCode-course",
                "condition": "field",
                "name": "subjectCode",
                "inputType": "subjectCodeSelect",
                "group": "course",
                "type": "isNot",
                "value": "GE",
                "customField": False
                },
                {
                "id": "sisId-course",
                "condition": "field",
                "name": "sisId",
                "inputType": "text",
                "group": "course",
                "type": "isNot",
                "value": "HS3240",
                "customField": False
                }
            ],
            "id": course_id,
            "condition": "and"
            }
        ]
        })
        
        # HTTP headers for CourseDog API request
        headers = {
        'accept': 'application/json, text/plain, */*',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'content-type': 'application/json',
        'origin': 'https://jcubulletin.coursedog.com',
        'referer': 'https://jcubulletin.coursedog.com/',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
        'x-requested-with': 'catalog'
        }
        
        # Send POST request to CourseDog API
        post_response = requests.request("POST", url, headers=headers, data=payload)
        
        json_datas = json.loads(post_response.text)
        blocks = json_datas['data']
        
        # Extract course details from API response
        for block in blocks:
            id = block.get('courseGroupId','')  
            courseNumber = block.get('courseNumber','')
            Name = re.sub('\s+',' ',block.get('longName','')).strip()  
            description = re.sub('\s+',' ',block.get('description','')).strip()
            source_url = f"https://jcubulletin.coursedog.com/courses/{id}"
            
            # Append normalized course record
            course_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": source_url,
                "Course Name": Name,
                "Course Description": description,
                "Class Number": courseNumber,
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
        
    def parse_directory(self):
        pass
        
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
        # Initialize list to collect all calendar row dictionaries
        calendar_rows = []
        
        term_names = response.xpath('//div[@id="wysiwyg-1"]//h3/text()').getall()
        for term in term_names:
            blocks = response.xpath(f'//h3[contains(text(),"{term}")]//following-sibling::ul[1]/li')
            
            # Loop through each calendar entry under the term
            for block in blocks:
                date = block.xpath('./strong/text()').get('').strip()
                event = block.xpath('./text()').get('').strip()
                
                # Append structured calendar data
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": term,
                    "Term Date": date.rstrip(":"),
                    "Term Date Description": event,
                })
                
        # Extract summary text from <details><summary> for summer term naming
        summary = " ".join(response.xpath('//details//summary/text()').getall()).replace('—','').strip()
        
        # Select all list items under "Important Summer Dates"
        blocks1 = response.xpath('//h4[contains(text(),"Important Summer Dates")]/following-sibling::ul/li')
        
        for block1 in blocks1:
                date = block1.xpath('./strong/text()').get('').strip()
                event = block1.xpath('./text()').get('').strip()
                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": summary,
                    "Term Date": date.rstrip(":"),
                    "Term Date Description": event,
                })
        
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
