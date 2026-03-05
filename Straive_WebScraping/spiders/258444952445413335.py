import re
import json
import scrapy
import pandas as pd
from ..utils import *


class ClovisSpider(scrapy.Spider):

    name = "clovis"
    institution_id = 258444952445413335
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_source_url = "https://selfservice.scccd.edu/Student/Courses/SearchResult"
    course_url = "https://selfservice.scccd.edu/Student/Courses/PostSearchCriteria"
    course_payload = "{\"keyword\":null,\"terms\":[],\"requirement\":null,\"subrequirement\":null,\"courseIds\":null,\"sectionIds\":null,\"requirementText\":null,\"subrequirementText\":\"\",\"group\":null,\"startTime\":null,\"endTime\":null,\"openSections\":null,\"subjects\":[],\"academicLevels\":[],\"courseLevels\":[],\"synonyms\":[],\"courseTypes\":[],\"topicCodes\":[],\"days\":[],\"locations\":[\"CCCSS\"],\"faculty\":[],\"onlineCategories\":null,\"keywordComponents\":[],\"startDate\":null,\"endDate\":null,\"startsAtTime\":null,\"endsByTime\":null,\"pageNumber\":1,\"sortOn\":\"SectionName\",\"sortDirection\":\"Ascending\",\"subjectsBadge\":[],\"locationsBadge\":[],\"termFiltersBadge\":[],\"daysBadge\":[],\"facultyBadge\":[],\"academicLevelsBadge\":[],\"courseLevelsBadge\":[],\"courseTypesBadge\":[],\"topicCodesBadge\":[],\"onlineCategoriesBadge\":[],\"openSectionsBadge\":\"\",\"openAndWaitlistedSectionsBadge\":\"\",\"subRequirementText\":null,\"quantityPerPage\":30,\"openAndWaitlistedSections\":null,\"searchResultsView\":\"SectionListing\"}"
    course_headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json, charset=UTF-8',
    'Origin': 'https://selfservice.scccd.edu',
    'Referer': 'https://selfservice.scccd.edu/Student/Courses/SearchResult',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    # DIRECTORY CONFIG
    directory_url = "http://www.clovis.edu/"

    # CALENDAR CONFIG
    calendar_source_url = "https://www.cloviscollege.edu/current-students/academic-calendar.html"
    
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course and Calendar urls return proper response so using scrapy
        for data collection
        
        -Campus url not found in the website so we skipping that.
        """

        # Single functions
        if mode == "course":
            yield scrapy.Request(url=self.course_url, method="POST", body=self.course_payload, headers=self.course_headers, callback=self.parse_course)

        elif mode == "directory":
            self.parse_directory()

        elif mode == "calendar":
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            yield scrapy.Request(url=self.course_url, method="POST", body=self.course_payload, headers=self.course_headers, callback=self.parse_course)
            self.parse_directory()

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.course_url, method="POST", body=self.course_payload, headers=self.course_headers, callback=self.parse_course)

        elif mode in ["directory_calendar", "calendar_directory"]:
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

        #  All three (default)
        else:
            yield scrapy.Request(url=self.course_url, method="POST", body=self.course_payload, headers=self.course_headers, callback=self.parse_course)
            self.parse_directory()
            yield scrapy.Request(url=self.calendar_source_url, callback=self.parse_calendar)

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
        
        json_data =  json.loads(response.text)
        page_number = json_data.get('TotalPages', '')
        for page in range(1, page_number + 1):
            url = "https://selfservice.scccd.edu/Student/Courses/PostSearchCriteria"
            payload = "{\"keyword\":null,\"terms\":[],\"requirement\":null,\"subrequirement\":null,\"courseIds\":null,\"sectionIds\":null,\"requirementText\":null,\"subrequirementText\":\"\",\"group\":null,\"startTime\":null,\"endTime\":null,\"openSections\":null,\"subjects\":[],\"academicLevels\":[],\"courseLevels\":[],\"synonyms\":[],\"courseTypes\":[],\"topicCodes\":[],\"days\":[],\"locations\":[\"CCCSS\"],\"faculty\":[],\"onlineCategories\":null,\"keywordComponents\":[],\"startDate\":null,\"endDate\":null,\"startsAtTime\":null,\"endsByTime\":null,\"pageNumber\":" + str(page) + ",\"sortOn\":\"SectionName\",\"sortDirection\":\"Ascending\",\"subjectsBadge\":[],\"locationsBadge\":[],\"termFiltersBadge\":[],\"daysBadge\":[],\"facultyBadge\":[],\"academicLevelsBadge\":[],\"courseLevelsBadge\":[],\"courseTypesBadge\":[],\"topicCodesBadge\":[],\"onlineCategoriesBadge\":[],\"openSectionsBadge\":\"\",\"openAndWaitlistedSectionsBadge\":\"\",\"subRequirementText\":null,\"quantityPerPage\":30,\"openAndWaitlistedSections\":null,\"searchResultsView\":\"SectionListing\"}"
            headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
            'Connection': 'keep-alive',
            'Content-Type': 'application/json, charset=UTF-8',
            'Origin': 'https://selfservice.scccd.edu',
            'Referer': 'https://selfservice.scccd.edu/Student/Courses/SearchResult',
            }
            yield scrapy.Request(url=url, headers=headers, method="POST", body=payload, callback=self.parse_course_final,dont_filter=True)
    
    def parse_course_final(self, response):
        json_data =  json.loads(response.text)
        blocks = json_data.get('Sections', [])
        for block in blocks:
            meetings = block.get('FormattedMeetingTimes', [])
            for meeting in meetings:
                location_display = block.get('LocationDisplay', '').strip()
                loc = meeting.get('BuildingDisplay', '').strip()
                inst = meeting.get('InstructionalMethodDisplay', '').strip()
                room = meeting.get('RoomDisplay', '').strip()
                location = f'{location_display}, {loc} {room} ({inst})'.strip()
                name = block.get('FullTitleDisplay', '').strip()
                parts = name.split()
                course_and_section = parts[0]    
                course_parts = course_and_section.split('-')
                class_num = course_parts[2]
                section = f'{course_parts[0]}-{course_parts[1]}'
                name = f"{section} {name.split(class_num)[-1].strip()}"
                description = block.get('CourseDescription', '').strip()
                instructor = block['FacultyDisplay']
                self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_source_url,
                    "Course Name": re.sub(r'\s+',' ',name),
                    "Course Description": re.sub(r'\s+',' ',description),
                    "Class Number": class_num,
                    "Section": '',
                    "Instructor": ', '.join(instructor),
                    "Enrollment": f"{block.get('Enrolled', '')} of {block.get('Capacity', '')}",
                    "Course Dates": meeting.get('DatesDisplay', '').strip(),
                    "Location": re.sub(r'\s+',' ',location),
                    "Textbook/Course Materials": block.get('BookstoreUrl', '').strip(),
                })

    def parse_directory(self):
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
    
        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": self.directory_url,
        "Name": 'Data not found',
        "Title": 'Data not found',
        "Email": 'Data not found',
        "Phone Number": 'Data not found',
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
            description = block.xpath('.//td[3]/text()').get('').strip()
            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_source_url,
                "Term Name": block.xpath('.//parent::tbody/parent::table/parent::div/parent::div/parent::div/h2/text()').get('').strip(),
                "Term Date": block.xpath('.//td[1]/text()').get('').strip(),
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

        