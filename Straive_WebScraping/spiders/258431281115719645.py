import json
import math
import scrapy
import pandas as pd
from ..utils import *
from inline_requests import inline_requests


class HillcollegeSpider(scrapy.Spider):

    name = "hillcollege"
    institution_id = 258431281115719645
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    codes = ["SP","FA","SU"]
    code_values = {'SU':'1103','SP':'719','FA':'718'}
    course_main_url = 'https://myhc.hillcollege.edu/ICS/Academics/Course_Schedule__Syllabus.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView'

    # DIRECTORY CONFIG
    directory_source_url = "https://www.hillcollege.edu/Directory/EmpDir.html"

    # CALENDAR CONFIG
    calendar_source_urls = ["https://www.hillcollege.edu/Calendars/Calendar25-26.html",
                                    "https://www.hillcollege.edu/Calendars/Calendar26-27.html"]

    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")

        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        -all three method datas scraing using scrapy
        """
        # Single functions
        if mode == "course":
            for code in self.codes:
                id = self.code_values[code]
                course_source_url = f"https://myhc.hillcollege.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={id}&IdNumber=-1&YearCode=2025&TermCode={code}"
                course_payload = json.dumps({
                "pageState": {
                    "enabled": True,
                    "keywordFilter": "",
                    "quickFilters": [],
                    "sortColumn": "",
                    "sortAscending": True,
                    "currentPage": 1,
                    "pageSize": 200,
                    "showingAll": False,
                    "selectedAll": False,
                    "excludedFromSelection": [],
                    "includedInSelection": [],
                    "advancedFilters": [
                    {
                        "name": "courseCode",
                        "value": ""
                    },
                    {
                        "name": "courseCodeType",
                        "value": "0"
                    },
                    {
                        "name": "courseTitle",
                        "value": ""
                    },
                    {
                        "name": "courseTitleType",
                        "value": "0"
                    },
                    {
                        "name": "requestNumber",
                        "value": ""
                    },
                    {
                        "name": "instructorIds",
                        "value": ""
                    },
                    {
                        "name": "departmentIds",
                        "value": ""
                    },
                    {
                        "name": "locationIds",
                        "value": ""
                    },
                    {
                        "name": "competencyIds",
                        "value": ""
                    },
                    {
                        "name": "beginsAfter",
                        "value": ""
                    },
                    {
                        "name": "beginsBefore",
                        "value": ""
                    },
                    {
                        "name": "instructionalMethods",
                        "value": ""
                    },
                    {
                        "name": "sectionStatus",
                        "value": ""
                    },
                    {
                        "name": "startCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "endCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "division",
                        "value": ""
                    },
                    {
                        "name": "place",
                        "value": ""
                    },
                    {
                        "name": "subterm",
                        "value": ""
                    },
                    {
                        "name": "meetsOnDays",
                        "value": "0,0,0,0,0,0,0"
                    }
                    ],
                }
                })
                course_headers = {
                'accept': 'text/html, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
                'content-type': 'application/json',
                'origin': 'https://myhc.hillcollege.edu',
                'priority': 'u=1, i',
                'referer': 'https://myhc.hillcollege.edu/ICS/Academics/Course_Schedule__Syllabus.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView',
                'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url = course_source_url, method='POST', body=course_payload, headers=course_headers, callback=self.parse_course, dont_filter=True,cb_kwargs={'course_source_url': course_source_url, 'course_headers': course_headers})

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            for url in self.calendar_source_urls:
                yield scrapy.Request(url = url, callback=self.parse_calendar, dont_filter=True,cb_kwargs={'calendar_source_url': url})

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            for code in self.codes:
                id = self.code_values[code]
                course_source_url = f"https://myhc.hillcollege.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={id}&IdNumber=-1&YearCode=2025&TermCode={code}"
                course_payload = json.dumps({
                "pageState": {
                    "enabled": True,
                    "keywordFilter": "",
                    "quickFilters": [],
                    "sortColumn": "",
                    "sortAscending": True,
                    "currentPage": 1,
                    "pageSize": 200,
                    "showingAll": False,
                    "selectedAll": False,
                    "excludedFromSelection": [],
                    "includedInSelection": [],
                    "advancedFilters": [
                    {
                        "name": "courseCode",
                        "value": ""
                    },
                    {
                        "name": "courseCodeType",
                        "value": "0"
                    },
                    {
                        "name": "courseTitle",
                        "value": ""
                    },
                    {
                        "name": "courseTitleType",
                        "value": "0"
                    },
                    {
                        "name": "requestNumber",
                        "value": ""
                    },
                    {
                        "name": "instructorIds",
                        "value": ""
                    },
                    {
                        "name": "departmentIds",
                        "value": ""
                    },
                    {
                        "name": "locationIds",
                        "value": ""
                    },
                    {
                        "name": "competencyIds",
                        "value": ""
                    },
                    {
                        "name": "beginsAfter",
                        "value": ""
                    },
                    {
                        "name": "beginsBefore",
                        "value": ""
                    },
                    {
                        "name": "instructionalMethods",
                        "value": ""
                    },
                    {
                        "name": "sectionStatus",
                        "value": ""
                    },
                    {
                        "name": "startCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "endCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "division",
                        "value": ""
                    },
                    {
                        "name": "place",
                        "value": ""
                    },
                    {
                        "name": "subterm",
                        "value": ""
                    },
                    {
                        "name": "meetsOnDays",
                        "value": "0,0,0,0,0,0,0"
                    }
                    ],
                }
                })
                course_headers = {
                'accept': 'text/html, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
                'content-type': 'application/json',
                'origin': 'https://myhc.hillcollege.edu',
                'priority': 'u=1, i',
                'referer': 'https://myhc.hillcollege.edu/ICS/Academics/Course_Schedule__Syllabus.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView',
                'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url = course_source_url, method='POST', body=course_payload, headers=course_headers, callback=self.parse_course, dont_filter=True,cb_kwargs={'course_source_url': course_source_url, 'course_headers': course_headers})

            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            for url in self.calendar_source_urls:
                yield scrapy.Request(url = url, callback=self.parse_calendar, dont_filter=True,cb_kwargs={'calendar_source_url': url})
            for code in self.codes:
                id = self.code_values[code]
                course_source_url = f"https://myhc.hillcollege.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={id}&IdNumber=-1&YearCode=2025&TermCode={code}"
                course_payload = json.dumps({
                "pageState": {
                    "enabled": True,
                    "keywordFilter": "",
                    "quickFilters": [],
                    "sortColumn": "",
                    "sortAscending": True,
                    "currentPage": 1,
                    "pageSize": 200,
                    "showingAll": False,
                    "selectedAll": False,
                    "excludedFromSelection": [],
                    "includedInSelection": [],
                    "advancedFilters": [
                    {
                        "name": "courseCode",
                        "value": ""
                    },
                    {
                        "name": "courseCodeType",
                        "value": "0"
                    },
                    {
                        "name": "courseTitle",
                        "value": ""
                    },
                    {
                        "name": "courseTitleType",
                        "value": "0"
                    },
                    {
                        "name": "requestNumber",
                        "value": ""
                    },
                    {
                        "name": "instructorIds",
                        "value": ""
                    },
                    {
                        "name": "departmentIds",
                        "value": ""
                    },
                    {
                        "name": "locationIds",
                        "value": ""
                    },
                    {
                        "name": "competencyIds",
                        "value": ""
                    },
                    {
                        "name": "beginsAfter",
                        "value": ""
                    },
                    {
                        "name": "beginsBefore",
                        "value": ""
                    },
                    {
                        "name": "instructionalMethods",
                        "value": ""
                    },
                    {
                        "name": "sectionStatus",
                        "value": ""
                    },
                    {
                        "name": "startCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "endCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "division",
                        "value": ""
                    },
                    {
                        "name": "place",
                        "value": ""
                    },
                    {
                        "name": "subterm",
                        "value": ""
                    },
                    {
                        "name": "meetsOnDays",
                        "value": "0,0,0,0,0,0,0"
                    }
                    ],
                }
                })
                course_headers = {
                'accept': 'text/html, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
                'content-type': 'application/json',
                'origin': 'https://myhc.hillcollege.edu',
                'priority': 'u=1, i',
                'referer': 'https://myhc.hillcollege.edu/ICS/Academics/Course_Schedule__Syllabus.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView',
                'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url = course_source_url, method='POST', body=course_payload, headers=course_headers, callback=self.parse_course, dont_filter=True,cb_kwargs={'course_source_url': course_source_url, 'course_headers': course_headers})

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            for url in self.calendar_source_urls:
                yield scrapy.Request(url = url, callback=self.parse_calendar, dont_filter=True,cb_kwargs={'calendar_source_url': url})

        #  All three (default)
        else:
            for code in self.codes:
                id = self.code_values[code]
                course_source_url = f"https://myhc.hillcollege.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={id}&IdNumber=-1&YearCode=2025&TermCode={code}"
                course_payload = json.dumps({
                "pageState": {
                    "enabled": True,
                    "keywordFilter": "",
                    "quickFilters": [],
                    "sortColumn": "",
                    "sortAscending": True,
                    "currentPage": 1,
                    "pageSize": 200,
                    "showingAll": False,
                    "selectedAll": False,
                    "excludedFromSelection": [],
                    "includedInSelection": [],
                    "advancedFilters": [
                    {
                        "name": "courseCode",
                        "value": ""
                    },
                    {
                        "name": "courseCodeType",
                        "value": "0"
                    },
                    {
                        "name": "courseTitle",
                        "value": ""
                    },
                    {
                        "name": "courseTitleType",
                        "value": "0"
                    },
                    {
                        "name": "requestNumber",
                        "value": ""
                    },
                    {
                        "name": "instructorIds",
                        "value": ""
                    },
                    {
                        "name": "departmentIds",
                        "value": ""
                    },
                    {
                        "name": "locationIds",
                        "value": ""
                    },
                    {
                        "name": "competencyIds",
                        "value": ""
                    },
                    {
                        "name": "beginsAfter",
                        "value": ""
                    },
                    {
                        "name": "beginsBefore",
                        "value": ""
                    },
                    {
                        "name": "instructionalMethods",
                        "value": ""
                    },
                    {
                        "name": "sectionStatus",
                        "value": ""
                    },
                    {
                        "name": "startCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "endCourseNumRange",
                        "value": ""
                    },
                    {
                        "name": "division",
                        "value": ""
                    },
                    {
                        "name": "place",
                        "value": ""
                    },
                    {
                        "name": "subterm",
                        "value": ""
                    },
                    {
                        "name": "meetsOnDays",
                        "value": "0,0,0,0,0,0,0"
                    }
                    ],
                }
                })
                course_headers = {
                'accept': 'text/html, */*; q=0.01',
                'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
                'content-type': 'application/json',
                'origin': 'https://myhc.hillcollege.edu',
                'priority': 'u=1, i',
                'referer': 'https://myhc.hillcollege.edu/ICS/Academics/Course_Schedule__Syllabus.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView',
                'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }
                yield scrapy.Request(url = course_source_url, method='POST', body=course_payload, headers=course_headers, callback=self.parse_course, dont_filter=True,cb_kwargs={'course_source_url': course_source_url, 'course_headers': course_headers})
            yield scrapy.Request(url = self.directory_source_url, callback=self.parse_directory, dont_filter=True)
            for url in self.calendar_source_urls:
                yield scrapy.Request(url = url, callback=self.parse_calendar, dont_filter=True,cb_kwargs={'calendar_source_url': url})

    # PARSE COURSE
    def parse_course(self,response, course_source_url, course_headers):
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
        - "Course Dates"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        json_data = json.loads(response.text)
        total_count = json_data.get('filteredRows','')
        page_count = math.ceil(int(total_count)/200)
        for page in range(0,page_count+1):
            url = course_source_url
            course_payload = json.dumps({
            "pageState": {
                "enabled": True,
                "keywordFilter": "",
                "quickFilters": [],
                "sortColumn": "",
                "sortAscending": True,
                "currentPage": page,
                "pageSize": 200,
                "showingAll": False,
                "selectedAll": False,
                "excludedFromSelection": [],
                "includedInSelection": [],
                "advancedFilters": [
                {
                    "name": "courseCode",
                    "value": ""
                },
                {
                    "name": "courseCodeType",
                    "value": "0"
                },
                {
                    "name": "courseTitle",
                    "value": ""
                },
                {
                    "name": "courseTitleType",
                    "value": "0"
                },
                {
                    "name": "requestNumber",
                    "value": ""
                },
                {
                    "name": "instructorIds",
                    "value": ""
                },
                {
                    "name": "departmentIds",
                    "value": ""
                },
                {
                    "name": "locationIds",
                    "value": ""
                },
                {
                    "name": "competencyIds",
                    "value": ""
                },
                {
                    "name": "beginsAfter",
                    "value": ""
                },
                {
                    "name": "beginsBefore",
                    "value": ""
                },
                {
                    "name": "instructionalMethods",
                    "value": ""
                },
                {
                    "name": "sectionStatus",
                    "value": ""
                },
                {
                    "name": "startCourseNumRange",
                    "value": ""
                },
                {
                    "name": "endCourseNumRange",
                    "value": ""
                },
                {
                    "name": "division",
                    "value": ""
                },
                {
                    "name": "place",
                    "value": ""
                },
                {
                    "name": "subterm",
                    "value": ""
                },
                {
                    "name": "meetsOnDays",
                    "value": "0,0,0,0,0,0,0"
                }
                ],
            }
            })
            header = course_headers
            yield scrapy.Request(url=url, method="POST", body=course_payload, headers=header,callback=self.parse_pagination_course, dont_filter=True)

    @inline_requests
    def parse_pagination_course(self,response):
        json_data = json.loads(response.text)
        blocks = json_data['rows']
        for block in blocks:
            schedule = block.get('schedule','')
            dates = schedule.split("strong-text'>")[-1].rsplit("<span>", 1)[0].strip()
            course_code = block.get('courseCode','').split('</a>')[0].split('>')[-1].strip()
            course_code = re.sub(r'\s+',' ',course_code)
            course_name = block.get('title','').split('>')[-1].strip()
            section_block = block.get('courseCode','')
            if re.search(r"data-sectionid\=\'(.*?)'",section_block):
                section_id = re.findall(r"data-sectionid\=\'(.*?)'",section_block)[0]
            if re.search(r"data-advisingrequirementcode\=\'(.*?)'",section_block):
                section_code = re.findall(r"data-advisingrequirementcode\=\'(.*?)'",section_block)[0]
            url = "https://myhc.hillcollege.edu/ICS/Portlets/CRM/Common/Jenzabar.EX.Common/Handlers/AjaxLoadablePortletPartialHandler.ashx"
            payload = f"IdNumber=-1&stuRegTermSelect=2025%3BFA&SectionId={section_id}&IsFreeElectiveSearch=False&FromCourseSearch=True&AdvisingRequirementCode={section_code}&StudentPlanDetailId=0&WeekdayCode=&StudentStatus=&%7CInsertOption%7C=&%7CLocationSelector%7C=&%7CControlPath%7C=~%2FPortlets%2FCRM%2FStudent%2FPortlet.StudentRegistration%2FControls%2FSectionDetailsModal.ascx&%7CUseForm%7C=False"
            headers = {
            'accept': 'application/json, text/javascript, */*; q=0.01',
            'accept-language': 'en-US,en;q=0.9,ta;q=0.8',
            'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'origin': 'https://myhc.hillcollege.edu',
            'priority': 'u=0, i',
            'referer': 'https://myhc.hillcollege.edu/ICS/Academics/Course_Schedule__Syllabus.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            }
            detail_response = yield scrapy.Request(url=url,method='POST',body= payload,headers=headers)

            if re.search(r'course-description\\\"\>(.*?)\<\/span\>',detail_response.text):
                description = re.findall(r'course-description\\\"\>(.*?)\<\/span\>',detail_response.text)[0].replace('\\r',' ').replace('\\n',' ').replace('\\t',' ')
            location = schedule.rsplit("<span>", 1)[-1].split("</span>")[0]
            if location == 'No schedule available' or location == 'See all schedules':
                location = ''
            else:
                location = location
            if re.search(r'\d{2}\/\d{2}\/\d{2}\s*-\s*\d{2}\/\d{2}\/\d{2}',detail_response.text):
                dates = re.findall(r'\d{2}\/\d{2}\/\d{2}\s*-\s*\d{2}\/\d{2}\/\d{2}',detail_response.text)[0].replace('\\r',' ').replace('\\n',' ').replace('\\t',' ')
            self.course_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_main_url,
                    "Course Name": f'{course_code} - {course_name}',
                    "Course Description": re.sub('\s+',' ',description).replace('\r',' ').replace('\n',' ').replace('\t',' ').strip(),
                    "Class Number": course_code,
                    "Section": '',
                    "Instructor": block.get('faculty','').split("title='")[-1].split("'>")[0].split('-')[0].strip(),
                    "Enrollment": block.get('seatsOpen','').split('>')[-1].strip(),
                    "Course Dates": dates,
                    "Location": location,
                    "Textbook/Course Materials": '',
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

        blocks = response.xpath('//tbody[@id="myTable"]/tr')
        for block in blocks:
            name = ' '.join(block.xpath('.//td[@class="dir-name"]/text()').getall()).strip()
            if name:
                self.directory_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_source_url,
                        "Name": name,
                        "Title": ' '.join(block.xpath('.//td[@class="dir-title"]//text()').getall()).strip(),
                        "Email": block.xpath('.//td[@class="dir-email"]/a/@href').get('').replace('mailto:', '').replace('//','').replace('#','').strip(),
                        "Phone Number": block.xpath('.//td[@class="dir-phone"]/text() | .//td[@class="dir-phone"]/span/text()').get('').strip(),
                    })

    # PARSE CALENDAR
    def parse_calendar(self,response,calendar_source_url):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """        
        
        blocks = response.xpath('//div[@class="d-flex justify-content-between mb-3"]')
        for block in blocks:
            self.calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": calendar_source_url,
                    "Term Name": block.xpath('./preceding-sibling::div[contains(@class,"d-flex justify-content-center bg")]/parent::section/parent::div/div/h3/text() | ./preceding-sibling::div/parent::div/div/h3/text() | .//preceding-sibling::div/parent::td/parent::tr/parent::tbody/parent::table/parent::div/parent::section/parent::div/div/h3/text() | .//preceding-sibling::div/parent::td/parent::tr/parent::tbody/parent::table/parent::section/parent::div/div/h3/text()').get().replace('Academic Calendar', '').strip(),
                    "Term Date": ' '.join(block.xpath('.//div[2]/text() | .//td[1]//text()').getall()).strip(),
                    "Term Date Description": block.xpath('.//div[1]/text() | .//td[2]//text()').get('').strip(),
                })

        term_map_25_26 = {
            0: "Fall 2025",
            1: "Summer 2026",
            2: "Spring 2026"
        }

        term_map_26_27 = {
            0: "Fall 2026",
            1: "Summer 2027",
            2: "Spring 2027"
        }
        holiday_blocks = response.xpath(
            '//h4[contains(text(),"Holidays")]/parent::caption/parent::table/tbody'
        )

        for index, tbody in enumerate(holiday_blocks):
            for block in tbody.xpath('./tr'):
                term_name = block.xpath('.//preceding-sibling::div/parent::td/parent::tr/parent::tbody/parent::table/parent::div/parent::section/parent::div/div/h3/text() | .//preceding-sibling::div/parent::td/parent::tr/parent::tbody/parent::table/parent::section/parent::div/div/h3/text() | .//preceding-sibling::div/parent::td/parent::tr/parent::tbody/parent::table/parent::div/parent::section/parent::div/div/h3/text() | .//preceding-sibling::tr//div//parent::td/parent::tr/parent::tbody/parent::table/parent::section/parent::div/div/h3/text() | .//preceding-sibling::tr//div//parent::td/parent::tr/parent::tbody/parent::table/parent::div/parent::section/parent::div/div/h3/text()').get('').replace('Academic Calendar', '').strip()
                if not term_name:
                    if '26-27' in calendar_source_url:
                        term_name = term_map_26_27.get(index)
                    else:
                        term_name = term_map_25_26.get(index)
    
                self.calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": calendar_source_url,
                        "Term Name": term_name,
                        "Term Date": block.xpath('.//td[2]//text()').get('').strip(),
                        "Term Date Description": ' '.join(block.xpath('.//td[1]//text()').getall()).strip(),
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