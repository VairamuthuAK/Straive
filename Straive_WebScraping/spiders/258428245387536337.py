import re
import json
import time
import scrapy
import requests
import pdfplumber
import pandas as pd
from io import BytesIO
from ..utils import *
from parsel import Selector
from datetime import datetime
from inline_requests import inline_requests


def clean_string(data):
    return re.sub(r"\s+", " ", data.replace("\t", "").replace("\n", " ").replace("\\r\\n", "")).strip()

def create_session():
    """
    Create and return a configured requests session
    """
    session = requests.Session()
    session.headers.update({
        'accept': 'application/json, text/javascript, */*; q=0.01',
        'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'origin': 'https://myssu.shawnee.edu',
        'referer': 'https://myssu.shawnee.edu/ICS/Course_Search.jnz',
        'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'empty',
        'sec-fetch-mode': 'cors',
        'sec-fetch-site': 'same-origin',
        'user-agent': (
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/144.0.0.0 Safari/537.36'
        ),
        'x-requested-with': 'XMLHttpRequest'
    })
    return session


def fetch_course_search(session):
    """
    Perform the course search POST request
    """
    url = (
        "https://myssu.shawnee.edu/ICS/Course_Search.jnz"
        "?portlet=Course_Search"
        "&screen=StudentRegistrationPortlet_CourseSearchView"
        "&screenType=next"
    )
    response = session.get(url, timeout=30)
    response.raise_for_status()  # fail fast if HTTP error
    return response.text


class ShawneeSpider(scrapy.Spider):
    name = "shawnee"
    institution_id = 258428245387536337
    course_rows = []
    
    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://myssu.shawnee.edu/ICS/Course_Search.jnz?portlet=Course_Search&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next"
    directory_source_url = "https://ssubears.com/staff-directory"
    calendar_url = "https://www.shawnee.edu/academic-calendar"
    
    course_headers = {
    'accept': 'text/html, */*; q=0.01',
    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
    'content-type': 'application/json',
    'origin': 'https://myssu.shawnee.edu',
    'referer': 'https://myssu.shawnee.edu/ICS/Course_Search.jnz?portlet=Course_Search&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    "X-Requested-With": "XMLHttpRequest",
    }

   
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
        
        term_years = response.xpath('//select[@id="stuRegTermSelect"]//option/@value').getall()
        term_names = response.xpath('//select[@id="stuRegTermSelect"]//option/text()').getall()
        for term_year, term_name in zip(term_years, term_names):
            year = term_year.split(';')[0]
            term_code = term_year.split(';')[-1]
            current_year = datetime.now().year
            match = re.search(r"\d+\-\b(20\d{2})\b", term_name)
            if match and int(match.group(1)) >= current_year:
                print(term_name)
                time.sleep(5)
                self.parse_course1(year, term_code, term_name)
                
        # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df,    self.institution_id, "course")
                
    def parse_course1(self, year, term_code, term_name):
        # Create a persistent session for multiple POST requests
        session = create_session()

        try:
            # Determine internal term ID based on term name
            id = ''
            if "Fall" in term_name:
                id = "200"
            elif "Spring" in term_name:
                id = "199"
            elif "Summer" in term_name:
                id = "198"
            
            # Build search API URL with year and term parameters
            url = f"https://myssu.shawnee.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={id}&IdNumber=-1&YearCode={year}&TermCode={term_code}"
            
            # Initial search payload (page 0)
            payload = json.dumps({
            "pageState": {
                "enabled": True,
                "keywordFilter": "",
                "quickFilters": [],
                "sortColumn": "",
                "sortAscending": True,
                "currentPage": 0,
                "pageSize": 15,
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
                "totalRows": 0,
                "filteredRows": 0,
                "quickFilterCounts": None
            }
            })
            headers = {
            'accept': 'text/html, */*; q=0.01',
            'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
            'content-type': 'application/json',
            'origin': 'https://myssu.shawnee.edu',
            'referer': 'https://myssu.shawnee.edu/ICS/Course_Search.jnz?portlet=Course_Search&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
            'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            }

            post_response = session.post(url, headers=headers, data=payload)
            post_json_data = json.loads(post_response.text)
            blocks = post_json_data['rows']
            
            # Process each course block
            for block in blocks:
                
                post_html_xpath = Selector(text=str(str(block)))
                r_code = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/@data-advisingrequirementcode').get('').strip()
                data_id = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/@data-idnumber').get('').strip()
                sec_id = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/@data-sectionid').get('').strip()
                code = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/text()').get('').strip()
                sec = code.split('-')[-1]
                course_no = code.split('-')[1]
                sub = code.split('-')[0]
                course = f"{sub}-{course_no}"
                
                seats = ""
                match = re.search(r"Seats\s*open</label>([^\"<]+)", str(block))
                if match:
                    seats = match.group(1).strip()

                enrollment = ""
                if seats and "/" in seats:
                    taken, capacity = seats.split("/", 1)
                    taken = int(taken)
                    capacity = int(capacity)
                    enroll = capacity - taken
                    enrollment = f"{enroll}/{capacity}"
                
                title = ''
                if re.search(r'Title\<\/label\>(.*?)\"',str(block)):
                    title = re.findall(r'Title\<\/label\>(.*?)\"',str(block))[0]
                full_title = f"{course} {title}"
                detail_url = (
                    "https://myssu.shawnee.edu/ICS/Portlets/CRM/Common/"
                    "Jenzabar.EX.Common/Handlers/AjaxLoadablePortletPartialHandler.ashx"
                )

                payload = {
                    "IdNumber": data_id,
                    "stuRegTermSelect": f"{year};{term_code}",
                    "SectionId": sec_id,
                    "IsFreeElectiveSearch": "False",
                    "FromCourseSearch": "True",
                    "AdvisingRequirementCode": r_code,
                    "StudentPlanDetailId": "0",
                    "WeekdayCode": "",
                    "StudentStatus": "",
                    "|InsertOption|": "",
                    "|LocationSelector|": "",
                    "|ControlPath|": "~/Portlets/CRM/Student/Portlet.StudentRegistration/Controls/SectionDetailsModal.ascx",
                    "|UseForm|": "False",
                }
                detail_headers = {
                'accept': 'text/html, */*; q=0.01',
                'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
                'content-type': 'application/json',
                'origin': 'https://myssu.shawnee.edu',
                'referer': 'https://myssu.shawnee.edu/ICS/Course_Search.jnz?portlet=Course_Search&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
                'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
                }

                # Fetch detailed course information
                detail_headers.update({
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "X-Requested-With": "XMLHttpRequest",
                    "Referer": "https://myssu.shawnee.edu/ICS/Course_Search.jnz",
                })

                detail_response = session.post(
                    detail_url,
                    headers=detail_headers,
                    data=payload,
                    timeout=30
                )

                print("STATUS:", detail_response.status_code)
                json_data = json.loads(detail_response.text)
                html = json_data.get('Html','')
                html_xpath = Selector(text=html)
                desc = html_xpath.xpath('//span[@class="course-description"]/text()').get('').strip()
                instructor = " | ".join(html_xpath.xpath('//div[contains(text(),"Instructor")]/following-sibling::div[@class="row"]/div[@class="col-xs-6 ju-text-overflow-ellipsis"]/text()').getall()).strip()
                dates = " | ".join(html_xpath.xpath('//div[contains(text(),"Schedule:")]/following-sibling::div/span[@class="text-nowrap"]/text()').getall()).strip()
                
                # Extract date ranges (MM/DD/YYYY - MM/DD/YYYY)
                date_all = ''
                if re.search(r'(\d+\/\d+\/\d+\s*\-\s*\d+\/\d+\/\d+)',dates):
                    date_all = re.findall(r'(\d+\/\d+\/\d+\s*\-\s*\d+\/\d+\/\d+)',dates)
                    date_all = list(set(date_all))
                
                desc = clean_string(desc)
                cleaned_desc = re.sub(r'^Course Description:?\s*', '', desc)
                
                # Append course rows
                # If multiple date ranges exist, create multiple rows
                if date_all:
                    for date in date_all:
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": 'https://myssu.shawnee.edu/ICS/Course_Search.jnz',
                            "Course Name": full_title,
                            "Course Description": cleaned_desc,
                            "Class Number": course,
                            "Section": sec,
                            "Instructor": clean_string(instructor),
                            "Enrollment": enrollment,
                            "Course Dates": clean_string(date),
                            "Location": '',   
                            "Textbook/Course Materials": '',
                        })
                else:
                    # If no dates found, still append record with empty date
                    self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": 'https://myssu.shawnee.edu/ICS/Course_Search.jnz',
                            "Course Name": full_title,
                            "Course Description": cleaned_desc,
                            "Class Number": course,
                            "Section": sec,
                            "Instructor": clean_string(instructor),
                            "Enrollment": enrollment,
                            "Course Dates": '',
                            "Location": '',   
                            "Textbook/Course Materials": '',
                        })
                    
            # Pagination Handling
            json_data = json.loads(post_response.text)
            filteredRows = json_data.get('filteredRows','')
            totalRows = json_data.get('totalRows','')
            
            # Calculate number of additional pages
            page_count = (filteredRows + 15) // totalRows if totalRows != 0 else 0
            for page in range(1,page_count,1):
                print(f"page----{page}")
                data_url = f"https://myssu.shawnee.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={id}&IdNumber=-1&YearCode={year}&TermCode={term_code}"

                payload = json.dumps({
                "pageState": {
                    "enabled": True,
                    "keywordFilter": "",
                    "quickFilters": [],
                    "sortColumn": "",
                    "sortAscending": True,
                    "currentPage": page,
                    "pageSize": totalRows,
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
                    "totalRows": totalRows,
                    "filteredRows": filteredRows,
                    "quickFilterCounts": None
                }
                })
            

                page_response = session.post( data_url, headers=headers, data=payload)
                page_json_data = json.loads(page_response.text)
                blocks1 = page_json_data['rows']
                for block in blocks1:
                    post_html_xpath = Selector(text=str(str(block)))
                    r_code = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/@data-advisingrequirementcode').get('').strip()
                    data_id = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/@data-idnumber').get('').strip()
                    sec_id = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/@data-sectionid').get('').strip()
                    code = post_html_xpath.xpath('//a[@class="viewsectiondetails menu-option"]/text()').get('').strip()
                    sec = code.split('-')[-1]
                    course_no = code.split('-')[1]
                    sub = code.split('-')[0]
                    course = f"{sub}-{course_no}"
                    
                    seats = ""
                    match = re.search(r"Seats\s*open</label>([^\"<]+)", str(block))
                    if match:
                        seats = match.group(1).strip()

                    enrollment = ""
                    if seats and "/" in seats:
                        taken, capacity = seats.split("/", 1)
                        taken = int(taken)
                        capacity = int(capacity)
                        enroll = capacity - taken
                        enrollment = f"{enroll}/{capacity}"
                    
                    title = ''
                    if re.search(r'Title\<\/label\>(.*?)\"',str(block)):
                        title = re.findall(r'Title\<\/label\>(.*?)\"',str(block))[0]
                    full_title = f"{course} {title}"
                    detail_url = (
                        "https://myssu.shawnee.edu/ICS/Portlets/CRM/Common/"
                        "Jenzabar.EX.Common/Handlers/AjaxLoadablePortletPartialHandler.ashx"
                    )

                    payload = {
                        "IdNumber": data_id,
                        "stuRegTermSelect": f"{year};{term_code}",
                        "SectionId": sec_id,
                        "IsFreeElectiveSearch": "False",
                        "FromCourseSearch": "True",
                        "AdvisingRequirementCode": r_code,
                        "StudentPlanDetailId": "0",
                        "WeekdayCode": "",
                        "StudentStatus": "",
                        "|InsertOption|": "",
                        "|LocationSelector|": "",
                        "|ControlPath|": "~/Portlets/CRM/Student/Portlet.StudentRegistration/Controls/SectionDetailsModal.ascx",
                        "|UseForm|": "False",
                    }

                    detail_headers = {
                    'accept': 'text/html, */*; q=0.01',
                    'accept-language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
                    'content-type': 'application/json',
                    'origin': 'https://myssu.shawnee.edu',
                    'referer': 'https://myssu.shawnee.edu/ICS/Course_Search.jnz?portlet=Course_Search&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest',
                    }

                    detail_headers.update({
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": "https://myssu.shawnee.edu/ICS/Course_Search.jnz",
                    })

                    detail_response = session.post(
                        detail_url,
                        headers=detail_headers,
                        data=payload,
                        timeout=30
                    )

                    print("STATUS:", detail_response.status_code)
                    json_data = json.loads(detail_response.text)
                    html = json_data.get('Html','')
                    html_xpath = Selector(text=html)
                    desc = html_xpath.xpath('//span[@class="course-description"]/text()').get('').strip()
                    instructor = " | ".join(html_xpath.xpath('//div[contains(text(),"Instructor")]/following-sibling::div[@class="row"]/div[@class="col-xs-6 ju-text-overflow-ellipsis"]/text()').getall()).strip()
                    dates = " | ".join(html_xpath.xpath('//div[contains(text(),"Schedule:")]/following-sibling::div/span[@class="text-nowrap"]/text()').getall()).strip()
                
                    date_all = ''
                    if re.search(r'(\d+\/\d+\/\d+\s*\-\s*\d+\/\d+\/\d+)',dates):
                        date_all = re.findall(r'(\d+\/\d+\/\d+\s*\-\s*\d+\/\d+\/\d+)',dates)
                        date_all = list(set(date_all))
                    desc = clean_string(desc)
                    cleaned_desc = re.sub(r'^Course Description:?\s*', '', desc)
                    if date_all:
                        for date in date_all:
                            self.course_rows.append({
                                "Cengage Master Institution ID": self.institution_id,
                                "Source URL": 'https://myssu.shawnee.edu/ICS/Course_Search.jnz',
                                "Course Name": full_title,
                                "Course Description": cleaned_desc,
                                "Class Number": course,
                                "Section": sec,
                                "Instructor": clean_string(instructor),
                                "Enrollment": enrollment,
                                "Course Dates": clean_string(date),
                                "Location": '',   
                                "Textbook/Course Materials": '',
                            })
                            
                    else:
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": 'https://myssu.shawnee.edu/ICS/Course_Search.jnz',
                            "Course Name": full_title,
                            "Course Description": cleaned_desc,
                            "Class Number": course,
                            "Section": sec,
                            "Instructor": clean_string(instructor),
                            "Enrollment": enrollment,
                            "Course Dates": '',
                            "Location": '',   
                            "Textbook/Course Materials": '',
                        })

        except requests.RequestException as e:
            print("Request failed:", e)
    
    
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
        
        blocks = response.xpath('//table//tr[@class="sidearm-staff-member "]')
        for block in blocks:
            
            staff_url = block.xpath('./td[1]/a/@href').get('').strip()
            staff_url = f"https://ssubears.com{staff_url}"
            
            dir_res = requests.get(staff_url)
            dir_response = Selector(text=dir_res.text)
            
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": staff_url,
                        "Name": block.xpath('./td[1]/a/text()').get('').strip(),
                        "Title": block.xpath('./td[2]//text()').get('').strip(),
                        "Email": dir_response.xpath('//dt[contains(text(),"Email")]/parent::dl//a/text()').get('').strip(),
                        "Phone Number": block.xpath('./td[4]/a/text()').get('').strip(),
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
        # Initialize list to store extracted calendar records
        calendar_rows = []
        
        MONTHS = (
            "January|February|March|April|May|June|July|August|September|October|November|December|"
            "Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec"
        )

        DATE_START_PATTERN = re.compile(rf"^({MONTHS})\s+\d{{1,2}}")

        DATE_EVENT_PATTERN = re.compile(
            rf"^(?P<date>"
            rf"({MONTHS})\s+\d{{1,2}}"
            rf"(?:\s*[-–]\s*({MONTHS})?\s*\d{{1,2}})?"
            rf"(?:,\s*\d{{4}})?"
            rf")\s+(?P<event>.+)$"
        )

        TERM_PATTERN = re.compile(r"(Fall|Spring|Summer)\s+Semester\s+\d{4}-\d{4}")
        
        cal_pdf_urls = [f"https://www.shawnee.edu{url}" for url in response.xpath('//h3[contains(text(),"Academic Year Calendars")]/following-sibling::ul[1]//a/@href').getall()]
        for cal_url in cal_pdf_urls:
            logical_lines = []
            current_term = None

            # ---------- STEP 1: DOWNLOAD PDF ----------
            response = requests.get(cal_url, timeout=30)
            response.raise_for_status()

            pdf_bytes = BytesIO(response.content)

            # ---------- STEP 2: READ & MERGE LINES ----------
            with pdfplumber.open(pdf_bytes) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    if not text:
                        continue

                    buffer = ""
                    for raw_line in text.split("\n"):
                        line = raw_line.strip()
                        if not line:
                            continue

                        # Stop at footer but keep last event
                        if "According to Ohio Department of Higher Education" in line:
                            if buffer:
                                logical_lines.append(buffer.strip())
                                buffer = ""
                            break

                        if TERM_PATTERN.search(line) or DATE_START_PATTERN.match(line):
                            if buffer:
                                logical_lines.append(buffer.strip())
                            buffer = line
                        else:
                            buffer += " " + line

                    # flush buffer at end of page
                    if buffer:
                        logical_lines.append(buffer.strip())

            # ---------- STEP 3: PARSE EVENTS ----------
            for line in logical_lines:

                term_match = TERM_PATTERN.search(line)
                if term_match:
                    current_term = term_match.group(0)
                    continue

                if line.startswith("Office of the Registrar") or "ACADEMIC CALENDAR" in line:
                    continue

                match = DATE_EVENT_PATTERN.match(line)
                if match and current_term:
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": cal_url,
                        "Term Name": current_term,
                        "Term Date": match.group("date").strip(),
                        "Term Date Description": match.group("event").strip(),
                    })
    
        
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")
