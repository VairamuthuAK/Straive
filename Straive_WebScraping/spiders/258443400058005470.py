import re
import io
import json
import scrapy
import pdfplumber
import pandas as pd
from ..utils import *
from inline_requests import inline_requests

class RegisSpider(scrapy.Spider):

    name = "regis"
    institution_id = 258443400058005470

    course_rows = []
    course_url = "https://app.coursedog.com/api/v1/cm/regis_colleague_ethos/courses/search/%24filters?catalogId=f9p9xYJ6cDz63IXxca8v&skip=0&limit=1247&orderBy=code&formatDependents=true&effectiveDatesRange=2025-08-20%2C2025-08-20&ignoreEffectiveDating=false&columns=credits.creditHours%2CcustomFields.rawCourseId%2CcustomFields.crseOfferNbr%2CcustomFields.catalogAttributes%2CcustomFields.MHGSd%2CdisplayName%2Cdepartment%2Cdescription%2Cname%2CcourseNumber%2CsubjectCode%2Ccode%2CcourseGroupId%2Ccareer%2Ccollege%2ClongName%2Cstatus%2Cinstitution%2CinstitutionId%2CacademicLevels%2Cdependencies"
    course_payload = json.dumps({
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
            "id": "subjectCode-course",
            "condition": "field",
            "name": "subjectCode",
            "inputType": "subjectCodeSelect",
            "group": "course",
            "type": "isNot",
            "value": "LL"
            },
            {
            "id": "subjectCode-course",
            "condition": "field",
            "name": "subjectCode",
            "inputType": "subjectCodeSelect",
            "group": "course",
            "type": "isNot",
            "value": "CT"
            },
            {
            "id": "subjectCode-course",
            "condition": "field",
            "name": "subjectCode",
            "inputType": "subjectCodeSelect",
            "group": "course",
            "type": "isNot",
            "value": "TR"
            },
            {
            "id": "subjectCode-course",
            "condition": "field",
            "name": "subjectCode",
            "inputType": "subjectCodeSelect",
            "group": "course",
            "type": "isNot",
            "value": "SABR"
            },
            {
            "id": "catalogPrint-course",
            "condition": "field",
            "name": "catalogPrint",
            "inputType": "boolean",
            "group": "course",
            "type": "isNot",
            "value": False
            },
            {
            "id": "name-course",
            "condition": "field",
            "name": "name",
            "inputType": "text",
            "group": "course",
            "type": "isNot",
            "value": "Lab"
            }
        ],
        "id": "EP7h1Uud",
        "condition": "and"
        }
    ]
    })
    course_headers = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'content-type': 'application/json',
    'origin': 'https://catalog.regiscollege.edu',
    'priority': 'u=1, i',
    'referer': 'https://catalog.regiscollege.edu/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'x-requested-with': 'catalog'
    }


    directory_url = "https://www.regiscollege.edu/about-regis/find-us/faculty-and-staff-directory?page=0"
    directory_headers = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
    'priority': 'u=0, i',
    'referer': 'https://www.regiscollege.edu/about-regis/find-us/faculty-and-staff-directory?page=0',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    }

    calendar_url ="https://www.regiscollege.edu/sites/default/files/academics/calendars/academic-calendar-undergraduate-2025-2026-v4.pdf"

    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        if mode == "course":
            yield scrapy.Request(self.course_url,headers=self.course_headers,body=self.course_payload,method="POST", callback=self.parse_course, dont_filter=True)

        elif mode == "directory":
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        # If only calendar data is required
        elif mode == "calendar":
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers,body=self.course_payload,method="POST", callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(self.course_url,headers=self.course_headers,body=self.course_payload,method="POST", callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)

        # All three (default)
        else:
            yield scrapy.Request(self.course_url,headers=self.course_headers,body=self.course_payload,method="POST", callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(self.calendar_url, callback=self.parse_calendar)
    
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
        - "Enrollment"                    : int
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        
        json_data = response.json()
        datas = json_data.get("data",{})
        rows=[]

        for data in datas:
            title = data.get('longName','').strip()
            course_id = data.get("courseGroupId","").strip()
            courseDependencyId =data.get('programDependents',[])
            
            if courseDependencyId:
                courseDependencyId = courseDependencyId[0][0].get('courseDependencyId','').strip()
            else:
                courseDependencyId = course_id
            class_number = data.get("code","").strip()
            course_title =f"{class_number} {title}"
            desc = data.get("description","").strip()
            
            rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": f"https://catalog.regiscollege.edu/courses/{courseDependencyId}",
                        "Course Name": course_title or "",
                        "Course Description":desc or "",
                        "Class Number":  class_number or "",
                        "Section": "",
                        "Instructor": "",
                        "Enrollment": "",
                        "Course Dates": "",
                        "Location": "",
                        "Textbook/Course Materials": '',
                    })
            
        if rows:
            course_df = pd.DataFrame(rows)
            save_df(course_df, self.institution_id, "course")
            


    @inline_requests
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

        last_page = response.xpath('//a[@title="Go to last page"]/@href').get('').strip().split("=")[-1]
        rows=[]

        for page in range(0,int(last_page)+1):
            url = f"https://www.regiscollege.edu/about-regis/find-us/faculty-and-staff-directory?page={page}"
            headers = {
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
                'accept-language': 'en-GB,en-US;q=0.9,en;q=0.8',
                'priority': 'u=0, i',
                'referer': url,
                'upgrade-insecure-requests': '1',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                }
            response = yield scrapy.Request(url=url,headers=headers,dont_filter=True)
            links = response.xpath('//div[@class="vm-longcard__people--content"]/h3/a/@href').getall()

            for link in links:
                
                url = response.urljoin(link)
                details_response= yield scrapy.Request(url=url,dont_filter=True)

                title= details_response.xpath('//h2[contains(@class,"field--name-field-position-title")]/text()').get('').strip()
                dept = details_response.xpath('//div[contains(@class,"people--department")]/span[@class="field__item"]/text()').get('').strip()
                main_title = f"{title}, {dept}"

                rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": details_response.url,
                        "Name": details_response.xpath('//h1/span/text()').get('').strip(),
                        "Title":main_title,
                        "Email":details_response.xpath('//div[contains(@class,"field--name-field-email-address")]/a/text()').get('').strip(),
                        "Phone Number": details_response.xpath('//div[contains(@class,"field--name-field-phone")]/a/text()').get('').strip(),
                    })
                
        if rows:
            df = pd.DataFrame(rows)
            save_df(df, self.institution_id, "campus")
                

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

        with pdfplumber.open(io.BytesIO(response.body)) as pdf:
            current_term_name = None
            rows = []

            # Term pattern
            term_pattern = re.compile(
                r'(?:'
                r'(?:Fall|Spring|Summer|Winter)\s+\d{4}\s+Semester'
                r'|January Intersession\s+\d{4}'
                r'|Summer Sessions/Semesters\s+\d{4}'
                r')',
                re.I
            )

            # Capture description + weekday + full date range
            # Example: "Registration Meeting with Faculty Advisor Monday, November 3 – Friday, November 7"
            date_pattern = re.compile(
                r'^(.*?)(Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday),?\s*([A-Z][a-z]+ \d{1,2}(?:\s*[-–]\s*(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?,?\s*[A-Z][a-z]+ \d{1,2})?)',
                re.I
            )

            buffer_line = ""  # for multi-line events

            for page in pdf.pages:
                text = page.extract_text()
                lines = text.split("\n")

                for line in lines:
                    line = line.strip()
                    if not line or "Academic Calendar" in line:
                        continue

                    # Check for term
                    term_match = term_pattern.search(line)
                    if term_match:
                        current_term_name = term_match.group(0)
                        continue

                    # Check if line contains a weekday (new event)
                    date_match = date_pattern.search(line)
                    if date_match:
                        # Flush previous buffer
                        if buffer_line:
                            prev_match = date_pattern.search(buffer_line)
                            if prev_match:
                                # Term Date = weekday + full date(s)
                                term_date = f"{prev_match.group(2)}, {prev_match.group(3).replace('–','-').strip()}"
                                # Term Date Description = everything else (before weekday + continuation lines)
                                desc = (prev_match.group(1) + buffer_line.replace(prev_match.group(0), "")).replace('–','-').replace("”","'").replace("“","'").strip()
                                rows.append({
                                    "Cengage Master Institution ID": self.institution_id,
                                    "Source URL": response.url,
                                    "Term Name": current_term_name,
                                    "Term Date": term_date.replace('–','-').strip(),
                                    "Term Date Description": re.sub(r'\s*Revised.*$',"",desc).strip()
                                })
                            buffer_line = ""

                        # Start new buffer
                        buffer_line = line
                    else:
                        # Continuation line → append
                        buffer_line += " " + line

                # Flush last buffered line
                if buffer_line:
                    last_match = date_pattern.search(buffer_line)
                    if last_match:
                       
                        term_date = f"{last_match.group(2)}, {last_match.group(3).replace('–','-').strip()}"
                        desc = (last_match.group(1) + buffer_line.replace(last_match.group(0), "")).replace('–','-').replace("”","'").replace("“","'").strip().strip()
                        rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": response.url,
                            "Term Name": current_term_name,
                            "Term Date": term_date.replace('–','-').strip(),
                            "Term Date Description": re.sub(r'\s*Revised.*$',"",desc).strip()
                        })

            if rows:
                calendar_df = pd.DataFrame(rows)
                save_df(calendar_df, self.institution_id, "calendar")

    