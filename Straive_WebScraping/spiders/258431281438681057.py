import re
import io
import json
import math
import copy 
import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from ..utils import save_df
from parsel import Selector
from bs4 import BeautifulSoup


def clean_cell(html):
    """
    Cleans raw HTML strings from JSON cells. 
    Removes screen-reader only labels and collapses whitespace.
    """
    if not html:
        return None

    sel = Selector(text=html)

    # remove sr-only labels
    sel.root.xpath('//label').clear()
    texts = sel.xpath('.//text()').getall()
    text = ' '.join(t.strip() for t in texts if t.strip())
    return text

class SeuSpider(scrapy.Spider):

    name = "seu"
    institution_id = 258431281438681057

    # In-memory storage
    course_rows = []
    calendar_rows = []
    directory_rows = []
    

    course_urls = [
                  'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=758&IdNumber=-1&YearCode=2025&TermCode=CF',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=775&IdNumber=-1&YearCode=2025&TermCode=CS',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=795&IdNumber=-1&YearCode=2025&TermCode=CU',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1804&IdNumber=-1&YearCode=2025&TermCode=AF',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1805&IdNumber=-1&YearCode=2025&TermCode=AS',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=793&IdNumber=-1&YearCode=2025&TermCode=AC',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1721&IdNumber=-1&YearCode=2025&TermCode=UF',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1800&IdNumber=-1&YearCode=2025&TermCode=NF',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=762&IdNumber=-1&YearCode=2025&TermCode=GF',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1722&IdNumber=-1&YearCode=2025&TermCode=US',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=773&IdNumber=-1&YearCode=2025&TermCode=GS',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1799&IdNumber=-1&YearCode=2025&TermCode=NS',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1723&IdNumber=-1&YearCode=2025&TermCode=UU',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=789&IdNumber=-1&YearCode=2025&TermCode=GU',
                   'https://jics.seu.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id=1806&IdNumber=-1&YearCode=2026&TermCode=NF'
                   ]
    
    # Payload template for the course search API
    course_payload = {
        "pageState": {
            "enabled": True,
            "keywordFilter": "",
            "quickFilters": [],
            "sortColumn": "",
            "sortAscending": True,
            "currentPage": 1,
            "pageSize": 15,
            "showingAll": False,
            "selectedAll": False,
            "excludedFromSelection": [],
            "includedInSelection": [],
            "advancedFilters": [
                {"name": "courseCode", "value": ""},
                {"name": "courseCodeType", "value": "0"},
                {"name": "courseTitle", "value": ""},
                {"name": "courseTitleType", "value": "0"},
                {"name": "requestNumber", "value": ""},
                {"name": "instructorIds", "value": ""},
                {"name": "departmentIds", "value": ""},
                {"name": "locationIds", "value": ""},
                {"name": "competencyIds", "value": ""},
                {"name": "beginsAfter", "value": ""},
                {"name": "beginsBefore", "value": ""},
                {"name": "instructionalMethods", "value": ""},
                {"name": "sectionStatus", "value": ""},
                {"name": "startCourseNumRange", "value": ""},
                {"name": "endCourseNumRange", "value": ""},
                {"name": "division", "value": ""},
                {"name": "place", "value": ""},
                {"name": "subterm", "value": ""},
                {"name": "meetsOnDays", "value": "0,0,0,0,0,0,0"},
            ],
        }
    }


    course_headers = {
    'accept': 'text/html, */*; q=0.01',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://jics.seu.edu',
    'referer': 'https://jics.seu.edu/ICS/Student_Information_System.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'x-requested-with': 'XMLHttpRequest'
    }
    
    directory_url = "https://seu.edu/academics/faculty/"
    calendar_URLs = ['https://seu.edu/wp-content/uploads/25-26-Traditional-UG-Academic-Calendar-4.pdf',
                     'https://seu.edu/wp-content/uploads/26-27-Traditional-UG-Academic-Calendar-.pdf',
                     'https://seu.edu/wp-content/uploads/25-26-SEU-Network-Academic-Calendar.pdf',
                     'https://seu.edu/wp-content/uploads/25-26SEU-Network-Academic-Calendar-.pdf',
                     'https://seu.edu/wp-content/uploads/26-27-SEU-Network-Academic-Calendar-.pdf'
                     ]
    


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url,method='POST',headers=self.course_headers,body=json.dumps(self.course_payload), callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
        
        elif mode == 'calendar':
             for calendar_url in self.calendar_URLs:
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar,dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url,method='POST',headers=self.course_headers,body=json.dumps(self.course_payload), callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
        
        elif mode in ['course_calendar', 'calendar_course']:
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url,method='POST',headers=self.course_headers,body=json.dumps(self.course_payload), callback=self.parse_course, dont_filter=True)
            for calendar_url in self.calendar_URLs:
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar,dont_filter=True)

        elif mode in ['directory_calendar', 'calendar_directory']:
            for calendar_url in self.calendar_URLs:
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar,dont_filter=True)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
        
        # All three (default)
        else:
            for course_url  in self.course_urls:
                yield scrapy.Request(url=course_url,method='POST',headers=self.course_headers,body=json.dumps(self.course_payload), callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,callback=self.parse_directory,dont_filter=True)
            for calendar_url in self.calendar_URLs:
                yield scrapy.Request(url=calendar_url,callback=self.parse_calendar,dont_filter=True)
       

    # Parse methods UNCHANGED from your original
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

        """
        Parse course Excel files and normalize rows.
        """
        
        page_datas = response.json()
        record_count = page_datas['filteredRows']
        
        # Calculate pagination
        page_count = math.ceil(record_count / 15)
        for page in range(0,page_count + 1):
            self.logger.info(f"Requesting page {page}")
            payload = copy.deepcopy(self.course_payload)
            payload["pageState"]["currentPage"] = page
            res = requests.post(response.url,headers=self.course_headers,json=payload)

            json_datas = res.json()
            for datas in json_datas['rows']:
                raw_html = datas['courseCode']
                course_code = Selector(text=raw_html).xpath('//a/text()').get()
                title = clean_cell(datas['title']).replace('Title','').strip()
                faculty = clean_cell(datas['faculty']).replace('Faculty','').strip()
                seats_open = clean_cell(datas['seatsOpen']).replace('Seats open','').strip()

                class_number = '-'.join(course_code.split('-')[:2])
                course_name = course_code + ' ' + title

                section = course_code.split('-')[2:]
                section = '-'.join(section)
                
                yearcode = response.url.split('YearCode=')[-1].split('&')[0]
                term_code = response.url.split('TermCode=')[-1]
                match = re.search(r"data-sectionid='(\d+)'", raw_html)
                section_id = match.group(1)
                AdvisingRequirementCode = ''.join(course_code.split('-')[:2])

                # Secondary AJAX call to fetch descriptions and locations
                description_url = "https://jics.seu.edu/ICS/Portlets/CRM/Common/Jenzabar.EX.Common/Handlers/AjaxLoadablePortletPartialHandler.ashx"
                description_payload = f"IdNumber=-1&stuRegTermSelect={yearcode}%3B{term_code}&SectionId={section_id}&IsFreeElectiveSearch=False&FromCourseSearch=True&AdvisingRequirementCode={AdvisingRequirementCode}&StudentPlanDetailId=0&WeekdayCode=&StudentStatus=&%7CInsertOption%7C=&%7CLocationSelector%7C=&%7CControlPath%7C=~%2FPortlets%2FCRM%2FStudent%2FPortlet.StudentRegistration%2FControls%2FSectionDetailsModal.ascx&%7CUseForm%7C=False"
                description_headers = {
                    'accept': 'application/json, text/javascript, */*; q=0.01',
                    'accept-language': 'en-US,en;q=0.9',
                    'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                    'origin': 'https://jics.seu.edu',
                    'referer': 'https://jics.seu.edu/ICS/Student_Information_System.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    'x-requested-with': 'XMLHttpRequest',
                }
                desc_res = requests.request("POST", description_url, headers=description_headers, data=description_payload)
                desc_datas = desc_res.json()
                html = desc_datas["Html"]
                soup = BeautifulSoup(html, "html.parser")

                # Detail extraction
                desc_tag = soup.select_one("span.course-description")
                description = desc_tag.get_text(strip=True) if desc_tag else None

                # Extract complex meeting patterns (dates and locations)
                dates = []
                locations = []

                date_spans = soup.find_all(
                    "span",
                    class_="text-nowrap",
                    string=lambda x: x and "-" in x
                )

                for span in date_spans:
                    text = span.get_text(strip=True)

                    if re.search(r"\d{1,2}/\d{1,2}/\d{4}", text):
                        dates.append(text)
                        loc = ""

                        # Sibling navigation logic to find the Location div near the Date span
                        node = span.parent
                        while True:
                            node = node.find_next_sibling()
                            if not node:
                                break

                            # stop if instructor section reached
                            if "Instructor(s)" in node.get_text():
                                break

                            # location row found
                            if node.name == "div" and "row" in node.get("class", []):
                                loc_div = node.find("div", class_="col-xs-6")
                                if loc_div:
                                    loc = loc_div.get_text(strip=True)
                                break
                        locations.append(loc)

                # remove duplicates
                locations = list(dict.fromkeys(locations))
                course_date = dates[0] if len(dates) == 1 else ", ".join(dates)
                location = locations[0] if len(locations) == 1 else ", ".join(locations)
        
                self.course_rows.append(
                    {
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": 'https://jics.seu.edu/ICS/Student_Information_System.jnz?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
                    "Course Name": course_name,
                    "Course Description": description,
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": faculty,
                    "Enrollment": seats_open,
                    "Course Dates": course_date,
                    "Location": location,
                    "Textbook/Course Materials": '',
                    }
                )

        # SAVE OUTPUT CSV
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")

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

        """
        Parse employee directory profiles and extract emails via hCaptcha.
        """
        blocks = response.xpath('//div[@class="people-wrapper"]//a')
        for index, block in enumerate(blocks):
            links = block.xpath('./@href').get('')
            links = response.urljoin(links)
            title = block.xpath('.//following::div[1]/text()').get('')
            res = requests.get(links,timeout=30)
            people_response = Selector(text=res.text)
            name = people_response.xpath('//h1/text()').get('').strip()
            email = people_response.xpath("//div[@class='email']/text()").get('').strip()
           
            self.directory_rows.append(
            {
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": links,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": '',
                }
            )
    
    def parse_calendar(self,response):

        response = requests.get(response.url)
        response.raise_for_status()
        month_names = ["Aug", "Sept", "Sep", "Oct", "Nov", "Dec", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul"]
        
        # 1. Initialize current_term OUTSIDE the loops to persist between pages/columns
        current_term = "Fall 2025 Semester" 
        term_pattern = re.compile(r'(Fall|Spring|Summer)\s+\d{4}\s+Semester', re.IGNORECASE)
        with pdfplumber.open(io.BytesIO(response.content)) as pdf:
            for page in pdf.pages:
                width, height = page.width, page.height
                left_col = page.within_bbox((0, 0, width/2, height))
                right_col = page.within_bbox((width/2, 0, width, height))
                
                for col_obj in [left_col, right_col]:
                    text = col_obj.extract_text()
                    if not text: continue
                    
                    current_month = ""
                    lines = text.split('\n')
                    
                    for line in lines:
                        clean_line = line.strip()
                        if not clean_line: continue

                        # 2. Update the term name only when a new header is found
                        term_match = term_pattern.search(clean_line)
                        if term_match:
                            current_term = term_match.group(0).strip()
                            continue

                        if any(clean_line == m for m in month_names):
                            current_month = clean_line
                            continue
                        
                        match = re.search(r'(([A-Z][a-z]{2,3})\s+)?(\d{1,2}(?:-\d{1,2})?)\s+(.*)', clean_line)
                        if match:
                            m_header, m_inline, day, desc = match.groups()
                            if m_inline:
                                current_month = m_inline
                            
                            if current_month and day:
                                display_month = "Sep" if current_month.startswith("Sept") else current_month
                                
                                self.calendar_rows.append({
                                    "Cengage Master Institution ID":self.institution_id,
                                    "Source URL":response.url,
                                    "Term Name":current_term,
                                    "Term Date":f"{display_month}-{day}",
                                    "Term Date Description":desc.strip()
                                })

      
    def closed(self, reason):
        """
        Final cleanup and persistence.

        Saves:
        - Directory dataset
        - Calendar dataset
        - Closes all file handles
        """
        if self.directory_rows:
            save_df(pd.DataFrame(self.directory_rows), self.institution_id, "campus")

        if self.calendar_rows:
            save_df(pd.DataFrame(self.calendar_rows), self.institution_id, "calendar")




 