import re
import scrapy
import math
import time
import json
import pandas as pd
from html import unescape
from parsel import Selector
from datetime import datetime, timedelta
from inline_requests import inline_requests
from ..utils import *



class montcalmSpider(scrapy.Spider):
    """
    Scrapy Spider for Montcalm Community College.

    This spider scrapes:
    1) Courses (via internal API + modal details)
    2) Academic calendar (via JSON API)
    3) Staff directory (via HTML)
    """
    
    name="montcalm"
    institution_id = 258424916594943966

    # URLs
    calendar_url = "https://www.montcalm.edu/academic-calendar/"
    course_url = "https://my.montcalm.edu/ICS/Academics/?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next"
    directory_url = "https://www.montcalm.edu/about/leadership/staff-faculty-directory/"
    
    # Calendar API
    calendar_API_url = "https://www.montcalm.edu/api/v1/academic-calendar/get-events?contentId=2441&start=2026-01-01T00%3A00%3A00&end=2026-12-31T00%3A00%3A00"
    
    # Browser headers
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    
    # Maps UI term codes → API term IDs
    TERM_ID_MAP = {
        "10": "211",
        "20": "212",
        "30": "213"
    }

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_API_url, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_API_url, callback=self.parse_calendar, dont_filter=True)
            # self.parse_calendar(self.calendar_url)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_API_url, callback=self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_API_url, callback=self.parse_calendar, dont_filter=True)

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
        
        all_courses = []
        
        # Get all term values from dropdown
        term_values = response.xpath('.//select[@name="stuRegTermSelect"]//option/@value').getall()
        
        # Loop through each term
        for term in term_values:
            if ";" not in term:
                continue
            
            # Example: "2026;10"
            year_code, term_code = term.split(";")
            
            # Convert term code to API ID
            term_id = self.TERM_ID_MAP.get(term_code)
            
            if not term_id:
                continue
            
            # Pagination setup
            page = 0
            page_size = 100
            total_pages = None
            
            api_url = (f"https://my.montcalm.edu/ICS/webserviceproxy/exi/rest/studentregistration/pagedsectiondataforsearch?Id={term_id}&IdNumber=-1&YearCode={year_code}&TermCode={term_code}")

            # Payload used to fetch paginated course list       
            course_payload = {
                    "pageState": {
                        "enabled": True,
                        "keywordFilter": "",
                        "quickFilters": [],
                        "sortColumn": "",
                        "sortAscending": True,
                        "currentPage": page,
                        "pageSize": 100,
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
                            {"name": "meetsOnDays", "value": "0,0,0,0,0,0,0"}
                        ],
                        "quickFilterCounts": None
                    }
                }

            headers = {
                    "accept": "text/html, */*; q=0.01",
                    "content-type": "application/json",
                    "x-requested-with": "XMLHttpRequest",
                    "referer": self.course_url,
                    "user-agent": "Mozilla/5.0"
                }

            # Pagination loop
            while True:    
                course_payload["pageState"]["currentPage"] = page
                course_payload["pageState"]["pageSize"] = page_size

                list_response = yield scrapy.Request(
                    url=api_url,
                    method="POST",
                    body=json.dumps(course_payload),
                    headers=headers,
                    dont_filter=True
                )

                try:
                    data = json.loads(list_response.text)
                except:
                    self.logger.error("Invalid JSON for term " + term)
                    continue
                
                rows = data.get("rows", [])

                # first page → compute total pages
                if page == 0:
                    total_rows = data.get("filteredRows", 0)
                    if total_rows == 0:
                        break
                    total_pages = math.ceil(total_rows / page_size)

                # stop when no rows
                if not rows:
                    break
                
                for row in rows:
                    course_html = row.get("courseCode", "")

                    def clean_text(html):
                        texts = Selector(text=html).xpath('//text()[not(parent::label)]').getall()
                        return " ".join(t.strip() for t in texts if t.strip())

                    course_code = clean_text(course_html)

                    class_no, section = course_code.split("--")
                    title = clean_text(row.get("title", ""))
                    faculty = clean_text(row.get("faculty", ""))
                    seats = clean_text(row.get("seatsOpen", ""))
                    
                    enrollment = seats

                    # Normalize seat format
                    if "/" in seats:
                        filled, total = seats.split("/")
                        try:
                            filled = int(filled.strip())
                            total = int(total.strip())
                            available = total - filled
                            enrollment = f"{available}/{total}"
                        except ValueError:
                            enrollment = seats

                    # Extract section ID
                    sel = Selector(text=course_html)
                    section_id = sel.xpath('//a/@data-sectionid').get()
                    advising_code = sel.xpath('//a/@data-advisingrequirementcode').get()

                    if not section_id:
                        continue

                    # Details modal request
                    detail_payload = {
                        "IdNumber": "-1",
                        "stuRegTermSelect": term,
                        "SectionId": section_id,
                        "IsFreeElectiveSearch": "False",
                        "FromCourseSearch": "True",
                        "AdvisingRequirementCode": advising_code or "",
                        "StudentPlanDetailId": "0",
                        "WeekdayCode": "",
                        "StudentStatus": "",
                        "|InsertOption|": "",
                        "|LocationSelector|": "",
                        "|ControlPath|": "~/Portlets/CRM/Student/Portlet.StudentRegistration/Controls/SectionDetailsModal.ascx",
                        "|UseForm|": "False"
                    }

                    detail_headers = {
                        'Referer': 'https://my.montcalm.edu/ICS/Academics/?portlet=Student_Registration&screen=StudentRegistrationPortlet_CourseSearchView&screenType=next',
                        "User-Agent": "Mozilla/5.0",
                        "Accept": "application/json, text/javascript, */*; q=0.01",
                        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                        "X-Requested-With": "XMLHttpRequest"
                        }

                    detail_url = "https://my.montcalm.edu/ICS/Portlets/CRM/Common/Jenzabar.EX.Common/Handlers/AjaxLoadablePortletPartialHandler.ashx"

                    detail_response = yield scrapy.FormRequest(
                        url=detail_url,
                        method="POST",
                        formdata=detail_payload,
                        headers=detail_headers,
                        dont_filter=True
                    )

                    try:
                        detail_json = json.loads(detail_response.text)
                        html_content = detail_json.get("Html", "")
                    except:
                        html_content = detail_response.text

                    detail_sel = Selector(text=html_content)

                    description = re.sub(
                                r'\s+',
                                ' ',
                                " ".join(
                        detail_sel.xpath('//span[@class="course-description"]//text()').getall()
                    ).strip())
                    
                    textbook_url = detail_sel.xpath(
                        '//div[contains(text(),"Textbooks")]/following::a[1]/@href'
                    ).get(default='')
                    
                    # ---------- Extract schedule blocks ----------
                    schedule_root = detail_sel.xpath('//div[contains(@class,"jzb-strong-text") and contains(normalize-space(),"Schedule")]/following-sibling::*')
                    
                    schedule_dates = []
                    schedule_locations = []

                    current_date = ""
                    current_location = ""

                    for node in schedule_root:
                        
                        # Get date
                        date_text = node.xpath('.//span[contains(text(),"/")]/text()').get()
                        
                        if date_text:
                            current_date = date_text.strip()

                        # Get location (first col-xs-6)
                        loc_text = node.xpath('.//div[contains(@class,"col-xs-6")]/text()').get()
                        
                        if loc_text and loc_text.strip():
                            current_location = loc_text.strip()

                        # When both found → store row
                        if current_date and current_location:
                            schedule_dates.append(current_date)
                            schedule_locations.append(current_location)
                            current_date = ""
                            current_location = ""

                    # fallback
                    if not schedule_dates:
                        schedule_dates = [""]
                        schedule_locations = [""]

                    for i in range(len(schedule_dates)):

                        date_val = schedule_dates[i] if i < len(schedule_dates) else ""
                        loc_val = ("Online" if schedule_locations[i].lower().count("online") > 1 else schedule_locations[i]) if i < len(schedule_locations) else ""
                        
                        all_courses.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": f"{class_no} {title}",
                        "Course Description": description,
                        "Class Number": class_no,
                        "Section": section,
                        "Instructor": faculty,
                        "Enrollment": enrollment,
                        "Course Dates": date_val,
                        "Location": loc_val,
                        "Textbook/Course Materials": textbook_url
                        })
                    
                page += 1
                # stop when last page reached
                if total_pages is not None and page >= total_pages:
                    break
            time.sleep(2)
            
        df = pd.DataFrame(all_courses)
        save_df(df, self.institution_id, "courses")

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

        directory_data = []  # This list will store all staff records

        # Find all staff blocks using XPath
        blocks = response.xpath('.//main//div[contains(@class,"slim-grid") and contains(@class,"slim-grid-item")]//div[contains(@class,"mb-last-child-0")][.//p/strong]')

        # Regex pattern to match phone numbers
        phone_pattern = re.compile(
            r'(\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4})(\s*(ext|extn|x)\s*\d+)?',
            re.I
        )

        # Loop through each staff block
        for block in blocks:
            name = block.xpath('.//p/strong//text()').get(default='').strip()
            all_texts = block.xpath('.//p//text() | .//a//text()').getall()
            # Clean whitespace and remove empty strings
            all_texts = [t.strip() for t in all_texts if t.strip()]
            
            # Initialize empty fields
            title = ""
            phone = ""
            email = ""

            # Skip first item (name), process remaining
            for t in all_texts[1:]:

                # If it contains @, it is an email
                if "@" in t:
                    email = t

                # If it matches phone pattern, it is a phone number
                elif phone_pattern.search(t):
                    phone = t

                # Otherwise, assume it is the job title
                else:
                    if not title:
                        title = t

            # Skip entry if name is missing
            if not name:
                continue

            # Append staff record
            directory_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": phone
            })

        df = pd.DataFrame(directory_data)
        save_df(df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parses academic calendar from JSON API.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """
        calendar_data = [] # This list will store all calendar rows

        try:
            # Convert JSON string into Python list/dict
            events = json.loads(response.text)
        except Exception as e:
            # If JSON parsing fails, safely exit function
            self.logger.error(f"JSON parse error: {e}")
            return

        # Loop through each event in the calendar
        for event in events:

            # Get event title (e.g., "Spring Break"), remove extra spaces
            title = event.get("title", "").strip()

            # Get extra properties dictionary (if exists)
            ext = event.get("extendedProps") or {}
            raw_desc = ext.get("description", "") or ""
            
            # Remove all HTML tags using regex, Convert HTML entities like &nbsp; → space, &amp; → &
            desc = re.sub(r"<.*?>", "", raw_desc)
            desc = unescape(desc)
            desc = re.sub(r"\s+", " ", desc).strip()

            # ---------- SAFE DATE ----------
            start_date = (
                event.get("start")
                or event.get("startDate")
                or event.get("startDatetime")
                or ""
            )

            end_date = (
                event.get("end")
                or event.get("endDate")
                or event.get("endDatetime")
                or ""
            )

            # This will hold the final formatted date
            term_date = ""

            try:
                # Only format if both start and end exist
                if start_date and end_date:

                    # Convert ISO string → Python datetime
                    s = datetime.fromisoformat(start_date.split(".")[0])
                    e = datetime.fromisoformat(end_date.split(".")[0])
                    
                    # Subtract 1 day from end date
                    e = e - timedelta(days=1)
                    
                    # If both start and end are same day
                    if s.date() == e.date():
                        # Format as: January 15, 2026
                        term_date = s.strftime("%B %d, %Y")

                    elif s.month == e.month:
                        # Format as: January 15, 2026 – January 20, 2026
                        term_date = f"{s.strftime('%B %d')} – {e.strftime('%d, %Y')}"
                    
                    else:
                        term_date = f"{s.strftime('%B %d, %Y')} – {e.strftime('%B %d, %Y')}"
            
            except Exception as e:
                self.logger.warning(f"Date parse error: {start_date}, {end_date} | {e}")

            # Add one calendar record to the list
            calendar_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": title,
                "Term Date": term_date,
                "Term Date Description": desc
            })

        calendar_df = pd.DataFrame(calendar_data)
        save_df(calendar_df, self.institution_id, "calendar")

