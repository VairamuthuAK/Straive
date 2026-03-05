import re
import json
import scrapy
import pandas as pd
from ..utils import *
from datetime import datetime
from inline_requests import inline_requests
from playwright.sync_api import sync_playwright


class TuftsSpider(scrapy.Spider):
    name = "tufts"
    institution_id = 258437238679955410

    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://sis.it.tufts.edu/psp/paprd/EMPLOYEE/EMPL/h/?tab=TFP_CLASS_SEARCH"
    directory_source_url = "https://directory.tufts.edu/faulty-staff-by-department/"
    calendar_url = "https://students.tufts.edu/calendars"

   
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
        
        # STEP 1: Use Playwright to load page & collect cookies
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()
            
            # Open the course search page
            page.goto(self.course_url)
            
            # Wait until all network requests finish
            page.wait_for_load_state("networkidle")
            
            # Extract session cookies (required for API calls)
            cookies = context.cookies()
            browser.close()
            
        # Convert cookies list into dictionary for Scrapy
        cookies_dict = {cookie["name"]: cookie["value"] for cookie in cookies}
        
        # STEP 2: Fetch available careers & terms
        terms_api_url = "https://siscs.it.tufts.edu/psc/csprd/EMPLOYEE/HRMS/s/WEBLIB_CLS_SRCH.ISCRIPT1.FieldFormula.IScript_getCareers"
        headers = {
            "Accept": "*/*",
            "Referer": "https://sis.it.tufts.edu/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/143.0.0.0 Safari/537.36"
        }
        
        # Make first API request to retrieve career & term data
        terms_api_response = yield scrapy.Request(
                            url=terms_api_url,
                            cookies=cookies_dict,
                            headers=headers,
                            dont_filter=True,
                        )
        
        # Parse JSON response
        json_data = json.loads(terms_api_response.text)
        
        # Timestamp used by Tufts API (cache-busting)
        current_time_ms = int(datetime.now().timestamp() * 1000)
        
        # STEP 3: Loop through careers and filter terms
        for data in json_data:
            value = data.get('value','').strip()
            
            # We only care about "ALL" careers
            if "ALL" == value:
                inner_blocks = data['terms']
                for block in inner_blocks:
                    id = block.get('value','')
                    term = block.get('desc','').strip()
                    
                    # Filter for 2025 and 2026 terms only
                    if "2026" in term or "2025" in term:
                        print(term)
                        
                        # STEP 4: Fetch course search results for the term
                        api_term_url = f"https://siscs.it.tufts.edu/psc/csprd/EMPLOYEE/HRMS/s/WEBLIB_CLS_SRCH.ISCRIPT1.FieldFormula.IScript_getSearchresultsAll3?callback=jQuery18207904189690771428_1768798589106&term={id}&career=ALL&subject=&crs_number=&attribute=&keyword=&instructor=&searchby=crs_number&_={str(current_time_ms)}"
                       
                        term_response = yield scrapy.Request(
                            url=api_term_url,
                            cookies=cookies_dict,
                            headers=headers,
                            dont_filter=True,
                        )
                        # STEP 5: Handle JSONP safely
                        text = term_response.text.strip()
                        
                        if text.startswith('{'):
                            json_data = json.loads(text)
                        else:
                            # Remove callback wrapper
                            json_str = text[text.find('(') + 1 : text.rfind(')')]
                            json_data = json.loads(json_str)
                            
                        blocks = json_data['searchResults']
                        blocks_count = len(blocks)
                        
                        # STEP 6: Loop through courses    
                        for i, block in enumerate(blocks, start=1):
                            print(f"{i}/{blocks_count}")
                            # Clean course name and description
                            course_title  = re.sub('\s+',' ',block.get('course_title','')).strip()
                            desc = re.sub('\s+',' ',block.get('desc_long','')).strip()
                            course_no = re.sub('\s+',' ',block.get('course_num',''))
                            course_name = f"{course_no} {course_title }"
                            
                            section_blocks = block['sections']
                            
                            # STEP 7: Loop through sections & components
                            for section_block in section_blocks:
                                components = section_block['components']
                                for component in components:
                                    
                                    current_time_ms = int(datetime.now().timestamp() * 1000)
                                    sec_no = component.get('section_num','')
                                    print(sec_no)
                                    class_num = component.get('class_num','')
                                    instrutor = component['locations'][0].get('instructor','')
                                    location = component.get('campus','')
                                    
                                    # STEP 8: Fetch detailed class information
                                    detail_url = f"https://siscs.it.tufts.edu/psc/csprd/EMPLOYEE/HRMS/s/WEBLIB_CLS_SRCH.ISCRIPT1.FieldFormula.IScript_getResultsDetails?callback=jQuery18206729955841617082_1768807690355&term={id}&class_num={class_num}&_={str(current_time_ms)}"
                                    response3 = yield scrapy.Request(
                                        url=detail_url,
                                        cookies=cookies_dict,
                                        headers=headers,
                                        dont_filter=True,
                                    )
                                    text1 = response3.text.strip()
                                    
                                    # Handle JSONP response
                                    if text1.startswith('{'):
                                        json_data1 = json.loads(text1)
                                    else:
                                        # Remove function name and trailing );
                                        json_str1 = text1[text1.find('(') + 1 : text1.rfind(')')]
                                        json_data1 = json.loads(json_str1)
                                        
                                    # STEP 9: Extract enrollment & date info
                                    start_date = json_data1.get('start_date','')
                                    end_date = json_data1.get('end_date','')
                                    date = f"{start_date} - {end_date}"
                                    en_total = json_data1['reserved_cap'][0].get('cap','')
                                    enrolled = json_data1['reserved_cap'][0].get('total','')
                                    enrollment = f"{enrolled}/{en_total}"
                                    
                                    # STEP 10: Store final row
                                    course_rows.append({
                                        "Cengage Master Institution ID": self.institution_id,
                                        "Source URL": self.course_url,
                                        "Course Name": course_name,
                                        "Course Description": desc,  
                                        "Class Number": class_num,
                                        "Section": sec_no,
                                        "Instructor": instrutor,
                                        "Enrollment": enrollment,
                                        "Course Dates": date,
                                        "Location": location,   
                                        "Textbook/Course Materials": '',
                                    })
                                    
        # STEP 11: Save all collected course data
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
        
        # Fetch list of departments (Faculty & Staff)
        departments_api_url = "https://directory.tufts.edu/api/departments"
        
        departments_payload = json.dumps({
        "type": "FACULTY_STAFF"
        })
        
        departments_headers = {
        'Accept': 'application/json, */*;q=0.8',
        'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'Content-Type': 'application/json',
        'Origin': 'https://directory.tufts.edu',
        'Referer': 'https://directory.tufts.edu/faulty-staff-by-department/',
      
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }
        # POST request to retrieve departments
        departments_response =yield scrapy.Request(url=departments_api_url,method="POST",headers=departments_headers,body=departments_payload)
        json_data = json.loads(departments_response.text)
        blocks = json_data['results']
        
        # Loop through each department
        for block in blocks:
            department = block.get('department','')
            print(department)
            
            dept_url = "https://directory.tufts.edu/api/departments/details"

            dept_payload = json.dumps({
            "department": department,
            "type": "FACULTY_STAFF"
            })
            
            dept_response =yield scrapy.Request(url=dept_url,method="POST",headers=departments_headers,body=dept_payload)
            json_data = json.loads(dept_response.text)
            blocks = json_data['results']
            
            # Loop through department LDAP IDs
            for block in blocks:
                ldapId = block.get('ldapId','')
                detail_url = "https://directory.tufts.edu/api/search/by-department"
                
                detail_payload = json.dumps({
                "departmentId": ldapId
                })  
                
                details_response =yield scrapy.Request(url=detail_url,method="POST",headers=departments_headers,body=detail_payload)
                json_data = json.loads(details_response.text)
                
                # Regex pattern to extract valid phone numbers
                pattern = r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}(?:\s*(?:x|ext\.?)\s*\d+)?'
                blocks = json_data['results']
                
                # Extract staff details
                for block in blocks:
                    phone = block.get('phone', '')
                    phone = phone.replace('Work:', '').replace('+1-', '').replace('page', '').replace('or', '')
                    phone = re.sub(r'\s+', ' ', phone).strip()
                    phone_numbers = re.findall(pattern, phone, flags=re.IGNORECASE)

                    # Extract first valid phone number
                    phone_number = phone_numbers[0] if phone_numbers else ''
                    
                    # Append extracted staff data to the results list
                    directory_rows.append(
                        {
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.directory_source_url,
                            "Name": block.get('displayNameLF',''),
                            "Title": block.get('title',''),
                            "Email": block.get('email',''),
                            "Phone Number": phone_number, 
                        }
                    )
        
        # Save directory data
        directory_df = pd.DataFrame(directory_rows)
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
        # List to collect all calendar records
        calendar_rows = []
        
        url = 'https://www.trumba.com/s.aspx?calendar=student-life-registrar-academic-calendar&widget=main&date=20250101&spudformat=xhr'
        
        # Request headers to mimic browser behavior
        headers = {
        'Accept': '*/*',
        'Accept-Language': 'en-IN,en-GB;q=0.9,en-US;q=0.8,en;q=0.7,ta;q=0.6,hi;q=0.5,fr;q=0.4',
        'Origin': 'https://students.tufts.edu',
        'Referer': 'https://students.tufts.edu/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        }
        # Fetch first calendar page
        cal_response =yield scrapy.Request(url,headers=headers)
        
        # Variable to store the current term name (persists across rows)
        term_name = ''
        
        # Parse calendar table rows
        blocks = cal_response.xpath('//table[@class="twSimpleTableTable"]/tbody/tr')
        for block in blocks:
            
            # Check if the row defines a new term group
            term = block.xpath('.//div[@class="twSimpleTableGroupHead"]/text()').get('').strip()
            
            # Header detection (Date / Event rows)
            header_name = block.xpath('.//th[2]/text()').get('').strip()
            header_name1 = block.xpath('.//td[2]/text()').get('').strip()
            
            # Identify row type
            if term:
                # Update current term name when a group header is found
                term_name =term
            
            elif "Date" == header_name or "Date" == header_name1:
                # Skip header rows
                print("Skip header rows")
            elif "Event" == header_name or "Event" == header_name1:
                # Skip header rows
                print("Skip header rows")
            else:
                
                # Extract date & description
                date =block.xpath('./td[2]//text()').get('').strip() 
                if not date:
                    date =block.xpath('./td[1]//text()').get('').strip() 
                    date = date.split(',')[0].strip()
                desc = block.xpath('.//th/span/a/text()').get('').strip()
                
                # Store valid calendar entries
                if desc:
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": term_name,
                        "Term Date": date ,
                        "Term Date Description": desc,
                    })
                    
        # Handle pagination (Next Page)
        next_page = cal_response.xpath('//a[@ title="Next Page"]/@href').get('').strip()
        
        while next_page:
            if not next_page:
                break
            
            # Extract date parameter for next page
            page_frame = re.findall(r'date\=(.*?)\'', next_page)[0]
            
            page_url = f"https://www.trumba.com/s.aspx?calendar=student-life-registrar-academic-calendar&widget=main&date={str(page_frame)}&spudformat=xhr"
            
            # Fetch next page
            page_response =yield scrapy.Request(page_url,headers=headers)
            blocks = page_response.xpath('//table[@class="twSimpleTableTable"]/tbody/tr')
            for block in blocks:
                term = block.xpath('.//div[@class="twSimpleTableGroupHead"]/text()').get('').strip()
                header_name = block.xpath('.//th[2]/text()').get('').strip()
                header_name1 = block.xpath('.//td[2]/text()').get('').strip()
                if term:
                    term_name =term
                
                elif "Date" == header_name or "Date" == header_name1:
                    print("header rows")
                else:
                    date =block.xpath('./td[2]//text()').get('').strip() 
                    if not date:
                        date =block.xpath('./td[1]//text()').get('').strip() 
                        date = date.split(',')[0].strip()
                    desc = block.xpath('.//th/span/a/text()').get('').strip()
                    if desc:
                        calendar_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": self.calendar_url,
                            "Term Name": term_name,
                            "Term Date": date,
                            "Term Date Description": desc,
                        })
            # Move to next page if available
            next_page = page_response.xpath('//a[@ title="Next Page"]/@href').get('').strip()
        
        # Save calendar data 
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")