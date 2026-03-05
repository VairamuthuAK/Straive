import re
import json
import time
import scrapy
import requests
import pandas as pd
from ..utils import *
from datetime import datetime
from inline_requests import inline_requests
from playwright.sync_api import sync_playwright


class NmcSpider(scrapy.Spider):
    name = "nmc"
    institution_id = 258440212986750941
    course_rows = []
    
    # Course page URL, Directory source URL, Academic calendar URL
    course_url = "https://nmcp.ssbxe.nmc.edu/studentregistrationssb/ssb/term/termSelection?mode=search"
    directory_source_url = "https://www.nmc.edu/departments/human-resources/staff-list.html"
    calendar_url = "https://www.nmc.edu/news/calendars/academic.html"

   
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            # parse_course()
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
        
        # Generate current timestamp in milliseconds
        current_time_ms_str = str(int(datetime.now().timestamp() * 1000))
        
        # API endpoint to fetch available academic terms
        term_url = f"https://nmcp.ssbxe.nmc.edu/studentregistrationssb/ssb/classSearch/getTerms?searchTerm=&offset=1&max=10&_={current_time_ms_str}"
        term_response = yield scrapy.Request(term_url)
        json_datas = json.loads(term_response.text)
        for i,data in enumerate(json_datas):
            
            # Extract term code and description
            code = data.get('code','').strip()
            term = data.get('description','').strip()
            
            match = re.search(r'\b(20\d{2})\b', term)
            
            # Process only terms from 2025 onward
            if match and int(match.group(1)) >= 2025:
                print(term)
                
                # Scrape courses for the selected term
                self.parse_course1(i, code)
                
        # # Convert scraped course rows into a DataFrame and save it
        course_df = pd.DataFrame(self.course_rows)
        save_df(course_df, self.institution_id, "course")
     
    def parse_course1(self,i, code): 
        
        # Launch Playwright Chromium browser
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context()
            page = context.new_page()

            # Open course search page and wait for network to settle
            page.goto(self.course_url, wait_until="networkidle")
            page.wait_for_timeout(5000)
            
            # ---------- OPEN TERM DROPDOWN ----------
            page.click('//b[@role="presentation"]')
            page.wait_for_selector('li[role="presentation"]')

            # Count available terms in dropdown
            term_count = page.locator('li[role="presentation"]').count()
            print(f"\nProcessing term {i+1}/{term_count}")

            # Select the desired term by index
            item = page.locator('li[role="presentation"]').nth(i)
            item.scroll_into_view_if_needed()
            item.click()

            page.wait_for_timeout(1000)

            # Submit selected term
            page.click('//button[@id="term-go"]')
            
            # Trigger search
            page.wait_for_selector('//button[@id="search-go"]')
            page.click('//button[@id="search-go"]')
            
            # Wait for results table to load
            page.wait_for_selector('//table[@role="grid"]//tr')
            page.wait_for_timeout(3000)
            
            # Capture HTML to extract total record count
            content = page.content()
            
            total_counts = ''
            
            # Extract total number of available records
            if re.search(r'Records\:\s*(\d+)', str(content)):
                total_counts = re.findall(r'Records\:\s*(\d+)', str(content))[0]
                
            # Extract authentication cookies from Playwright
            cookies = context.cookies()
            
            # Close browser once cookies are captured
            browser.close()
            
            # Loop through paginated results (500 records per page)
            for page in range(0, int(total_counts)+500, 500):
                
                # Create requests session using Playwright cookies
                session = requests.Session()
                for c in cookies:
                    session.cookies.set(
                        name=c["name"],
                        value=c["value"],
                        domain=c["domain"],
                        path=c["path"]
                    )
                # Generate unique session ID for API call
                unique_session_id = f"x088e{int(time.time() * 1000)}"
                
                url = f"https://nmcp.ssbxe.nmc.edu/studentregistrationssb/ssb/searchResults/searchResults?txt_campus=A&txt_term={code}&startDatepicker=&endDatepicker=&uniqueSessionId={unique_session_id}&pageOffset={page}&pageMaxSize=500&sortColumn=sortNMC&sortDirection=asc"
                
                # Now make authenticated request
                api_response = session.get(url)
                
                json_data = json.loads(api_response.text)
                blocks = json_data['data']
                for block in blocks:
                    subject = block.get('subject','')
                    
                    course_no = block.get('courseDisplay','')
                    
                    course_ref_no = block.get('courseReferenceNumber','')
                    sequenceNumber = block.get('sequenceNumber','')
                    title = block.get('courseTitle','')
                    
                    max = block.get('maximumEnrollment','')
                    low = block.get('seatsAvailable','')
                    enroll = f'{low} of {max}'
                    
                    desc =''
                    
                    instructor =''
                    instructors = []
                    ins_blocks = block['faculty']
                    for ins_block in ins_blocks:
                        ins = ins_block.get('displayName','')
                        if ins:
                            instructors.append(ins)
                            
                    main_date = ''
                    date_blocks = block['meetingsFaculty']
                    for i, date_block in enumerate(date_blocks, start=0):
                        start_date = date_block['meetingTime'].get('startDate','')
                        end_date = date_block['meetingTime'].get('endDate','')
                        buildingDescription = date_block['meetingTime'].get('buildingDescription','')
                        campusDescription = date_block['meetingTime'].get('campusDescription','')
                        room = date_block['meetingTime'].get('room','')
                        if start_date and end_date:
                            main_date = f"{start_date} - {end_date}"
                            
                        try:
                            instructor = instructors[i]
                        except Exception as e:
                            instructor = ''
                            
                        # Build formatted location string
                        location = f"{campusDescription} Campus | {buildingDescription} | Room {room}"
                        
                        # Append structured course record
                        self.course_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": f"{subject} {course_no} {title}",
                        "Course Description": desc,
                        "Class Number": course_ref_no,
                        "Section": sequenceNumber,
                        "Instructor": instructor,
                        "Enrollment": enroll,
                        "Course Dates": main_date,
                        "Location": location,   
                        "Textbook/Course Materials": '',
                    })  

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
        
        blocks  = response.xpath('//div[@id="faculty"]//div[@class="closed"]')
        for block in blocks:
            
            title = block.xpath('./div[@class="title"]/text()').get('').strip()
            clean_title = re.sub(r'\s+', ' ', title.replace('\xa0', ' ').replace('\x97', '——')).strip()
            name = clean_title.split('——')[0]
            dept = clean_title.split('——')[-1]
            phone = " ".join(block.xpath('.//div[@class="phone"]//text()').getall()).replace('Phone:','').strip()
            email = block.xpath('.//div[@class="email"]/a/text()').get('').strip()
            
            # Append extracted staff data to the results list
            directory_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.directory_source_url,
                        "Name": name,
                        "Title": dept,
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
        # List to store parsed calendar rows
        calendar_rows = []
        
        term_names = response.xpath('//div[@id="columns-in-lt"]//caption/h2/text()').getall()
        for term in term_names:
            
            blocks = response.xpath(f'//h2[contains(text(),"{term}")]/ancestor::table[1]/tbody[1]/tr')
            for block in blocks:
                
                date = " ".join(block.xpath('./td[2]//text()').getall()).strip()
                desc = " ".join(block.xpath('./td[1]//text()').getall()).strip()
                if desc:
                    # Append parsed data to the calendar rows list
                    calendar_rows.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.calendar_url,
                        "Term Name": term,
                        "Term Date": date,
                        "Term Date Description": desc,
                    })
        
        
        # Convert scraped calendar rows into a DataFrame and save it
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df,  self.institution_id, "calendar")