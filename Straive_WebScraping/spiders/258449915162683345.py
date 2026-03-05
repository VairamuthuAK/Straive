import re
import scrapy
import pandas as pd
import requests
from parsel import Selector
from inline_requests import inline_requests
from ..utils import save_df


class RichlandSpider(scrapy.Spider):
    # Name of the Scrapy spider
    # This is used when running the spider from the command line
    name="richland"
    
    # Unique identifier for the institution (used in final output files)
    institution_id = 258449915162683345
    
    # Base website URL of Richland College
    base_url = "http://www.richland.edu"
    
    # URL of the interactive course search page (ASP.NET based)
    course_url = "https://jics.richland.edu/ICS/Academic/Interactive_Course_Search.jnz"
    
    # URL of the staff/faculty directory page
    directory_url = "https://www.richland.edu/about-richland/directory/"
    
    # URL of the academic calendar page
    calendar_url = "https://richland.smartcatalogiq.com/en/2025-2026/course-catalog/academic-calendar"
    
    # Default HTTP headers used for normal page requests
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    
    # Separate headers specifically for calendar page requests
    calendar_headers = {
        'accept-language': 'en-IN,en-US;q=0.9,en-CA;q=0.8,en;q=0.7,ta;q=0.6,en-GB-oxendict;q=0.5',
        'origin': base_url,
        'referer': base_url,
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url= self.calendar_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
            
    def parse_course(self, response):
        """
        Parse course data using request session response.

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
        
        # Create a requests session to maintain cookies and ASP.NET state
        session = requests.Session()
        
        # Load the course search page using requests (not Scrapy)
        response = session.get(self.course_url)
        
        # Convert HTML response into Selector for XPath usage
        sel = Selector(text=response.text)
        
        # Extract ASP.NET hidden fields required for POST requests
        viewstate = sel.xpath('//input[@id="__VIEWSTATE"]/@value').get()
        browser_refresh = sel.xpath('//input[@name="___BrowserRefresh"]/@value').get()
        viewstate_gen = sel.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get()

        # Browser-like headers to avoid bot detection
        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Origin': 'https://jics.richland.edu',
            'Referer': 'https://jics.richland.edu/ICS/Academic/Interactive_Course_Search.jnz',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # Payload to submit the course search form (Spring 2026)
        payload = {
            '_scriptManager_HiddenField': '',
            '__EVENTTARGET': '',
            '__EVENTARGUMENT': '',
            '__LASTFOCUS': '',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_gen,
            '___BrowserRefresh': browser_refresh,
            'siteNavBar$ctl00$tbSearch': '',
            'userName': '',
            'password': '',
            'pg0$V$ddlTerm': '2026;SP',
            'pg0$V$ddlDept': '',
            'pg0$V$ddlCourseFrom': '',
            'pg0$V$ddlCourseTo': '',
            'pg0$V$ddlTitleRestrictor': 'BeginsWith',
            'pg0$V$txtTitleRestrictor': '',
            'pg0$V$ddlCourseRestrictor': 'BeginsWith',
            'pg0$V$txtCourseRestrictor': '',
            'pg0$V$ddlDivision': '',
            'pg0$V$ddlMethod': '',
            'pg0$V$ddlAdditional': '',
            'pg0$V$dpDateFrom$d': '8/13/2025',
            'pg0$V$dpDateTo$d': '6/5/2026',
            'pg0$V$ddlTimeFrom': '',
            'pg0$V$ddlTimeTo': '',
            'pg0$V$days': 'rdAnyDay',
            'pg0$V$ddlFaculty': '',
            'pg0$V$ddlCampus': '',
            'pg0$V$ddlBuilding': '',
            'pg0$V$ddlSecStatus': 'OpenFull',
            'pg0$V$txtMin': '',
            'pg0$V$txtMax': '',
            'pg0$V$hiddenCache': 'false',
            'pg0$V$btnSearch': 'Search'
        }
        
        # Submit the search form
        searchResponse = session.post(self.course_url, data=payload, headers=headers)
        searchSel = Selector(text=searchResponse.text)

        # Update ASP.NET hidden fields after search
        viewstate = searchSel.xpath('//input[@id="__VIEWSTATE"]/@value').get()
        browser_refresh = searchSel.xpath('//input[@name="___BrowserRefresh"]/@value').get()
        viewstate_gen = searchSel.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get()

        # Payload to click "Show All" courses
        payload = {
            '_scriptManager_HiddenField': '',
            '__EVENTTARGET': 'pg0$V$lnkShowAll',
            '__EVENTARGUMENT': '',
            '__VIEWSTATE': viewstate,
            '__VIEWSTATEGENERATOR': viewstate_gen,
            '___BrowserRefresh': browser_refresh,
            'siteNavBar$ctl00$tbSearch': '',
            'userName': '',
            'password': '',
            'pg0$V$ddlTerm': '2026;SP',
            'pg0$V$ddlDivision': '',
            'pg0$V$hdnStudentProgram': 'UNDG'
        }
        
        # Load full course list
        response1 = session.post(self.course_url, headers=headers, data=payload)
        sel1 = Selector(text=response1.text)
        
        # Update viewstate again after loading all courses
        viewstate = sel1.xpath('//input[@id="__VIEWSTATE"]/@value').get()
        browser_refresh = sel1.xpath('//input[@name="___BrowserRefresh"]/@value').get()
        viewstate_gen = sel1.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get()
        
        # Extract all rows from course list table
        rows = sel1.xpath("//table//tr")

        # Map to store course dates using EVENTTARGET as key
        event_date_map = {}

        for row in rows:
            # Extract JavaScript postback link
            link = row.xpath('.//a[contains(@href,"__doPostBack")]/@href').get()
            
            if not link:
                continue
            
            # Extract EVENTTARGET value
            m = re.search(r"__doPostBack\('([^']+)'", link)
            
            if not m:
                continue

            event_target = m.group(1)
            
            # Extract begin and end dates from table
            begin_date = row.xpath('.//td[contains(@id,"litBeginDateValue")]/text()').get(default="").strip()
            end_date = row.xpath('.//td[contains(@id,"litEndDateValue")]/text()').get(default="").strip()

            # Store date range if available
            if begin_date or end_date:
                event_date_map[event_target] = f"{begin_date} - {end_date}"
                
        # Get all clickable course links
        links = sel1.xpath("//table//a[contains(@href,'__doPostBack')]/@href").getall()
        
        results = []
        
        # Loop through each course
        for  link in links:
            
            # Extract EVENTTARGET and EVENTARGUMENT
            m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", link)
            if not m:
                print(f"⚠️  Could not parse link: {link}")
                continue
            
            event_target = m.group(1)
            event_argument = m.group(2)
            
            # Payload to open course details page
            payload = {
                '_scriptManager_HiddenField': '',
                '__EVENTTARGET': event_target,
                '__EVENTARGUMENT': event_argument,
                '__VIEWSTATE': viewstate,
                '__VIEWSTATEGENERATOR': viewstate_gen,
                '___BrowserRefresh': browser_refresh,
                'siteNavBar$ctl00$tbSearch': '',
                'userName': '',
                'password': '',
                'pg0$V$ddlTerm': '2026;SP',
                'pg0$V$ddlDivision': '',
                'pg0$V$hdnStudentProgram': 'UNDG'
            }
            
            # Load course details page
            courseRes = session.post(self.course_url, headers=headers, data=payload)
            courseSel = Selector(text=courseRes.text)
            
            # Extract new viewstate for back navigation
            new_viewstate = courseSel.xpath('//input[@id="__VIEWSTATE"]/@value').get()
            new_browser_refresh = courseSel.xpath('//input[@name="___BrowserRefresh"]/@value').get()
            new_viewstate_gen = courseSel.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get()
            
            # Extract course title text
            title = courseSel.xpath("(//h5)[1]/text()").get(default="").strip()
            
            # Skip invalid titles
            if not title or "(" not in title or ")" not in title:
                continue
            if ";" in title:
                continue
            
            course_name = ""
            class_number = ""
            section = ""
            
            # Parse course name, code, number, section
            title_match = re.search(r'^(.+?)\s*\(([\w\s]+?)(\d+)-([A-Z0-9]+)\)', title)
            
            if title_match:
                course_name = title_match.group(1).strip()
                course_name = re.sub(r'\s+', ' ',course_name).strip()
                raw_code = title_match.group(2)
                code = raw_code.replace(" ", "")
                number = title_match.group(3)
                section = title_match.group(4)
                class_number = f"{code} {number}"
            else:
                course_name = title
            
            # Extract instructor names
            raw_texts = courseSel.xpath(
            './/span[@id="pg0_V_rptInst_ctl01_lblInstructorValue"]/following-sibling::text()'
            ).getall()

            instructors = []
            seen = set()

            for text in raw_texts:
                name = text.strip()
                if (
                    name and
                    name != ";" and
                    "Instructor(s):" not in name and
                    "No Instructor Found" not in name
                ):
                    if name not in seen:
                        seen.add(name)
                        instructors.append(name)

            instructor = ", ".join(instructors) if instructors else ""
            
            # Extract enrollment information
            enrollment = ""
            status_text = courseSel.xpath('//span[@id="pg0_V_lblStatusValue"]/text()').get(default="").strip()
            enrollment_match = re.search(r'\((-?\d+)\s+out\s+of\s+(-?\d+)\s+seats\)', status_text)
            if enrollment_match:
                enrolled = int(enrollment_match.group(1))
                total = int(enrollment_match.group(2))
                available = total - enrolled
                enrollment = f"{available}/{total}"
            
            # Extract course description
            course_description = courseSel.xpath('//span[@id="pg0_V_lblCourseDescValue"]/text()').get(default="").strip()
            
            # Extract textbook/bookstore URL
            textbook_url = courseSel.xpath('//a[@id="pg0_V_lnkBookstore"]/@href').get(default="")
            
            # Extract course schedule
            schedule_rows = courseSel.xpath('//table[contains(@class, "table")]//tbody/tr')
            
            # Check if the course details page has a schedule table
            # Some courses have multiple schedules (multiple rows)
            if schedule_rows:
                
                # Loop through each schedule row (each class meeting)
                for row in schedule_rows:
                    
                    # Extract date range text from second column
                    # Example: "01/13/2026 - 05/10/2026"
                    date_text = row.xpath('./td[2]//text()').get(default="").strip()
                    
                    # Extract location (room / building) from third column
                    loc = row.xpath('./td[3]//text()').get(default="").strip()

                    # Initialize date variables
                    start_date = ""
                    end_date = ""
                    course_date = ""

                    # If the schedule row contains a date range
                    if date_text and " - " in date_text:
                        
                        # Split the date range into start and end dates
                        start_date, end_date = [d.strip() for d in date_text.split(" - ", 1)]
                        course_date = f"{start_date} - {end_date}"
                    
                    else:
                        # If date is NOT present in schedule table,
                        # fallback to the date collected earlier from course list page
                        course_date = event_date_map[event_target]
                    
                    # Append one row of course data for this schedule entry
                    results.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": f"{class_number} {course_name}",
                        "Course Description": re.sub(r'\s+', ' ',course_description).strip(),
                        "Class Number": class_number,
                        "Section": section,
                        "Instructor": instructor,
                        "Enrollment": enrollment,
                        "Course Dates": course_date,
                        "Location": loc,
                        "Textbook/Course Materials": textbook_url
                    })
            else:
                # 🔹 If there is NO schedule table at all
                # This means the course has only one schedule or no detailed timing
                # Get course dates from previously stored event_date_map
                course_date = event_date_map.get(event_target, "")
                
                # Append a single row for the course
                results.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.course_url,
                    "Course Name": f"{class_number} {course_name}",
                    "Course Description": re.sub(r'\s+', ' ',course_description).strip(),
                    "Class Number": class_number,
                    "Section": section,
                    "Instructor": instructor,
                    "Enrollment": enrollment,
                    "Course Dates": course_date,
                    "Location": "",
                    "Textbook/Course Materials": textbook_url
                })

            # Go back to course list page
            back_payload = {
                '__PORTLET': event_target,
                '_scriptManager_HiddenField': '',
                '__EVENTTARGET': 'pg0$V$lnkBack',
                '__EVENTARGUMENT': '',
                '__VIEWSTATE': new_viewstate,
                '__VIEWSTATEGENERATOR': new_viewstate_gen,
                '___BrowserRefresh': new_browser_refresh,
                'siteNavBar$ctl00$tbSearch': '',
                'userName': '',
                'password': ''
            }
            
            back_response = session.post(self.course_url, headers=headers, data=back_payload)
            back_sel = Selector(text=back_response.text)
            
            # Get FRESH viewstate after going back to list
            viewstate = back_sel.xpath('//input[@id="__VIEWSTATE"]/@value').get()
            browser_refresh = back_sel.xpath('//input[@name="___BrowserRefresh"]/@value').get()
            viewstate_gen = back_sel.xpath('//input[@id="__VIEWSTATEGENERATOR"]/@value').get()
                    
        df = pd.DataFrame(results)
        save_df(df, self.institution_id, "course")

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
        
        directory_data = response.meta.get("directory_data", [])

        # Parse staff/faculty cards 
        cards = response.xpath('//div[contains(@class,"card")]')

        # loop for all staff cards
        for card in cards:
            
            # Extract name, title, department and email
            name = card.xpath('.//h3[contains(@class,"name")]/text()').get(default='').strip()
            title = card.xpath('.//strong[contains(@class,"title")]/text()').get(default='').strip()
            dept  = card.xpath('.//p[contains(@class,"dept ")]/text()').get(default='').strip()
            email = card.xpath('.//p[contains(@class,"email")]//a/text()').get(default='').strip()

            # Extract phone number (may be split across nodes)
            phone = card.xpath('.//p[contains(@class,"phone")]//text()[normalize-space()]').getall()
            phone = " ".join(phone).strip()
            
            # Combine title and department
            raw_title = f"{title}, {dept}".strip(", ")
            
            # Deduplicate title parts (case-insensitive) while preserving order
            parts = [p.strip() for p in raw_title.split(",") if p.strip()]
            seen = set()
            result = []

            for p in parts:
                key = p.lower()
                if key not in seen:
                    seen.add(key)
                    result.append(p)

            final_title = ", ".join(result)
            
            ## Skip rows where all values are empty
            if not any([name, title, dept, phone, email]):
                continue
            
            # Append parsed directory record
            directory_data.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": final_title,
                "Email": email,
                "Phone Number": phone
            })


        # Handle pagination
        next_url = response.xpath('//a[contains(text(),"Next")]/@href').get()

        if next_url:
            # Follow next page and carry forward collected data
            yield scrapy.Request(
                next_url,
                callback=self.parse_directory,
                meta={"directory_data": directory_data}
            )
        else:
            # Last page reached → save final directory data
            directory_df = pd.DataFrame(directory_data)
            save_df(directory_df, self.institution_id, "campus")

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
        
        calendar_data = []

        # Select all calendar tables inside the main content
        tables = response.xpath('//div[@id="main"]//table')

        for table in tables:
            rows = table.xpath('.//tr')
            
            # Skip empty tables
            if not rows:
                continue

            # First row contains the term name
            term_name = rows[0].xpath('.//td//strong/text()').get(default='').strip()

            # Skip table if term name is missing
            if not term_name:
                continue

            # Remaining rows contain date events
            for row in rows[1:]:
                cols = row.xpath('.//td')

                # Require at least date and description columns
                if len(cols) < 2:
                    continue

                term_date = cols[0].xpath('.//text()').get(default='').strip()
                term_desc = cols[-1].xpath('.//text()').get(default='').strip()

                if not any([term_date, term_desc]):
                    continue

                calendar_data.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": response.url,
                    "Term Name": term_name,
                    "Term Date": term_date,
                    "Term Date Description": term_desc
                })

        df = pd.DataFrame(calendar_data)
        save_df(df, self.institution_id, "calendar")
