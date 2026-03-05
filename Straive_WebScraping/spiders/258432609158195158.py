import re
import scrapy
import pandas as pd
import json
from datetime import datetime, timezone, timedelta
from inline_requests import inline_requests
from ..utils import save_df


class HighSpider(scrapy.Spider):
    # Spider name used while running Scrapy
    name="highland"
    
    # Unique institution ID (used while saving data)
    institution_id = 258432609158195158
    
    # List to store all course data
    courseData =[]
    
    # Base URLs used in this spider
    base_url = "http://www.highlandcc.edu/"
    course_url = "https://myhcc.highlandcc.edu/SelfService/Search/Section?&period=2026%2FSPRING&session=4WEEK&campusId=O100000000"
    optionUrl = "https://myhcc.highlandcc.edu/SelfService/Sections/AdvancedSearchOptions"
    descUrl = "https://myhcc.highlandcc.edu/SelfService/Sections/AnonymousDetails"
    directory_url = "https://highlandcc.edu/pages/directory_0"
    calendar_url = "https://highlandcc.edu/pages/calendar"
    
    # Common headers for normal HTML requests
    headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
        }
    
    # Headers required for calendar API call
    calendar_headers = {
            'accept-language': 'en-IN,en-US;q=0.9,en-CA;q=0.8,en;q=0.7,ta;q=0.6,en-GB-oxendict;q=0.5',
            'origin': base_url,
            'referer': base_url,
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
        }

    # ------------------ Helpers ------------------
    
    # Convert date (DD-MM-YYYY) to milliseconds
    def parse_calendar_to_ms(self, date_str):
        dt = datetime.strptime(date_str, "%d-%m-%Y")
        dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1000)
    
    # Combine start and end milliseconds into readable date range
    def parse_calendar_combine_dates(self, start_ms, end_ms):
        start_dt = datetime.utcfromtimestamp(start_ms / 1000)
        end_dt = datetime.utcfromtimestamp(end_ms / 1000)

        # subtract 1 day from end
        end_dt -= timedelta(days=1)

        start_date = start_dt.date()   # date object
        end_date = end_dt.date()       # date object

        # if end <= start → single date
        if end_date <= start_date:
            return start_date.strftime("%d-%m-%Y")
        
        # Return date range
        return f"{start_date.strftime('%d-%m-%Y')} - {end_date.strftime('%d-%m-%Y')}"

# ------------------ Requests ------------------
    
    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')
        
        # Convert start and end dates to milliseconds for calendar API
        startms = self.parse_calendar_to_ms("01-01-2026")
        endms = self.parse_calendar_to_ms("01-02-2027")
        
        # Build calendar API URL
        calendar_api_url = (
            "https://tockify.com/api/ngevent"
            "?max=-1"
            "&max-events-after=0"
            "&calname=hcccalendar"
            "&showAll=false"
            f"&startms={startms}"
            "&start-inclusive=true"
            "&end-inclusive=true"
            f"&endms={endms}"
        )
        
        # Single functions
        if mode == 'course':
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode == 'calendar':
            yield scrapy.Request(url=calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=calendar_api_url, headers=self.calendar_headers, callback=self.parse_calendar, dont_filter=True)
            
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
        - "Enrollment"                    : str
        - "Course Date"                   : str
        - "Location"                      : str
        - "Textbook/Course Materials"     : str
        """
        
        # Call API to get available periods and sessions
        responseOptions = yield scrapy.Request(
            url=self.optionUrl,
            method="POST",
            body=json.dumps(None),  # API expects JSON null
            headers=self.headers,
            dont_filter=True
        )
        
        data = json.loads(responseOptions.text).get("data", {})
        
        # Extract period values
        period_values = [p.get("value") for p in data.get("periods", []) if p.get("value")]

        # Extract session values
        session_values = [s.get("value") for s in data.get("sessions", []) if s.get("value")]
        
        # Loop through each period and session
        for period in period_values:
            
            for session in session_values:
                
                # Course search API
                searchUrl = "https://myhcc.highlandcc.edu/SelfService/Sections/Search"
                
                # Search payload
                searchPayload = json.dumps({
                    "sectionSearchParameters": {
                        "eventId": "",
                        "keywords": "",
                        "period": f"{period}",
                        "registrationType": "TRAD",
                        "session": f"{session}",
                        "campusId": "O100000000",
                        "classLevel": "",
                        "college": "",
                        "creditType": "",
                        "curriculum": "",
                        "department": "",
                        "endDate": "",
                        "endTime": "",
                        "eventSubType": "",
                        "eventType": "",
                        "generalEd": "",
                        "meeting": "",
                        "nonTradProgram": "",
                        "population": "",
                        "program": "",
                        "startDate": "",
                        "startTime": "",
                        "status": "",
                        "academicTerm": "",
                        "academicYear": ""
                    },
                    "startIndex": 0,
                    "length": 1000
                    })

                refererUrl = f'https://myhcc.highlandcc.edu/SelfService/Search/Section?&period={period}&session={session}&campusId=O100000000'

                searchheaders = {
                    'accept': 'application/json',
                    'accept-language': 'en-IN,en-US;q=0.9,en-CA;q=0.8,en;q=0.7,ta;q=0.6,en-GB-oxendict;q=0.5',
                    'cache-control': 'max-age=0',
                    'content-type': 'application/json',
                    'origin': 'https://myhcc.highlandcc.edu',
                    'priority': 'u=1, i',
                    'referer': refererUrl,
                    'sec-ch-ua': '"Not(A:Brand";v="8", "Chromium";v="144", "Google Chrome";v="144"',
                    'sec-ch-ua-mobile': '?0',
                    'sec-ch-ua-platform': '"Windows"',
                    'sec-fetch-dest': 'empty',
                    'sec-fetch-mode': 'cors',
                    'sec-fetch-site': 'same-origin',
                    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                    }
                
                # Call search API
                searchResponse = yield scrapy.Request(
                    url=searchUrl,
                    method="POST",
                    body=searchPayload,
                    headers=searchheaders,
                    dont_filter=True
                    )
                
                search_response_json = json.loads(searchResponse.text)

                sections = search_response_json.get("data", {}).get("sections") or []
                
                # If sections is None or empty, safely exit this request
                if not sections:
                    continue
                
                # Loop through each course
                for sec in sections:
                    
                    class_number = sec.get("eventId")
                    event_name = sec.get("eventName", "")
                    title = f"{class_number} {event_name}" if class_number else event_name

                    # Instructor names
                    instructors = [
                        ins.get("fullName")
                        for ins in sec.get("instructors", [])
                        if ins.get("fullName")
                    ]
                    
                    instructor_str = ", ".join(instructors)
                    
                    # Course dates
                    start_date = sec.get("startDate")
                    end_date = sec.get("endDate")
                    date_range = f"{start_date} - {end_date}"

                    # Enrollment calculation
                    max_seats = sec.get("maximumSeats")
                    seats_left = sec.get("seatsLeft")
                    
                    if max_seats is not None and seats_left is not None:
                        try:
                            max_seats = int(max_seats)
                            seats_left = int(seats_left)

                            enrolled = max_seats - seats_left
                            enrollment = f"{enrolled}/{max_seats}"
                        except (ValueError, TypeError):
                            enrollment = ""
                    else:
                        enrollment = ""
                        
                    ids = sec.get('id','')
                    payload = json.dumps(ids)
                    
                    # Fetch course description
                    desc_response = yield  scrapy.Request(url=self.descUrl,method="POST",headers=searchheaders,body=payload, dont_filter=True)
                    desc_response=desc_response.json() 
                    desc_data= desc_response.get('data',{}) 
                    desc = (desc_data.get("description") or "").strip()
                    
                    self.courseData.append({
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": refererUrl,
                        "Course Name": re.sub(r"\s+", " ", title) if title else "",
                        "Course Description": re.sub(r"\s+", " ", desc) if desc else "",
                        "Class Number": class_number,
                        "Section": sec.get("section"),
                        "Instructor": re.sub(r"\s+", " ", instructor_str) if instructor_str else "",
                        "Enrollment": enrollment,
                        "Course Dates": date_range,
                        "Location": "",
                        "Textbook/Course Materials": ""
                    })
                
        df = pd.DataFrame(self.courseData )
        save_df(df, self.institution_id, "course")


    def parse_directory(self, response):
        """
        parse the highland employee directory

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Name"                          : str
        - "Title"                         : str
        - "Email"                         : str
        - "Phone Number"                  : str
        """
        
        # Create empty list to store all directory rows
        directoryRows = []
        
        # Select all employee blocks from the page
        rows = response.xpath('//div[contains(@class,"employee-block ")]')
        
        # Loop through each employee block
        for row in rows:
            
            # Extract faculty/staff name, title, email, phone
            name = row.xpath('.//h3/text()').get()
            title = row.xpath('.//h4/text()').get()
            email = row.xpath('.//a[starts-with(@href,"mailto:")]/text()').get()
            phone = row.xpath('.//a[starts-with(@href,"tel:")]/text()').get()

            directoryRows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.directory_url,
                "Name": re.sub(r"\s+", " ", name).strip() if name else "",
                "Title": re.sub(r"\s+", " ", title).strip() if title else "",
                "Email": re.sub(r"\s+", " ", email).strip() if email else "",
                "Phone Number": re.sub(r"\s+", " ", phone).strip() if phone else "",
            })
        directory_df = pd.DataFrame(directoryRows)
        save_df(directory_df, self.institution_id, "campus")

    def parse_calendar(self, response):
        """
        Parses the academic calendar from a Json.

        Must output columns:

        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """

        # Convert API response text (JSON string) into Python dictionary
        data = json.loads(response.text)
        
        # Get list of events; if not present, use empty list
        events = data.get("events", [])
        
        # This list will store final calendar rows
        calendar_rows = []
        
        # Loop through each calendar event
        for event in events:
            
            # Get date information of the event
            when = event.get("when", {})
            
            # Extract start and end time in milliseconds
            start_ms = when.get("start", {}).get("millis")
            end_ms = when.get("end", {}).get("millis")
            
            # Convert milliseconds to readable date range
            # Example: 10-03-2026 - 15-03-2026
            date_value = (
                self.parse_calendar_combine_dates(start_ms, end_ms)
                if start_ms and end_ms else ""
            )
            
            # Term name will be Month + Year (example: May 2026)
            term_name = ""
            if end_ms:
                term_name = datetime.fromtimestamp(
                    end_ms / 1000
                ).strftime("%B %Y")   # e.g. "May 2026"
            
            # Get event title / description text
            term = (
                event.get("content", {})
                    .get("summary", {})
                    .get("text", "")
            )
            
            # Append one event as one row in output
            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_url,
                "Term Name": re.sub(r"\s+", " ", term_name).strip() if term_name else "",
                "Term Date":  date_value,
                "Term Date Description":  re.sub(r"\s+", " ", term).strip() if term else "",
            })
        
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")

