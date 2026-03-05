import json
import scrapy
import pandas as pd
from ..utils import *
from urllib.parse import urlencode
from inline_requests import inline_requests


class LeemoreSpider(scrapy.Spider):
    name = "leemore"
    institution_id = 258429943317293010

    # course url
    course_url = "https://lemoorecollege.edu/schedule/"

    # Employee directory API endpoint
    directory_url = (
        "https://lemoorecollege.edu/catalog/2020-2021/faculty.php"
    )

    # Headers for employee directory request
    directory_headers = {
            'accept': '*/*',
            'accept-language': 'en-US,en;q=0.9',
            'moduleid': '5535',
            'priority': 'u=1, i',
            'referer': 'https://www.iccms.edu/EmployeeDirectory?portalid=0?portalid=0?portalid=0?portalid=0',
            'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
            'tabid': '360',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
            'x-requested-with': 'XMLHttpRequest',
            }
    
     # Academic calendar page URL
    calendar_url="https://lemoorecollege.edu/events/#events/tag/Academic%20Calendar"

    calendar_api_url = "https://api.calendar.moderncampus.net/pubcalendar/bf251ff3-d4a3-4166-8961-a2dcb1240427/events"

    calendar_categories = [
    "4c37ea7c-9903-4aa0-adf6-154ab9c570f6",
    "b5e49eed-2301-4ec0-a584-12f54b01dc85",
    "a3b27499-90a4-4321-b306-4f8dbebb3d94",
    "b29fde8f-a9ad-4a49-9eeb-c4c0aa623f5b",
    "e94932d6-49c8-4fb8-bd83-21e7622b4a8e",
    "b712728c-04c2-4f91-94c6-8b05ac7dc84f",
    "e29d5080-bac2-4b86-abd5-44dc3b6c6682",
    "56d9990a-7246-409a-bfee-cab449237bb9",
    "bb5d4b44-bfe1-49b1-b174-795badfbd048",
    ]


    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

        # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            # Loop through terms for POST requests
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url,callback=self.parse_calendar,dont_filter=True)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

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
        """
        Parse course data from Lemoore College schedule.

        - Read course rows from the HTML table
        - Use sectionId to fetch courseId via PostSearchCriteria
        - Fetch detailed section data using Courses/Sections
        - Save all rows once at the end
        """

        rows = []
        
        # Locate course listing table rows
        course_table_rows = response.xpath(
            "//table[@id='ActiveSectionsTable']/tbody/tr"
        )
        
        # Loop over rows
        for idx, course_table_row in enumerate(course_table_rows, start=1):

            # Extract section URL (contains sectionId)
            section_link = course_table_row.xpath(
                ".//td[2]//a/@href"
            ).get()

            if not section_link:
                continue

            class_number = section_link.split("/sectionids/")[-1].strip()
            # Extract section label
            section = ""
            section_title_text = course_table_row.xpath(
                ".//td[2]//a/text()"
            ).get(default="").strip()

            if "(" in section_title_text and ")" in section_title_text:
                section_block = section_title_text.split("(")[1].split(")")[0]
                section_parts = section_block.split("-")
                if len(section_parts) >= 3:
                    section = section_parts[-1]

            textbook = ""
            
            # Payload to search course using sectionId
            search_payload = {
                "sectionIds": [class_number],
                "pageNumber": 1,
                "quantityPerPage": 30,
            }
            # POST request to fetch course metadata
            search_response = yield scrapy.Request(
                url="https://ellucianssui.whccd.edu/Student/Courses/PostSearchCriteria",
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": "https://ellucianssui.whccd.edu",
                    "Referer": (
                        "https://ellucianssui.whccd.edu/"
                        f"Student/Courses/Search?sectionids={class_number}"
                    ),
                },
                body=json.dumps(search_payload),
                dont_filter=True,
            )

            search_data = json.loads(search_response.text)
            
            # Extract courseId and course code
            course_id = ""
            course_name = ""

            courses = search_data.get("Courses", [])

            if courses:
                course_id = courses[0].get("Id", "")
                subject = courses[0].get("SubjectCode", "")
                number = courses[0].get("Number", "")

                if subject and number:
                    course_name = f"{subject}-{number}"

            # Fetch section-level details
            section_payload = {
                "courseId": course_id,
                "sectionIds": [class_number],
            }

            section_response = yield scrapy.Request(
                url="https://ellucianssui.whccd.edu/Student/Courses/Sections",
                method="POST",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Origin": "https://ellucianssui.whccd.edu",
                },
                body=json.dumps(section_payload),
                dont_filter=True,
            )

            section_data = json.loads(section_response.text)

            instructor = ""
            enrollment = ""
            course_dates = ""
            location = ""
            course_description = ""

            sections_retrieved = section_data.get("SectionsRetrieved", {})

            # Course-level
            course_obj = sections_retrieved.get("Course", {})
            course_description = course_obj.get("Description", "")

            # Section-level
            terms = sections_retrieved.get("TermsAndSections", [])

            if terms:
                sections = terms[0].get("Sections", [])

                if sections:
                    section_obj = sections[0]

                    instructor = section_obj.get("FacultyDisplay", "")

                    section_inner = section_obj.get("Section", {})

                    # Enrollment
                    enrolled = section_inner.get("Enrolled")
                    capacity = section_inner.get("Capacity")

                    if enrolled is not None and capacity is not None:
                        enrollment = f"{enrolled} of {capacity}"
                    else:
                        enrollment = ""

                    # Course Name + Section Title 
                    section_title_display = section_inner.get("SectionTitleDisplay", "")

                    if section_title_display:
                        course_name = f"{course_name} - {section_title_display}"

                    # Dates (merge unique)
                    date_values = set()

                    for m in section_inner.get("FormattedMeetingTimes", []):
                        date = m.get("DatesDisplay")

                        if date:
                            date_values.add(date)
                    course_dates = ", ".join(sorted(date_values))

                    # Location
                    location_code = section_inner.get("LocationCode", "")
                    rooms = set()

                    for meeting in section_inner.get("Meetings", []):
                        if not meeting.get("IsOnline"):
                            room = meeting.get("Room", "")
                            for r in room.split("*"):
                                if r and r != "*":
                                    rooms.add(r)

                    if location_code and rooms:
                        location = f"{location_code} - {', '.join(sorted(rooms))}"
                    else:
                        location = section_inner.get("LocationDisplay", "")

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.course_url,
                "Course Name": course_name,
                "Course Description": course_description,
                "Class Number": class_number,
                "Section": section,
                "Instructor": instructor,
                "Enrollment": enrollment,
                "Course Dates": course_dates,
                "Location": location,
                "Textbook/Course Materials": textbook,
            })

        df = pd.DataFrame(rows)
        save_df(df, self.institution_id, "course")

    
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

        rows = []
        # Select all faculty rows
        table_rows = response.xpath(
            '//table[contains(@class,"faculty-table")]//tbody/tr'
        )
        # Loop through each faculty table row
        for tr in table_rows:
            # Extract faculty name from anchor text
            name = tr.xpath('.//td[1]//a/text()').get()
            name = name.strip() if name else ""

            # Extract email 
            email = tr.xpath('.//td[1]//a/@href').get()

            if email and email.startswith("mailto:"):
                email = email.replace("mailto:", "").strip()
            else:
                email = ""

            # Title 
            title = tr.xpath('.//td[2]//text()').getall()
            title = " ".join(t.strip() for t in title if t.strip())

            rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Name": name,
                "Title": title,
                "Email": email,
                "Phone Number": "",
            })

        # Save
        directory_df = pd.DataFrame(rows)
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

        calendar_rows = []

        # date to cover all calendar events
        date_ranges = [
            ("2026-01-28", "2026-02-27"),
            ("2026-02-28", "2026-06-08"),
            ("2026-06-09", "2026-09-17"),
            ("2026-09-18", "2026-12-27"),
            ("2026-12-28", "2027-04-07"),
            ("2027-04-08", "2027-07-17"),
            ("2027-07-18", "2027-10-26"),
            ("2027-10-27", "2028-02-04"),
        ]
        
        # Loop through each date window
        for start_date, end_date in date_ranges:
            # Base query parameters for calendar API request
            params = [
                ("tag", "Academic Calendar"),
                ("start", start_date),
                ("end", end_date),
            ]
            
            # Add all required category filters to the query
            for cat in self.calendar_categories:
                params.append(("category", cat))
            
             # Build final query string
            query = urlencode(params, doseq=True)
            url = f"{self.calendar_api_url}?{query}"

            self.logger.warning(
                f"Calendar API inline call: {start_date} → {end_date}"
            )

            api_response = yield scrapy.Request(
                url=url,
                headers={"accept": "application/json"},
                dont_filter=True,
            )
            # Parse JSON response
            events = api_response.json()

            if not isinstance(events, list):
                self.logger.warning("Unexpected calendar response format")
                continue
            # Process each calendar event
            for event in events:
                start_date = event.get("startDate")

                if not start_date:
                    start_datetime = event.get("startDatetime", "")
                    start_date = start_datetime.split("T")[0] if start_datetime else ""

                calendar_rows.append({
                    "Cengage Master Institution ID": self.institution_id,
                    "Source URL": self.calendar_url,
                    "Term Name": event.get("categoryName", "").strip(),
                    "Term Date": start_date,
                    "Term Date Description": event.get("title", "").strip(),
                })

        #  SAVE
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")
