import re
import json
import scrapy
import PyPDF2
import pandas as pd
from ..utils import *
from io import BytesIO
from inline_requests import inline_requests


class ICCMSSpider(scrapy.Spider):
    name = "iccms"
    institution_id = 258453479314450384

    # Academic terms used for course scraping
    terms = ["202620", "202610"]

    # course_link and headers for POST request

    course_url = "https://www.iccms.edu/DesktopModules/ICC_Live_Class_Schedule/api/Main/getCourses"
    course_headers = {
            "accept": "*/*",
            "accept-language": "en-US,en;q=0.9",
            "content-type": "application/json",
            "moduleid": "1774",
            "origin": "https://www.iccms.edu",
            "referer": "https://www.iccms.edu/CourseSchedule?portalid=0",
            "tabid": "483",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "x-requested-with": "XMLHttpRequest",
        }
    
    # Employee directory API endpoint
    directory_api_url = (
        "https://www.iccms.edu/DesktopModules/edu_iccms_employee_directory/api/Main/getDirectory"
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
    calendar_url = "https://www.iccms.edu/events?portalid=0" 

    def start_requests(self):
        mode = self.settings.get('SCRAPE_MODE', 'all').lower().replace('-', '_')

      # Scrapy spider supports three modes: course, directory, and calendar
        # Single functions
        if mode == 'course':
            # Loop through terms for POST requests
            for term in self.terms:
                self.logger.warning(f"Sending request for TERM = {term}")
                # course payload for each term
                payload = {
                    "term": term,
                    "subject": "%",
                    "courseNum": "%",
                    "courseName": "ELT 1143",
                    "useName": False,
                    "monday": False,
                    "tuesday": False,
                    "wednesday": False,
                    "thursday": False,
                    "friday": False,
                    "saturday": False,
                    "sunday": False,
                    "morning": False,
                    "afternoon": False,
                    "evening": False,
                    "online": False,
                }
                print(payload)
                yield scrapy.Request(url=self.course_url,headers=self.course_headers, method="POST", body=json.dumps(payload), callback=self.parse_course, dont_filter=True)

        elif mode == 'directory':
            yield scrapy.Request(url=self.directory_api_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == 'calendar':
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
        
        # Two combinations (ORDER INDEPENDENT)
        elif mode in ['course_directory', 'directory_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)

            # self.parse_directory()
        elif mode in ['course_calendar', 'calendar_course']:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)

        elif mode in ['directory_calendar', 'calendar_directory']:
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
            yield scrapy.Request(url=self.directory_api_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
        
        # All three (default)
        else:
            yield scrapy.Request(url=self.course_url, callback=self.parse_course, dont_filter=True)
            yield scrapy.Request(url=self.directory_api_url,headers=self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url=self.calendar_url, callback=self.parse_calendar)
       

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
        course_rows = []
        json_data = json.loads(response.text)  # load json response
        keys = json_data.keys()  # get all keys in json
        for key in keys:  # iterate through keys
            blocks = json_data.get(key, [])  # get list of course blocks
            for block in blocks:
                enroll = block.get("seatsavail", "").strip()
                # If enrollment contains '-', it means no seats available
                if '-' in enroll:
                    enrollment = 0
                else:
                    enrollment = enroll
                course_rows.append(
                    {
                        "Cengage Master Institution ID": self.institution_id,
                        "Source URL": self.course_url,
                        "Course Name": block.get("title", "").strip(),
                        "Course Description": block.get("crsdesc", "").strip(),
                        "Class Number": block.get("course", "").strip(),
                        "Section": block.get("section", ""),
                        "Instructor": block.get("instructor", "").strip(),
                        "Enrollment": enrollment,
                        "Course Date": block.get("ptermstart", "").strip(),
                        "Location": block.get("location", ""),
                        "Textbook/Course Materials": block.get("bookurl", "").strip(),
                    }
                )
        course_df = pd.DataFrame(course_rows)  # load to dataframe
        save_df(course_df, self.institution_id, "course")  # save dataframe to csv
        
    def parse_directory(self, response ):
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

        data = response.json()
        rows = []

        for item in data:
            first = item.get("firstName") or ""
            last = item.get("lastName") or ""
            preferredName = item.get("preferredName") or ""
            name = ", ".join(part for part in [last, first] if part).strip()
            if preferredName:
                name = f"{name} ({preferredName})"

            rows.append(
                    {
                        "Cengage Master Institution ID": 123456,
                        "Source URL": "https://www.iccms.edu/EmployeeDirectory?portalid=0",
                        "Name": name,
                        "Title": item.get("title"),
                        "Email": item.get("email"),
                        "Phone Number": item.get("phone"),
                    }
                )

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
        pdf_links = response.xpath(
            '//div[@class="col-md-4"]/a[contains(text(),"Calendar")]/@href'
        ).getall()

        for pdf_link in pdf_links:
            pdf_url = response.urljoin(pdf_link)
            pdf_response=yield scrapy.Request(
                url=pdf_url,
            )
        calendar_rows = []
        reader = PyPDF2.PdfReader(BytesIO(pdf_response.body))

        # -------- EXTRACT RAW TEXT --------

        text_raw = ""
        for page in reader.pages:
            if page.extract_text():
                text_raw += page.extract_text() + " "

        # -------- TEXT CLEANING PIPELINE --------

        # Normalize spaces (newlines, tabs → single space)
        text_normalized = re.sub(r"\s+", " ", text_raw).strip()

        # Fix spaced digits from OCR
        # Example: "1 0" → "10"
        text_digits_fixed = re.sub(r'(\d)\s+(\d)', r'\1\2', text_normalized)

        # Fix broken numeric ranges
        # Example: "9,-16" or "9, -16" → "9-16"
        text_broken_ranges_fixed = re.sub(
            r'(\d{1,2})\s*,?\s*-\s*(\d{1,2})',
            r'\1-\2',
            text_digits_fixed
        )

        # Fix open-ended ranges
        # Example: "21,-" → "21-"
        text_clean = re.sub(
            r'(\d{1,2}),-\b',
            r'\1-',
            text_broken_ranges_fixed
        )

        # Final text used for parsing
        text = text_clean

        # -------- TERM --------
        term_match = re.search(
            r"(SPRING|SUMMER|FALL)\s+(20\d{2})",
            text,
            re.IGNORECASE
        )
        if not term_match:
            return

        term = f"{term_match.group(1).upper()} {term_match.group(2)}"

        # -------- CROSS-MONTH DATE RANGES --------
        cross_month_pattern = re.compile(
            r"(JAN\.|FEB\.|MAR\.|APR\.|MAY|JUNE|JULY|AUG\.|SEPT\.|SEP\.|OCT\.|NOV\.|DEC\.)\s+"
            r"(\d{1,2})\s*-\s*"
            r"(JAN\.|FEB\.|MAR\.|APR\.|MAY|JUNE|JULY|AUG\.|SEPT\.|SEP\.|OCT\.|NOV\.|DEC\.)\s+"
            r"(\d{1,2}),?\s*"
            r"(.*?)(?=(JAN\.|FEB\.|MAR\.|APR\.|MAY|JUNE|JULY|AUG\.|SEPT\.|SEP\.|OCT\.|NOV\.|DEC\.|\Z))",
            re.IGNORECASE
        )

        for match in cross_month_pattern.finditer(text):
            description = re.sub(r"\s+", " ", match.group(5)).strip()
            if not description:
                continue

            term_date = f"{match.group(1).upper()} {match.group(2)}- {match.group(3).upper()} {match.group(4)}"

            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": term,
                "Term Date": term_date,
                "Term Date Description": description,
            })

        # Remove cross-month matches before next parsing
        text = cross_month_pattern.sub("", text)

        # -------- SAME-MONTH / OPEN DATE RANGES --------
        pattern = re.compile(
            r"(JAN\.|FEB\.|MAR\.|APR\.|MAY|JUNE|JULY|AUG\.|SEPT\.|SEP\.|OCT\.|NOV\.|DEC\.)\s+"
            r"(\d{1,2}(?:-\d{0,2})?(?:;\s*\d{1,2})?)\s*,?\s*"
            r"(.*?)(?=(JAN\.|FEB\.|MAR\.|APR\.|MAY|JUNE|JULY|AUG\.|SEPT\.|SEP\.|OCT\.|NOV\.|DEC\.|\Z))",
            re.IGNORECASE
        )

        for match in pattern.finditer(text):
            description = re.sub(r"\s+", " ", match.group(3)).strip()
            if not description:
                continue

            term_date = f"{match.group(1).upper()} {match.group(2)}"

            calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": response.url,
                "Term Name": term,
                "Term Date": term_date,
                "Term Date Description": description,
            })

        # -------- SAVE OUTPUT --------
        calendar_df = pd.DataFrame(calendar_rows)
        save_df(calendar_df, self.institution_id, "calendar")


###############################################################################################

 