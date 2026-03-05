import scrapy
import requests
import pdfplumber
import pandas as pd
from ..utils import *
from io import BytesIO


class CanadaSpider(scrapy.Spider):
    
    name = "canada"
    institution_id = 258441419524433877
    course_rows = []
    directory_rows = []
    calendar_rows = []

    # COURSE CONFIG
    course_urls = ["https://webschedule.smccd.edu/schedules/can_newcrn_202603.pdf",
                    "https://webschedule.smccd.edu/schedules/can_open_202603.pdf",
                    "https://webschedule.smccd.edu/schedules/can_short_202603.pdf"]
    
    # DIRECTORY CONFIG
    directory_source_url = "https://homeofthecolts.com/information/directory/index"
    directory_headers = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Language': 'en-US,en;q=0.9,ta;q=0.8',
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36',
        'sec-ch-ua': '"Google Chrome";v="143", "Chromium";v="143", "Not A(Brand";v="24"',
        }

    # CALENDAR CONFIG
    calendar_source_url = "https://canadacollege.edu/admissions/calendar.php"
    
    # START REQUESTS
    def start_requests(self):
        mode = self.settings.get("SCRAPE_MODE", "all").lower().replace("-", "_")
        """
        Entry point for the spider based on the configured SCRAPE_MODE.

        - Course data is scraped using Playwright inside `parse_course`
        because the course URL does not return a usable Scrapy response
        (JavaScript-rendered content).

        - The website does not have a directory URL, so `parse_directory`
        safely skips extraction and passes without scraping.

        - The calendar URL returns a proper response, so data is collected
        using Scrapy in the "parse_calendar" function.
        """
        # Single functions
        if mode == "course":
            self.parse_course()

        elif mode == "directory":
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode == "calendar":
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        # Two combinations (ORDER INDEPENDENT)
        elif mode in ["course_directory", "directory_course"]:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)

        elif mode in ["course_calendar", "calendar_course"]:
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)
            self.parse_course()

        elif mode in ["directory_calendar", "calendar_directory"]:
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

        #  All three (default)
        else:
            self.parse_course()
            yield scrapy.Request(url = self.directory_source_url,headers = self.directory_headers, callback=self.parse_directory, dont_filter=True)
            yield scrapy.Request(url = self.calendar_source_url, callback=self.parse_calendar, dont_filter=True)

    # PARSE COURSE
    def parse_course(self):
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
        The logic handles:
        - Identification of course entries using structured patterns
        - Extraction of CRN, subject, course number, section, and title
        - Handling of course date values that may span multiple lines
        - Detection of instructor names and course location information
        - Normalization of extracted values into structured course records

        All extracted course data is stored in a consistent format and appended
        to the course output collection for further processing.
        """
        # Regex pattern to match full course entries: CRN, subject, course number, section, title, dates
        full_pattern = r'^(\d{5})\s+([A-Z\.\s]{2,7})\s+([\d\.]+[A-Z]?)\s+([A-Z0-9]{2,3})\s+(.*?)\s+(\d{2}/\d{2}-\d{2}/\d{2})'
        # Regex pattern to match just the course dates (MM/DD-MM/DD)
        date_pattern = r'(\d{2}/\d{2}-\d{2}/\d{2})'
        
        for url in self.course_urls:
            response = requests.get(url, timeout=20)
            pdf_bytes = BytesIO(response.content)
            crn = subj = crse = sect = title = ""
            with pdfplumber.open(pdf_bytes) as pdf:

                for page in pdf.pages:
                    text = page.extract_text()
                    if not text: continue  # Skip pages with no text

                    for line in text.split('\n'):
                        line = line.strip()
                        
                        if not line: continue 

                        full_match = re.search(full_pattern, line)
                        date_match = re.search(date_pattern, line)
                        if full_match:
                            # Extract all course info from full_match groups
                            crn = full_match.group(1)
                            subj = full_match.group(2).strip()
                            crse = full_match.group(3)
                            sect = full_match.group(4)
                            title = full_match.group(5).strip()
                            dates = full_match.group(6)
                        elif date_match and crn:
                            dates = date_match.group(1)

                        else:
                            continue

                        # Extract everything after the dates (usually instructor/location info)
                        after_dates = line.split(dates)[-1].strip()
                        # Default instructor if none found
                        name_match = re.search(r'([A-Z][A-Za-z\s\-\']+, [A-Z][A-Za-z\s\.\']+)', after_dates)
                        if name_match:
                            instructor = name_match.group(1).strip() 

                        location = "0000"
                        loc_match = re.search(r'(\d{4}[A-Z]?|POOL|GYM|FIELD|ROOF|TBA)\s+(HYBR|FACE|ONLN|SYNC|OFFS|ROOF)', after_dates)
                        if loc_match:
                            location = loc_match.group(1)# Extract classroom/location code

                        # Construct full course name for clarity
                        full_course_name = f"{subj} {crse} - {title}"
                        self.course_rows.append({
                            "Cengage Master Institution ID": self.institution_id,
                            "Source URL": url,
                            "Course Name": full_course_name,
                            "Course Description": "",
                            "Class Number": crn,
                            "Section": sect,
                            "Instructor": instructor,
                            "Enrollment": "",
                            "Course Dates": dates,
                            "Location": location,
                            "Textbook/Course Materials": ""
                        })

    # PARSE DIRECTORY
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
        urls = response.xpath('//td[@data-title="Name"]/a/@href').getall()
        for url in urls:
            url = response.urljoin(url)
            yield scrapy.Request(url,callback=self.parse_directory_final)

    def parse_directory_final(self,response):
        name = response.xpath('//div[contains(@class,"player-heading animated")]/span/text()').get('').strip()
        title = response.xpath('//dt[contains(text(),"Title: ")]/parent::dl/dd/text()').get('').strip()
        self.directory_rows.append( {
        "Cengage Master Institution ID": self.institution_id,
        "Source URL": response.url,
        "Name": name,
        "Title": re.sub(r'\s+',' ',title),
        "Email":response.xpath('//dt[contains(text(),"Email: ")]/parent::dl/dd/a/text()').get('').strip(),
        "Phone Number": response.xpath('//dt[contains(text(),"Phone: ")]/parent::dl/dd/text()').get('').strip(),
        })

    # PARSE CALENDAR
    def parse_calendar(self,response):
        """
        Parse calendar using Scrapy response.
        Must output columns:
        - "Cengage Master Institution ID" : int
        - "Source URL"                    : str
        - "Term Name"                     : str
        - "Term Date"                     : str
        - "Term Date Description"         : str
        """     
        blocks = response.xpath('//table/tbody/tr')
        for block in blocks:    
            term_name = block.xpath('.//parent::tbody/parent::table/preceding-sibling::h2/text()').get('').strip()
            term_date = block.xpath('.//td[1]/text()').get(default='').strip()
            if 'June' in term_date or 'July' in term_date:
                term_name = 'Summer Session 2026'

            self.calendar_rows.append({
                "Cengage Master Institution ID": self.institution_id,
                "Source URL": self.calendar_source_url,
                "Term Name": term_name,
                "Term Date": term_date,
                "Term Date Description": block.xpath('.//td[2]/text()').get('').strip(),
            })
    
    #Called automatically when the Scrapy spider finishes scraping.
    def closed(self, reason):
        if self.course_rows:
            df = pd.DataFrame(self.course_rows)
            save_df(df, self.institution_id, "course")

        if self.directory_rows:
            df = pd.DataFrame(self.directory_rows)
            save_df(df, self.institution_id, "campus")

        if self.calendar_rows:
            df = pd.DataFrame(self.calendar_rows)
            save_df(df, self.institution_id, "calendar")

        